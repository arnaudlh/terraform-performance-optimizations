// Package graph provides optimized graph construction and traversal for Terraform operations
package graph

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/dag"
)

// OptimizedGraphBuilder provides enhanced graph construction with caching and parallelism
type OptimizedGraphBuilder struct {
	// Cache for dependency resolutions
	depCache     map[string][]addrs.ConfigResource
	depCacheMu   sync.RWMutex
	
	// Cache for graph fragments
	fragmentCache map[string]*dag.Graph
	fragmentMu    sync.RWMutex
	
	// Configuration
	config *GraphBuilderConfig
	
	// Metrics
	buildTime     time.Duration
	cacheHits     int64
	cacheMisses   int64
	parallelTasks int64
}

// GraphBuilderConfig controls graph building behavior
type GraphBuilderConfig struct {
	// EnableDependencyCache enables caching of dependency resolutions
	EnableDependencyCache bool
	
	// EnableFragmentCache enables caching of graph fragments
	EnableFragmentCache bool
	
	// ParallelWorkers is the number of workers for parallel graph construction
	ParallelWorkers int
	
	// CacheTTL is the time-to-live for cached entries
	CacheTTL time.Duration
	
	// MaxCacheSize limits the number of cached entries
	MaxCacheSize int
}

// DefaultGraphBuilderConfig returns sensible defaults
func DefaultGraphBuilderConfig() *GraphBuilderConfig {
	return &GraphBuilderConfig{
		EnableDependencyCache: true,
		EnableFragmentCache:   true,
		ParallelWorkers:       runtime.NumCPU() * 2,
		CacheTTL:              10 * time.Minute,
		MaxCacheSize:          10000,
	}
}

// NewOptimizedGraphBuilder creates a new optimized graph builder
func NewOptimizedGraphBuilder(config *GraphBuilderConfig) *OptimizedGraphBuilder {
	if config == nil {
		config = DefaultGraphBuilderConfig()
	}
	
	return &OptimizedGraphBuilder{
		depCache:      make(map[string][]addrs.ConfigResource),
		fragmentCache: make(map[string]*dag.Graph),
		config:        config,
	}
}

// ResourceNode represents a resource in the dependency graph
type ResourceNode struct {
	Address      addrs.ConfigResource
	Dependencies []addrs.ConfigResource
	Config       interface{} // Resource configuration
	Metadata     map[string]interface{}
}

// GraphFragment represents a reusable portion of a dependency graph
type GraphFragment struct {
	ID       string
	Graph    *dag.Graph
	Nodes    []ResourceNode
	Created  time.Time
	LastUsed time.Time
}

// IsExpired checks if the fragment has expired
func (gf *GraphFragment) IsExpired(ttl time.Duration) bool {
	return time.Since(gf.Created) > ttl
}

// BuildOptimizedGraph constructs a dependency graph with optimizations
func (ogb *OptimizedGraphBuilder) BuildOptimizedGraph(ctx context.Context, resources []ResourceNode) (*dag.Graph, error) {
	start := time.Now()
	defer func() {
		ogb.buildTime = time.Since(start)
	}()
	
	// Try to use cached fragments first
	if ogb.config.EnableFragmentCache {
		if cachedGraph := ogb.tryGetCachedGraph(resources); cachedGraph != nil {
			ogb.cacheHits++
			return cachedGraph, nil
		}
	}
	
	ogb.cacheMisses++
	
	// Build graph with parallel processing
	graph, err := ogb.buildGraphParallel(ctx, resources)
	if err != nil {
		return nil, err
	}
	
	// Cache the resulting graph
	if ogb.config.EnableFragmentCache {
		ogb.cacheGraph(resources, graph)
	}
	
	return graph, nil
}

// buildGraphParallel constructs the graph using parallel workers
func (ogb *OptimizedGraphBuilder) buildGraphParallel(ctx context.Context, resources []ResourceNode) (*dag.Graph, error) {
	graph := &dag.Graph{}
	
	// Create channels for work distribution
	workChan := make(chan ResourceNode, len(resources))
	resultChan := make(chan graphBuildResult, len(resources))
	
	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < ogb.config.ParallelWorkers; i++ {
		wg.Add(1)
		go ogb.graphBuildWorker(ctx, workChan, resultChan, &wg)
	}
	
	// Send work to workers
	go func() {
		defer close(workChan)
		for _, resource := range resources {
			select {
			case workChan <- resource:
			case <-ctx.Done():
				return
			}
		}
	}()
	
	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	
	// Collect results and build graph
	graphMu := sync.Mutex{}
	nodeMap := make(map[string]dag.Vertex)
	
	for result := range resultChan {
		if result.err != nil {
			return nil, fmt.Errorf("failed to process resource %s: %w", result.resource.Address, result.err)
		}
		
		graphMu.Lock()
		
		// Add vertex to graph
		vertex := &GraphVertex{Resource: result.resource}
		graph.Add(vertex)
		nodeMap[result.resource.Address.String()] = vertex
		
		// Add dependencies
		for _, dep := range result.dependencies {
			depAddr := dep.String()
			if depVertex, exists := nodeMap[depAddr]; exists {
				graph.Connect(dag.BasicEdge(depVertex, vertex))
			}
		}
		
		graphMu.Unlock()
		ogb.parallelTasks++
	}
	
	// Validate the graph
	if err := ogb.validateGraph(graph); err != nil {
		return nil, fmt.Errorf("graph validation failed: %w", err)
	}
	
	return graph, nil
}

// graphBuildResult represents the result of processing a resource
type graphBuildResult struct {
	resource     ResourceNode
	dependencies []addrs.ConfigResource
	err          error
}

// graphBuildWorker processes resources in parallel
func (ogb *OptimizedGraphBuilder) graphBuildWorker(
	ctx context.Context,
	workChan <-chan ResourceNode,
	resultChan chan<- graphBuildResult,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	
	for {
		select {
		case resource, ok := <-workChan:
			if !ok {
				return // Channel closed
			}
			
			dependencies, err := ogb.resolveDependencies(ctx, resource)
			
			resultChan <- graphBuildResult{
				resource:     resource,
				dependencies: dependencies,
				err:          err,
			}
			
		case <-ctx.Done():
			return
		}
	}
}

// resolveDependencies resolves dependencies for a resource with caching
func (ogb *OptimizedGraphBuilder) resolveDependencies(ctx context.Context, resource ResourceNode) ([]addrs.ConfigResource, error) {
	cacheKey := ogb.getDependencyCacheKey(resource)
	
	// Check cache first
	if ogb.config.EnableDependencyCache {
		if deps := ogb.getCachedDependencies(cacheKey); deps != nil {
			ogb.cacheHits++
			return deps, nil
		}
	}
	
	ogb.cacheMisses++
	
	// Resolve dependencies (this would integrate with actual Terraform dependency resolution)
	dependencies := ogb.computeDependencies(resource)
	
	// Cache the result
	if ogb.config.EnableDependencyCache {
		ogb.cacheDependencies(cacheKey, dependencies)
	}
	
	return dependencies, nil
}

// computeDependencies performs the actual dependency computation
func (ogb *OptimizedGraphBuilder) computeDependencies(resource ResourceNode) []addrs.ConfigResource {
	// This is a simplified version - real implementation would analyze
	// resource references, data source dependencies, etc.
	
	var dependencies []addrs.ConfigResource
	
	// Example: Extract dependencies from resource configuration
	if resource.Dependencies != nil {
		dependencies = append(dependencies, resource.Dependencies...)
	}
	
	return dependencies
}

// getDependencyCacheKey generates a cache key for dependency resolution
func (ogb *OptimizedGraphBuilder) getDependencyCacheKey(resource ResourceNode) string {
	// Create a deterministic key based on resource address and configuration
	return fmt.Sprintf("%s:%v", resource.Address.String(), resource.Config)
}

// getCachedDependencies retrieves cached dependencies
func (ogb *OptimizedGraphBuilder) getCachedDependencies(key string) []addrs.ConfigResource {
	ogb.depCacheMu.RLock()
	defer ogb.depCacheMu.RUnlock()
	
	if deps, exists := ogb.depCache[key]; exists {
		return deps
	}
	
	return nil
}

// cacheDependencies stores dependencies in cache
func (ogb *OptimizedGraphBuilder) cacheDependencies(key string, dependencies []addrs.ConfigResource) {
	ogb.depCacheMu.Lock()
	defer ogb.depCacheMu.Unlock()
	
	// Implement LRU eviction if cache is full
	if len(ogb.depCache) >= ogb.config.MaxCacheSize {
		ogb.evictOldestDependency()
	}
	
	ogb.depCache[key] = dependencies
}

// evictOldestDependency removes the oldest cached dependency
func (ogb *OptimizedGraphBuilder) evictOldestDependency() {
	// Simple eviction - in real implementation, would use proper LRU
	for key := range ogb.depCache {
		delete(ogb.depCache, key)
		break
	}
}

// tryGetCachedGraph attempts to retrieve a cached graph
func (ogb *OptimizedGraphBuilder) tryGetCachedGraph(resources []ResourceNode) *dag.Graph {
	fragmentKey := ogb.getFragmentKey(resources)
	
	ogb.fragmentMu.RLock()
	defer ogb.fragmentMu.RUnlock()
	
	if fragment, exists := ogb.fragmentCache[fragmentKey]; exists {
		if !fragment.IsExpired(ogb.config.CacheTTL) {
			fragment.LastUsed = time.Now()
			return fragment.Graph
		} else {
			// Remove expired fragment
			delete(ogb.fragmentCache, fragmentKey)
		}
	}
	
	return nil
}

// cacheGraph stores a graph in the fragment cache
func (ogb *OptimizedGraphBuilder) cacheGraph(resources []ResourceNode, graph *dag.Graph) {
	fragmentKey := ogb.getFragmentKey(resources)
	
	ogb.fragmentMu.Lock()
	defer ogb.fragmentMu.Unlock()
	
	// Implement LRU eviction if cache is full
	if len(ogb.fragmentCache) >= ogb.config.MaxCacheSize {
		ogb.evictOldestFragment()
	}
	
	fragment := &GraphFragment{
		ID:       fragmentKey,
		Graph:    graph,
		Nodes:    resources,
		Created:  time.Now(),
		LastUsed: time.Now(),
	}
	
	ogb.fragmentCache[fragmentKey] = fragment
}

// getFragmentKey generates a cache key for graph fragments
func (ogb *OptimizedGraphBuilder) getFragmentKey(resources []ResourceNode) string {
	// Create a deterministic key based on resource addresses
	key := ""
	for _, resource := range resources {
		key += resource.Address.String() + ":"
	}
	return key
}

// evictOldestFragment removes the least recently used fragment
func (ogb *OptimizedGraphBuilder) evictOldestFragment() {
	var oldestKey string
	var oldestTime time.Time
	
	for key, fragment := range ogb.fragmentCache {
		if oldestKey == "" || fragment.LastUsed.Before(oldestTime) {
			oldestKey = key
			oldestTime = fragment.LastUsed
		}
	}
	
	if oldestKey != "" {
		delete(ogb.fragmentCache, oldestKey)
	}
}

// validateGraph validates the constructed graph for cycles and other issues
func (ogb *OptimizedGraphBuilder) validateGraph(graph *dag.Graph) error {
	// Check for cycles
	if cycles := graph.Cycles(); len(cycles) > 0 {
		return fmt.Errorf("dependency cycle detected: %v", cycles[0])
	}
	
	// Additional validations could be added here
	return nil
}

// GraphVertex represents a vertex in the optimized graph
type GraphVertex struct {
	Resource ResourceNode
}

// String implements the dag.Vertex interface
func (gv *GraphVertex) String() string {
	return gv.Resource.Address.String()
}

// ParallelGraphWalker provides parallel graph traversal
type ParallelGraphWalker struct {
	graph    *dag.Graph
	config   *WalkerConfig
	
	// Execution state
	visited  map[dag.Vertex]bool
	visitedMu sync.RWMutex
	
	// Metrics
	walkTime      time.Duration
	parallelTasks int64
}

// WalkerConfig controls graph walking behavior
type WalkerConfig struct {
	ParallelWorkers int
	MaxConcurrency  int
	ErrorBehavior   ErrorBehavior
}

// ErrorBehavior defines how errors are handled during graph walking
type ErrorBehavior int

const (
	// StopOnError stops execution on first error
	StopOnError ErrorBehavior = iota
	// ContinueOnError continues execution despite errors
	ContinueOnError
	// CollectErrors collects all errors and returns at the end
	CollectErrors
)

// DefaultWalkerConfig returns default walker configuration
func DefaultWalkerConfig() *WalkerConfig {
	return &WalkerConfig{
		ParallelWorkers: runtime.NumCPU(),
		MaxConcurrency:  runtime.NumCPU() * 2,
		ErrorBehavior:   StopOnError,
	}
}

// NewParallelGraphWalker creates a new parallel graph walker
func NewParallelGraphWalker(graph *dag.Graph, config *WalkerConfig) *ParallelGraphWalker {
	if config == nil {
		config = DefaultWalkerConfig()
	}
	
	return &ParallelGraphWalker{
		graph:   graph,
		config:  config,
		visited: make(map[dag.Vertex]bool),
	}
}

// Walk traverses the graph in parallel, executing the provided function on each vertex
func (pgw *ParallelGraphWalker) Walk(ctx context.Context, walkFn func(dag.Vertex) error) error {
	start := time.Now()
	defer func() {
		pgw.walkTime = time.Since(start)
	}()
	
	// Create semaphore for concurrency control
	semaphore := make(chan struct{}, pgw.config.MaxConcurrency)
	
	// Error handling
	errChan := make(chan error, pgw.config.ParallelWorkers)
	var walkErr error
	
	// Walk the graph level by level to respect dependencies
	levels := pgw.computeLevels()
	
	for _, level := range levels {
		if err := pgw.walkLevel(ctx, level, walkFn, semaphore, errChan); err != nil {
			walkErr = err
			if pgw.config.ErrorBehavior == StopOnError {
				break
			}
		}
	}
	
	return walkErr
}

// computeLevels organizes vertices into dependency levels
func (pgw *ParallelGraphWalker) computeLevels() [][]dag.Vertex {
	var levels [][]dag.Vertex
	remaining := make(map[dag.Vertex]bool)
	
	// Initialize remaining vertices
	pgw.graph.Walk(func(v dag.Vertex) dag.WalkFunc {
		remaining[v] = true
		return nil
	})
	
	for len(remaining) > 0 {
		var currentLevel []dag.Vertex
		
		// Find vertices with no unprocessed dependencies
		for vertex := range remaining {
			canProcess := true
			for _, dep := range pgw.graph.UpEdges(vertex).List() {
				if remaining[dep] {
					canProcess = false
					break
				}
			}
			
			if canProcess {
				currentLevel = append(currentLevel, vertex)
			}
		}
		
		// Remove processed vertices from remaining
		for _, vertex := range currentLevel {
			delete(remaining, vertex)
		}
		
		levels = append(levels, currentLevel)
	}
	
	return levels
}

// walkLevel processes all vertices in a level in parallel
func (pgw *ParallelGraphWalker) walkLevel(
	ctx context.Context,
	level []dag.Vertex,
	walkFn func(dag.Vertex) error,
	semaphore chan struct{},
	errChan chan error,
) error {
	var wg sync.WaitGroup
	
	for _, vertex := range level {
		wg.Add(1)
		
		go func(v dag.Vertex) {
			defer wg.Done()
			
			// Acquire semaphore
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
			
			// Execute walk function
			if err := walkFn(v); err != nil {
				errChan <- fmt.Errorf("error processing vertex %s: %w", v, err)
				return
			}
			
			// Mark as visited
			pgw.visitedMu.Lock()
			pgw.visited[v] = true
			pgw.visitedMu.Unlock()
			
			pgw.parallelTasks++
			
		}(vertex)
	}
	
	// Wait for level completion
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	// Check for errors or completion
	select {
	case err := <-errChan:
		return err
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetMetrics returns performance metrics
type GraphMetrics struct {
	BuildTime     time.Duration
	WalkTime      time.Duration
	CacheHits     int64
	CacheMisses   int64
	ParallelTasks int64
	CacheHitRate  float64
}

// GetMetrics returns current performance metrics
func (ogb *OptimizedGraphBuilder) GetMetrics() GraphMetrics {
	total := ogb.cacheHits + ogb.cacheMisses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(ogb.cacheHits) / float64(total)
	}
	
	return GraphMetrics{
		BuildTime:     ogb.buildTime,
		CacheHits:     ogb.cacheHits,
		CacheMisses:   ogb.cacheMisses,
		ParallelTasks: ogb.parallelTasks,
		CacheHitRate:  hitRate,
	}
}

// Clear clears all caches
func (ogb *OptimizedGraphBuilder) Clear() {
	ogb.depCacheMu.Lock()
	ogb.fragmentMu.Lock()
	defer ogb.depCacheMu.Unlock()
	defer ogb.fragmentMu.Unlock()
	
	ogb.depCache = make(map[string][]addrs.ConfigResource)
	ogb.fragmentCache = make(map[string]*dag.Graph)
}