// Package batching provides resource batching framework for improved Terraform performance
package batching

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/plans"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// BatchProcessor provides intelligent resource grouping and batch processing
type BatchProcessor struct {
	config    *BatchConfig
	analyzer  *DependencyAnalyzer
	scheduler *BatchScheduler
	
	// Execution state
	batches   []*ResourceBatch
	batchesMu sync.RWMutex
	
	// Metrics
	totalResources   int64
	batchesCreated   int64
	executionTime    time.Duration
	parallelismGain  float64
}

// BatchConfig controls batch processing behavior
type BatchConfig struct {
	// MaxBatchSize is the maximum number of resources per batch
	MaxBatchSize int
	
	// MinBatchSize is the minimum number of resources per batch
	MinBatchSize int
	
	// MaxParallelBatches is the maximum number of batches to process concurrently
	MaxParallelBatches int
	
	// BatchingStrategy defines the strategy for grouping resources
	BatchingStrategy BatchStrategy
	
	// EnableDependencyOptimization enables dependency-aware batching
	EnableDependencyOptimization bool
	
	// EnableProviderGrouping groups resources by provider for efficiency
	EnableProviderGrouping bool
	
	// ResourceTimeout is the timeout for individual resource operations
	ResourceTimeout time.Duration
	
	// BatchTimeout is the timeout for entire batch operations
	BatchTimeout time.Duration
}

// BatchStrategy defines different batching strategies
type BatchStrategy int

const (
	// StrategyProvider groups resources by provider type
	StrategyProvider BatchStrategy = iota
	// StrategyDependency groups resources by dependency level
	StrategyDependency
	// StrategySize groups resources to optimize batch sizes
	StrategySize
	// StrategyHybrid combines multiple strategies
	StrategyHybrid
	// StrategyCustom allows custom batching logic
	StrategyCustom
)

// String returns a string representation of the batch strategy
func (bs BatchStrategy) String() string {
	switch bs {
	case StrategyProvider:
		return "provider"
	case StrategyDependency:
		return "dependency"
	case StrategySize:
		return "size"
	case StrategyHybrid:
		return "hybrid"
	case StrategyCustom:
		return "custom"
	default:
		return "unknown"
	}
}

// DefaultBatchConfig returns sensible batch processing defaults
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		MaxBatchSize:                 50,
		MinBatchSize:                 5,
		MaxParallelBatches:           runtime.NumCPU(),
		BatchingStrategy:             StrategyHybrid,
		EnableDependencyOptimization: true,
		EnableProviderGrouping:       true,
		ResourceTimeout:              30 * time.Second,
		BatchTimeout:                 5 * time.Minute,
	}
}

// ResourceBatch represents a group of resources to be processed together
type ResourceBatch struct {
	ID          string
	Resources   []*BatchResource
	Provider    addrs.Provider
	DependencyLevel int
	EstimatedDuration time.Duration
	Status      BatchStatus
	
	// Execution metadata
	StartTime   time.Time
	EndTime     time.Time
	ErrorCount  int
	Diagnostics tfdiags.Diagnostics
}

// BatchStatus represents the execution status of a batch
type BatchStatus int

const (
	// StatusPending indicates the batch is waiting to be processed
	StatusPending BatchStatus = iota
	// StatusRunning indicates the batch is currently being processed
	StatusRunning
	// StatusCompleted indicates the batch completed successfully
	StatusCompleted
	// StatusFailed indicates the batch failed with errors
	StatusFailed
	// StatusCancelled indicates the batch was cancelled
	StatusCancelled
)

// String returns a string representation of the batch status
func (bs BatchStatus) String() string {
	switch bs {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// BatchResource represents a resource within a batch
type BatchResource struct {
	Address     addrs.AbsResourceInstance
	Change      *plans.ResourceInstanceChange
	Provider    providers.Interface
	Config      interface{}
	Dependencies []addrs.AbsResourceInstance
	
	// Execution metadata
	StartTime   time.Time
	Duration    time.Duration
	Status      ResourceStatus
	Error       error
}

// ResourceStatus represents the execution status of a resource
type ResourceStatus int

const (
	// ResourcePending indicates the resource is waiting to be processed
	ResourcePending ResourceStatus = iota
	// ResourceRunning indicates the resource is currently being processed
	ResourceRunning
	// ResourceCompleted indicates the resource completed successfully
	ResourceCompleted
	// ResourceFailed indicates the resource failed with an error
	ResourceFailed
	// ResourceSkipped indicates the resource was skipped
	ResourceSkipped
)

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(config *BatchConfig) *BatchProcessor {
	if config == nil {
		config = DefaultBatchConfig()
	}
	
	return &BatchProcessor{
		config:    config,
		analyzer:  NewDependencyAnalyzer(),
		scheduler: NewBatchScheduler(config),
		batches:   make([]*ResourceBatch, 0),
	}
}

// ProcessResources processes resources using batching optimization
func (bp *BatchProcessor) ProcessResources(
	ctx context.Context,
	resources []*BatchResource,
	operation BatchOperation,
) (*BatchExecutionResult, error) {
	start := time.Now()
	defer func() {
		bp.executionTime = time.Since(start)
	}()
	
	bp.totalResources = int64(len(resources))
	
	// Analyze dependencies
	if bp.config.EnableDependencyOptimization {
		if err := bp.analyzer.AnalyzeDependencies(resources); err != nil {
			return nil, fmt.Errorf("dependency analysis failed: %w", err)
		}
	}
	
	// Create batches using the configured strategy
	batches, err := bp.createBatches(resources)
	if err != nil {
		return nil, fmt.Errorf("batch creation failed: %w", err)
	}
	
	bp.batchesCreated = int64(len(batches))
	
	// Execute batches
	result, err := bp.executeBatches(ctx, batches, operation)
	if err != nil {
		return nil, fmt.Errorf("batch execution failed: %w", err)
	}
	
	// Calculate parallelism gain
	sequentialTime := bp.estimateSequentialTime(resources)
	if sequentialTime > 0 {
		bp.parallelismGain = float64(sequentialTime) / float64(bp.executionTime)
	}
	
	return result, nil
}

// BatchOperation defines the operation to perform on resources
type BatchOperation int

const (
	// OperationPlan performs planning operations
	OperationPlan BatchOperation = iota
	// OperationApply performs apply operations
	OperationApply
	// OperationDestroy performs destroy operations
	OperationDestroy
	// OperationRefresh performs refresh operations
	OperationRefresh
)

// createBatches groups resources into batches based on the configured strategy
func (bp *BatchProcessor) createBatches(resources []*BatchResource) ([]*ResourceBatch, error) {
	switch bp.config.BatchingStrategy {
	case StrategyProvider:
		return bp.createProviderBatches(resources)
	case StrategyDependency:
		return bp.createDependencyBatches(resources)
	case StrategySize:
		return bp.createSizeBatches(resources)
	case StrategyHybrid:
		return bp.createHybridBatches(resources)
	default:
		return bp.createSizeBatches(resources) // Fallback
	}
}

// createProviderBatches groups resources by provider type
func (bp *BatchProcessor) createProviderBatches(resources []*BatchResource) ([]*ResourceBatch, error) {
	providerGroups := make(map[addrs.Provider][]*BatchResource)
	
	// Group resources by provider
	for _, resource := range resources {
		provider := resource.Address.Resource.Resource.Provider
		providerGroups[provider] = append(providerGroups[provider], resource)
	}
	
	var batches []*ResourceBatch
	
	// Create batches for each provider group
	for provider, providerResources := range providerGroups {
		providerBatches := bp.splitIntoSizedBatches(providerResources, provider)
		batches = append(batches, providerBatches...)
	}
	
	return batches, nil
}

// createDependencyBatches groups resources by dependency level
func (bp *BatchProcessor) createDependencyBatches(resources []*BatchResource) ([]*ResourceBatch, error) {
	// Calculate dependency levels
	levels := bp.analyzer.CalculateDependencyLevels(resources)
	
	var batches []*ResourceBatch
	
	// Create batches for each dependency level
	for level, levelResources := range levels {
		levelBatches := bp.splitIntoSizedBatches(levelResources, addrs.Provider{})
		for _, batch := range levelBatches {
			batch.DependencyLevel = level
		}
		batches = append(batches, levelBatches...)
	}
	
	return batches, nil
}

// createSizeBatches groups resources to optimize batch sizes
func (bp *BatchProcessor) createSizeBatches(resources []*BatchResource) ([]*ResourceBatch, error) {
	return bp.splitIntoSizedBatches(resources, addrs.Provider{}), nil
}

// createHybridBatches combines multiple strategies for optimal batching
func (bp *BatchProcessor) createHybridBatches(resources []*BatchResource) ([]*ResourceBatch, error) {
	// First, group by dependency levels
	levels := bp.analyzer.CalculateDependencyLevels(resources)
	
	var batches []*ResourceBatch
	
	// For each level, group by provider and then by size
	for level, levelResources := range levels {
		providerGroups := make(map[addrs.Provider][]*BatchResource)
		
		for _, resource := range levelResources {
			provider := resource.Address.Resource.Resource.Provider
			providerGroups[provider] = append(providerGroups[provider], resource)
		}
		
		// Create sized batches within each provider group
		for provider, providerResources := range providerGroups {
			providerBatches := bp.splitIntoSizedBatches(providerResources, provider)
			for _, batch := range providerBatches {
				batch.DependencyLevel = level
			}
			batches = append(batches, providerBatches...)
		}
	}
	
	return batches, nil
}

// splitIntoSizedBatches splits resources into appropriately sized batches
func (bp *BatchProcessor) splitIntoSizedBatches(resources []*BatchResource, provider addrs.Provider) []*ResourceBatch {
	var batches []*ResourceBatch
	
	for i := 0; i < len(resources); i += bp.config.MaxBatchSize {
		end := i + bp.config.MaxBatchSize
		if end > len(resources) {
			end = len(resources)
		}
		
		batchResources := resources[i:end]
		
		// Skip batches that are too small (unless it's the last batch)
		if len(batchResources) < bp.config.MinBatchSize && end < len(resources) {
			continue
		}
		
		batch := &ResourceBatch{
			ID:        fmt.Sprintf("batch-%d-%s", len(batches), provider.String()),
			Resources: batchResources,
			Provider:  provider,
			Status:    StatusPending,
		}
		
		batch.EstimatedDuration = bp.estimateBatchDuration(batch)
		batches = append(batches, batch)
	}
	
	return batches
}

// executeBatches executes all batches according to dependency constraints
func (bp *BatchProcessor) executeBatches(
	ctx context.Context,
	batches []*ResourceBatch,
	operation BatchOperation,
) (*BatchExecutionResult, error) {
	// Sort batches by dependency level for correct execution order
	sort.Slice(batches, func(i, j int) bool {
		return batches[i].DependencyLevel < batches[j].DependencyLevel
	})
	
	bp.batchesMu.Lock()
	bp.batches = batches
	bp.batchesMu.Unlock()
	
	return bp.scheduler.ExecuteBatches(ctx, batches, operation)
}

// estimateBatchDuration estimates how long a batch will take to execute
func (bp *BatchProcessor) estimateBatchDuration(batch *ResourceBatch) time.Duration {
	// Simple estimation based on resource count and type
	baseTime := 100 * time.Millisecond // Base time per resource
	resourceTime := time.Duration(len(batch.Resources)) * baseTime
	
	// Add overhead for batch coordination
	overhead := 50 * time.Millisecond
	
	return resourceTime + overhead
}

// estimateSequentialTime estimates how long sequential execution would take
func (bp *BatchProcessor) estimateSequentialTime(resources []*BatchResource) time.Duration {
	total := time.Duration(0)
	for range resources {
		total += 100 * time.Millisecond // Estimated time per resource
	}
	return total
}

// BatchExecutionResult represents the result of batch processing
type BatchExecutionResult struct {
	TotalBatches     int
	SuccessfulBatches int
	FailedBatches    int
	TotalResources   int
	SuccessfulResources int
	FailedResources  int
	ExecutionTime    time.Duration
	ParallelismGain  float64
	Diagnostics      tfdiags.Diagnostics
}

// DependencyAnalyzer analyzes resource dependencies for batching optimization
type DependencyAnalyzer struct {
	dependencyCache map[string][]addrs.AbsResourceInstance
	cacheMu         sync.RWMutex
}

// NewDependencyAnalyzer creates a new dependency analyzer
func NewDependencyAnalyzer() *DependencyAnalyzer {
	return &DependencyAnalyzer{
		dependencyCache: make(map[string][]addrs.AbsResourceInstance),
	}
}

// AnalyzeDependencies analyzes dependencies between resources
func (da *DependencyAnalyzer) AnalyzeDependencies(resources []*BatchResource) error {
	for _, resource := range resources {
		if err := da.analyzeSingleResource(resource); err != nil {
			return fmt.Errorf("failed to analyze dependencies for %s: %w", resource.Address, err)
		}
	}
	return nil
}

// analyzeSingleResource analyzes dependencies for a single resource
func (da *DependencyAnalyzer) analyzeSingleResource(resource *BatchResource) error {
	cacheKey := resource.Address.String()
	
	da.cacheMu.RLock()
	if deps, exists := da.dependencyCache[cacheKey]; exists {
		da.cacheMu.RUnlock()
		resource.Dependencies = deps
		return nil
	}
	da.cacheMu.RUnlock()
	
	// Analyze resource dependencies (simplified implementation)
	var dependencies []addrs.AbsResourceInstance
	
	// In a real implementation, this would analyze:
	// - Direct resource references
	// - Data source dependencies  
	// - Provider dependencies
	// - Module dependencies
	
	da.cacheMu.Lock()
	da.dependencyCache[cacheKey] = dependencies
	da.cacheMu.Unlock()
	
	resource.Dependencies = dependencies
	return nil
}

// CalculateDependencyLevels calculates dependency levels for resources
func (da *DependencyAnalyzer) CalculateDependencyLevels(resources []*BatchResource) map[int][]*BatchResource {
	levels := make(map[int][]*BatchResource)
	resourceLevels := make(map[string]int)
	
	// Build resource address to resource mapping
	resourceMap := make(map[string]*BatchResource)
	for _, resource := range resources {
		resourceMap[resource.Address.String()] = resource
	}
	
	// Calculate levels using topological sort
	for _, resource := range resources {
		level := da.calculateResourceLevel(resource, resourceMap, resourceLevels)
		levels[level] = append(levels[level], resource)
		resourceLevels[resource.Address.String()] = level
	}
	
	return levels
}

// calculateResourceLevel calculates the dependency level for a resource
func (da *DependencyAnalyzer) calculateResourceLevel(
	resource *BatchResource,
	resourceMap map[string]*BatchResource,
	levels map[string]int,
) int {
	addr := resource.Address.String()
	
	// Check if already calculated
	if level, exists := levels[addr]; exists {
		return level
	}
	
	maxDepLevel := -1
	
	// Calculate levels of all dependencies
	for _, depAddr := range resource.Dependencies {
		if depResource, exists := resourceMap[depAddr.String()]; exists {
			depLevel := da.calculateResourceLevel(depResource, resourceMap, levels)
			if depLevel > maxDepLevel {
				maxDepLevel = depLevel
			}
		}
	}
	
	level := maxDepLevel + 1
	levels[addr] = level
	
	return level
}

// BatchScheduler manages the execution of resource batches
type BatchScheduler struct {
	config    *BatchConfig
	semaphore chan struct{}
}

// NewBatchScheduler creates a new batch scheduler
func NewBatchScheduler(config *BatchConfig) *BatchScheduler {
	return &BatchScheduler{
		config:    config,
		semaphore: make(chan struct{}, config.MaxParallelBatches),
	}
}

// ExecuteBatches executes batches according to dependency constraints
func (bs *BatchScheduler) ExecuteBatches(
	ctx context.Context,
	batches []*ResourceBatch,
	operation BatchOperation,
) (*BatchExecutionResult, error) {
	result := &BatchExecutionResult{
		TotalBatches: len(batches),
	}
	
	start := time.Now()
	defer func() {
		result.ExecutionTime = time.Since(start)
	}()
	
	// Group batches by dependency level
	levelBatches := make(map[int][]*ResourceBatch)
	for _, batch := range batches {
		levelBatches[batch.DependencyLevel] = append(levelBatches[batch.DependencyLevel], batch)
	}
	
	// Execute levels sequentially, batches within levels in parallel
	for level := 0; len(levelBatches[level]) > 0; level++ {
		if err := bs.executeBatchLevel(ctx, levelBatches[level], operation, result); err != nil {
			return result, err
		}
	}
	
	return result, nil
}

// executeBatchLevel executes all batches in a dependency level
func (bs *BatchScheduler) executeBatchLevel(
	ctx context.Context,
	batches []*ResourceBatch,
	operation BatchOperation,
	result *BatchExecutionResult,
) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(batches))
	
	for _, batch := range batches {
		wg.Add(1)
		
		go func(b *ResourceBatch) {
			defer wg.Done()
			
			// Acquire semaphore
			select {
			case bs.semaphore <- struct{}{}:
				defer func() { <-bs.semaphore }()
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
			
			if err := bs.executeSingleBatch(ctx, b, operation); err != nil {
				errChan <- fmt.Errorf("batch %s failed: %w", b.ID, err)
				result.FailedBatches++
			} else {
				result.SuccessfulBatches++
			}
			
		}(batch)
	}
	
	// Wait for all batches in this level to complete
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case err := <-errChan:
		return err
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// executeSingleBatch executes a single resource batch
func (bs *BatchScheduler) executeSingleBatch(
	ctx context.Context,
	batch *ResourceBatch,
	operation BatchOperation,
) error {
	batch.Status = StatusRunning
	batch.StartTime = time.Now()
	defer func() {
		batch.EndTime = time.Now()
		if batch.ErrorCount > 0 {
			batch.Status = StatusFailed
		} else {
			batch.Status = StatusCompleted
		}
	}()
	
	// Create batch context with timeout
	batchCtx, cancel := context.WithTimeout(ctx, bs.config.BatchTimeout)
	defer cancel()
	
	// Execute resources in the batch
	for _, resource := range batch.Resources {
		if err := bs.executeResource(batchCtx, resource, operation); err != nil {
			batch.ErrorCount++
			resource.Status = ResourceFailed
			resource.Error = err
			
			// Continue with other resources in batch
			continue
		}
		
		resource.Status = ResourceCompleted
	}
	
	return nil
}

// executeResource executes a single resource operation
func (bs *BatchScheduler) executeResource(
	ctx context.Context,
	resource *BatchResource,
	operation BatchOperation,
) error {
	resource.Status = ResourceRunning
	resource.StartTime = time.Now()
	defer func() {
		resource.Duration = time.Since(resource.StartTime)
	}()
	
	// Create resource context with timeout
	resourceCtx, cancel := context.WithTimeout(ctx, bs.config.ResourceTimeout)
	defer cancel()
	
	// Execute the operation (simplified implementation)
	switch operation {
	case OperationPlan:
		return bs.executePlanOperation(resourceCtx, resource)
	case OperationApply:
		return bs.executeApplyOperation(resourceCtx, resource)
	case OperationDestroy:
		return bs.executeDestroyOperation(resourceCtx, resource)
	case OperationRefresh:
		return bs.executeRefreshOperation(resourceCtx, resource)
	default:
		return fmt.Errorf("unsupported operation: %v", operation)
	}
}

// executePlanOperation executes a plan operation on a resource
func (bs *BatchScheduler) executePlanOperation(ctx context.Context, resource *BatchResource) error {
	// Simulate plan operation
	select {
	case <-time.After(10 * time.Millisecond): // Simulate work
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// executeApplyOperation executes an apply operation on a resource
func (bs *BatchScheduler) executeApplyOperation(ctx context.Context, resource *BatchResource) error {
	// Simulate apply operation
	select {
	case <-time.After(50 * time.Millisecond): // Simulate work
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// executeDestroyOperation executes a destroy operation on a resource
func (bs *BatchScheduler) executeDestroyOperation(ctx context.Context, resource *BatchResource) error {
	// Simulate destroy operation
	select {
	case <-time.After(30 * time.Millisecond): // Simulate work
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// executeRefreshOperation executes a refresh operation on a resource
func (bs *BatchScheduler) executeRefreshOperation(ctx context.Context, resource *BatchResource) error {
	// Simulate refresh operation
	select {
	case <-time.After(20 * time.Millisecond): // Simulate work
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetMetrics returns batch processing metrics
type BatchMetrics struct {
	TotalResources   int64
	BatchesCreated   int64
	ExecutionTime    time.Duration
	ParallelismGain  float64
	AverageBatchSize float64
}

// GetMetrics returns current batch processing metrics
func (bp *BatchProcessor) GetMetrics() BatchMetrics {
	bp.batchesMu.RLock()
	defer bp.batchesMu.RUnlock()
	
	avgBatchSize := 0.0
	if bp.batchesCreated > 0 {
		avgBatchSize = float64(bp.totalResources) / float64(bp.batchesCreated)
	}
	
	return BatchMetrics{
		TotalResources:   bp.totalResources,
		BatchesCreated:   bp.batchesCreated,
		ExecutionTime:    bp.executionTime,
		ParallelismGain:  bp.parallelismGain,
		AverageBatchSize: avgBatchSize,
	}
}