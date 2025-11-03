// Package schema provides parallel loading of provider schemas
package schema

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// ParallelLoaderConfig controls the behavior of parallel schema loading
type ParallelLoaderConfig struct {
	// MaxWorkers is the maximum number of concurrent schema loading operations
	MaxWorkers int
	
	// Timeout is the maximum time to wait for schema loading
	Timeout time.Duration
	
	// RetryAttempts is the number of retry attempts for failed loads
	RetryAttempts int
	
	// RetryDelay is the delay between retry attempts
	RetryDelay time.Duration
}

// DefaultParallelLoaderConfig returns sensible defaults
func DefaultParallelLoaderConfig() *ParallelLoaderConfig {
	return &ParallelLoaderConfig{
		MaxWorkers:    min(runtime.NumCPU()*2, 10),
		Timeout:       30 * time.Second,
		RetryAttempts: 3,
		RetryDelay:    100 * time.Millisecond,
	}
}

// LoadRequest represents a schema loading request
type LoadRequest struct {
	Provider addrs.Provider
	Version  string
}

// LoadResult represents the result of a schema loading operation
type LoadResult struct {
	Provider addrs.Provider
	Schema   *providers.GetProviderSchemaResponse
	Error    error
	Duration time.Duration
}

// ParallelSchemaLoader loads provider schemas concurrently
type ParallelSchemaLoader struct {
	config     *ParallelLoaderConfig
	client     providers.Interface
	semaphore  chan struct{}
	
	// Metrics
	mutex      sync.RWMutex
	totalLoads int64
	avgDuration time.Duration
}

// NewParallelSchemaLoader creates a new parallel schema loader
func NewParallelSchemaLoader(client providers.Interface, config *ParallelLoaderConfig) *ParallelSchemaLoader {
	if config == nil {
		config = DefaultParallelLoaderConfig()
	}
	
	return &ParallelSchemaLoader{
		config:    config,
		client:    client,
		semaphore: make(chan struct{}, config.MaxWorkers),
	}
}

// LoadSchemas loads multiple provider schemas concurrently
func (psl *ParallelSchemaLoader) LoadSchemas(ctx context.Context, requests []LoadRequest) (map[addrs.Provider]*providers.GetProviderSchemaResponse, tfdiags.Diagnostics) {
	if len(requests) == 0 {
		return nil, nil
	}
	
	var diags tfdiags.Diagnostics
	results := make(map[addrs.Provider]*providers.GetProviderSchemaResponse)
	resultChan := make(chan LoadResult, len(requests))
	
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, psl.config.Timeout)
	defer cancel()
	
	// Launch workers
	var wg sync.WaitGroup
	for _, req := range requests {
		wg.Add(1)
		go func(request LoadRequest) {
			defer wg.Done()
			psl.loadSchemaWorker(ctx, request, resultChan)
		}(req)
	}
	
	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	
	// Collect results
	for result := range resultChan {
		if result.Error != nil {
			diags = diags.Append(tfdiags.Whole(
				tfdiags.Error,
				"Failed to load provider schema",
				fmt.Sprintf("Provider %s: %v", result.Provider, result.Error),
			))
			continue
		}
		
		results[result.Provider] = result.Schema
		psl.updateMetrics(result.Duration)
	}
	
	return results, diags
}

// loadSchemaWorker loads a single provider schema with retry logic
func (psl *ParallelSchemaLoader) loadSchemaWorker(ctx context.Context, req LoadRequest, resultChan chan<- LoadResult) {
	// Acquire semaphore
	select {
	case psl.semaphore <- struct{}{}:
		defer func() { <-psl.semaphore }()
	case <-ctx.Done():
		resultChan <- LoadResult{
			Provider: req.Provider,
			Error:    ctx.Err(),
		}
		return
	}
	
	var lastErr error
	for attempt := 0; attempt <= psl.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			// Wait before retry
			select {
			case <-time.After(psl.config.RetryDelay * time.Duration(attempt)):
			case <-ctx.Done():
				resultChan <- LoadResult{
					Provider: req.Provider,
					Error:    ctx.Err(),
				}
				return
			}
		}
		
		start := time.Now()
		schema, err := psl.loadSingleSchema(ctx, req)
		duration := time.Since(start)
		
		if err == nil {
			resultChan <- LoadResult{
				Provider: req.Provider,
				Schema:   schema,
				Duration: duration,
			}
			return
		}
		
		lastErr = err
		
		// Check if context was cancelled
		select {
		case <-ctx.Done():
			resultChan <- LoadResult{
				Provider: req.Provider,
				Error:    ctx.Err(),
			}
			return
		default:
		}
	}
	
	// All retries failed
	resultChan <- LoadResult{
		Provider: req.Provider,
		Error:    fmt.Errorf("failed after %d attempts: %w", psl.config.RetryAttempts+1, lastErr),
	}
}

// loadSingleSchema loads a single provider schema
func (psl *ParallelSchemaLoader) loadSingleSchema(ctx context.Context, req LoadRequest) (*providers.GetProviderSchemaResponse, error) {
	// This would integrate with the actual Terraform provider loading mechanism
	// For this implementation, we simulate the schema loading process
	
	resp := psl.client.GetProviderSchema(&providers.GetProviderSchemaRequest{})
	
	if resp.Diagnostics.HasErrors() {
		return nil, fmt.Errorf("schema loading failed: %s", resp.Diagnostics.Err())
	}
	
	return resp, nil
}

// updateMetrics updates loading metrics
func (psl *ParallelSchemaLoader) updateMetrics(duration time.Duration) {
	psl.mutex.Lock()
	defer psl.mutex.Unlock()
	
	psl.totalLoads++
	if psl.avgDuration == 0 {
		psl.avgDuration = duration
	} else {
		// Calculate running average
		psl.avgDuration = time.Duration(
			(int64(psl.avgDuration)*psl.totalLoads + int64(duration)) / (psl.totalLoads + 1),
		)
	}
}

// LoadMetrics represents schema loading performance metrics
type LoadMetrics struct {
	TotalLoads      int64
	AverageDuration time.Duration
	ActiveWorkers   int
	MaxWorkers      int
}

// GetMetrics returns current loading metrics
func (psl *ParallelSchemaLoader) GetMetrics() LoadMetrics {
	psl.mutex.RLock()
	defer psl.mutex.RUnlock()
	
	return LoadMetrics{
		TotalLoads:      psl.totalLoads,
		AverageDuration: psl.avgDuration,
		ActiveWorkers:   len(psl.semaphore),
		MaxWorkers:      psl.config.MaxWorkers,
	}
}

// SchemaCache provides caching for loaded schemas
type SchemaCache struct {
	mutex   sync.RWMutex
	schemas map[addrs.Provider]*providers.GetProviderSchemaResponse
	ttl     time.Duration
	
	// Cache metadata
	loadTimes map[addrs.Provider]time.Time
}

// NewSchemaCache creates a new schema cache
func NewSchemaCache(ttl time.Duration) *SchemaCache {
	return &SchemaCache{
		schemas:   make(map[addrs.Provider]*providers.GetProviderSchemaResponse),
		loadTimes: make(map[addrs.Provider]time.Time),
		ttl:       ttl,
	}
}

// Get retrieves a schema from the cache
func (sc *SchemaCache) Get(provider addrs.Provider) (*providers.GetProviderSchemaResponse, bool) {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	
	schema, exists := sc.schemas[provider]
	if !exists {
		return nil, false
	}
	
	// Check if schema is expired
	if loadTime, ok := sc.loadTimes[provider]; ok {
		if time.Since(loadTime) > sc.ttl {
			return nil, false
		}
	}
	
	return schema, true
}

// Put stores a schema in the cache
func (sc *SchemaCache) Put(provider addrs.Provider, schema *providers.GetProviderSchemaResponse) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	
	sc.schemas[provider] = schema
	sc.loadTimes[provider] = time.Now()
}

// Clear removes all entries from the cache
func (sc *SchemaCache) Clear() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	
	sc.schemas = make(map[addrs.Provider]*providers.GetProviderSchemaResponse)
	sc.loadTimes = make(map[addrs.Provider]time.Time)
}

// Size returns the number of cached schemas
func (sc *SchemaCache) Size() int {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()
	
	return len(sc.schemas)
}

// CachedParallelSchemaLoader combines parallel loading with caching
type CachedParallelSchemaLoader struct {
	loader *ParallelSchemaLoader
	cache  *SchemaCache
}

// NewCachedParallelSchemaLoader creates a new cached parallel schema loader
func NewCachedParallelSchemaLoader(
	client providers.Interface,
	loaderConfig *ParallelLoaderConfig,
	cacheTTL time.Duration,
) *CachedParallelSchemaLoader {
	return &CachedParallelSchemaLoader{
		loader: NewParallelSchemaLoader(client, loaderConfig),
		cache:  NewSchemaCache(cacheTTL),
	}
}

// LoadSchemas loads schemas with caching
func (cpsl *CachedParallelSchemaLoader) LoadSchemas(
	ctx context.Context,
	requests []LoadRequest,
) (map[addrs.Provider]*providers.GetProviderSchemaResponse, tfdiags.Diagnostics) {
	results := make(map[addrs.Provider]*providers.GetProviderSchemaResponse)
	var pendingRequests []LoadRequest
	
	// Check cache first
	for _, req := range requests {
		if schema, found := cpsl.cache.Get(req.Provider); found {
			results[req.Provider] = schema
		} else {
			pendingRequests = append(pendingRequests, req)
		}
	}
	
	// Load missing schemas
	if len(pendingRequests) > 0 {
		loadedSchemas, diags := cpsl.loader.LoadSchemas(ctx, pendingRequests)
		
		// Cache loaded schemas
		for provider, schema := range loadedSchemas {
			cpsl.cache.Put(provider, schema)
			results[provider] = schema
		}
		
		return results, diags
	}
	
	return results, nil
}

// GetMetrics returns combined metrics
func (cpsl *CachedParallelSchemaLoader) GetMetrics() (LoadMetrics, int) {
	loadMetrics := cpsl.loader.GetMetrics()
	cacheSize := cpsl.cache.Size()
	return loadMetrics, cacheSize
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}