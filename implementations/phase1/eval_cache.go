// Package eval provides optimized expression evaluation with caching
package eval

import (
	"context"
	"crypto/md5"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/zclconf/go-cty/cty"
)

// CacheConfig controls the behavior of the expression cache
type CacheConfig struct {
	// MaxSize is the maximum number of cached evaluations
	MaxSize int
	
	// TTL is the time-to-live for cached entries
	TTL time.Duration
	
	// EnableMetrics enables cache hit/miss tracking
	EnableMetrics bool
}

// DefaultCacheConfig returns sensible defaults for expression caching
func DefaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		MaxSize:       10000,
		TTL:           5 * time.Minute,
		EnableMetrics: true,
	}
}

// CacheKey represents a unique key for cached expression evaluations
type CacheKey struct {
	ExprHash    string
	ContextHash string
	Variables   map[string]cty.Value
}

// String returns a string representation of the cache key
func (k *CacheKey) String() string {
	return fmt.Sprintf("%s:%s", k.ExprHash, k.ContextHash)
}

// CacheEntry represents a cached expression evaluation result
type CacheEntry struct {
	Result     cty.Value
	Diags      hcl.Diagnostics
	Timestamp  time.Time
	AccessTime time.Time
	HitCount   int64
}

// IsExpired checks if the cache entry has exceeded its TTL
func (e *CacheEntry) IsExpired(ttl time.Duration) bool {
	return time.Since(e.Timestamp) > ttl
}

// ExpressionCache provides LRU caching for expression evaluations
type ExpressionCache struct {
	config  *CacheConfig
	cache   map[string]*CacheEntry
	lruList []string
	mutex   sync.RWMutex
	
	// Metrics
	hits   int64
	misses int64
}

// NewExpressionCache creates a new expression cache with the given configuration
func NewExpressionCache(config *CacheConfig) *ExpressionCache {
	if config == nil {
		config = DefaultCacheConfig()
	}
	
	return &ExpressionCache{
		config:  config,
		cache:   make(map[string]*CacheEntry),
		lruList: make([]string, 0, config.MaxSize),
	}
}

// hashExpression creates a hash of the expression for caching
func (ec *ExpressionCache) hashExpression(expr hcl.Expression) string {
	// Use the expression's source range and content for hashing
	content := fmt.Sprintf("%s:%d:%d", 
		expr.Range().Filename,
		expr.Range().Start.Line,
		expr.Range().Start.Column)
	
	hash := md5.Sum([]byte(content))
	return fmt.Sprintf("%x", hash)
}

// hashContext creates a hash of the evaluation context for caching
func (ec *ExpressionCache) hashContext(ctx *hcl.EvalContext) string {
	if ctx == nil {
		return "nil"
	}
	
	// Create a deterministic hash of the context variables
	var contextStr string
	if ctx.Variables != nil {
		for name, val := range ctx.Variables {
			contextStr += fmt.Sprintf("%s:%s;", name, val.Type().FriendlyName())
		}
	}
	
	hash := md5.Sum([]byte(contextStr))
	return fmt.Sprintf("%x", hash)
}

// createCacheKey generates a cache key for the given expression and context
func (ec *ExpressionCache) createCacheKey(expr hcl.Expression, ctx *hcl.EvalContext) string {
	exprHash := ec.hashExpression(expr)
	contextHash := ec.hashContext(ctx)
	return fmt.Sprintf("%s:%s", exprHash, contextHash)
}

// Get retrieves a cached evaluation result
func (ec *ExpressionCache) Get(expr hcl.Expression, ctx *hcl.EvalContext) (cty.Value, hcl.Diagnostics, bool) {
	key := ec.createCacheKey(expr, ctx)
	
	ec.mutex.RLock()
	entry, exists := ec.cache[key]
	ec.mutex.RUnlock()
	
	if !exists {
		if ec.config.EnableMetrics {
			ec.misses++
		}
		return cty.NilVal, nil, false
	}
	
	// Check if entry is expired
	if entry.IsExpired(ec.config.TTL) {
		ec.mutex.Lock()
		delete(ec.cache, key)
		ec.removeLRU(key)
		ec.mutex.Unlock()
		
		if ec.config.EnableMetrics {
			ec.misses++
		}
		return cty.NilVal, nil, false
	}
	
	// Update access time and hit count
	ec.mutex.Lock()
	entry.AccessTime = time.Now()
	entry.HitCount++
	ec.updateLRU(key)
	ec.mutex.Unlock()
	
	if ec.config.EnableMetrics {
		ec.hits++
	}
	
	return entry.Result, entry.Diags, true
}

// Put stores an evaluation result in the cache
func (ec *ExpressionCache) Put(expr hcl.Expression, ctx *hcl.EvalContext, result cty.Value, diags hcl.Diagnostics) {
	key := ec.createCacheKey(expr, ctx)
	
	entry := &CacheEntry{
		Result:     result,
		Diags:      diags,
		Timestamp:  time.Now(),
		AccessTime: time.Now(),
		HitCount:   0,
	}
	
	ec.mutex.Lock()
	defer ec.mutex.Unlock()
	
	// Check if we need to evict entries
	if len(ec.cache) >= ec.config.MaxSize {
		ec.evictLRU()
	}
	
	ec.cache[key] = entry
	ec.addLRU(key)
}

// updateLRU moves the key to the front of the LRU list
func (ec *ExpressionCache) updateLRU(key string) {
	// Remove from current position
	ec.removeLRU(key)
	// Add to front
	ec.addLRU(key)
}

// addLRU adds a key to the front of the LRU list
func (ec *ExpressionCache) addLRU(key string) {
	ec.lruList = append([]string{key}, ec.lruList...)
}

// removeLRU removes a key from the LRU list
func (ec *ExpressionCache) removeLRU(key string) {
	for i, k := range ec.lruList {
		if k == key {
			ec.lruList = append(ec.lruList[:i], ec.lruList[i+1:]...)
			break
		}
	}
}

// evictLRU removes the least recently used entry
func (ec *ExpressionCache) evictLRU() {
	if len(ec.lruList) == 0 {
		return
	}
	
	// Remove the last (least recently used) entry
	lastKey := ec.lruList[len(ec.lruList)-1]
	delete(ec.cache, lastKey)
	ec.lruList = ec.lruList[:len(ec.lruList)-1]
}

// Clear removes all entries from the cache
func (ec *ExpressionCache) Clear() {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()
	
	ec.cache = make(map[string]*CacheEntry)
	ec.lruList = ec.lruList[:0]
}

// Stats returns cache statistics
type CacheStats struct {
	Size       int
	MaxSize    int
	Hits       int64
	Misses     int64
	HitRate    float64
	Efficiency float64
}

// GetStats returns current cache statistics
func (ec *ExpressionCache) GetStats() CacheStats {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()
	
	total := ec.hits + ec.misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(ec.hits) / float64(total)
	}
	
	return CacheStats{
		Size:       len(ec.cache),
		MaxSize:    ec.config.MaxSize,
		Hits:       ec.hits,
		Misses:     ec.misses,
		HitRate:    hitRate,
		Efficiency: hitRate * 100,
	}
}

// CachedEvaluator wraps an expression with caching capabilities
type CachedEvaluator struct {
	cache *ExpressionCache
}

// NewCachedEvaluator creates a new cached expression evaluator
func NewCachedEvaluator(config *CacheConfig) *CachedEvaluator {
	return &CachedEvaluator{
		cache: NewExpressionCache(config),
	}
}

// Evaluate evaluates an expression with caching
func (ce *CachedEvaluator) Evaluate(expr hcl.Expression, ctx *hcl.EvalContext) (cty.Value, hcl.Diagnostics) {
	// Check cache first
	if result, diags, found := ce.cache.Get(expr, ctx); found {
		return result, diags
	}
	
	// Evaluate expression
	result, diags := expr.Value(ctx)
	
	// Only cache successful evaluations or known error types
	if !diags.HasErrors() || len(diags) == 0 {
		ce.cache.Put(expr, ctx, result, diags)
	}
	
	return result, diags
}

// GetStats returns cache statistics
func (ce *CachedEvaluator) GetStats() CacheStats {
	return ce.cache.GetStats()
}

// Clear clears the cache
func (ce *CachedEvaluator) Clear() {
	ce.cache.Clear()
}