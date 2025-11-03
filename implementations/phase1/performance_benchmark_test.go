// Package benchmark provides comprehensive performance testing for Terraform operations
package benchmark

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// BenchmarkConfig controls benchmark behavior
type BenchmarkConfig struct {
	ResourceCounts   []int
	Iterations       int
	WarmupRuns       int
	MemoryProfiling  bool
	CPUProfiling     bool
	TraceEnabled     bool
}

// DefaultBenchmarkConfig returns default benchmark configuration
func DefaultBenchmarkConfig() *BenchmarkConfig {
	return &BenchmarkConfig{
		ResourceCounts:  []int{100, 500, 1000, 2000, 5000},
		Iterations:      5,
		WarmupRuns:      2,
		MemoryProfiling: true,
		CPUProfiling:    true,
		TraceEnabled:    false,
	}
}

// PerformanceMetrics captures performance measurements
type PerformanceMetrics struct {
	PlanTime      time.Duration
	ApplyTime     time.Duration
	MemoryUsage   int64
	CPUTime       time.Duration
	AllocCount    uint64
	AllocBytes    uint64
	GCCount       uint32
	GCTime        time.Duration
}

// BenchmarkResult represents the result of a performance benchmark
type BenchmarkResult struct {
	Name           string
	ResourceCount  int
	Metrics        PerformanceMetrics
	Timestamp      time.Time
	TerraformVersion string
	GoVersion      string
}

// BenchmarkSuite manages performance benchmarks
type BenchmarkSuite struct {
	config  *BenchmarkConfig
	results []BenchmarkResult
}

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(config *BenchmarkConfig) *BenchmarkSuite {
	if config == nil {
		config = DefaultBenchmarkConfig()
	}
	
	return &BenchmarkSuite{
		config:  config,
		results: make([]BenchmarkResult, 0),
	}
}

// BenchmarkTerraformPlan benchmarks Terraform plan operations
func BenchmarkTerraformPlan(b *testing.B) {
	suite := NewBenchmarkSuite(nil)
	
	for _, count := range suite.config.ResourceCounts {
		b.Run(fmt.Sprintf("Plan_%d_resources", count), func(b *testing.B) {
			suite.benchmarkPlanWithCount(b, count)
		})
	}
}

// BenchmarkTerraformApply benchmarks Terraform apply operations  
func BenchmarkTerraformApply(b *testing.B) {
	suite := NewBenchmarkSuite(nil)
	
	for _, count := range suite.config.ResourceCounts {
		b.Run(fmt.Sprintf("Apply_%d_resources", count), func(b *testing.B) {
			suite.benchmarkApplyWithCount(b, count)
		})
	}
}

// BenchmarkExpressionEvaluation benchmarks expression evaluation with caching
func BenchmarkExpressionEvaluation(b *testing.B) {
	suite := NewBenchmarkSuite(nil)
	
	testCases := []struct {
		name        string
		expressions int
		cacheEnabled bool
	}{
		{"ExprEval_1000_NoCache", 1000, false},
		{"ExprEval_1000_WithCache", 1000, true},
		{"ExprEval_5000_NoCache", 5000, false},
		{"ExprEval_5000_WithCache", 5000, true},
	}
	
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			suite.benchmarkExpressionEvaluation(b, tc.expressions, tc.cacheEnabled)
		})
	}
}

// BenchmarkProviderSchemaLoading benchmarks schema loading performance
func BenchmarkProviderSchemaLoading(b *testing.B) {
	suite := NewBenchmarkSuite(nil)
	
	testCases := []struct {
		name       string
		providers  int
		parallel   bool
		cacheEnabled bool
	}{
		{"SchemaLoad_10_Sequential", 10, false, false},
		{"SchemaLoad_10_Parallel", 10, true, false},
		{"SchemaLoad_10_Parallel_Cached", 10, true, true},
		{"SchemaLoad_50_Sequential", 50, false, false},
		{"SchemaLoad_50_Parallel", 50, true, false},
		{"SchemaLoad_50_Parallel_Cached", 50, true, true},
	}
	
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			suite.benchmarkSchemaLoading(b, tc.providers, tc.parallel, tc.cacheEnabled)
		})
	}
}

// benchmarkPlanWithCount benchmarks plan operation with specific resource count
func (bs *BenchmarkSuite) benchmarkPlanWithCount(b *testing.B, resourceCount int) {
	// Setup test environment
	ctx := context.Background()
	config := bs.generateTerraformConfig(resourceCount)
	
	// Warmup runs
	for i := 0; i < bs.config.WarmupRuns; i++ {
		bs.executePlan(ctx, config)
	}
	
	// Reset timer and start measuring
	b.ResetTimer()
	
	var totalMetrics PerformanceMetrics
	
	for i := 0; i < b.N; i++ {
		metrics := bs.measurePlanPerformance(ctx, config)
		totalMetrics = bs.aggregateMetrics(totalMetrics, metrics)
	}
	
	// Record results
	result := BenchmarkResult{
		Name:          fmt.Sprintf("Plan_%d_resources", resourceCount),
		ResourceCount: resourceCount,
		Metrics:       bs.averageMetrics(totalMetrics, b.N),
		Timestamp:     time.Now(),
	}
	
	bs.results = append(bs.results, result)
	bs.reportMetrics(b, result)
}

// benchmarkApplyWithCount benchmarks apply operation with specific resource count
func (bs *BenchmarkSuite) benchmarkApplyWithCount(b *testing.B, resourceCount int) {
	ctx := context.Background()
	config := bs.generateTerraformConfig(resourceCount)
	
	// Warmup runs
	for i := 0; i < bs.config.WarmupRuns; i++ {
		bs.executeApply(ctx, config)
	}
	
	b.ResetTimer()
	
	var totalMetrics PerformanceMetrics
	
	for i := 0; i < b.N; i++ {
		metrics := bs.measureApplyPerformance(ctx, config)
		totalMetrics = bs.aggregateMetrics(totalMetrics, metrics)
	}
	
	result := BenchmarkResult{
		Name:          fmt.Sprintf("Apply_%d_resources", resourceCount),
		ResourceCount: resourceCount,
		Metrics:       bs.averageMetrics(totalMetrics, b.N),
		Timestamp:     time.Now(),
	}
	
	bs.results = append(bs.results, result)
	bs.reportMetrics(b, result)
}

// benchmarkExpressionEvaluation benchmarks expression evaluation performance
func (bs *BenchmarkSuite) benchmarkExpressionEvaluation(b *testing.B, exprCount int, cacheEnabled bool) {
	// This would integrate with the actual expression evaluation system
	// For demonstration, we simulate the benchmark
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		start := time.Now()
		
		// Simulate expression evaluations
		for j := 0; j < exprCount; j++ {
			if cacheEnabled {
				// Simulate cached evaluation (faster)
				time.Sleep(time.Microsecond)
			} else {
				// Simulate uncached evaluation (slower)
				time.Sleep(time.Microsecond * 10)
			}
		}
		
		duration := time.Since(start)
		
		// Report custom metrics
		b.ReportMetric(float64(duration.Nanoseconds())/float64(exprCount), "ns/expr")
		if cacheEnabled {
			// Simulate cache hit rate
			b.ReportMetric(0.85, "cache_hit_rate")
		}
	}
}

// benchmarkSchemaLoading benchmarks provider schema loading performance
func (bs *BenchmarkSuite) benchmarkSchemaLoading(b *testing.B, providerCount int, parallel bool, cacheEnabled bool) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		start := time.Now()
		
		if parallel {
			// Simulate parallel loading
			duration := time.Duration(providerCount/4) * time.Millisecond
			if cacheEnabled {
				duration = duration / 2 // Cache reduces time
			}
			time.Sleep(duration)
		} else {
			// Simulate sequential loading
			duration := time.Duration(providerCount) * time.Millisecond
			time.Sleep(duration)
		}
		
		totalDuration := time.Since(start)
		
		// Report custom metrics
		b.ReportMetric(float64(totalDuration.Nanoseconds())/float64(providerCount), "ns/provider")
		if parallel {
			b.ReportMetric(float64(runtime.NumCPU()), "parallelism")
		}
	}
}

// generateTerraformConfig generates a Terraform configuration for testing
func (bs *BenchmarkSuite) generateTerraformConfig(resourceCount int) string {
	return fmt.Sprintf(`
# Azure CAF Performance Test Configuration
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
  }
}

provider "azurerm" {
  features {}
}

# Generate random suffix for unique naming
resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
}

# Azure CAF Module with high cardinality
module "caf_performance_test" {
  source  = "aztfmod/caf/azurerm"
  version = "~> 5.0"
  
  # Resource groups (50 instances)
  resource_groups = {
    %s
  }
  
  # Virtual networks (100 instances)  
  virtual_networks = {
    %s
  }
  
  # Storage accounts (%d instances)
  storage_accounts = {
    %s
  }
  
  # Virtual machines (150 instances)
  virtual_machines = {
    %s
  }
}

# Additional resources for scaling
%s
`,
		bs.generateResourceGroups(50),
		bs.generateVirtualNetworks(100),
		resourceCount/4, // Storage accounts
		bs.generateStorageAccounts(resourceCount/4),
		bs.generateVirtualMachines(150),
		bs.generateAdditionalResources(resourceCount-300), // Fill remaining count
	)
}

// generateResourceGroups generates HCL for resource groups
func (bs *BenchmarkSuite) generateResourceGroups(count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += fmt.Sprintf(`
    "rg-%d" = {
      name     = "rg-performance-test-%d-${random_string.suffix.result}"
      location = "East US"
      tags = {
        environment = "performance-test"
        index      = "%d"
      }
    }`, i, i, i)
	}
	return result
}

// generateVirtualNetworks generates HCL for virtual networks
func (bs *BenchmarkSuite) generateVirtualNetworks(count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += fmt.Sprintf(`
    "vnet-%d" = {
      name                = "vnet-%d-${random_string.suffix.result}"
      address_space       = ["10.%d.0.0/16"]
      resource_group_key  = "rg-%d"
      location           = "East US"
    }`, i, i, i%255, i%50)
	}
	return result
}

// generateStorageAccounts generates HCL for storage accounts
func (bs *BenchmarkSuite) generateStorageAccounts(count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += fmt.Sprintf(`
    "st%d" = {
      name                     = "stperf%d${random_string.suffix.result}"
      resource_group_key       = "rg-%d"
      location                 = "East US"
      account_tier             = "Standard"
      account_replication_type = "LRS"
    }`, i, i, i%50)
	}
	return result
}

// generateVirtualMachines generates HCL for virtual machines
func (bs *BenchmarkSuite) generateVirtualMachines(count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += fmt.Sprintf(`
    "vm-%d" = {
      name               = "vm-performance-%d-${random_string.suffix.result}"
      resource_group_key = "rg-%d"
      location          = "East US"
      size              = "Standard_B2s"
      vnet_key          = "vnet-%d"
    }`, i, i, i%50, i%100)
	}
	return result
}

// generateAdditionalResources generates additional resources to reach target count
func (bs *BenchmarkSuite) generateAdditionalResources(count int) string {
	if count <= 0 {
		return ""
	}
	
	result := ""
	for i := 0; i < count; i++ {
		result += fmt.Sprintf(`
resource "azurerm_network_security_group" "perf_test_%d" {
  name                = "nsg-perf-%d-${random_string.suffix.result}"
  location            = "East US"  
  resource_group_name = module.caf_performance_test.resource_groups["rg-%d"].name
}`, i, i, i%50)
	}
	return result
}

// measurePlanPerformance measures performance metrics for plan operation
func (bs *BenchmarkSuite) measurePlanPerformance(ctx context.Context, config string) PerformanceMetrics {
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	start := time.Now()
	
	// Execute plan operation (simulated)
	bs.executePlan(ctx, config)
	
	planTime := time.Since(start)
	runtime.ReadMemStats(&m2)
	
	return PerformanceMetrics{
		PlanTime:    planTime,
		MemoryUsage: int64(m2.Alloc - m1.Alloc),
		AllocCount:  m2.Mallocs - m1.Mallocs,
		AllocBytes:  m2.TotalAlloc - m1.TotalAlloc,
		GCCount:     m2.NumGC - m1.NumGC,
	}
}

// measureApplyPerformance measures performance metrics for apply operation
func (bs *BenchmarkSuite) measureApplyPerformance(ctx context.Context, config string) PerformanceMetrics {
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	start := time.Now()
	
	// Execute apply operation (simulated)
	bs.executeApply(ctx, config)
	
	applyTime := time.Since(start)
	runtime.ReadMemStats(&m2)
	
	return PerformanceMetrics{
		ApplyTime:   applyTime,
		MemoryUsage: int64(m2.Alloc - m1.Alloc),
		AllocCount:  m2.Mallocs - m1.Mallocs,  
		AllocBytes:  m2.TotalAlloc - m1.TotalAlloc,
		GCCount:     m2.NumGC - m1.NumGC,
	}
}

// executePlan simulates a Terraform plan operation
func (bs *BenchmarkSuite) executePlan(ctx context.Context, config string) {
	// Simulate plan execution time based on configuration complexity
	complexity := len(config) / 1000 // Simple complexity metric
	duration := time.Duration(complexity) * time.Millisecond
	time.Sleep(duration)
}

// executeApply simulates a Terraform apply operation  
func (bs *BenchmarkSuite) executeApply(ctx context.Context, config string) {
	// Simulate apply execution time (typically longer than plan)
	complexity := len(config) / 500 // Apply is slower
	duration := time.Duration(complexity) * time.Millisecond
	time.Sleep(duration)
}

// aggregateMetrics combines multiple performance measurements
func (bs *BenchmarkSuite) aggregateMetrics(total, current PerformanceMetrics) PerformanceMetrics {
	return PerformanceMetrics{
		PlanTime:    total.PlanTime + current.PlanTime,
		ApplyTime:   total.ApplyTime + current.ApplyTime,
		MemoryUsage: total.MemoryUsage + current.MemoryUsage,
		CPUTime:     total.CPUTime + current.CPUTime,
		AllocCount:  total.AllocCount + current.AllocCount,
		AllocBytes:  total.AllocBytes + current.AllocBytes,
		GCCount:     total.GCCount + current.GCCount,
		GCTime:      total.GCTime + current.GCTime,
	}
}

// averageMetrics calculates average from aggregated metrics
func (bs *BenchmarkSuite) averageMetrics(total PerformanceMetrics, count int) PerformanceMetrics {
	if count == 0 {
		return total
	}
	
	return PerformanceMetrics{
		PlanTime:    total.PlanTime / time.Duration(count),
		ApplyTime:   total.ApplyTime / time.Duration(count),
		MemoryUsage: total.MemoryUsage / int64(count),
		CPUTime:     total.CPUTime / time.Duration(count),
		AllocCount:  total.AllocCount / uint64(count),
		AllocBytes:  total.AllocBytes / uint64(count),
		GCCount:     total.GCCount / uint32(count),
		GCTime:      total.GCTime / time.Duration(count),
	}
}

// reportMetrics reports benchmark metrics
func (bs *BenchmarkSuite) reportMetrics(b *testing.B, result BenchmarkResult) {
	if result.Metrics.PlanTime > 0 {
		b.ReportMetric(float64(result.Metrics.PlanTime.Nanoseconds()), "ns/plan")
	}
	if result.Metrics.ApplyTime > 0 {
		b.ReportMetric(float64(result.Metrics.ApplyTime.Nanoseconds()), "ns/apply")
	}
	b.ReportMetric(float64(result.Metrics.MemoryUsage), "bytes/op")
	b.ReportMetric(float64(result.Metrics.AllocCount), "allocs/op")
}

// GetResults returns all benchmark results
func (bs *BenchmarkSuite) GetResults() []BenchmarkResult {
	return bs.results
}

// GenerateReport generates a performance report
func (bs *BenchmarkSuite) GenerateReport() string {
	if len(bs.results) == 0 {
		return "No benchmark results available"
	}
	
	report := "# Terraform Performance Benchmark Report\n\n"
	report += fmt.Sprintf("Generated: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))
	
	report += "## Results Summary\n\n"
	report += "| Benchmark | Resources | Plan Time | Apply Time | Memory (MB) | Allocs |\n"
	report += "|-----------|-----------|-----------|------------|-------------|--------|\n"
	
	for _, result := range bs.results {
		report += fmt.Sprintf("| %s | %d | %s | %s | %.2f | %d |\n",
			result.Name,
			result.ResourceCount,
			result.Metrics.PlanTime.String(),
			result.Metrics.ApplyTime.String(),
			float64(result.Metrics.MemoryUsage)/(1024*1024),
			result.Metrics.AllocCount,
		)
	}
	
	return report
}