// Package cypher provides parallel query execution support for NornicDB.
//
// Parallel execution significantly improves query performance on large datasets
// by distributing work across multiple CPU cores. This is especially beneficial for:
//   - Filtering large node sets (WHERE clauses)
//   - Aggregation operations (COUNT, SUM, AVG, COLLECT)
//   - Relationship traversal
//   - UNION queries (parallel branch execution)
//
// # Neo4j Compatibility
//
// Neo4j Enterprise uses parallel execution for query processing. This implementation
// brings similar capabilities to NornicDB, enabling comparable performance for
// analytical workloads.
//
// # Configuration
//
// Parallel execution can be configured via ParallelConfig:
//
//	config := cypher.ParallelConfig{
//	    Enabled:      true,
//	    MaxWorkers:   8,           // Use 8 cores max
//	    MinBatchSize: 500,         // Parallelize when >500 items
//	}
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you have 1000 books to check for a specific word. Instead of checking
// one by one, you get 4 friends to help. Each friend takes 250 books and checks
// them at the same time. That's parallel execution - doing work simultaneously!
package cypher

import (
	"runtime"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ParallelConfig controls parallel execution behavior.
type ParallelConfig struct {
	// Enabled enables/disables parallel execution globally
	Enabled bool

	// MaxWorkers is the maximum number of goroutines to use
	// Default: runtime.NumCPU()
	MaxWorkers int

	// MinBatchSize is the minimum number of items before parallelizing
	// Below this threshold, sequential execution is used (overhead not worth it)
	// Default: 1000
	MinBatchSize int
}

// DefaultParallelConfig returns the default parallel execution configuration.
func DefaultParallelConfig() ParallelConfig {
	return ParallelConfig{
		Enabled:      true,
		MaxWorkers:   runtime.NumCPU(),
		MinBatchSize: 200, // OPTIMIZATION: Reduced from 1000 - parallelization benefits smaller batches
	}
}

// parallelConfig is the active configuration
var parallelConfig = DefaultParallelConfig()

// SetParallelConfig updates the parallel execution configuration.
func SetParallelConfig(config ParallelConfig) {
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = runtime.NumCPU()
	}
	if config.MinBatchSize <= 0 {
		config.MinBatchSize = 1000
	}
	parallelConfig = config
}

// GetParallelConfig returns the current parallel execution configuration.
func GetParallelConfig() ParallelConfig {
	return parallelConfig
}

// =============================================================================
// Parallel Node Filtering
// =============================================================================

// FilterFunc is a function that tests whether a node matches filter criteria.
type FilterFunc func(node *storage.Node) bool

// parallelFilterNodes filters nodes in parallel using multiple workers.
// Falls back to sequential filtering if the dataset is small.
//
// Parameters:
//   - nodes: The slice of nodes to filter
//   - filterFn: Function that returns true if node should be included
//
// Returns:
//   - Slice of nodes that passed the filter
func parallelFilterNodes(nodes []*storage.Node, filterFn FilterFunc) []*storage.Node {
	if !parallelConfig.Enabled || len(nodes) < parallelConfig.MinBatchSize {
		// Sequential fallback for small datasets
		return sequentialFilterNodes(nodes, filterFn)
	}

	numWorkers := parallelConfig.MaxWorkers
	if numWorkers > len(nodes) {
		numWorkers = len(nodes)
	}

	chunkSize := (len(nodes) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	results := make([][]*storage.Node, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(nodes) {
			break
		}
		if end > len(nodes) {
			end = len(nodes)
		}

		wg.Add(1)
		go func(workerID int, chunk []*storage.Node) {
			defer wg.Done()
			filtered := make([]*storage.Node, 0, len(chunk)/4) // Estimate 25% pass rate
			for _, node := range chunk {
				if filterFn(node) {
					filtered = append(filtered, node)
				}
			}
			results[workerID] = filtered
		}(i, nodes[start:end])
	}

	wg.Wait()

	// Merge results
	totalSize := 0
	for _, r := range results {
		totalSize += len(r)
	}

	merged := make([]*storage.Node, 0, totalSize)
	for _, r := range results {
		merged = append(merged, r...)
	}

	return merged
}

// sequentialFilterNodes filters nodes sequentially (fallback for small datasets).
func sequentialFilterNodes(nodes []*storage.Node, filterFn FilterFunc) []*storage.Node {
	result := make([]*storage.Node, 0, len(nodes)/4)
	for _, node := range nodes {
		if filterFn(node) {
			result = append(result, node)
		}
	}
	return result
}

// =============================================================================
// Parallel Aggregation
// =============================================================================

// AggregateResult holds partial aggregation results from a worker.
type AggregateResult struct {
	Count   int64
	Sum     float64
	Values  []interface{} // For COLLECT
	Min     interface{}
	Max     interface{}
	HasData bool
}

// parallelCount counts nodes matching a filter in parallel.
func parallelCount(nodes []*storage.Node, filterFn FilterFunc) int64 {
	if !parallelConfig.Enabled || len(nodes) < parallelConfig.MinBatchSize {
		var count int64
		for _, node := range nodes {
			if filterFn == nil || filterFn(node) {
				count++
			}
		}
		return count
	}

	numWorkers := parallelConfig.MaxWorkers
	chunkSize := (len(nodes) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	counts := make([]int64, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(nodes) {
			break
		}
		if end > len(nodes) {
			end = len(nodes)
		}

		wg.Add(1)
		go func(workerID int, chunk []*storage.Node) {
			defer wg.Done()
			var localCount int64
			for _, node := range chunk {
				if filterFn == nil || filterFn(node) {
					localCount++
				}
			}
			counts[workerID] = localCount
		}(i, nodes[start:end])
	}

	wg.Wait()

	var total int64
	for _, c := range counts {
		total += c
	}
	return total
}

// parallelSum computes sum of a property across nodes in parallel.
func parallelSum(nodes []*storage.Node, property string) float64 {
	if !parallelConfig.Enabled || len(nodes) < parallelConfig.MinBatchSize {
		var sum float64
		for _, node := range nodes {
			if val, ok := node.Properties[property]; ok {
				if f, ok := toFloat64(val); ok {
					sum += f
				}
			}
		}
		return sum
	}

	numWorkers := parallelConfig.MaxWorkers
	chunkSize := (len(nodes) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	sums := make([]float64, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(nodes) {
			break
		}
		if end > len(nodes) {
			end = len(nodes)
		}

		wg.Add(1)
		go func(workerID int, chunk []*storage.Node) {
			defer wg.Done()
			var localSum float64
			for _, node := range chunk {
				if val, ok := node.Properties[property]; ok {
					if f, ok := toFloat64(val); ok {
						localSum += f
					}
				}
			}
			sums[workerID] = localSum
		}(i, nodes[start:end])
	}

	wg.Wait()

	var total float64
	for _, s := range sums {
		total += s
	}
	return total
}

// parallelCollect collects property values from nodes in parallel.
func parallelCollect(nodes []*storage.Node, property string) []interface{} {
	if !parallelConfig.Enabled || len(nodes) < parallelConfig.MinBatchSize {
		result := make([]interface{}, 0, len(nodes))
		for _, node := range nodes {
			if val, ok := node.Properties[property]; ok {
				result = append(result, val)
			}
		}
		return result
	}

	numWorkers := parallelConfig.MaxWorkers
	chunkSize := (len(nodes) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	results := make([][]interface{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(nodes) {
			break
		}
		if end > len(nodes) {
			end = len(nodes)
		}

		wg.Add(1)
		go func(workerID int, chunk []*storage.Node) {
			defer wg.Done()
			localResult := make([]interface{}, 0, len(chunk))
			for _, node := range chunk {
				if val, ok := node.Properties[property]; ok {
					localResult = append(localResult, val)
				}
			}
			results[workerID] = localResult
		}(i, nodes[start:end])
	}

	wg.Wait()

	// Merge results
	totalSize := 0
	for _, r := range results {
		totalSize += len(r)
	}

	merged := make([]interface{}, 0, totalSize)
	for _, r := range results {
		merged = append(merged, r...)
	}

	return merged
}

// =============================================================================
// Parallel Map Operations
// =============================================================================

// MapFunc transforms a node into a result value.
type MapFunc func(node *storage.Node) interface{}

// parallelMap applies a function to all nodes in parallel.
func parallelMap(nodes []*storage.Node, mapFn MapFunc) []interface{} {
	if !parallelConfig.Enabled || len(nodes) < parallelConfig.MinBatchSize {
		result := make([]interface{}, len(nodes))
		for i, node := range nodes {
			result[i] = mapFn(node)
		}
		return result
	}

	numWorkers := parallelConfig.MaxWorkers
	chunkSize := (len(nodes) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	results := make([][]interface{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(nodes) {
			break
		}
		if end > len(nodes) {
			end = len(nodes)
		}

		wg.Add(1)
		go func(workerID int, chunk []*storage.Node) {
			defer wg.Done()
			localResult := make([]interface{}, len(chunk))
			for j, node := range chunk {
				localResult[j] = mapFn(node)
			}
			results[workerID] = localResult
		}(i, nodes[start:end])
	}

	wg.Wait()

	// Merge results maintaining order
	merged := make([]interface{}, 0, len(nodes))
	for _, r := range results {
		merged = append(merged, r...)
	}

	return merged
}

// =============================================================================
// Parallel Edge Operations
// =============================================================================

// EdgeFilterFunc tests whether an edge matches filter criteria.
type EdgeFilterFunc func(edge *storage.Edge) bool

// parallelFilterEdges filters edges in parallel.
func parallelFilterEdges(edges []*storage.Edge, filterFn EdgeFilterFunc) []*storage.Edge {
	if !parallelConfig.Enabled || len(edges) < parallelConfig.MinBatchSize {
		result := make([]*storage.Edge, 0, len(edges)/4)
		for _, edge := range edges {
			if filterFn(edge) {
				result = append(result, edge)
			}
		}
		return result
	}

	numWorkers := parallelConfig.MaxWorkers
	chunkSize := (len(edges) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	results := make([][]*storage.Edge, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(edges) {
			break
		}
		if end > len(edges) {
			end = len(edges)
		}

		wg.Add(1)
		go func(workerID int, chunk []*storage.Edge) {
			defer wg.Done()
			filtered := make([]*storage.Edge, 0, len(chunk)/4)
			for _, edge := range chunk {
				if filterFn(edge) {
					filtered = append(filtered, edge)
				}
			}
			results[workerID] = filtered
		}(i, edges[start:end])
	}

	wg.Wait()

	// Merge results
	totalSize := 0
	for _, r := range results {
		totalSize += len(r)
	}

	merged := make([]*storage.Edge, 0, totalSize)
	for _, r := range results {
		merged = append(merged, r...)
	}

	return merged
}

// =============================================================================
// Worker Pool for Complex Operations
// =============================================================================

// WorkerPool manages a pool of worker goroutines for parallel execution.
type WorkerPool struct {
	numWorkers int
	jobs       chan func()
	wg         sync.WaitGroup
	started    bool
	mu         sync.Mutex
}

// NewWorkerPool creates a new worker pool with the specified number of workers.
func NewWorkerPool(numWorkers int) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	return &WorkerPool{
		numWorkers: numWorkers,
		jobs:       make(chan func(), numWorkers*2),
	}
}

// Start starts the worker goroutines.
func (p *WorkerPool) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return
	}

	for i := 0; i < p.numWorkers; i++ {
		go func() {
			for job := range p.jobs {
				job()
				p.wg.Done()
			}
		}()
	}
	p.started = true
}

// Submit submits a job to the worker pool.
func (p *WorkerPool) Submit(job func()) {
	p.wg.Add(1)
	p.jobs <- job
}

// Wait waits for all submitted jobs to complete.
func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

// Stop stops the worker pool and waits for all jobs to complete.
func (p *WorkerPool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return
	}

	close(p.jobs)
	p.wg.Wait()
	p.started = false
}
