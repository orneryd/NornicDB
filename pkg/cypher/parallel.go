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
	"context"
	"runtime"
	"sync"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
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
		MinBatchSize: 1000, // Reverted: 200 caused contention on Mac M-series
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
func parallelFilterNodes(ctx context.Context, nodes []*storage.Node, filterFn FilterFunc) ([]*storage.Node, error) {
	if !parallelConfig.Enabled || len(nodes) < parallelConfig.MinBatchSize || len(nodes) < 4*parallelConfig.MinBatchSize {
		// Sequential fallback for small datasets
		return sequentialFilterNodes(ctx, nodes, filterFn)
	}

	numWorkers := parallelConfig.MaxWorkers
	// For medium-sized rowsets, reduce scheduling overhead by capping worker fanout.
	// This avoids runtime condvar churn dominating predicate evaluation costs.
	if capByRows := len(nodes) / 5000; capByRows > 0 && capByRows < numWorkers {
		numWorkers = capByRows
	}
	if numWorkers > len(nodes) {
		numWorkers = len(nodes)
	}
	if numWorkers <= 1 {
		return sequentialFilterNodes(ctx, nodes, filterFn)
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
			// A stop leaves the chunk incomplete; the merge below returns the
			// statement's error.
			var stopped error
			defer bulk.RecoverStop(&stopped)
			filtered := make([]*storage.Node, 0, len(chunk)/4) // Estimate 25% pass rate
			for batch, batchErr := range materializedBatches(ctx, len(chunk)) {
				if batchErr != nil {
					return
				}
				for _, node := range chunk[batch.start:batch.end] {
					if filterFn(node) {
						filtered = append(filtered, node)
					}
				}
			}
			results[workerID] = filtered
		}(i, nodes[start:end])
	}

	wg.Wait()
	// A worker that observed the cancellation stopped early: its chunk is
	// incomplete.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Merge results
	totalSize := 0
	for _, r := range results {
		totalSize += len(r)
	}

	merged := make([]*storage.Node, 0, totalSize)
	for _, r := range results {
		var err error
		if merged, err = appendPopulation(ctx, merged, r); err != nil {
			return nil, err
		}
	}

	return merged, nil
}

// sequentialFilterNodes filters nodes sequentially (fallback for small datasets).
func sequentialFilterNodes(ctx context.Context, nodes []*storage.Node, filterFn FilterFunc) ([]*storage.Node, error) {
	result := make([]*storage.Node, 0, len(nodes)/4)
	for batch, batchErr := range materializedBatches(ctx, len(nodes)) {
		if batchErr != nil {
			return nil, batchErr
		}
		for _, node := range nodes[batch.start:batch.end] {
			if filterFn(node) {
				result = append(result, node)
			}
		}
	}
	return result, nil
}

// AggregateResult holds partial aggregation results from a worker.
type AggregateResult struct {
	Count   int64
	Sum     float64
	Values  []interface{} // For COLLECT
	Min     interface{}
	Max     interface{}
	HasData bool
}

// MapFunc transforms a node into a result value.
type MapFunc func(node *storage.Node) interface{}

// EdgeFilterFunc tests whether an edge matches filter criteria.
type EdgeFilterFunc func(edge *storage.Edge) bool

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
		go p.work()
	}
	p.started = true
}

// work runs the pool's jobs until the pool stops.
func (p *WorkerPool) work() {
	var stopped error
	defer bulk.RecoverStop(&stopped)
	for job := range p.jobs {
		p.run(job)
	}
}

// run runs one job. A job that stops on its statement's cancellation ends
// there (bulk.RecoverStop), and it is done however it ends, so Wait and Stop
// never wait for a job that will not finish.
func (p *WorkerPool) run(job func()) {
	defer p.wg.Done()
	var stopped error
	defer bulk.RecoverStop(&stopped)
	job()
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
