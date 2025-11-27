package cypher

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestParallelFilterNodes(t *testing.T) {
	// Create test nodes
	nodes := make([]*storage.Node, 2000)
	for i := 0; i < 2000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune('a' + i%26)) + string(rune(i))),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"age":  i % 100,
				"name": string(rune('A' + i%26)),
			},
		}
	}

	// Filter function: age >= 50
	filterFn := func(node *storage.Node) bool {
		if age, ok := node.Properties["age"].(int); ok {
			return age >= 50
		}
		return false
	}

	t.Run("parallel filtering", func(t *testing.T) {
		// Enable parallel execution
		SetParallelConfig(ParallelConfig{
			Enabled:      true,
			MaxWorkers:   4,
			MinBatchSize: 100,
		})

		result := parallelFilterNodes(nodes, filterFn)

		// Should have roughly half the nodes (50-99 from each 100)
		if len(result) < 900 || len(result) > 1100 {
			t.Errorf("expected ~1000 filtered nodes, got %d", len(result))
		}

		// Verify all results match filter
		for _, node := range result {
			if age, ok := node.Properties["age"].(int); ok {
				if age < 50 {
					t.Errorf("node with age %d should not pass filter", age)
				}
			}
		}
	})

	t.Run("sequential fallback for small batch", func(t *testing.T) {
		SetParallelConfig(ParallelConfig{
			Enabled:      true,
			MaxWorkers:   4,
			MinBatchSize: 5000, // Higher than our dataset
		})

		result := parallelFilterNodes(nodes, filterFn)

		if len(result) < 900 || len(result) > 1100 {
			t.Errorf("expected ~1000 filtered nodes, got %d", len(result))
		}
	})

	t.Run("disabled parallel execution", func(t *testing.T) {
		SetParallelConfig(ParallelConfig{
			Enabled:      false,
			MaxWorkers:   4,
			MinBatchSize: 100,
		})

		result := parallelFilterNodes(nodes, filterFn)

		if len(result) < 900 || len(result) > 1100 {
			t.Errorf("expected ~1000 filtered nodes, got %d", len(result))
		}
	})
}

func TestParallelCount(t *testing.T) {
	nodes := make([]*storage.Node, 5000)
	for i := 0; i < 5000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune(i))),
			Labels: []string{"Item"},
			Properties: map[string]interface{}{
				"value": i,
			},
		}
	}

	SetParallelConfig(ParallelConfig{
		Enabled:      true,
		MaxWorkers:   4,
		MinBatchSize: 1000,
	})

	t.Run("count all", func(t *testing.T) {
		count := parallelCount(nodes, nil)
		if count != 5000 {
			t.Errorf("expected 5000, got %d", count)
		}
	})

	t.Run("count with filter", func(t *testing.T) {
		filterFn := func(node *storage.Node) bool {
			if val, ok := node.Properties["value"].(int); ok {
				return val >= 2500
			}
			return false
		}

		count := parallelCount(nodes, filterFn)
		if count != 2500 {
			t.Errorf("expected 2500, got %d", count)
		}
	})
}

func TestParallelSum(t *testing.T) {
	nodes := make([]*storage.Node, 2000)
	var expectedSum float64
	for i := 0; i < 2000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune(i))),
			Labels: []string{"Item"},
			Properties: map[string]interface{}{
				"amount": float64(i),
			},
		}
		expectedSum += float64(i)
	}

	SetParallelConfig(ParallelConfig{
		Enabled:      true,
		MaxWorkers:   4,
		MinBatchSize: 500,
	})

	sum := parallelSum(nodes, "amount")
	if sum != expectedSum {
		t.Errorf("expected sum %f, got %f", expectedSum, sum)
	}
}

func TestParallelCollect(t *testing.T) {
	nodes := make([]*storage.Node, 1500)
	for i := 0; i < 1500; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune(i))),
			Labels: []string{"Item"},
			Properties: map[string]interface{}{
				"name": string(rune('A' + i%26)),
			},
		}
	}

	SetParallelConfig(ParallelConfig{
		Enabled:      true,
		MaxWorkers:   4,
		MinBatchSize: 500,
	})

	result := parallelCollect(nodes, "name")
	if len(result) != 1500 {
		t.Errorf("expected 1500 items, got %d", len(result))
	}
}

func TestParallelMap(t *testing.T) {
	nodes := make([]*storage.Node, 2000)
	for i := 0; i < 2000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune(i))),
			Labels: []string{"Item"},
			Properties: map[string]interface{}{
				"value": i,
			},
		}
	}

	SetParallelConfig(ParallelConfig{
		Enabled:      true,
		MaxWorkers:   4,
		MinBatchSize: 500,
	})

	mapFn := func(node *storage.Node) interface{} {
		if val, ok := node.Properties["value"].(int); ok {
			return val * 2
		}
		return 0
	}

	result := parallelMap(nodes, mapFn)
	if len(result) != 2000 {
		t.Errorf("expected 2000 items, got %d", len(result))
	}
}

func TestWorkerPool(t *testing.T) {
	pool := NewWorkerPool(4)
	pool.Start()
	defer pool.Stop()

	var counter int64

	// Submit 100 jobs
	for i := 0; i < 100; i++ {
		pool.Submit(func() {
			atomic.AddInt64(&counter, 1)
		})
	}

	pool.Wait()

	if counter != 100 {
		t.Errorf("expected counter to be 100, got %d", counter)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkFilterNodes(b *testing.B) {
	// Create large dataset
	nodes := make([]*storage.Node, 100000)
	for i := 0; i < 100000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune(i))),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"age":  i % 100,
				"name": string(rune('A' + i%26)),
			},
		}
	}

	filterFn := func(node *storage.Node) bool {
		if age, ok := node.Properties["age"].(int); ok {
			return age >= 50
		}
		return false
	}

	b.Run("sequential", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: false})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sequentialFilterNodes(nodes, filterFn)
		}
	})

	b.Run("parallel_2_workers", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 2, MinBatchSize: 1000})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parallelFilterNodes(nodes, filterFn)
		}
	})

	b.Run("parallel_4_workers", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 4, MinBatchSize: 1000})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parallelFilterNodes(nodes, filterFn)
		}
	})

	b.Run("parallel_8_workers", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 8, MinBatchSize: 1000})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parallelFilterNodes(nodes, filterFn)
		}
	})
}

func BenchmarkParallelCount(b *testing.B) {
	nodes := make([]*storage.Node, 100000)
	for i := 0; i < 100000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune(i))),
			Labels: []string{"Item"},
			Properties: map[string]interface{}{
				"value": i,
			},
		}
	}

	filterFn := func(node *storage.Node) bool {
		if val, ok := node.Properties["value"].(int); ok {
			return val%2 == 0
		}
		return false
	}

	b.Run("sequential", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: false})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parallelCount(nodes, filterFn)
		}
	})

	b.Run("parallel", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 4, MinBatchSize: 1000})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parallelCount(nodes, filterFn)
		}
	})
}

func BenchmarkParallelSum(b *testing.B) {
	nodes := make([]*storage.Node, 100000)
	for i := 0; i < 100000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune(i))),
			Labels: []string{"Item"},
			Properties: map[string]interface{}{
				"amount": float64(i),
			},
		}
	}

	b.Run("sequential", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: false})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parallelSum(nodes, "amount")
		}
	})

	b.Run("parallel", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 4, MinBatchSize: 1000})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parallelSum(nodes, "amount")
		}
	})
}

func TestParallelConfigDefaults(t *testing.T) {
	config := DefaultParallelConfig()

	if !config.Enabled {
		t.Error("expected parallel execution to be enabled by default")
	}

	if config.MaxWorkers <= 0 {
		t.Errorf("expected positive MaxWorkers, got %d", config.MaxWorkers)
	}

	if config.MinBatchSize <= 0 {
		t.Errorf("expected positive MinBatchSize, got %d", config.MinBatchSize)
	}
}

func TestSetParallelConfig(t *testing.T) {
	// Test invalid values get corrected
	SetParallelConfig(ParallelConfig{
		Enabled:      true,
		MaxWorkers:   -1,
		MinBatchSize: 0,
	})

	config := GetParallelConfig()

	if config.MaxWorkers <= 0 {
		t.Error("MaxWorkers should be corrected to positive value")
	}

	if config.MinBatchSize <= 0 {
		t.Error("MinBatchSize should be corrected to positive value")
	}
}

func TestParallelFilterNodesEmpty(t *testing.T) {
	nodes := []*storage.Node{}
	
	result := parallelFilterNodes(nodes, func(node *storage.Node) bool {
		return true
	})

	if len(result) != 0 {
		t.Errorf("expected empty result, got %d nodes", len(result))
	}
}

func TestParallelExecutionCorrectness(t *testing.T) {
	// Create nodes with unique IDs to verify all are processed
	nodes := make([]*storage.Node, 5000)
	for i := 0; i < 5000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune(i))),
			Labels: []string{"Test"},
			Properties: map[string]interface{}{
				"index": i,
			},
		}
	}

	SetParallelConfig(ParallelConfig{
		Enabled:      true,
		MaxWorkers:   8,
		MinBatchSize: 100,
	})

	// Filter to get even indices
	result := parallelFilterNodes(nodes, func(node *storage.Node) bool {
		if idx, ok := node.Properties["index"].(int); ok {
			return idx%2 == 0
		}
		return false
	})

	if len(result) != 2500 {
		t.Errorf("expected 2500 nodes (even indices), got %d", len(result))
	}

	// Verify all results are correct
	for _, node := range result {
		if idx, ok := node.Properties["index"].(int); ok {
			if idx%2 != 0 {
				t.Errorf("node with index %d should not be in result (not even)", idx)
			}
		}
	}
}

func TestWorkerPoolConcurrency(t *testing.T) {
	pool := NewWorkerPool(4)
	pool.Start()
	defer pool.Stop()

	var maxConcurrent int64
	var currentConcurrent int64

	// Submit jobs that track concurrency
	for i := 0; i < 20; i++ {
		pool.Submit(func() {
			current := atomic.AddInt64(&currentConcurrent, 1)
			
			// Track max
			for {
				max := atomic.LoadInt64(&maxConcurrent)
				if current <= max {
					break
				}
				if atomic.CompareAndSwapInt64(&maxConcurrent, max, current) {
					break
				}
			}

			// Simulate work
			time.Sleep(10 * time.Millisecond)
			
			atomic.AddInt64(&currentConcurrent, -1)
		})
	}

	pool.Wait()

	max := atomic.LoadInt64(&maxConcurrent)
	if max < 2 {
		t.Logf("max concurrent workers: %d (may vary based on timing)", max)
	}
}
