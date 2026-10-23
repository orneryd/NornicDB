package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParallelFilterNodes(t *testing.T) {
	// Create test nodes
	nodes := make([]*storage.Node, 2000)
	for i := 0; i < 2000; i++ {
		nodes[i] = &storage.Node{
			ID:     storage.NodeID(string(rune('a'+i%26)) + string(rune(i))),
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

		result, err := parallelFilterNodes(context.Background(), nodes, filterFn)
		require.NoError(t, err)

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

		result, err := parallelFilterNodes(context.Background(), nodes, filterFn)
		require.NoError(t, err)

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

		result, err := parallelFilterNodes(context.Background(), nodes, filterFn)
		require.NoError(t, err)

		if len(result) < 900 || len(result) > 1100 {
			t.Errorf("expected ~1000 filtered nodes, got %d", len(result))
		}
	})
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
			_, _ = sequentialFilterNodes(context.Background(), nodes, filterFn)
		}
	})

	b.Run("parallel_2_workers", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 2, MinBatchSize: 1000})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = parallelFilterNodes(context.Background(), nodes, filterFn)
		}
	})

	b.Run("parallel_4_workers", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 4, MinBatchSize: 1000})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = parallelFilterNodes(context.Background(), nodes, filterFn)
		}
	})

	b.Run("parallel_8_workers", func(b *testing.B) {
		SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 8, MinBatchSize: 1000})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = parallelFilterNodes(context.Background(), nodes, filterFn)
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

	result, _ := parallelFilterNodes(context.Background(), nodes, func(node *storage.Node) bool {
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
	result, _ := parallelFilterNodes(context.Background(), nodes, func(node *storage.Node) bool {
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
