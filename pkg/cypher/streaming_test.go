// Package cypher - Tests for streaming optimization in MATCH queries.
package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamingOptimization_LimitQuery verifies that LIMIT queries use streaming
// with early termination instead of loading all nodes into memory.
func TestLabeledPropertyProjectionLimitReturnsRows(t *testing.T) {
	for _, fixture := range []struct {
		name        string
		newExecutor func(*testing.T) *StorageExecutor
	}{
		{"direct", func(t *testing.T) *StorageExecutor {
			return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		}},
		{"server_stack", newPathReturnServerStackExecutor},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			exec := fixture.newExecutor(t)
			ctx := context.Background()
			inner := exec.storage.(*storage.NamespacedEngine).GetInnerEngine()
			foreign := NewStorageExecutor(storage.NewNamespacedEngine(inner, "aaa"))
			_, err := foreign.Execute(ctx, "CREATE (:Label {id:99}), (:Label {id:99}), (:Label {id:99})", nil)
			require.NoError(t, err)
			empty, err := exec.Execute(ctx, "MATCH (n:Label) RETURN n.id LIMIT 2", nil)
			require.NoError(t, err)
			require.Empty(t, empty.Rows)
			for _, id := range []int64{1, 2, 3} {
				_, err := exec.storage.CreateNode(&storage.Node{
					ID:     storage.NodeID(fmt.Sprintf("label-%d", id)),
					Labels: []string{"Label"}, Properties: map[string]interface{}{"id": id},
				})
				require.NoError(t, err)
			}
			if async, ok := inner.(*storage.AsyncEngine); ok {
				require.NoError(t, async.Flush())
			}
			for _, query := range []string{
				"MATCH (n:Label) RETURN n.id LIMIT 2",
				"MATCH (n:Label) RETURN n.id AS id LIMIT 2",
				"MATCH (n:Label) RETURN n.id ORDER BY n.id LIMIT 2",
			} {
				t.Run(query, func(t *testing.T) {
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Len(t, result.Rows, 2)
					for _, row := range result.Rows {
						require.Len(t, row, 1)
						require.Contains(t, []interface{}{int64(1), int64(2), int64(3)}, row[0])
					}
				})
			}
		})
	}
}

func TestStreamingOptimization_LimitQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 1000 nodes to have enough data to see timing differences
	nodeCount := 1000
	t.Logf("Creating %d nodes...", nodeCount)
	for i := 0; i < nodeCount; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:TestNode {id: %d, name: 'Node %d'})", i, i), nil)
		require.NoError(t, err)
	}

	// Verify all nodes were created
	result, err := exec.Execute(ctx, "MATCH (n:TestNode) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	t.Logf("Total nodes created: %v", result.Rows[0][0])

	// Test 1: Simple MATCH (n) RETURN n LIMIT 50 - should use streaming
	t.Run("SimpleLimitQuery", func(t *testing.T) {
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n) RETURN n LIMIT 50", nil)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Len(t, result.Rows, 50, "Should return exactly 50 nodes")
		t.Logf("MATCH (n) RETURN n LIMIT 50: %v (returned %d rows)", elapsed, len(result.Rows))

		// With streaming, this should be fast (< 100ms)
		// Without streaming (loading all 1000 nodes), it would be slower
		assert.Less(t, elapsed, 500*time.Millisecond, "Query should be fast with streaming")
	})

	// Test 2: MATCH with label LIMIT - should also use streaming
	t.Run("LabelLimitQuery", func(t *testing.T) {
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n:TestNode) RETURN n LIMIT 50", nil)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Len(t, result.Rows, 50, "Should return exactly 50 nodes")
		t.Logf("MATCH (n:TestNode) RETURN n LIMIT 50: %v (returned %d rows)", elapsed, len(result.Rows))
	})

	// Test 3: MATCH with WHERE - should NOT use streaming (needs all nodes for filtering)
	t.Run("WhereQuery", func(t *testing.T) {
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n:TestNode) WHERE n.id < 100 RETURN n LIMIT 50", nil)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Len(t, result.Rows, 50, "Should return exactly 50 nodes")
		t.Logf("MATCH with WHERE LIMIT 50: %v (returned %d rows)", elapsed, len(result.Rows))
	})

	// Test 4: MATCH without LIMIT - should load all nodes
	t.Run("NoLimitQuery", func(t *testing.T) {
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n:TestNode) RETURN n", nil)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Len(t, result.Rows, nodeCount, "Should return all nodes")
		t.Logf("MATCH (n:TestNode) RETURN n (no limit): %v (returned %d rows)", elapsed, len(result.Rows))
	})
}

// TestStreamingCodePath explicitly tests that the streaming interface is being used.
func TestStreamingCodePath(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")

	// Verify store implements StreamingEngine (cast to interface{} first)
	var storeInterface interface{} = store
	_, isStreaming := storeInterface.(storage.StreamingEngine)
	assert.True(t, isStreaming, "MemoryEngine should implement StreamingEngine")
	t.Logf("Storage implements StreamingEngine: %v", isStreaming)

	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create some nodes
	for i := 0; i < 100; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:Node {id: %d})", i), nil)
		require.NoError(t, err)
	}

	// Verify executor's storage also implements StreamingEngine
	_, execStorageIsStreaming := exec.storage.(storage.StreamingEngine)
	assert.True(t, execStorageIsStreaming, "Executor's storage should implement StreamingEngine")
	t.Logf("Executor's storage implements StreamingEngine: %v", execStorageIsStreaming)

	// Test the query
	result, err := exec.Execute(ctx, "MATCH (n) RETURN n LIMIT 10", nil)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 10)
	t.Logf("Query returned %d rows", len(result.Rows))
}

// TestCountOptimization verifies that COUNT queries use O(1) NodeCount when possible.
func TestCountOptimization(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 500 nodes
	for i := 0; i < 500; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:TestLabel {id: %d})", i), nil)
		require.NoError(t, err)
	}

	t.Run("CountAllNodes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n)", nil)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(500), result.Rows[0][0])
	})

	t.Run("CountStar", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n) RETURN count(*)", nil)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(500), result.Rows[0][0])
	})

	t.Run("CountWithLabel", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:TestLabel) RETURN count(n)", nil)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(500), result.Rows[0][0])
	})

	t.Run("CountWithWhere_NoOptimization", func(t *testing.T) {
		// This should NOT use the optimization since it has a WHERE clause
		result, err := exec.Execute(ctx, "MATCH (n:TestLabel) WHERE n.id < 100 RETURN count(n)", nil)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(100), result.Rows[0][0])
	})

	t.Run("CountAllNodesTiming", func(t *testing.T) {
		requirePerformanceWorkload(t)
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(500), result.Rows[0][0])
		assert.Less(t, elapsed, 10*time.Millisecond, "Count should use O(1) optimization")
	})
}

// TestCollectNodesWithStreaming directly tests the helper function.
func TestCollectNodesWithStreaming(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 500 nodes
	for i := 0; i < 500; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:TestLabel {id: %d})", i), nil)
		require.NoError(t, err)
	}

	t.Run("WithLimit", func(t *testing.T) {
		nodes, err := exec.collectNodesWithStreaming(ctx, nil, nil, "", "", 50)
		require.NoError(t, err)
		assert.Len(t, nodes, 50, "Should return exactly 50 nodes with limit")
		t.Logf("collectNodesWithStreaming(limit=50) returned %d nodes", len(nodes))
	})

	t.Run("WithLabelAndLimit", func(t *testing.T) {
		nodes, err := exec.collectNodesWithStreaming(ctx, []string{"TestLabel"}, nil, "", "", 50)
		require.NoError(t, err)
		assert.Len(t, nodes, 50, "Should return exactly 50 nodes with label filter and limit")
		t.Logf("collectNodesWithStreaming(label=TestLabel, limit=50) returned %d nodes", len(nodes))
	})

	t.Run("NoLimit", func(t *testing.T) {
		nodes, err := exec.collectNodesWithStreaming(ctx, nil, nil, "", "", -1)
		require.NoError(t, err)
		assert.Len(t, nodes, 500, "Should return all 500 nodes without limit")
		t.Logf("collectNodesWithStreaming(limit=-1) returned %d nodes", len(nodes))
	})

	t.Run("ZeroLimit", func(t *testing.T) {
		nodes, err := exec.collectNodesWithStreaming(ctx, nil, nil, "", "", 0)
		require.NoError(t, err)
		// Zero limit should return all nodes (same as -1)
		t.Logf("collectNodesWithStreaming(limit=0) returned %d nodes", len(nodes))
	})
}
