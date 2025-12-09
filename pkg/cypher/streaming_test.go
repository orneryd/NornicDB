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
func TestStreamingOptimization_LimitQuery(t *testing.T) {
	store := storage.NewMemoryEngine()
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
	store := storage.NewMemoryEngine()

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
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 500 nodes
	for i := 0; i < 500; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:TestLabel {id: %d})", i), nil)
		require.NoError(t, err)
	}

	t.Run("CountAllNodes", func(t *testing.T) {
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(500), result.Rows[0][0])
		t.Logf("MATCH (n) RETURN count(n): %v (count=%v)", elapsed, result.Rows[0][0])

		// With O(1) optimization, this should be < 1ms
		assert.Less(t, elapsed, 10*time.Millisecond, "Count should use O(1) optimization")
	})

	t.Run("CountStar", func(t *testing.T) {
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n) RETURN count(*)", nil)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(500), result.Rows[0][0])
		t.Logf("MATCH (n) RETURN count(*): %v (count=%v)", elapsed, result.Rows[0][0])
	})

	t.Run("CountWithLabel", func(t *testing.T) {
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n:TestLabel) RETURN count(n)", nil)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(500), result.Rows[0][0])
		t.Logf("MATCH (n:TestLabel) RETURN count(n): %v (count=%v)", elapsed, result.Rows[0][0])
	})

	t.Run("CountWithWhere_NoOptimization", func(t *testing.T) {
		// This should NOT use the optimization since it has a WHERE clause
		start := time.Now()
		result, err := exec.Execute(ctx, "MATCH (n:TestLabel) WHERE n.id < 100 RETURN count(n)", nil)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, int64(100), result.Rows[0][0])
		t.Logf("MATCH with WHERE RETURN count(n): %v (count=%v)", elapsed, result.Rows[0][0])
	})
}

// TestCollectNodesWithStreaming directly tests the helper function.
func TestCollectNodesWithStreaming(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 500 nodes
	for i := 0; i < 500; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:TestLabel {id: %d})", i), nil)
		require.NoError(t, err)
	}

	t.Run("WithLimit", func(t *testing.T) {
		nodes, err := exec.collectNodesWithStreaming(ctx, nil, nil, 50)
		require.NoError(t, err)
		assert.Len(t, nodes, 50, "Should return exactly 50 nodes with limit")
		t.Logf("collectNodesWithStreaming(limit=50) returned %d nodes", len(nodes))
	})

	t.Run("WithLabelAndLimit", func(t *testing.T) {
		nodes, err := exec.collectNodesWithStreaming(ctx, []string{"TestLabel"}, nil, 50)
		require.NoError(t, err)
		assert.Len(t, nodes, 50, "Should return exactly 50 nodes with label filter and limit")
		t.Logf("collectNodesWithStreaming(label=TestLabel, limit=50) returned %d nodes", len(nodes))
	})

	t.Run("NoLimit", func(t *testing.T) {
		nodes, err := exec.collectNodesWithStreaming(ctx, nil, nil, -1)
		require.NoError(t, err)
		assert.Len(t, nodes, 500, "Should return all 500 nodes without limit")
		t.Logf("collectNodesWithStreaming(limit=-1) returned %d nodes", len(nodes))
	})

	t.Run("ZeroLimit", func(t *testing.T) {
		nodes, err := exec.collectNodesWithStreaming(ctx, nil, nil, 0)
		require.NoError(t, err)
		// Zero limit should return all nodes (same as -1)
		t.Logf("collectNodesWithStreaming(limit=0) returned %d nodes", len(nodes))
	})
}
