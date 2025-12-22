package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_CountReturnsZeroWhenNodesExist reproduces the bug where:
// - MATCH (n) RETURN n returns actual nodes
// - MATCH (n) RETURN count(n) returns 0
// - Storage NodeCount() returns 0
// But nodes clearly exist in the database.
func TestBug_CountReturnsZeroWhenNodesExist(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 5 nodes
	for i := 0; i < 5; i++ {
		_, err := exec.Execute(ctx, "CREATE (n:TestNode {idx: $idx})", map[string]any{"idx": i})
		require.NoError(t, err)
	}

	// Verify nodes exist by returning them
	result, err := exec.Execute(ctx, "MATCH (n:TestNode) RETURN n", nil)
	require.NoError(t, err)
	actualNodeCount := len(result.Rows)
	t.Logf("MATCH (n) RETURN n returned %d rows", actualNodeCount)
	assert.Equal(t, 5, actualNodeCount, "Should return 5 nodes")

	// Now test count(n)
	result, err = exec.Execute(ctx, "MATCH (n:TestNode) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	countN := result.Rows[0][0]
	t.Logf("MATCH (n) RETURN count(n) returned: %v (type: %T)", countN, countN)
	
	// Convert to int64 for comparison
	var countNInt int64
	switch v := countN.(type) {
	case int64:
		countNInt = v
	case int:
		countNInt = int64(v)
	case float64:
		countNInt = int64(v)
	}
	assert.Equal(t, int64(5), countNInt, "count(n) should return 5")

	// Test count(*)
	result, err = exec.Execute(ctx, "MATCH (n:TestNode) RETURN count(*) as cnt", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	countStar := result.Rows[0][0]
	t.Logf("MATCH (n) RETURN count(*) returned: %v (type: %T)", countStar, countStar)
	
	var countStarInt int64
	switch v := countStar.(type) {
	case int64:
		countStarInt = v
	case int:
		countStarInt = int64(v)
	case float64:
		countStarInt = int64(v)
	}
	assert.Equal(t, int64(5), countStarInt, "count(*) should return 5")

	// Test storage layer NodeCount
	nodeCount, err := store.NodeCount()
	require.NoError(t, err)
	t.Logf("Storage NodeCount() returned: %d", nodeCount)
	assert.Equal(t, int64(5), nodeCount, "Storage NodeCount should return 5")
}

// TestBug_CountAfterDeleteAndRecreate tests count behavior after delete/recreate cycle
func TestBug_CountAfterDeleteAndRecreate(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial nodes
	for i := 0; i < 3; i++ {
		_, err := exec.Execute(ctx, "CREATE (n:CycleTest {idx: $idx})", map[string]any{"idx": i})
		require.NoError(t, err)
	}

	// Verify count
	nodeCount, _ := store.NodeCount()
	t.Logf("After initial create: NodeCount = %d", nodeCount)
	assert.Equal(t, int64(3), nodeCount)

	// Delete all nodes
	_, err := exec.Execute(ctx, "MATCH (n:CycleTest) DELETE n", nil)
	require.NoError(t, err)

	nodeCount, _ = store.NodeCount()
	t.Logf("After delete: NodeCount = %d", nodeCount)
	assert.Equal(t, int64(0), nodeCount)

	// Recreate nodes
	for i := 0; i < 5; i++ {
		_, err := exec.Execute(ctx, "CREATE (n:CycleTest {idx: $idx})", map[string]any{"idx": i})
		require.NoError(t, err)
	}

	nodeCount, _ = store.NodeCount()
	t.Logf("After recreate: NodeCount = %d", nodeCount)
	assert.Equal(t, int64(5), nodeCount, "NodeCount should be 5 after recreating nodes")

	// Verify with Cypher count
	result, err := exec.Execute(ctx, "MATCH (n:CycleTest) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	cypherCount := result.Rows[0][0]
	t.Logf("Cypher count(n) after recreate: %v", cypherCount)
}
