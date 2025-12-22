package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_CountDirectReturnVsWithReturn reproduces the bug where:
// - MATCH (n) RETURN count(n) returns 0 (WRONG)
// - MATCH (n) WITH n RETURN count(n) returns correct count (CORRECT)
func TestBug_CountDirectReturnVsWithReturn(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 10 nodes
	for i := 0; i < 10; i++ {
		_, err := exec.Execute(ctx, "CREATE (n:CountTest {idx: $idx})", map[string]any{"idx": i})
		require.NoError(t, err)
	}

	// Verify nodes exist
	result, err := exec.Execute(ctx, "MATCH (n:CountTest) RETURN n", nil)
	require.NoError(t, err)
	t.Logf("MATCH (n) RETURN n: %d rows", len(result.Rows))
	require.Len(t, result.Rows, 10, "Should have 10 nodes")

	// Test 1: Direct count in RETURN (this is the broken query)
	t.Run("direct_count_in_return", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:CountTest) RETURN count(n) as cnt", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		cnt := result.Rows[0][0]
		t.Logf("MATCH (n) RETURN count(n): %v (type: %T)", cnt, cnt)

		var countVal int64
		switch v := cnt.(type) {
		case int64:
			countVal = v
		case int:
			countVal = int64(v)
		case float64:
			countVal = int64(v)
		}
		assert.Equal(t, int64(10), countVal, "MATCH (n) RETURN count(n) should return 10")
	})

	// Test 2: Count with WITH clause (this works correctly)
	t.Run("count_with_with_clause", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:CountTest) WITH n RETURN count(n) as cnt", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		cnt := result.Rows[0][0]
		t.Logf("MATCH (n) WITH n RETURN count(n): %v (type: %T)", cnt, cnt)

		var countVal int64
		switch v := cnt.(type) {
		case int64:
			countVal = v
		case int:
			countVal = int64(v)
		case float64:
			countVal = int64(v)
		}
		assert.Equal(t, int64(10), countVal, "MATCH (n) WITH n RETURN count(n) should return 10")
	})

	// Test 3: count(*) direct
	t.Run("count_star_direct", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:CountTest) RETURN count(*) as cnt", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		cnt := result.Rows[0][0]
		t.Logf("MATCH (n) RETURN count(*): %v (type: %T)", cnt, cnt)

		var countVal int64
		switch v := cnt.(type) {
		case int64:
			countVal = v
		case int:
			countVal = int64(v)
		case float64:
			countVal = int64(v)
		}
		assert.Equal(t, int64(10), countVal, "MATCH (n) RETURN count(*) should return 10")
	})

	// Test 4: count without label filter
	t.Run("count_all_nodes_direct", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)

		cnt := result.Rows[0][0]
		t.Logf("MATCH (n) RETURN count(n) [all nodes]: %v (type: %T)", cnt, cnt)

		var countVal int64
		switch v := cnt.(type) {
		case int64:
			countVal = v
		case int:
			countVal = int64(v)
		case float64:
			countVal = int64(v)
		}
		assert.Equal(t, int64(10), countVal, "MATCH (n) RETURN count(n) [all nodes] should return 10")
	})
}

// TestBug_CountAfterDeleteRecreate tests count behavior after delete/recreate
func TestBug_CountAfterDeleteRecreate(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create initial nodes
	for i := 0; i < 5; i++ {
		_, err := exec.Execute(ctx, "CREATE (n:Cycle {idx: $idx})", map[string]any{"idx": i})
		require.NoError(t, err)
	}

	// Verify initial count
	result, err := exec.Execute(ctx, "MATCH (n:Cycle) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("Initial count: %v", result.Rows[0][0])

	// Delete all
	_, err = exec.Execute(ctx, "MATCH (n:Cycle) DELETE n", nil)
	require.NoError(t, err)

	// Verify delete count
	result, err = exec.Execute(ctx, "MATCH (n:Cycle) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("After delete count: %v", result.Rows[0][0])

	// Recreate
	for i := 0; i < 3; i++ {
		_, err := exec.Execute(ctx, "CREATE (n:Cycle {idx: $idx, recreated: true})", map[string]any{"idx": i + 100})
		require.NoError(t, err)
	}

	// Verify recreate count
	result, err = exec.Execute(ctx, "MATCH (n:Cycle) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	cnt := result.Rows[0][0]
	t.Logf("After recreate count: %v", cnt)

	var countVal int64
	switch v := cnt.(type) {
	case int64:
		countVal = v
	case int:
		countVal = int64(v)
	case float64:
		countVal = int64(v)
	}
	assert.Equal(t, int64(3), countVal, "Count after recreate should be 3")
}
