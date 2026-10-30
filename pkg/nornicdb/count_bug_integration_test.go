package nornicdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_CountAfterDeleteRecreate_FullStack tests the node count bug using
// the full production stack initialized through db.Open()
func TestBug_CountAfterDeleteRecreate_FullStack(t *testing.T) {
	ctx := context.Background()

	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()

	// Get initial stats
	initialStats := db.Stats()
	t.Logf("Initial: nodes=%d, edges=%d", initialStats.NodeCount, initialStats.EdgeCount)

	// Create nodes via Cypher
	t.Log("Creating 5 nodes via Cypher...")
	for i := 0; i < 5; i++ {
		_, err := db.ExecuteCypher(ctx, "CREATE (n:BugTest {idx: $idx})", map[string]any{"idx": i})
		require.NoError(t, err)
	}

	stats1 := db.Stats()
	t.Logf("After CREATE: nodes=%d, edges=%d", stats1.NodeCount, stats1.EdgeCount)
	assert.Equal(t, initialStats.NodeCount+5, stats1.NodeCount, "Should have 5 more nodes")

	// Query to verify nodes exist
	result, err := db.ExecuteCypher(ctx, "MATCH (n:BugTest) RETURN n", nil)
	require.NoError(t, err)
	t.Logf("MATCH returned %d rows", len(result.Rows))
	assert.Len(t, result.Rows, 5)

	// Test count()
	result, err = db.ExecuteCypher(ctx, "MATCH (n:BugTest) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	t.Logf("count(n) returned: %v", result.Rows[0][0])

	// Delete all nodes
	t.Log("Deleting all nodes...")
	_, err = db.ExecuteCypher(ctx, "MATCH (n:BugTest) DELETE n", nil)
	require.NoError(t, err)

	stats2 := db.Stats()
	t.Logf("After DELETE: nodes=%d, edges=%d", stats2.NodeCount, stats2.EdgeCount)
	assert.Equal(t, initialStats.NodeCount, stats2.NodeCount, "Should be back to initial count")

	// Recreate nodes
	t.Log("Recreating 3 nodes...")
	for i := 0; i < 3; i++ {
		_, err := db.ExecuteCypher(ctx, "CREATE (n:BugTest {idx: $idx, recreated: true})", map[string]any{"idx": i + 100})
		require.NoError(t, err)
	}

	stats3 := db.Stats()
	t.Logf("After RECREATE: nodes=%d, edges=%d", stats3.NodeCount, stats3.EdgeCount)
	assert.Equal(t, initialStats.NodeCount+3, stats3.NodeCount, "Should have 3 more nodes after recreate")

	// Verify with MATCH
	result, err = db.ExecuteCypher(ctx, "MATCH (n:BugTest) RETURN n", nil)
	require.NoError(t, err)
	t.Logf("MATCH after recreate returned %d rows", len(result.Rows))
	assert.Len(t, result.Rows, 3, "Should have 3 nodes after recreate")
}
