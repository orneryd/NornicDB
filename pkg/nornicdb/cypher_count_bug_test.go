package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_CypherCountVsStorageCount tests the discrepancy between:
// - MATCH (n) RETURN count(n) (uses storage.NodeCount() fast path)
// - MATCH (n:Label) RETURN count(n) (uses GetNodesByLabel)
// - storage.NodeCount() directly
func TestBug_CypherCountVsStorageCount(t *testing.T) {
	// Create DB with full stack (AsyncEngine + WAL + Badger)
	tmpDir := t.TempDir()
	config := &Config{
		AsyncWritesEnabled: true,
		AsyncFlushInterval: 50 * time.Millisecond,
	}

	db, err := Open(tmpDir, config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create 10 nodes via Cypher
	for i := 0; i < 10; i++ {
		_, err := db.ExecuteCypher(ctx, "CREATE (n:TestNode {idx: $idx})", map[string]interface{}{"idx": i})
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Check storage.NodeCount()
	storageCount, err := db.storage.NodeCount()
	require.NoError(t, err)
	t.Logf("storage.NodeCount(): %d", storageCount)

	// Check db.Stats().NodeCount
	stats := db.Stats()
	t.Logf("db.Stats().NodeCount: %d", stats.NodeCount)

	// Check MATCH (n:TestNode) RETURN count(n) - uses GetNodesByLabel
	result, err := db.ExecuteCypher(ctx, "MATCH (n:TestNode) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	labelCount := result.Rows[0][0]
	t.Logf("MATCH (n:TestNode) RETURN count(n): %v", labelCount)

	// Check MATCH (n) RETURN count(n) - uses NodeCount() fast path
	result, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	allCount := result.Rows[0][0]
	t.Logf("MATCH (n) RETURN count(n): %v", allCount)

	// All should be 10
	assert.Equal(t, int64(10), storageCount, "storage.NodeCount() should be 10")
	assert.Equal(t, int64(10), stats.NodeCount, "db.Stats().NodeCount should be 10")
	assert.Equal(t, int64(10), labelCount, "MATCH (n:TestNode) count should be 10")
	assert.Equal(t, int64(10), allCount, "MATCH (n) count should be 10")
}

// TestBug_CypherCountAfterDeleteRecreate tests the full flow
func TestBug_CypherCountAfterDeleteRecreate(t *testing.T) {
	tmpDir := t.TempDir()
	config := &Config{
		AsyncWritesEnabled: true,
		AsyncFlushInterval: 50 * time.Millisecond,
	}

	db, err := Open(tmpDir, config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Step 1: Create initial nodes
	for i := 0; i < 10; i++ {
		_, err := db.ExecuteCypher(ctx, "CREATE (n:Cycle {idx: $idx})", map[string]interface{}{"idx": i})
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Verify initial count
	result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("After CREATE: MATCH (n) RETURN count(n) = %v", result.Rows[0][0])
	assert.Equal(t, int64(10), result.Rows[0][0])

	// Step 2: Delete all
	_, err = db.ExecuteCypher(ctx, "MATCH (n:Cycle) DELETE n", nil)
	require.NoError(t, err)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Verify delete
	result, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("After DELETE: MATCH (n) RETURN count(n) = %v", result.Rows[0][0])
	assert.Equal(t, int64(0), result.Rows[0][0])

	// Step 3: Create NEW nodes
	for i := 0; i < 5; i++ {
		_, err := db.ExecuteCypher(ctx, "CREATE (n:NewNode {idx: $idx, recreated: true})", map[string]interface{}{"idx": i})
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Check all count methods
	storageCount, _ := db.storage.NodeCount()
	t.Logf("After RECREATE: storage.NodeCount() = %d", storageCount)

	result, err = db.ExecuteCypher(ctx, "MATCH (n:NewNode) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("After RECREATE: MATCH (n:NewNode) RETURN count(n) = %v", result.Rows[0][0])

	result, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("After RECREATE: MATCH (n) RETURN count(n) = %v", result.Rows[0][0])

	// All should be 5
	assert.Equal(t, int64(5), storageCount)
	assert.Equal(t, int64(5), result.Rows[0][0], "MATCH (n) count should be 5 after recreate")
}
