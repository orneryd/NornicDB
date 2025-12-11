package nornicdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_BulkImportCount tests the exact pattern used by Neo4j Python driver imports:
// 1. Clear database
// 2. Bulk CREATE nodes
// 3. Check count via MATCH (n) RETURN count(n)
// 4. Check count via storage.NodeCount()
// 5. Check count via metrics/stats
func TestBug_BulkImportCount(t *testing.T) {
	tmpDir := t.TempDir()
	config := &Config{
		AsyncWritesEnabled: true,
		AsyncFlushInterval: 50 * time.Millisecond,
	}

	db, err := Open(tmpDir, config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Step 1: Clear database (like Python script does first)
	_, err = db.ExecuteCypher(ctx, "MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Step 2: Verify database is empty
	result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("After clear: MATCH (n) RETURN count(n) = %v", result.Rows[0][0])
	assert.Equal(t, int64(0), result.Rows[0][0])

	// Step 3: Bulk CREATE nodes (simulating Python import)
	// Create 234 nodes like the Python script does
	nodeCount := 234
	for i := 0; i < nodeCount; i++ {
		query := fmt.Sprintf("CREATE (n:POC:Document {id: 'node-%d', name: 'Node %d'})", i, i)
		_, err := db.ExecuteCypher(ctx, query, nil)
		require.NoError(t, err)
	}

	// Wait for async writes to flush
	time.Sleep(200 * time.Millisecond)

	// Step 4: Check counts via different methods
	
	// Method 1: storage.NodeCount()
	storageCount, err := db.storage.NodeCount()
	require.NoError(t, err)
	t.Logf("storage.NodeCount(): %d", storageCount)

	// Method 2: db.Stats().NodeCount
	stats := db.Stats()
	t.Logf("db.Stats().NodeCount: %d", stats.NodeCount)

	// Method 3: MATCH (n:POC) RETURN count(n) - uses GetNodesByLabel
	result, err = db.ExecuteCypher(ctx, "MATCH (n:POC) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	labelCount := result.Rows[0][0]
	t.Logf("MATCH (n:POC) RETURN count(n): %v", labelCount)

	// Method 4: MATCH (n) RETURN count(n) - uses NodeCount() fast path
	result, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	allCount := result.Rows[0][0]
	t.Logf("MATCH (n) RETURN count(n): %v", allCount)

	// Method 5: Query actual nodes
	result, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN n LIMIT 5", nil)
	require.NoError(t, err)
	t.Logf("MATCH (n) RETURN n LIMIT 5: %d rows", len(result.Rows))

	// All should be 234
	assert.Equal(t, int64(nodeCount), storageCount, "storage.NodeCount() should be %d", nodeCount)
	assert.Equal(t, int64(nodeCount), stats.NodeCount, "db.Stats().NodeCount should be %d", nodeCount)
	assert.Equal(t, int64(nodeCount), labelCount, "MATCH (n:POC) count should be %d", nodeCount)
	assert.Equal(t, int64(nodeCount), allCount, "MATCH (n) count should be %d", nodeCount)
}

// TestBug_ClearAndImport tests clearing database then importing
func TestBug_ClearAndImport(t *testing.T) {
	tmpDir := t.TempDir()
	config := &Config{
		AsyncWritesEnabled: true,
		AsyncFlushInterval: 50 * time.Millisecond,
	}

	db, err := Open(tmpDir, config)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Create some initial data
	for i := 0; i < 50; i++ {
		_, err := db.ExecuteCypher(ctx, fmt.Sprintf("CREATE (n:Initial {id: %d})", i), nil)
		require.NoError(t, err)
	}
	time.Sleep(100 * time.Millisecond)

	result, err := db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("Initial count: %v", result.Rows[0][0])
	assert.Equal(t, int64(50), result.Rows[0][0])

	// Clear database
	_, err = db.ExecuteCypher(ctx, "MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	result, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("After clear: %v", result.Rows[0][0])
	assert.Equal(t, int64(0), result.Rows[0][0])

	// Import new data
	for i := 0; i < 75; i++ {
		_, err := db.ExecuteCypher(ctx, fmt.Sprintf("CREATE (n:NewData {id: %d})", i), nil)
		require.NoError(t, err)
	}
	time.Sleep(100 * time.Millisecond)

	// Check count
	result, err = db.ExecuteCypher(ctx, "MATCH (n) RETURN count(n) as cnt", nil)
	require.NoError(t, err)
	t.Logf("After new import: %v", result.Rows[0][0])
	assert.Equal(t, int64(75), result.Rows[0][0])

	// Verify storage count matches
	storageCount, _ := db.storage.NodeCount()
	t.Logf("storage.NodeCount(): %d", storageCount)
	assert.Equal(t, int64(75), storageCount)
}
