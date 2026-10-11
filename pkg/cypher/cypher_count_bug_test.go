package cypher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCypher_CountAfterDeleteRecreate tests the full Cypher flow
// to reproduce the bug where count returns 0 after delete+recreate.
func TestCypher_CountAfterDeleteRecreate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cypher_count_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create full stack: BadgerEngine -> WALEngine -> AsyncEngine
	badger, err := storage.NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	wal, err := storage.NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)
	defer wal.Close()

	walEngine := storage.NewWALEngine(badger, wal)
	asyncEngine := walEngine
	defer asyncEngine.Close()

	// Create Cypher executor
	store := storage.NewNamespacedEngine(asyncEngine, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Step 1: Create nodes via Cypher
	for i := 0; i < 10; i++ {
		query := fmt.Sprintf(`CREATE (n:Test {name: "Node %d"})`, i)
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	// Check count via Cypher
	result1, err := exec.Execute(ctx, `MATCH (n) RETURN count(n) as cnt`, nil)
	require.NoError(t, err)
	require.Len(t, result1.Rows, 1)
	cnt1, _ := result1.Rows[0][0].(int64)
	t.Logf("Count after CREATE: %d", cnt1)
	assert.Equal(t, int64(10), cnt1, "Should have 10 nodes after CREATE")

	// Step 2: Delete all via Cypher
	_, err = exec.Execute(ctx, `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err)

	result2, err := exec.Execute(ctx, `MATCH (n) RETURN count(n) as cnt`, nil)
	require.NoError(t, err)
	require.Len(t, result2.Rows, 1)
	cnt2, _ := result2.Rows[0][0].(int64)
	t.Logf("Count after DELETE: %d", cnt2)
	assert.Equal(t, int64(0), cnt2, "Should have 0 nodes after DELETE")

	// Step 3: Recreate nodes via MERGE (like the import script does)
	for i := 0; i < 5; i++ {
		query := fmt.Sprintf(`MERGE (n:NewTest {name: "New Node %d"})`, i)
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	result3, err := exec.Execute(ctx, `MATCH (n) RETURN count(n) as cnt`, nil)
	require.NoError(t, err)
	require.Len(t, result3.Rows, 1)
	cnt3, _ := result3.Rows[0][0].(int64)
	t.Logf("Count after MERGE: %d", cnt3)
	assert.Equal(t, int64(5), cnt3, "Should have 5 nodes after MERGE")

	// Also check storage layer directly
	storageCount, _ := asyncEngine.NodeCount()
	t.Logf("Storage NodeCount: %d", storageCount)
	assert.Equal(t, int64(5), storageCount, "Storage should also report 5 nodes")
}

// TestCypher_CountVsMatchCount compares count(n) vs MATCH (n) RETURN count(n)
// to see if the fast path is returning different results
func TestCypher_CountVsMatchCount(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cypher_count_test2")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	badger, err := storage.NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	wal, err := storage.NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)
	defer wal.Close()

	walEngine := storage.NewWALEngine(badger, wal)
	asyncEngine := walEngine
	defer asyncEngine.Close()

	store := storage.NewNamespacedEngine(asyncEngine, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 0; i < 10; i++ {
		query := fmt.Sprintf(`CREATE (n:Test {name: "Node %d"})`, i)
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	// Delete all
	_, err = exec.Execute(ctx, `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err)

	// Recreate
	for i := 0; i < 5; i++ {
		query := fmt.Sprintf(`MERGE (n:NewTest {name: "New Node %d"})`, i)
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}

	// Compare different count methods
	result1, _ := exec.Execute(ctx, `MATCH (n) RETURN count(n) as cnt`, nil)
	cnt1, _ := result1.Rows[0][0].(int64)
	t.Logf("MATCH (n) RETURN count(n): %d", cnt1)

	result2, _ := exec.Execute(ctx, `MATCH (n:NewTest) RETURN count(n) as cnt`, nil)
	cnt2, _ := result2.Rows[0][0].(int64)
	t.Logf("MATCH (n:NewTest) RETURN count(n): %d", cnt2)

	storageCount, _ := asyncEngine.NodeCount()
	t.Logf("Storage NodeCount(): %d", storageCount)

	assert.Equal(t, int64(5), cnt1, "MATCH (n) count should be 5")
	assert.Equal(t, int64(5), cnt2, "MATCH (n:NewTest) count should be 5")
	assert.Equal(t, int64(5), storageCount, "Storage count should be 5")
}
