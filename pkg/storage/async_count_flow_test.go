package storage

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_CountAfterDeleteRecreate_AsyncEngine tests the exact user flow:
// 1. Create nodes
// 2. Flush
// 3. Delete all nodes
// 4. Flush
// 5. Recreate new nodes with new IDs
// 6. Check count - should be > 0
func TestBug_CountAfterDeleteRecreate_AsyncEngine(t *testing.T) {
	// Create temp directory for BadgerDB
	tmpDir := t.TempDir()

	// Initialize BadgerEngine
	badgerEngine, err := NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badgerEngine.Close()

	// Wrap with NamespacedEngine so AsyncEngine can accept unprefixed IDs.
	namespaced := NewNamespacedEngine(badgerEngine, "test")

	// Wrap with AsyncEngine (skip WAL for simpler test)
	asyncConfig := &AsyncEngineConfig{
		FlushInterval: 100 * time.Millisecond,
	}
	asyncEngine := NewAsyncEngine(namespaced, asyncConfig)
	defer asyncEngine.Close()

	// Step 1: Create 10 nodes
	nodeIDs := make([]NodeID, 10)
	for i := 0; i < 10; i++ {
		nodeID := NodeID(uuid.New().String())
		nodeIDs[i] = nodeID
		node := &Node{
			ID:     nodeID,
			Labels: []string{"TestNode"},
			Properties: map[string]interface{}{
				"idx": i,
			},
		}
		_, err := asyncEngine.CreateNode(node)
		require.NoError(t, err)
	}

	// Check count before flush
	count, err := asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After CREATE (before flush): count = %d", count)
	assert.Equal(t, int64(10), count, "Count should be 10 before flush (pending creates)")

	// Step 2: Flush
	require.NoError(t, asyncEngine.Flush())
	time.Sleep(50 * time.Millisecond) // Allow flush to complete

	// Check count after flush
	count, err = asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After first FLUSH: count = %d", count)
	assert.Equal(t, int64(10), count, "Count should be 10 after flush")

	// Verify BadgerEngine count
	badgerCount, err := badgerEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("BadgerEngine count after flush: %d", badgerCount)
	assert.Equal(t, int64(10), badgerCount, "BadgerEngine count should be 10")

	// Step 3: Delete all nodes
	for _, nodeID := range nodeIDs {
		err := asyncEngine.DeleteNode(nodeID)
		require.NoError(t, err)
	}

	// Check count after delete (before flush)
	count, err = asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After DELETE (before flush): count = %d", count)
	assert.Equal(t, int64(0), count, "Count should be 0 after delete")

	// Step 4: Flush deletes
	require.NoError(t, asyncEngine.Flush())
	time.Sleep(50 * time.Millisecond)

	// Check count after delete flush
	count, err = asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After delete FLUSH: count = %d", count)
	assert.Equal(t, int64(0), count, "Count should be 0 after delete flush")

	// Verify BadgerEngine count is 0
	badgerCount, err = badgerEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("BadgerEngine count after delete flush: %d", badgerCount)
	assert.Equal(t, int64(0), badgerCount, "BadgerEngine count should be 0")

	// Step 5: Create NEW nodes with NEW IDs
	newNodeIDs := make([]NodeID, 5)
	for i := 0; i < 5; i++ {
		nodeID := NodeID(uuid.New().String())
		newNodeIDs[i] = nodeID
		node := &Node{
			ID:     nodeID,
			Labels: []string{"NewNode"},
			Properties: map[string]interface{}{
				"idx":       i,
				"recreated": true,
			},
		}
		_, err := asyncEngine.CreateNode(node)
		require.NoError(t, err)
	}

	// Step 6: Check count - THIS IS THE BUG
	count, err = asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After RECREATE (before flush): count = %d", count)
	assert.Equal(t, int64(5), count, "Count should be 5 after recreate")

	// Flush and check again
	require.NoError(t, asyncEngine.Flush())
	time.Sleep(50 * time.Millisecond)

	count, err = asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After recreate FLUSH: count = %d", count)
	assert.Equal(t, int64(5), count, "Count should be 5 after recreate flush")
}

// TestBug_CountWithWAL tests with the full WAL stack
func TestBug_CountWithWAL_AsyncEngine(t *testing.T) {
	tmpDir := t.TempDir()

	// Initialize BadgerEngine
	badgerEngine, err := NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badgerEngine.Close()

	// Add WAL layer
	walDir := tmpDir + "/wal"
	walConfig := &WALConfig{
		Dir:      walDir,
		SyncMode: "immediate",
	}
	wal, err := NewWAL("", walConfig)
	require.NoError(t, err)
	walEngine := NewWALEngine(badgerEngine, wal)
	defer walEngine.Close()

	// Wrap with NamespacedEngine so AsyncEngine can accept unprefixed IDs.
	namespaced := NewNamespacedEngine(walEngine, "test")

	// Add AsyncEngine
	asyncConfig := &AsyncEngineConfig{
		FlushInterval: 100 * time.Millisecond,
	}
	asyncEngine := NewAsyncEngine(namespaced, asyncConfig)
	defer asyncEngine.Close()

	// Create 10 nodes
	nodeIDs := make([]NodeID, 10)
	for i := 0; i < 10; i++ {
		nodeID := NodeID(uuid.New().String())
		nodeIDs[i] = nodeID
		node := &Node{
			ID:     nodeID,
			Labels: []string{"TestNode"},
			Properties: map[string]interface{}{
				"idx": i,
			},
		}
		_, err := asyncEngine.CreateNode(node)
		require.NoError(t, err)
	}

	// Check count before flush
	count, err := asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After CREATE (before flush): count = %d", count)
	assert.Equal(t, int64(10), count)

	// Flush
	require.NoError(t, asyncEngine.Flush())
	time.Sleep(50 * time.Millisecond)

	// Delete all
	for _, nodeID := range nodeIDs {
		err := asyncEngine.DeleteNode(nodeID)
		require.NoError(t, err)
	}

	// Flush deletes
	require.NoError(t, asyncEngine.Flush())
	time.Sleep(50 * time.Millisecond)

	count, err = asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After DELETE + FLUSH: count = %d", count)
	assert.Equal(t, int64(0), count)

	// Create NEW nodes
	for i := 0; i < 5; i++ {
		nodeID := NodeID(uuid.New().String())
		node := &Node{
			ID:     nodeID,
			Labels: []string{"NewNode"},
			Properties: map[string]interface{}{
				"idx": i,
			},
		}
		_, err := asyncEngine.CreateNode(node)
		require.NoError(t, err)
	}

	// Check count - should be 5
	count, err = asyncEngine.NodeCount()
	require.NoError(t, err)
	t.Logf("After RECREATE: count = %d", count)
	assert.Equal(t, int64(5), count, "Count should be 5 after recreate")
}
