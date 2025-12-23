package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBug_AsyncEngineNodeCountAfterDeleteRecreate reproduces the bug where:
// - Delete all nodes, count goes to 0
// - Recreate nodes, count stays at 0 or wrong value
// Uses AsyncEngine -> BadgerEngine stack (no WAL for simplicity)
func TestBug_AsyncEngineNodeCountAfterDeleteRecreate(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// Create BadgerEngine
	badger, err := NewBadgerEngine(dataDir)
	require.NoError(t, err)
	defer badger.Close()

	// Wrap Badger with NamespacedEngine so AsyncEngine can accept unprefixed IDs.
	namespaced := NewNamespacedEngine(badger, "test")

	// Wrap with AsyncEngine (skip WAL for this test)
	async := NewAsyncEngine(namespaced, &AsyncEngineConfig{
		FlushInterval: 10 * time.Millisecond,
	})
	defer async.Close()

	// Create initial nodes
	t.Log("Creating 5 initial nodes...")
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:         NodeID(string(rune('a' + i))),
			Labels:     []string{"TestNode"},
			Properties: map[string]any{"idx": i},
		}
		_, err := async.CreateNode(node)
		require.NoError(t, err)
	}

	// Check count before flush
	countBeforeFlush, _ := async.NodeCount()
	t.Logf("Count before flush: %d", countBeforeFlush)
	assert.Equal(t, int64(5), countBeforeFlush, "Should show 5 pending creates")

	// Flush
	err = async.Flush()
	require.NoError(t, err)

	countAfterFlush, _ := async.NodeCount()
	t.Logf("Count after flush: %d", countAfterFlush)
	assert.Equal(t, int64(5), countAfterFlush, "Should show 5 nodes after flush")

	// Verify BadgerEngine count
	badgerCount, _ := badger.NodeCount()
	t.Logf("BadgerEngine count: %d", badgerCount)
	assert.Equal(t, int64(5), badgerCount)

	// Delete all nodes
	t.Log("Deleting all nodes...")
	for i := 0; i < 5; i++ {
		err := async.DeleteNode(NodeID(string(rune('a' + i))))
		require.NoError(t, err)
	}

	countAfterDelete, _ := async.NodeCount()
	t.Logf("Count after delete (before flush): %d", countAfterDelete)
	assert.Equal(t, int64(0), countAfterDelete, "Should show 0 after delete")

	// Flush deletes
	err = async.Flush()
	require.NoError(t, err)

	countAfterDeleteFlush, _ := async.NodeCount()
	t.Logf("Count after delete flush: %d", countAfterDeleteFlush)
	assert.Equal(t, int64(0), countAfterDeleteFlush)

	badgerCountAfterDelete, _ := badger.NodeCount()
	t.Logf("BadgerEngine count after delete: %d", badgerCountAfterDelete)
	assert.Equal(t, int64(0), badgerCountAfterDelete)

	// NOW RECREATE - this is where the bug might occur
	t.Log("Recreating 3 nodes with NEW IDs...")
	for i := 0; i < 3; i++ {
		node := &Node{
			ID:         NodeID(string(rune('x' + i))), // New IDs: x, y, z
			Labels:     []string{"TestNode"},
			Properties: map[string]any{"idx": i + 100},
		}
		_, err := async.CreateNode(node)
		require.NoError(t, err)
	}

	countAfterRecreate, _ := async.NodeCount()
	t.Logf("Count after recreate (before flush): %d", countAfterRecreate)
	assert.Equal(t, int64(3), countAfterRecreate, "Should show 3 pending creates")

	// Flush recreated nodes
	err = async.Flush()
	require.NoError(t, err)

	countAfterRecreateFlush, _ := async.NodeCount()
	t.Logf("Count after recreate flush: %d", countAfterRecreateFlush)
	assert.Equal(t, int64(3), countAfterRecreateFlush, "Should show 3 nodes after recreate")

	badgerCountAfterRecreate, _ := badger.NodeCount()
	t.Logf("BadgerEngine count after recreate: %d", badgerCountAfterRecreate)
	assert.Equal(t, int64(3), badgerCountAfterRecreate, "BadgerEngine should have 3 nodes")
}

// TestBug_AsyncEngineNodeCountWithSameIDs tests recreating nodes with SAME IDs after delete
func TestBug_AsyncEngineNodeCountWithSameIDs(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	badger, err := NewBadgerEngine(dataDir)
	require.NoError(t, err)
	defer badger.Close()

	// Use AsyncEngine directly with BadgerEngine (skip WAL for simpler test)
	async := NewAsyncEngine(badger, &AsyncEngineConfig{
		FlushInterval: 10 * time.Millisecond,
	})
	defer async.Close()

	nodeIDs := []NodeID{NodeID(prefixTestID("node-1")), NodeID(prefixTestID("node-2")), NodeID(prefixTestID("node-3"))}

	// Create nodes
	t.Log("Creating 3 nodes...")
	for _, id := range nodeIDs {
		_, err := async.CreateNode(&Node{ID: id, Labels: []string{"Test"}})
		require.NoError(t, err)
	}
	async.Flush()

	count1, _ := async.NodeCount()
	t.Logf("After create: %d", count1)
	assert.Equal(t, int64(3), count1)

	// Delete nodes
	t.Log("Deleting all nodes...")
	for _, id := range nodeIDs {
		async.DeleteNode(id)
	}
	async.Flush()

	count2, _ := async.NodeCount()
	t.Logf("After delete: %d", count2)
	assert.Equal(t, int64(0), count2)

	// Recreate with SAME IDs
	t.Log("Recreating with SAME IDs...")
	for _, id := range nodeIDs {
		_, err := async.CreateNode(&Node{ID: id, Labels: []string{"Test"}, Properties: map[string]any{"recreated": true}})
		require.NoError(t, err)
	}

	count3BeforeFlush, _ := async.NodeCount()
	t.Logf("After recreate (before flush): %d", count3BeforeFlush)
	// This might be the bug - if nodes are marked as "updates" instead of "creates"
	assert.Equal(t, int64(3), count3BeforeFlush, "Should count as 3 new creates, not updates")

	async.Flush()

	count3AfterFlush, _ := async.NodeCount()
	t.Logf("After recreate (after flush): %d", count3AfterFlush)
	assert.Equal(t, int64(3), count3AfterFlush)
}
