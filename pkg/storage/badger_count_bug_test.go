package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBadgerEngine_UpdateNode_WasInsertAfterDelete tests that UpdateNode correctly
// detects new nodes as inserts after all nodes have been deleted.
// This reproduces the bug where nodeCount stays at 0/1 after delete+recreate.
func TestBadgerEngine_UpdateNode_WasInsertAfterDelete(t *testing.T) {
	// Create temp directory for BadgerDB
	tmpDir, err := os.MkdirTemp("", "badger_count_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create BadgerEngine directly (no AsyncEngine, no WALEngine)
	engine, err := NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	// Step 1: Create 10 nodes via CreateNode
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("node-%d", i))),
			Labels: []string{"Test"},
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("Node %d", i),
			},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	count1, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(10), count1, "Should have 10 nodes after creation")

	// Step 2: Delete all nodes
	for i := 0; i < 10; i++ {
		err := engine.DeleteNode(NodeID(prefixTestID(fmt.Sprintf("node-%d", i))))
		require.NoError(t, err)
	}

	count2, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count2, "Should have 0 nodes after deletion")

	// Step 3: Create NEW nodes with NEW IDs via UpdateNode (simulating AsyncEngine flush)
	// These are brand new UUIDs that never existed before
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("new-uuid-%d", i))),
			Labels: []string{"NewTest"},
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("New Node %d", i),
			},
		}
		// Use UpdateNode like AsyncEngine does during flush
		err := engine.UpdateNode(node)
		require.NoError(t, err)
	}

	count3, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(5), count3, "Should have 5 nodes after UpdateNode with new IDs")
}

// TestBadgerEngine_UpdateNode_WasInsertWithSameIDs tests what happens when
// UpdateNode is called with IDs that previously existed but were deleted.
func TestBadgerEngine_UpdateNode_WasInsertWithSameIDs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_count_test2")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	engine, err := NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	// Step 1: Create nodes
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("reuse-id-%d", i))),
			Labels: []string{"Test"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	count1, _ := engine.NodeCount()
	assert.Equal(t, int64(5), count1)

	// Step 2: Delete all
	for i := 0; i < 5; i++ {
		require.NoError(t, engine.DeleteNode(NodeID(prefixTestID(fmt.Sprintf("reuse-id-%d", i)))))
	}

	count2, _ := engine.NodeCount()
	assert.Equal(t, int64(0), count2)

	// Step 3: UpdateNode with SAME IDs (like MERGE might do)
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("reuse-id-%d", i))),
			Labels: []string{"Test"},
		}
		require.NoError(t, engine.UpdateNode(node))
	}

	count3, _ := engine.NodeCount()
	assert.Equal(t, int64(5), count3, "UpdateNode should detect these as NEW nodes (wasInsert=true)")
}

// TestBadgerEngine_DeleteActuallyRemovesKey verifies that DeleteNode actually
// removes the key from BadgerDB so that subsequent Get returns ErrKeyNotFound.
func TestBadgerEngine_DeleteActuallyRemovesKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger_delete_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	engine, err := NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer engine.Close()

	// Create a node
	node := &Node{ID: NodeID(prefixTestID("test-delete")), Labels: []string{"Test"}}
	_, err = engine.CreateNode(node)
	require.NoError(t, err)

	// Verify it exists
	retrieved, err := engine.GetNode(NodeID(prefixTestID("test-delete")))
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	// Delete it
	require.NoError(t, engine.DeleteNode(NodeID(prefixTestID("test-delete"))))

	// Verify it's gone
	retrieved, err = engine.GetNode(NodeID(prefixTestID("test-delete")))
	assert.Equal(t, ErrNotFound, err, "Node should not be found after delete")
	assert.Nil(t, retrieved)
}
