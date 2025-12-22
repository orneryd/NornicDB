package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWALEngine_CountAfterDeleteRecreate tests the full WAL+Badger stack
// to verify node counts are correct after delete and recreate cycle.

func TestWALEngine_CountAfterDeleteRecreate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_count_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create BadgerEngine
	badger, err := NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	// Create WAL
	wal, err := NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)
	defer wal.Close()

	// Wrap with WALEngine
	walEngine := NewWALEngine(badger, wal)

	// Step 1: Create 10 nodes
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("node-%d", i))),
			Labels: []string{"Test"},
		}
		_, err := walEngine.CreateNode(node)
		require.NoError(t, err)
	}

	count1, _ := walEngine.NodeCount()
	assert.Equal(t, int64(10), count1, "Should have 10 nodes")

	// Step 2: Delete all
	for i := 0; i < 10; i++ {
		require.NoError(t, walEngine.DeleteNode(NodeID(prefixTestID(fmt.Sprintf("node-%d", i)))))
	}

	count2, _ := walEngine.NodeCount()
	assert.Equal(t, int64(0), count2, "Should have 0 nodes after delete")

	// Step 3: Create NEW nodes via UpdateNode (like AsyncEngine flush does)
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("new-node-%d", i))),
			Labels: []string{"NewTest"},
		}
		require.NoError(t, walEngine.UpdateNode(node))
	}

	count3, _ := walEngine.NodeCount()
	assert.Equal(t, int64(5), count3, "Should have 5 nodes after UpdateNode")
}

// TestAsyncEngine_CountAfterDeleteRecreate tests the full Async+WAL+Badger stack
func TestAsyncEngine_CountAfterDeleteRecreate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "async_count_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create full stack: BadgerEngine -> WALEngine -> AsyncEngine
	badger, err := NewBadgerEngine(tmpDir)
	require.NoError(t, err)
	defer badger.Close()

	wal, err := NewWAL(tmpDir+"/wal", nil)
	require.NoError(t, err)
	defer wal.Close()

	walEngine := NewWALEngine(badger, wal)
	asyncEngine := NewAsyncEngine(walEngine, nil)
	defer asyncEngine.Close()

	// Step 1: Create 10 nodes
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("node-%d", i))),
			Labels: []string{"Test"},
		}
		_, err := asyncEngine.CreateNode(node)
		require.NoError(t, err)
	}
	asyncEngine.Flush()

	count1, _ := asyncEngine.NodeCount()
	assert.Equal(t, int64(10), count1, "Should have 10 nodes")

	// Step 2: Delete all
	for i := 0; i < 10; i++ {
		require.NoError(t, asyncEngine.DeleteNode(NodeID(prefixTestID(fmt.Sprintf("node-%d", i)))))
	}
	asyncEngine.Flush()

	count2, _ := asyncEngine.NodeCount()
	assert.Equal(t, int64(0), count2, "Should have 0 nodes after delete")

	// Step 3: Create NEW nodes (simulating what Cypher MERGE does)
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("new-node-%d", i))),
			Labels: []string{"NewTest"},
		}
		_, err := asyncEngine.CreateNode(node)
		require.NoError(t, err)
	}
	asyncEngine.Flush()

	count3, _ := asyncEngine.NodeCount()
	assert.Equal(t, int64(5), count3, "Should have 5 nodes after recreate")
}
