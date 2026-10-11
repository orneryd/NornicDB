package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealtimeCountTracking traces the issue where node counts
// stay at zero during live operations but are correct after restart.
func TestRealtimeCountTracking(t *testing.T) {
	// Create a fresh BadgerEngine (simulates server startup)
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	// Writes apply synchronously through the namespaced engine.
	namespaced := NewNamespacedEngine(badger, "test")

	t.Run("initial_count_is_zero", func(t *testing.T) {
		count, err := namespaced.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "Initial count should be 0")
		t.Logf("Initial count: %d", count)
	})

	t.Run("count_updates_immediately_after_create", func(t *testing.T) {
		node := &Node{
			ID:         "test-node-1",
			Labels:     []string{"TestNode"},
			Properties: map[string]interface{}{"name": "test1"},
		}
		_, err := namespaced.CreateNode(node)
		require.NoError(t, err)

		count, err := namespaced.NodeCount()
		require.NoError(t, err)
		t.Logf("Count immediately after CreateNode: %d", count)
		assert.Equal(t, int64(1), count, "Count should be 1 immediately after create")
	})

	t.Run("count_matches_badger_after_commit", func(t *testing.T) {
		count, err := namespaced.NodeCount()
		require.NoError(t, err)
		t.Logf("Count after commit: %d", count)
		assert.Equal(t, int64(1), count, "Count should still be 1")

		// Also check underlying BadgerEngine directly
		badgerCount, err := badger.NodeCount()
		require.NoError(t, err)
		t.Logf("BadgerEngine count after commit: %d", badgerCount)
		assert.Equal(t, int64(1), badgerCount, "BadgerEngine should show 1 node")
	})

	t.Run("count_updates_for_multiple_creates", func(t *testing.T) {
		for i := 2; i <= 5; i++ {
			node := &Node{
				ID:         NodeID("test-node-" + string(rune('0'+i))),
				Labels:     []string{"TestNode"},
				Properties: map[string]interface{}{"name": "test"},
			}
			_, err := namespaced.CreateNode(node)
			require.NoError(t, err)
		}

		count, err := namespaced.NodeCount()
		require.NoError(t, err)
		t.Logf("Count after creating 4 more nodes: %d", count)
		assert.Equal(t, int64(5), count, "Count should be 5")
	})
}

// TestRealtimeCountWithCypher simulates the flow when Cypher creates nodes
func TestRealtimeCountWithCypher(t *testing.T) {
	// Create fresh engines
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	namespaced := NewNamespacedEngine(badger, "test")

	// Simulate what Cypher executor does
	t.Run("cypher_create_flow", func(t *testing.T) {
		// Initial count
		initialCount, _ := namespaced.NodeCount()
		t.Logf("Initial count: %d", initialCount)

		// Cypher CREATE (n:Person {name: 'Alice'})
		node := &Node{
			ID:         "uuid-12345", // Cypher now uses UUIDs
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Alice"},
		}
		_, err := namespaced.CreateNode(node)
		require.NoError(t, err)

		// Stats endpoint called immediately
		count, _ := namespaced.NodeCount()
		t.Logf("Count after CREATE: %d", count)
		assert.Equal(t, initialCount+1, count, "Count should increment immediately")
	})
}

// TestCountAfterDeleteAndRecreate tests the scenario where IDs might collide
func TestCountAfterDeleteAndRecreate(t *testing.T) {
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	namespaced := NewNamespacedEngine(badger, "test")

	t.Run("delete_then_create_same_id", func(t *testing.T) {
		// Create a node
		node := &Node{
			ID:     "node-1",
			Labels: []string{"Test"},
		}
		_, err := namespaced.CreateNode(node)
		require.NoError(t, err)

		count, _ := namespaced.NodeCount()
		t.Logf("After create: count=%d", count)
		assert.Equal(t, int64(1), count)

		// Delete the node
		require.NoError(t, namespaced.DeleteNode("node-1"))

		count, _ = namespaced.NodeCount()
		t.Logf("After delete: count=%d", count)
		assert.Equal(t, int64(0), count, "Count should be 0 after delete")

		// Create with SAME ID
		node2 := &Node{
			ID:     "node-1", // Same ID!
			Labels: []string{"Test2"},
		}
		_, err = namespaced.CreateNode(node2)
		require.NoError(t, err)

		count, _ = namespaced.NodeCount()
		t.Logf("After recreate same ID: count=%d", count)
		assert.Equal(t, int64(1), count, "Count should be 1")
	})

	t.Run("delete_then_create_different_id", func(t *testing.T) {
		// Start fresh
		badger2 := createRealtimeTestBadgerEngine(t)
		defer badger2.Close()
		namespaced2 := NewNamespacedEngine(badger2, "test")

		// Create node-A
		_, err := namespaced2.CreateNode(&Node{ID: "node-A", Labels: []string{"Test"}})
		require.NoError(t, err)

		count, _ := namespaced2.NodeCount()
		assert.Equal(t, int64(1), count)

		// Delete node-A
		require.NoError(t, namespaced2.DeleteNode("node-A"))

		// Create node-B (different ID)
		_, err = namespaced2.CreateNode(&Node{ID: "node-B", Labels: []string{"Test"}})
		require.NoError(t, err)

		count, _ = namespaced2.NodeCount()
		t.Logf("After delete A + create B: count=%d", count)
		assert.Equal(t, int64(1), count)
	})
}

// TestCountAfterFlushAndRecreate tests creating a node with same ID after it was committed
func TestCountAfterFlushAndRecreate(t *testing.T) {
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	namespaced := NewNamespacedEngine(badger, "test")

	t.Run("create_create_same_id", func(t *testing.T) {
		// Create a node
		node := &Node{
			ID:     "node-1",
			Labels: []string{"Test"},
		}
		_, err := namespaced.CreateNode(node)
		require.NoError(t, err)

		count, _ := namespaced.NodeCount()
		t.Logf("After first create: count=%d", count)
		assert.Equal(t, int64(1), count)

		// Create SAME node again - the direct path rejects the duplicate key
		// instead of silently rewriting it.
		node2 := &Node{
			ID:     "node-1", // Same ID!
			Labels: []string{"Test2"},
		}
		_, err = namespaced.CreateNode(node2)
		require.ErrorContains(t, err, "already exists")

		count, _ = namespaced.NodeCount()
		t.Logf("After recreate same ID: count=%d", count)
		assert.Equal(t, int64(1), count, "Count stays 1")
	})
}

// Helper to create test BadgerEngine
func createRealtimeTestBadgerEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	return engine
}
