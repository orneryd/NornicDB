package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealtimeCountTracking traces the issue where node counts
// stay at zero during live operations but are correct after restart.
func TestRealtimeCountTracking(t *testing.T) {
	// Create a fresh BadgerEngine (simulates server startup)
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	// Wrap with AsyncEngine (like production)
	asyncConfig := &AsyncEngineConfig{
		FlushInterval: 50 * time.Millisecond, // Same as production default
	}
	namespaced := NewNamespacedEngine(badger, "test")
	async := NewAsyncEngine(namespaced, asyncConfig)
	defer async.Close()

	t.Run("initial_count_is_zero", func(t *testing.T) {
		count, err := async.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "Initial count should be 0")
		t.Logf("Initial count: %d", count)
	})

	t.Run("count_updates_immediately_after_create", func(t *testing.T) {
		// Create a node (goes to cache, not yet flushed)
		node := &Node{
			ID:         "test-node-1",
			Labels:     []string{"TestNode"},
			Properties: map[string]interface{}{"name": "test1"},
		}
		_, err := async.CreateNode(node)
		require.NoError(t, err)

		// Check count IMMEDIATELY (before flush)
		count, err := async.NodeCount()
		require.NoError(t, err)
		t.Logf("Count immediately after CreateNode (before flush): %d", count)
		assert.Equal(t, int64(1), count, "Count should be 1 immediately after create (pendingCreates=1)")
	})

	t.Run("count_stays_correct_after_flush", func(t *testing.T) {
		// Wait for flush to happen
		time.Sleep(100 * time.Millisecond)

		// Force a flush to ensure data is written
		async.Flush()

		// Check count after flush
		count, err := async.NodeCount()
		require.NoError(t, err)
		t.Logf("Count after flush: %d", count)
		assert.Equal(t, int64(1), count, "Count should still be 1 after flush (engineCount=1, pendingCreates=0)")

		// Also check underlying BadgerEngine directly
		badgerCount, err := badger.NodeCount()
		require.NoError(t, err)
		t.Logf("BadgerEngine count after flush: %d", badgerCount)
		assert.Equal(t, int64(1), badgerCount, "BadgerEngine should show 1 node after flush")
	})

	t.Run("count_updates_for_multiple_creates", func(t *testing.T) {
		// Create more nodes
		for i := 2; i <= 5; i++ {
			node := &Node{
				ID:         NodeID("test-node-" + string(rune('0'+i))),
				Labels:     []string{"TestNode"},
				Properties: map[string]interface{}{"name": "test"},
			}
			_, err := async.CreateNode(node)
			require.NoError(t, err)
		}

		// Check count immediately (mix of flushed and pending)
		count, err := async.NodeCount()
		require.NoError(t, err)
		t.Logf("Count after creating 4 more nodes (before flush): %d", count)
		assert.Equal(t, int64(5), count, "Count should be 5 (1 flushed + 4 pending)")

		// Flush and check again
		async.Flush()
		count, err = async.NodeCount()
		require.NoError(t, err)
		t.Logf("Count after flush: %d", count)
		assert.Equal(t, int64(5), count, "Count should be 5 after flush")
	})
}

// TestRealtimeCountWithCypher simulates the flow when Cypher creates nodes
func TestRealtimeCountWithCypher(t *testing.T) {
	// Create fresh engines
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	asyncConfig := &AsyncEngineConfig{
		FlushInterval: 50 * time.Millisecond,
	}
	namespaced := NewNamespacedEngine(badger, "test")
	async := NewAsyncEngine(namespaced, asyncConfig)
	defer async.Close()

	// Simulate what Cypher executor does
	t.Run("cypher_create_flow", func(t *testing.T) {
		// Initial count
		initialCount, _ := async.NodeCount()
		t.Logf("Initial count: %d", initialCount)

		// Cypher CREATE (n:Person {name: 'Alice'})
		node := &Node{
			ID:         "uuid-12345", // Cypher now uses UUIDs
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Alice"},
		}
		_, err := async.CreateNode(node)
		require.NoError(t, err)

		// Stats endpoint called immediately
		count, _ := async.NodeCount()
		t.Logf("Count after CREATE (before flush): %d", count)
		assert.Equal(t, initialCount+1, count, "Count should increment immediately")

		// Wait for async flush
		time.Sleep(100 * time.Millisecond)
		async.Flush()

		// Stats endpoint called after flush
		count, _ = async.NodeCount()
		t.Logf("Count after flush: %d", count)
		assert.Equal(t, initialCount+1, count, "Count should still be correct after flush")
	})
}

// TestCountAfterDeleteAndRecreate tests the scenario where IDs might collide
func TestCountAfterDeleteAndRecreate(t *testing.T) {
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	asyncConfig := &AsyncEngineConfig{
		FlushInterval: 50 * time.Millisecond,
	}
	namespaced := NewNamespacedEngine(badger, "test")
	async := NewAsyncEngine(namespaced, asyncConfig)
	defer async.Close()

	t.Run("delete_then_create_same_id", func(t *testing.T) {
		// Create a node
		node := &Node{
			ID:     "node-1",
			Labels: []string{"Test"},
		}
		async.CreateNode(node)
		async.Flush()

		count, _ := async.NodeCount()
		t.Logf("After create + flush: count=%d", count)
		assert.Equal(t, int64(1), count)

		// Delete the node
		async.DeleteNode("node-1")

		count, _ = async.NodeCount()
		t.Logf("After delete (before flush): count=%d", count)
		assert.Equal(t, int64(0), count, "Count should be 0 (pending delete)")

		// Create with SAME ID before flush
		node2 := &Node{
			ID:     "node-1", // Same ID!
			Labels: []string{"Test2"},
		}
		async.CreateNode(node2)

		count, _ = async.NodeCount()
		t.Logf("After recreate same ID (before flush): count=%d", count)
		// This should be 1 - the delete was cancelled by the create
		assert.Equal(t, int64(1), count, "Count should be 1 (delete cancelled)")

		async.Flush()
		count, _ = async.NodeCount()
		t.Logf("After flush: count=%d", count)
		assert.Equal(t, int64(1), count)
	})

	t.Run("delete_then_create_different_id", func(t *testing.T) {
		// Start fresh
		badger2 := createRealtimeTestBadgerEngine(t)
		defer badger2.Close()
		namespaced2 := NewNamespacedEngine(badger2, "test")
		async2 := NewAsyncEngine(namespaced2, asyncConfig)
		defer async2.Close()

		// Create node-A
		async2.CreateNode(&Node{ID: "node-A", Labels: []string{"Test"}})
		async2.Flush()

		count, _ := async2.NodeCount()
		assert.Equal(t, int64(1), count)

		// Delete node-A
		async2.DeleteNode("node-A")

		// Create node-B (different ID)
		async2.CreateNode(&Node{ID: "node-B", Labels: []string{"Test"}})

		count, _ = async2.NodeCount()
		t.Logf("After delete A + create B (before flush): count=%d", count)
		// Should be 1: engineCount=1, pendingCreates=1, pendingDeletes=1
		// 1 + 1 - 1 = 1
		assert.Equal(t, int64(1), count)

		async2.Flush()
		count, _ = async2.NodeCount()
		t.Logf("After flush: count=%d", count)
		assert.Equal(t, int64(1), count)
	})
}

// TestCountDuringFlushRace tests counting during the flush window
func TestCountDuringFlushRace(t *testing.T) {
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	// Very fast flush interval to trigger race conditions
	asyncConfig := &AsyncEngineConfig{
		FlushInterval: 5 * time.Millisecond,
	}
	namespaced := NewNamespacedEngine(badger, "test")
	async := NewAsyncEngine(namespaced, asyncConfig)
	defer async.Close()

	// Create many nodes while flushes are happening
	numNodes := 100
	for i := 0; i < numNodes; i++ {
		node := &Node{
			ID:     NodeID(fmt.Sprintf("race-node-%d", i)),
			Labels: []string{"RaceTest"},
		}
		async.CreateNode(node)

		// Check count periodically
		if i%10 == 0 {
			count, _ := async.NodeCount()
			t.Logf("After %d creates: count=%d", i+1, count)
		}
	}

	// Wait for all flushes
	time.Sleep(100 * time.Millisecond)
	async.Flush()

	finalCount, _ := async.NodeCount()
	t.Logf("Final count: %d (expected %d)", finalCount, numNodes)
	assert.Equal(t, int64(numNodes), finalCount, "Final count should match number of nodes created")

	// Verify BadgerEngine matches
	badgerCount, _ := badger.NodeCount()
	assert.Equal(t, int64(numNodes), badgerCount, "BadgerEngine count should match")
}

// TestCountAfterFlushAndRecreate tests creating a node with same ID after it was flushed
func TestCountAfterFlushAndRecreate(t *testing.T) {
	badger := createRealtimeTestBadgerEngine(t)
	defer badger.Close()

	asyncConfig := &AsyncEngineConfig{
		FlushInterval: 50 * time.Millisecond,
	}
	namespaced := NewNamespacedEngine(badger, "test")
	async := NewAsyncEngine(namespaced, asyncConfig)
	defer async.Close()

	t.Run("create_flush_create_same_id", func(t *testing.T) {
		// Create a node
		node := &Node{
			ID:     "node-1",
			Labels: []string{"Test"},
		}
		async.CreateNode(node)
		async.Flush()

		count, _ := async.NodeCount()
		t.Logf("After first create + flush: count=%d", count)
		assert.Equal(t, int64(1), count)

		// Create SAME node again (this is what happens when re-importing)
		node2 := &Node{
			ID:     "node-1", // Same ID!
			Labels: []string{"Test2"},
		}
		async.CreateNode(node2)

		// Note: Before flush, AsyncEngine may temporarily over-count because it
		// can't efficiently check if the node exists in the underlying engine.
		// This is a known limitation. The count is corrected after flush when
		// BadgerEngine scans actual nodes.
		countBeforeFlush, _ := async.NodeCount()
		t.Logf("After recreate same ID (before flush): count=%d (may be temporarily inflated)", countBeforeFlush)

		async.Flush()
		count, _ = async.NodeCount()
		t.Logf("After second flush: count=%d", count)
		// After flush, the count MUST be correct (1, not 2)
		assert.Equal(t, int64(1), count, "Count should be 1 after flush (update, not create)")
	})
}

// Helper to create test BadgerEngine
func createRealtimeTestBadgerEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	return engine
}
