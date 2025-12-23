package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDetachDeleteStatsTracking tests that node and edge counts are correctly
// updated after DETACH DELETE operations through the AsyncEngine layer.
//
// Bug reproduction: Stats showed incorrect counts after DETACH DELETE:
// - Edge count stayed high even after edges were deleted
// - Node count showed stale data
// - Counts went negative in some cases
func TestDetachDeleteStatsTracking(t *testing.T) {
	t.Run("async_engine_tracks_counts_correctly_after_detach_delete", func(t *testing.T) {
		// Setup: Create an AsyncEngine wrapping a MemoryEngine
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		asyncConfig := &AsyncEngineConfig{
			FlushInterval: 10 * time.Millisecond, // Fast flush for testing
		}
		asyncEngine := NewAsyncEngine(engine, asyncConfig)
		defer asyncEngine.Close()

		// Initial counts should be 0
		nodeCount, err := asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), nodeCount, "Initial node count should be 0")

		edgeCount, err := asyncEngine.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), edgeCount, "Initial edge count should be 0")

		// Create 50 test nodes
		for i := 1; i <= 50; i++ {
			node := &Node{
				ID:     NodeID(t.Name() + "_node_" + string(rune(i+'0'))),
				Labels: []string{"TestNode"},
			}
			_, err := asyncEngine.CreateNode(node)
			require.NoError(t, err)
		}

		// Create 49 relationships (chain pattern)
		for i := 1; i <= 49; i++ {
			edge := &Edge{
				ID:        EdgeID(t.Name() + "_edge_" + string(rune(i+'0'))),
				StartNode: NodeID(t.Name() + "_node_" + string(rune(i+'0'))),
				EndNode:   NodeID(t.Name() + "_node_" + string(rune((i+1)+'0'))),
				Type:      "TEST_REL",
			}
			require.NoError(t, asyncEngine.CreateEdge(edge))
		}

		// Check counts before flush (should be in cache)
		nodeCount, err = asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(50), nodeCount, "After create: 50 nodes expected")

		edgeCount, err = asyncEngine.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(49), edgeCount, "After create: 49 edges expected")

		// Flush to underlying engine
		require.NoError(t, asyncEngine.Flush())

		// Counts should still be correct after flush
		nodeCount, err = asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(50), nodeCount, "After flush: 50 nodes expected")

		edgeCount, err = asyncEngine.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(49), edgeCount, "After flush: 49 edges expected")

		// Now simulate DETACH DELETE for all nodes
		// First delete all edges, then delete all nodes
		for i := 1; i <= 49; i++ {
			edgeID := EdgeID(t.Name() + "_edge_" + string(rune(i+'0')))
			require.NoError(t, asyncEngine.DeleteEdge(edgeID))
		}
		for i := 1; i <= 50; i++ {
			nodeID := NodeID(t.Name() + "_node_" + string(rune(i+'0')))
			require.NoError(t, asyncEngine.DeleteNode(nodeID))
		}

		// Check counts BEFORE flush (deletes are pending)
		nodeCount, err = asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), nodeCount, "After delete (pre-flush): 0 nodes expected")

		edgeCount, err = asyncEngine.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), edgeCount, "After delete (pre-flush): 0 edges expected")

		// Flush deletes
		require.NoError(t, asyncEngine.Flush())

		// Check counts AFTER flush
		nodeCount, err = asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), nodeCount, "After flush: 0 nodes expected")

		edgeCount, err = asyncEngine.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), edgeCount, "After flush: 0 edges expected")
	})

	t.Run("counts_never_go_negative", func(t *testing.T) {
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		asyncConfig := &AsyncEngineConfig{
			FlushInterval: 100 * time.Millisecond,
		}
		asyncEngine := NewAsyncEngine(engine, asyncConfig)
		defer asyncEngine.Close()

		// Create a node
		node := &Node{ID: "single_node", Labels: []string{"Test"}}
		_, err := asyncEngine.CreateNode(node)
		require.NoError(t, err)

		// Flush to engine
		require.NoError(t, asyncEngine.Flush())

		// Delete the node
		require.NoError(t, asyncEngine.DeleteNode("single_node"))

		// Count should be 0, not negative
		count, err := asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, int64(0), "Node count should never be negative")

		// Flush and check again
		require.NoError(t, asyncEngine.Flush())

		count, err = asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, int64(0), "Node count should never be negative after flush")

		// Try deleting the same node again (should be idempotent)
		_ = asyncEngine.DeleteNode("single_node")
		require.NoError(t, asyncEngine.Flush())

		count, err = asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, int64(0), "Node count should never be negative after double delete")
	})

	t.Run("edge_delete_before_node_delete", func(t *testing.T) {
		// This simulates the DETACH DELETE pattern:
		// 1. Find edges for a node
		// 2. Delete edges
		// 3. Delete node
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		asyncConfig := &AsyncEngineConfig{
			FlushInterval: 100 * time.Millisecond,
		}
		asyncEngine := NewAsyncEngine(engine, asyncConfig)
		defer asyncEngine.Close()

		// Create two nodes with a relationship
		nodeA := &Node{ID: "nodeA", Labels: []string{"Test"}}
		nodeB := &Node{ID: "nodeB", Labels: []string{"Test"}}
		_, err := asyncEngine.CreateNode(nodeA)
		require.NoError(t, err)
		_, err = asyncEngine.CreateNode(nodeB)
		require.NoError(t, err)

		edge := &Edge{ID: "edge1", StartNode: "nodeA", EndNode: "nodeB", Type: "KNOWS"}
		require.NoError(t, asyncEngine.CreateEdge(edge))

		// Flush to underlying engine
		require.NoError(t, asyncEngine.Flush())

		// Verify initial counts
		nc, _ := asyncEngine.NodeCount()
		ec, _ := asyncEngine.EdgeCount()
		assert.Equal(t, int64(2), nc, "Should have 2 nodes")
		assert.Equal(t, int64(1), ec, "Should have 1 edge")

		// Now DETACH DELETE nodeA:
		// 1. Get outgoing edges
		outgoing, err := asyncEngine.GetOutgoingEdges("nodeA")
		require.NoError(t, err)
		assert.Len(t, outgoing, 1, "nodeA should have 1 outgoing edge")

		// 2. Delete edges
		for _, e := range outgoing {
			require.NoError(t, asyncEngine.DeleteEdge(e.ID))
		}

		// 3. Delete node
		require.NoError(t, asyncEngine.DeleteNode("nodeA"))

		// Check counts before flush
		nc, _ = asyncEngine.NodeCount()
		ec, _ = asyncEngine.EdgeCount()
		assert.Equal(t, int64(1), nc, "Should have 1 node (nodeB)")
		assert.Equal(t, int64(0), ec, "Should have 0 edges")

		// Flush
		require.NoError(t, asyncEngine.Flush())

		// Check counts after flush
		nc, _ = asyncEngine.NodeCount()
		ec, _ = asyncEngine.EdgeCount()
		assert.Equal(t, int64(1), nc, "After flush: 1 node (nodeB)")
		assert.Equal(t, int64(0), ec, "After flush: 0 edges")
	})

	t.Run("delete_nonexistent_node_does_not_affect_count", func(t *testing.T) {
		// This tests the critical fix: deleting a non-existent node should NOT
		// decrement the count and cause it to go negative
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		asyncConfig := &AsyncEngineConfig{
			FlushInterval: 100 * time.Millisecond,
		}
		asyncEngine := NewAsyncEngine(engine, asyncConfig)
		defer asyncEngine.Close()

		// Initial count should be 0
		count, err := asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "Initial count should be 0")

		// Try to delete a non-existent node
		err = asyncEngine.DeleteNode("nonexistent_node_id")
		assert.Equal(t, ErrNotFound, err, "Deleting non-existent node should return ErrNotFound")

		// Count should still be 0, not -1
		count, err = asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "Count should remain 0 after deleting non-existent node")

		// Flush and verify
		require.NoError(t, asyncEngine.Flush())

		count, err = asyncEngine.NodeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "Count should remain 0 after flush")
	})

	t.Run("delete_nonexistent_edge_does_not_affect_count", func(t *testing.T) {
		// Similar test for edges
		base := NewMemoryEngine()
		defer base.Close()
		engine := NewNamespacedEngine(base, "test")
		asyncConfig := &AsyncEngineConfig{
			FlushInterval: 100 * time.Millisecond,
		}
		asyncEngine := NewAsyncEngine(engine, asyncConfig)
		defer asyncEngine.Close()
		
		// Initial count should be 0
		count, err := asyncEngine.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "Initial edge count should be 0")

		// Try to delete a non-existent edge
		err = asyncEngine.DeleteEdge("nonexistent_edge_id")
		assert.Equal(t, ErrNotFound, err, "Deleting non-existent edge should return ErrNotFound")

		// Count should still be 0
		count, err = asyncEngine.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "Edge count should remain 0 after deleting non-existent edge")

		// Flush and verify
		require.NoError(t, asyncEngine.Flush())

		count, err = asyncEngine.EdgeCount()
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "Edge count should remain 0 after flush")
	})
}

// TestBadgerEngineDetachDeleteStats tests the underlying BadgerEngine directly
func TestBadgerEngineDetachDeleteStats(t *testing.T) {
	t.Run("bulk_delete_edges_updates_count_correctly", func(t *testing.T) {
		engine, cleanup := createDeleteStatsTestBadgerEngine(t)
		defer cleanup()

		// Create nodes
		for i := 0; i < 10; i++ {
			node := &Node{ID: NodeID(prefixTestID(string(rune('a' + i)))), Labels: []string{"Test"}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
		}

		// Create edges (chain)
		edgeIDs := make([]EdgeID, 0, 9)
		for i := 0; i < 9; i++ {
			edgeID := EdgeID(prefixTestID("edge_" + string(rune('a'+i))))
			edgeIDs = append(edgeIDs, edgeID)
			edge := &Edge{
				ID:        edgeID,
				StartNode: NodeID(prefixTestID(string(rune('a' + i)))),
				EndNode:   NodeID(prefixTestID(string(rune('a' + i + 1)))),
				Type:      "NEXT",
			}
			require.NoError(t, engine.CreateEdge(edge))
		}

		// Verify counts
		nc, _ := engine.NodeCount()
		ec, _ := engine.EdgeCount()
		assert.Equal(t, int64(10), nc)
		assert.Equal(t, int64(9), ec)

		// Bulk delete edges
		require.NoError(t, engine.BulkDeleteEdges(edgeIDs))

		// Edge count should be 0
		ec, _ = engine.EdgeCount()
		assert.Equal(t, int64(0), ec, "Edge count should be 0 after bulk delete")

		// Node count unchanged
		nc, _ = engine.NodeCount()
		assert.Equal(t, int64(10), nc, "Node count should still be 10")
	})

	t.Run("delete_nonexistent_edge_does_not_change_count", func(t *testing.T) {
		engine, cleanup := createDeleteStatsTestBadgerEngine(t)
		defer cleanup()

		// Create one edge
		node1 := &Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"Test"}}
		node2 := &Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"Test"}}
		_, _ = engine.CreateNode(node1)
		_, _ = engine.CreateNode(node2)

		edge := &Edge{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n2")), Type: "REL"}
		engine.CreateEdge(edge)

		ec1, _ := engine.EdgeCount()
		assert.Equal(t, int64(1), ec1)

		// Try to delete a nonexistent edge
		err := engine.DeleteEdge(EdgeID(prefixTestID("nonexistent")))
		assert.Error(t, err) // Should return ErrNotFound

		// Count should be unchanged
		ec2, _ := engine.EdgeCount()
		assert.Equal(t, int64(1), ec2, "Count should be unchanged after deleting nonexistent edge")

		// Bulk delete with nonexistent edge should not change count
		engine.BulkDeleteEdges([]EdgeID{EdgeID(prefixTestID("nonexistent1")), EdgeID(prefixTestID("nonexistent2"))})
		ec3, _ := engine.EdgeCount()
		assert.Equal(t, int64(1), ec3, "Count unchanged after bulk deleting nonexistent edges")
	})
}

// TestBulkOperationsUpdateCounts verifies that bulk operations correctly update
// both node and edge counts, especially when BulkDeleteNodes also deletes edges.
func TestBulkOperationsUpdateCounts(t *testing.T) {
	t.Run("bulk_delete_nodes_updates_edge_count_too", func(t *testing.T) {
		// This is the key bug scenario: BulkDeleteNodes deletes edges
		// connected to the deleted nodes, but the edge count wasn't updated.
		engine, cleanup := createDeleteStatsTestBadgerEngine(t)
		defer cleanup()

		// Create a graph: A -> B -> C -> D -> E (4 edges)
		for i := 0; i < 5; i++ {
			node := &Node{ID: NodeID(prefixTestID(string(rune('A' + i)))), Labels: []string{"Test"}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
		}
		for i := 0; i < 4; i++ {
			edge := &Edge{
				ID:        EdgeID(prefixTestID("edge_" + string(rune('A'+i)))),
				StartNode: NodeID(prefixTestID(string(rune('A' + i)))),
				EndNode:   NodeID(prefixTestID(string(rune('A' + i + 1)))),
				Type:      "NEXT",
			}
			require.NoError(t, engine.CreateEdge(edge))
		}

		// Verify initial counts
		nc, _ := engine.NodeCount()
		ec, _ := engine.EdgeCount()
		assert.Equal(t, int64(5), nc, "Should have 5 nodes")
		assert.Equal(t, int64(4), ec, "Should have 4 edges")

		// Bulk delete nodes A, B, C (should also delete edges A->B, B->C)
		err := engine.BulkDeleteNodes([]NodeID{NodeID(prefixTestID("A")), NodeID(prefixTestID("B")), NodeID(prefixTestID("C"))})
		require.NoError(t, err)

		// Node count should be 2 (D, E remain)
		nc, _ = engine.NodeCount()
		assert.Equal(t, int64(2), nc, "Should have 2 nodes remaining (D, E)")

		// Edge count should be 1 (only D->E remains)
		// C->D edge is deleted because C is deleted
		ec, _ = engine.EdgeCount()
		assert.Equal(t, int64(1), ec, "Should have 1 edge remaining (D->E)")

		// Verify the remaining edge is D->E
		_, err = engine.GetEdge(EdgeID(prefixTestID("edge_D")))
		assert.NoError(t, err, "edge_D (D->E) should still exist")
	})

	t.Run("single_delete_node_updates_edge_count", func(t *testing.T) {
		engine, cleanup := createDeleteStatsTestBadgerEngine(t)
		defer cleanup()

		// Create A -> B
		_, _ = engine.CreateNode(&Node{ID: NodeID(prefixTestID("A")), Labels: []string{"Test"}})
		_, _ = engine.CreateNode(&Node{ID: NodeID(prefixTestID("B")), Labels: []string{"Test"}})
		engine.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("A")), EndNode: NodeID(prefixTestID("B")), Type: "REL"})

		nc, _ := engine.NodeCount()
		ec, _ := engine.EdgeCount()
		assert.Equal(t, int64(2), nc)
		assert.Equal(t, int64(1), ec)

		// Delete node A - should also delete edge A->B
		err := engine.DeleteNode(NodeID(prefixTestID("A")))
		require.NoError(t, err)

		nc, _ = engine.NodeCount()
		ec, _ = engine.EdgeCount()
		assert.Equal(t, int64(1), nc, "Should have 1 node (B)")
		assert.Equal(t, int64(0), ec, "Should have 0 edges")
	})

	t.Run("delete_node_with_incoming_and_outgoing_edges", func(t *testing.T) {
		engine, cleanup := createDeleteStatsTestBadgerEngine(t)
		defer cleanup()

		// Create: A -> B -> C (B has both incoming and outgoing edges)
		_, _ = engine.CreateNode(&Node{ID: NodeID(prefixTestID("A")), Labels: []string{"Test"}})
		_, _ = engine.CreateNode(&Node{ID: NodeID(prefixTestID("B")), Labels: []string{"Test"}})
		_, _ = engine.CreateNode(&Node{ID: NodeID(prefixTestID("C")), Labels: []string{"Test"}})
		engine.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("A")), EndNode: NodeID(prefixTestID("B")), Type: "REL"})
		engine.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e2")), StartNode: NodeID(prefixTestID("B")), EndNode: NodeID(prefixTestID("C")), Type: "REL"})

		nc, _ := engine.NodeCount()
		ec, _ := engine.EdgeCount()
		assert.Equal(t, int64(3), nc)
		assert.Equal(t, int64(2), ec)

		// Delete B - should delete both edges
		err := engine.DeleteNode(NodeID(prefixTestID("B")))
		require.NoError(t, err)

		nc, _ = engine.NodeCount()
		ec, _ = engine.EdgeCount()
		assert.Equal(t, int64(2), nc, "Should have 2 nodes (A, C)")
		assert.Equal(t, int64(0), ec, "Should have 0 edges")
	})

	t.Run("double_delete_same_node_doesnt_double_decrement", func(t *testing.T) {
		engine, cleanup := createDeleteStatsTestBadgerEngine(t)
		defer cleanup()

		_, _ = engine.CreateNode(&Node{ID: NodeID(prefixTestID("A")), Labels: []string{"Test"}})
		_, _ = engine.CreateNode(&Node{ID: NodeID(prefixTestID("B")), Labels: []string{"Test"}})
		engine.CreateEdge(&Edge{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("A")), EndNode: NodeID(prefixTestID("B")), Type: "REL"})

		// Delete A
		engine.DeleteNode(NodeID(prefixTestID("A")))

		// Try to delete A again
		err := engine.DeleteNode(NodeID(prefixTestID("A")))
		assert.Error(t, err) // Should return ErrNotFound

		// Counts should still be correct
		nc, _ := engine.NodeCount()
		ec, _ := engine.EdgeCount()
		assert.Equal(t, int64(1), nc, "Should have 1 node (B)")
		assert.Equal(t, int64(0), ec, "Should have 0 edges")
	})

	t.Run("bulk_delete_with_nonexistent_nodes_partial_success", func(t *testing.T) {
		engine, cleanup := createDeleteStatsTestBadgerEngine(t)
		defer cleanup()

		// Create only A
		_, _ = engine.CreateNode(&Node{ID: NodeID(prefixTestID("A")), Labels: []string{"Test"}})

		nc, _ := engine.NodeCount()
		assert.Equal(t, int64(1), nc)

		// Bulk delete A, B, C (B and C don't exist)
		err := engine.BulkDeleteNodes([]NodeID{NodeID(prefixTestID("A")), NodeID(prefixTestID("B")), NodeID(prefixTestID("C"))})
		assert.NoError(t, err) // Should not error, just skip nonexistent

		// Count should be 0 (A was deleted, B/C didn't exist)
		nc, _ = engine.NodeCount()
		assert.Equal(t, int64(0), nc, "Should have 0 nodes")
	})
}

// createDeleteStatsTestBadgerEngine creates a temporary BadgerEngine for testing
func createDeleteStatsTestBadgerEngine(t *testing.T) (*BadgerEngine, func()) {
	t.Helper()
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	return engine, func() {
		engine.Close()
	}
}
