package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Test Helpers
// ============================================================================

// createTestBadgerEngine creates an in-memory BadgerEngine for testing.
func createTestBadgerEngine(t *testing.T) *BadgerEngine {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() {
		engine.Close()
	})
	return engine
}

// createTestBadgerEngineOnDisk creates a disk-based BadgerEngine for persistence tests.
func createTestBadgerEngineOnDisk(t *testing.T) (*BadgerEngine, string) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	return engine, dir
}

// prefixTestID ensures an ID is prefixed for BadgerEngine tests.
// BadgerEngine requires all node/edge IDs to be prefixed (e.g., "test:node-1").
func prefixTestID(id string) string {
	if strings.Contains(id, ":") {
		return id
	}
	return "test:" + id
}

// testNode creates a test node with the given ID.
// For BadgerEngine tests, IDs must be prefixed (e.g., "test:node-1").
// If the ID doesn't contain ":", it will be prefixed with "test:".
func testNode(id string) *Node {
	prefixedID := prefixTestID(id)
	return &Node{
		ID:         NodeID(prefixedID),
		Labels:     []string{"TestNode"},
		Properties: map[string]any{"name": id},
		CreatedAt:  time.Now(),
	}
}

// testEdge creates a test edge between two nodes.
// For BadgerEngine tests, IDs must be prefixed (e.g., "test:edge-1").
// If the ID doesn't contain ":", it will be prefixed with "test:".
// Node IDs (start/end) can be strings or NodeIDs - they will be prefixed automatically.
func testEdge(id string, start, end interface{}, edgeType string) *Edge {
	prefixedEdgeID := prefixTestID(id)

	// Handle start node ID (string or NodeID)
	var prefixedStart NodeID
	switch v := start.(type) {
	case string:
		prefixedStart = NodeID(prefixTestID(v))
	case NodeID:
		prefixedStart = NodeID(prefixTestID(string(v)))
	default:
		panic(fmt.Sprintf("testEdge: start must be string or NodeID, got %T", start))
	}

	// Handle end node ID (string or NodeID)
	var prefixedEnd NodeID
	switch v := end.(type) {
	case string:
		prefixedEnd = NodeID(prefixTestID(v))
	case NodeID:
		prefixedEnd = NodeID(prefixTestID(string(v)))
	default:
		panic(fmt.Sprintf("testEdge: end must be string or NodeID, got %T", end))
	}

	return &Edge{
		ID:         EdgeID(prefixedEdgeID),
		StartNode:  prefixedStart,
		EndNode:    prefixedEnd,
		Type:       edgeType,
		Properties: map[string]any{},
		CreatedAt:  time.Now(),
	}
}

// ============================================================================
// Node CRUD Tests
// ============================================================================

func TestBadgerEngine_CreateNode(t *testing.T) {
	engine := createTestBadgerEngine(t)

	t.Run("creates node successfully", func(t *testing.T) {
		node := testNode("n1")
		_, err := engine.CreateNode(node)
		assert.NoError(t, err)
	})

	t.Run("returns ErrAlreadyExists for duplicate", func(t *testing.T) {
		node := testNode("n2")
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		_, err = engine.CreateNode(node)
		assert.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("returns ErrInvalidData for nil node", func(t *testing.T) {
		_, err := engine.CreateNode(nil)
		assert.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("returns ErrInvalidID for empty ID", func(t *testing.T) {
		node := &Node{ID: "", Labels: []string{"Test"}}
		_, err := engine.CreateNode(node)
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestBadgerEngine_GetNode(t *testing.T) {
	engine := createTestBadgerEngine(t)

	t.Run("gets existing node", func(t *testing.T) {
		original := &Node{
			ID:         NodeID(prefixTestID("n1")),
			Labels:     []string{"Person", "User"},
			Properties: map[string]any{"name": "Alice", "age": 30},
			CreatedAt:  time.Now().Truncate(time.Second),
		}
		_, err := engine.CreateNode(original)
		require.NoError(t, err)

		retrieved, err := engine.GetNode(NodeID(prefixTestID("n1")))
		require.NoError(t, err)

		assert.Equal(t, original.ID, retrieved.ID)
		assert.Equal(t, original.Labels, retrieved.Labels)
		assert.Equal(t, original.Properties["name"], retrieved.Properties["name"])
	})

	t.Run("returns ErrNotFound for missing node", func(t *testing.T) {
		_, err := engine.GetNode(NodeID(prefixTestID("nonexistent")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns ErrInvalidID for empty ID", func(t *testing.T) {
		_, err := engine.GetNode("")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestBadgerEngine_UpdateNode(t *testing.T) {
	engine := createTestBadgerEngine(t)

	t.Run("updates existing node", func(t *testing.T) {
		node := testNode("n1")
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		node.Properties["name"] = "Updated"
		node.Labels = []string{"UpdatedLabel"}
		err = engine.UpdateNode(node)
		require.NoError(t, err)

		retrieved, err := engine.GetNode(NodeID(prefixTestID("n1")))
		require.NoError(t, err)
		assert.Equal(t, "Updated", retrieved.Properties["name"])
		assert.Equal(t, []string{"UpdatedLabel"}, retrieved.Labels)
	})

	t.Run("updates label index correctly", func(t *testing.T) {
		node := &Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"OldLabel"}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Check old label
		oldNodes, err := engine.GetNodesByLabel("OldLabel")
		require.NoError(t, err)
		assert.Len(t, oldNodes, 1)

		// Update labels
		node.Labels = []string{"NewLabel"}
		err = engine.UpdateNode(node)
		require.NoError(t, err)

		// Old label should be empty
		oldNodes, err = engine.GetNodesByLabel("OldLabel")
		require.NoError(t, err)
		assert.Len(t, oldNodes, 0)

		// New label should have node
		newNodes, err := engine.GetNodesByLabel("NewLabel")
		require.NoError(t, err)
		assert.Len(t, newNodes, 1)
	})

	t.Run("creates node if missing (upsert behavior)", func(t *testing.T) {
		// UpdateNode now has upsert behavior - creates if not exists
		node := testNode("upsert-test")
		node.Properties["foo"] = "bar"
		err := engine.UpdateNode(node)
		require.NoError(t, err, "UpdateNode should create if not exists")

		// Verify node was created
		retrieved, err := engine.GetNode(NodeID(prefixTestID("upsert-test")))
		require.NoError(t, err)
		assert.Equal(t, "bar", retrieved.Properties["foo"])
	})
}

func TestBadgerEngine_DeleteNode(t *testing.T) {
	engine := createTestBadgerEngine(t)

	t.Run("deletes existing node", func(t *testing.T) {
		node := testNode("n1")
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		err = engine.DeleteNode(NodeID(prefixTestID("n1")))
		require.NoError(t, err)

		_, err = engine.GetNode(NodeID(prefixTestID("n1")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("removes from label index", func(t *testing.T) {
		node := &Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"DeleteTest"}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		nodes, err := engine.GetNodesByLabel("DeleteTest")
		require.NoError(t, err)
		assert.Len(t, nodes, 1)

		err = engine.DeleteNode(NodeID(prefixTestID("n2")))
		require.NoError(t, err)

		nodes, err = engine.GetNodesByLabel("DeleteTest")
		require.NoError(t, err)
		assert.Len(t, nodes, 0)
	})

	t.Run("deletes connected edges", func(t *testing.T) {
		// Create nodes
		n1 := testNode("source")
		n2 := testNode("target")
		_, err := engine.CreateNode(n1)
		require.NoError(t, err)
		_, err = engine.CreateNode(n2)
		require.NoError(t, err)

		// Create edge
		edge := testEdge("e1", "source", "target", "CONNECTS")
		err = engine.CreateEdge(edge)
		require.NoError(t, err)

		// Delete source node
		err = engine.DeleteNode(NodeID(prefixTestID("source")))
		require.NoError(t, err)

		// Edge should be gone
		_, err = engine.GetEdge(EdgeID(prefixTestID("e1")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns ErrNotFound for missing node", func(t *testing.T) {
		err := engine.DeleteNode(NodeID(prefixTestID("nonexistent")))
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

// ============================================================================
// Edge CRUD Tests
// ============================================================================

func TestBadgerEngine_CreateEdge(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes first
	n1 := testNode("n1")
	n2 := testNode("n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	t.Run("creates edge successfully", func(t *testing.T) {
		edge := testEdge("e1", n1.ID, n2.ID, "KNOWS")
		err := engine.CreateEdge(edge)
		assert.NoError(t, err)
	})

	t.Run("returns ErrAlreadyExists for duplicate", func(t *testing.T) {
		edge := testEdge("e2", n1.ID, n2.ID, "FOLLOWS")
		err := engine.CreateEdge(edge)
		require.NoError(t, err)

		err = engine.CreateEdge(edge)
		assert.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("returns ErrNotFound for missing start node", func(t *testing.T) {
		edge := testEdge("e3", NodeID(prefixTestID("missing")), n2.ID, "TEST")
		err := engine.CreateEdge(edge)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns ErrNotFound for missing end node", func(t *testing.T) {
		edge := testEdge("e4", n1.ID, NodeID(prefixTestID("missing")), "TEST")
		err := engine.CreateEdge(edge)
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestBadgerEngine_GetEdge(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("n1")
	n2 := testNode("n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	t.Run("gets existing edge", func(t *testing.T) {
		original := &Edge{
			ID:            EdgeID(prefixTestID("e1")),
			StartNode:     NodeID(prefixTestID("n1")),
			EndNode:       NodeID(prefixTestID("n2")),
			Type:          "KNOWS",
			Properties:    map[string]any{"since": "2020"},
			CreatedAt:     time.Now().Truncate(time.Second),
			Confidence:    0.9,
			AutoGenerated: true,
		}
		err := engine.CreateEdge(original)
		require.NoError(t, err)

		retrieved, err := engine.GetEdge(EdgeID(prefixTestID("e1")))
		require.NoError(t, err)

		assert.Equal(t, original.ID, retrieved.ID)
		assert.Equal(t, original.StartNode, retrieved.StartNode)
		assert.Equal(t, original.EndNode, retrieved.EndNode)
		assert.Equal(t, original.Type, retrieved.Type)
		assert.InDelta(t, original.Confidence, retrieved.Confidence, 0.001)
		assert.Equal(t, original.AutoGenerated, retrieved.AutoGenerated)
	})

	t.Run("returns ErrNotFound for missing edge", func(t *testing.T) {
		_, err := engine.GetEdge(EdgeID(prefixTestID("nonexistent")))
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestBadgerEngine_UpdateEdge(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("n1")
	n2 := testNode("n2")
	n3 := testNode("n3")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	_, err = engine.CreateNode(n3)
	require.NoError(t, err)

	t.Run("updates edge properties", func(t *testing.T) {
		edge := testEdge("e1", n1.ID, n2.ID, "KNOWS")
		err := engine.CreateEdge(edge)
		require.NoError(t, err)

		edge.Properties["strength"] = "strong"
		edge.Confidence = 0.95
		err = engine.UpdateEdge(edge)
		require.NoError(t, err)

		retrieved, err := engine.GetEdge(EdgeID(prefixTestID("e1")))
		require.NoError(t, err)
		assert.Equal(t, "strong", retrieved.Properties["strength"])
		assert.InDelta(t, 0.95, retrieved.Confidence, 0.001)
	})

	t.Run("updates edge endpoints", func(t *testing.T) {
		edge := testEdge("e2", n1.ID, n2.ID, "FOLLOWS")
		err := engine.CreateEdge(edge)
		require.NoError(t, err)

		// Change endpoint
		edge.EndNode = n3.ID
		err = engine.UpdateEdge(edge)
		require.NoError(t, err)

		// Check indexes updated
		outgoing, err := engine.GetOutgoingEdges(n1.ID)
		require.NoError(t, err)

		found := false
		for _, e := range outgoing {
			if e.ID == edge.ID {
				assert.Equal(t, n3.ID, e.EndNode)
				found = true
				break
			}
		}
		assert.True(t, found)
	})

	t.Run("returns ErrNotFound for missing edge", func(t *testing.T) {
		edge := testEdge("nonexistent", n1.ID, n2.ID, "TEST")
		err := engine.UpdateEdge(edge)
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestBadgerEngine_DeleteEdge(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("n1")
	n2 := testNode("n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	t.Run("deletes existing edge", func(t *testing.T) {
		edge := testEdge("e1", n1.ID, n2.ID, "KNOWS")
		err := engine.CreateEdge(edge)
		require.NoError(t, err)

		err = engine.DeleteEdge(edge.ID)
		require.NoError(t, err)

		_, err = engine.GetEdge(edge.ID)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("removes from indexes", func(t *testing.T) {
		edge := testEdge("e2", "n1", "n2", "FOLLOWS")
		err := engine.CreateEdge(edge)
		require.NoError(t, err)

		outgoing, _ := engine.GetOutgoingEdges(NodeID(prefixTestID("n1")))
		initialCount := len(outgoing)

		err = engine.DeleteEdge(EdgeID(prefixTestID("e2")))
		require.NoError(t, err)

		outgoing, _ = engine.GetOutgoingEdges(NodeID(prefixTestID("n1")))
		assert.Len(t, outgoing, initialCount-1)
	})

	t.Run("returns ErrNotFound for missing edge", func(t *testing.T) {
		err := engine.DeleteEdge(EdgeID(prefixTestID("nonexistent")))
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestBadgerEngine_BulkDeleteNodes(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create multiple nodes
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("bulk-del-node-%d", i))),
			Labels: []string{"BulkTest"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	count, _ := engine.NodeCount()
	assert.Equal(t, int64(10), count)

	t.Run("deletes multiple nodes in single transaction", func(t *testing.T) {
		ids := []NodeID{NodeID(prefixTestID("bulk-del-node-0")), NodeID(prefixTestID("bulk-del-node-1")), NodeID(prefixTestID("bulk-del-node-2"))}
		err := engine.BulkDeleteNodes(ids)
		require.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(7), count)

		// Verify nodes are gone
		_, err = engine.GetNode(NodeID(prefixTestID("bulk-del-node-0")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("handles empty slice", func(t *testing.T) {
		err := engine.BulkDeleteNodes([]NodeID{})
		require.NoError(t, err)
	})

	t.Run("continues on not found", func(t *testing.T) {
		ids := []NodeID{NodeID(prefixTestID("nonexistent")), NodeID(prefixTestID("bulk-del-node-3")), NodeID(prefixTestID("also-nonexistent"))}
		err := engine.BulkDeleteNodes(ids)
		require.NoError(t, err) // Should not error

		_, err = engine.GetNode(NodeID(prefixTestID("bulk-del-node-3")))
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestBadgerEngine_BulkDeleteEdges(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes
	_, err := engine.CreateNode(&Node{ID: NodeID(prefixTestID("n1"))})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("n2"))})
	require.NoError(t, err)

	// Create multiple edges
	for i := 0; i < 10; i++ {
		edge := &Edge{
			ID:        EdgeID(prefixTestID(fmt.Sprintf("bulk-del-edge-%d", i))),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
			Type:      "TEST",
		}
		require.NoError(t, engine.CreateEdge(edge))
	}

	count, _ := engine.EdgeCount()
	assert.Equal(t, int64(10), count)

	t.Run("deletes multiple edges in single transaction", func(t *testing.T) {
		ids := []EdgeID{EdgeID(prefixTestID("bulk-del-edge-0")), EdgeID(prefixTestID("bulk-del-edge-1")), EdgeID(prefixTestID("bulk-del-edge-2"))}
		err := engine.BulkDeleteEdges(ids)
		require.NoError(t, err)

		count, _ := engine.EdgeCount()
		assert.Equal(t, int64(7), count)
	})

	t.Run("handles empty slice", func(t *testing.T) {
		err := engine.BulkDeleteEdges([]EdgeID{})
		require.NoError(t, err)
	})
}

// ============================================================================
// Query Tests
// ============================================================================

func TestBadgerEngine_GetNodesByLabel(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes with different labels
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID("user-" + string(rune('0'+i)))),
			Labels: []string{"User", "Person"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID("org-" + string(rune('0'+i)))),
			Labels: []string{"Organization"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	t.Run("returns nodes with label", func(t *testing.T) {
		users, err := engine.GetNodesByLabel("User")
		require.NoError(t, err)
		assert.Len(t, users, 5)
	})

	t.Run("returns nodes with shared label", func(t *testing.T) {
		persons, err := engine.GetNodesByLabel("Person")
		require.NoError(t, err)
		assert.Len(t, persons, 5)
	})

	t.Run("returns different label set", func(t *testing.T) {
		orgs, err := engine.GetNodesByLabel("Organization")
		require.NoError(t, err)
		assert.Len(t, orgs, 3)
	})

	t.Run("returns empty for unknown label", func(t *testing.T) {
		nodes, err := engine.GetNodesByLabel("Unknown")
		require.NoError(t, err)
		assert.Len(t, nodes, 0)
	})
}

func TestBadgerEngine_GetAllNodes(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create several nodes
	for i := 0; i < 10; i++ {
		node := testNode("n" + string(rune('0'+i)))
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	nodes := engine.GetAllNodes()
	assert.Len(t, nodes, 10)
}

func TestBadgerEngine_GetOutgoingEdges(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create a graph: n1 -> n2, n1 -> n3, n2 -> n3
	n1 := testNode("n1")
	n2 := testNode("n2")
	n3 := testNode("n3")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	_, err = engine.CreateNode(n3)
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(testEdge("e1", "n1", "n2", "A")))
	require.NoError(t, engine.CreateEdge(testEdge("e2", "n1", "n3", "B")))
	require.NoError(t, engine.CreateEdge(testEdge("e3", "n2", "n3", "C")))

	t.Run("returns outgoing edges", func(t *testing.T) {
		edges, err := engine.GetOutgoingEdges(NodeID(prefixTestID("n1")))
		require.NoError(t, err)
		assert.Len(t, edges, 2)
	})

	t.Run("returns one edge", func(t *testing.T) {
		edges, err := engine.GetOutgoingEdges(NodeID(prefixTestID("n2")))
		require.NoError(t, err)
		assert.Len(t, edges, 1)
	})

	t.Run("returns empty for leaf node", func(t *testing.T) {
		edges, err := engine.GetOutgoingEdges(NodeID(prefixTestID("n3")))
		require.NoError(t, err)
		assert.Len(t, edges, 0)
	})
}

func TestBadgerEngine_GetIncomingEdges(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("n1")
	n2 := testNode("n2")
	n3 := testNode("n3")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	_, err = engine.CreateNode(n3)
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(testEdge("e1", "n1", "n3", "A")))
	require.NoError(t, engine.CreateEdge(testEdge("e2", "n2", "n3", "B")))

	t.Run("returns incoming edges", func(t *testing.T) {
		edges, err := engine.GetIncomingEdges(NodeID(prefixTestID("n3")))
		require.NoError(t, err)
		assert.Len(t, edges, 2)
	})

	t.Run("returns empty for root node", func(t *testing.T) {
		edges, err := engine.GetIncomingEdges(NodeID(prefixTestID("n1")))
		require.NoError(t, err)
		assert.Len(t, edges, 0)
	})
}

func TestBadgerEngine_GetEdgesBetween(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("n1")
	n2 := testNode("n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(testEdge("e1", n1.ID, n2.ID, "A")))
	require.NoError(t, engine.CreateEdge(testEdge("e2", n1.ID, n2.ID, "B")))

	t.Run("returns all edges between nodes", func(t *testing.T) {
		edges, err := engine.GetEdgesBetween(n1.ID, n2.ID)
		require.NoError(t, err)
		assert.Len(t, edges, 2)
	})

	t.Run("returns empty for no connection", func(t *testing.T) {
		edges, err := engine.GetEdgesBetween(n2.ID, n1.ID)
		require.NoError(t, err)
		assert.Len(t, edges, 0)
	})
}

func TestBadgerEngine_GetEdgeBetween(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("n1")
	n2 := testNode("n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(testEdge("e1", n1.ID, n2.ID, "KNOWS")))
	require.NoError(t, engine.CreateEdge(testEdge("e2", n1.ID, n2.ID, "FOLLOWS")))

	t.Run("returns edge with matching type", func(t *testing.T) {
		edge := engine.GetEdgeBetween(n1.ID, n2.ID, "KNOWS")
		require.NotNil(t, edge)
		assert.Equal(t, "KNOWS", edge.Type)
	})

	t.Run("returns any edge with empty type", func(t *testing.T) {
		edge := engine.GetEdgeBetween(n1.ID, n2.ID, "")
		require.NotNil(t, edge)
	})

	t.Run("returns nil for no matching type", func(t *testing.T) {
		edge := engine.GetEdgeBetween(NodeID(prefixTestID("n1")), NodeID(prefixTestID("n2")), "BLOCKS")
		assert.Nil(t, edge)
	})
}

func TestBadgerEngine_GetEdgeBetweenIndexMaintenance(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("edge-index-n1")
	n2 := testNode("edge-index-n2")
	n3 := testNode("edge-index-n3")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	_, err = engine.CreateNode(n3)
	require.NoError(t, err)

	edge := testEdge("edge-index-e1", n1.ID, n2.ID, "KNOWS")
	require.NoError(t, engine.CreateEdge(edge))
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, true)

	updated := copyEdge(edge)
	updated.EndNode = n3.ID
	updated.Type = "FOLLOWS"
	require.NoError(t, engine.UpdateEdge(updated))

	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, false)
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n3.ID, "FOLLOWS", edge.ID, true)
	assert.Nil(t, engine.GetEdgeBetween(n1.ID, n2.ID, "KNOWS"))
	require.NotNil(t, engine.GetEdgeBetween(n1.ID, n3.ID, "FOLLOWS"))

	require.NoError(t, engine.DeleteEdge(edge.ID))
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n3.ID, "FOLLOWS", edge.ID, false)
	assert.Nil(t, engine.GetEdgeBetween(n1.ID, n3.ID, "FOLLOWS"))
}

func TestBadgerEngine_GetEdgeBetweenHeadIndexSelfHealsFromLegacyScan(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("edge-head-heal-n1")
	n2 := testNode("edge-head-heal-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	edge := testEdge("edge-head-heal-e1", n1.ID, n2.ID, "KNOWS")
	require.NoError(t, engine.CreateEdge(edge))
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Delete(engine.edgeBetweenHeadKeyFromStringIDs(n1.ID, n2.ID, "KNOWS")); err != nil {
			return err
		}
		return txn.Delete(engine.edgeBetweenIndexKeyFromStringIDs(n1.ID, n2.ID, "KNOWS", edge.ID))
	}))

	requireEdgeBetweenHeadEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, false)
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, false)
	got := engine.GetEdgeBetween(n1.ID, n2.ID, "KNOWS")
	require.NotNil(t, got)
	assert.Equal(t, edge.ID, got.ID)
	requireEdgeBetweenHeadEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, true)
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, true)
}

func TestBadgerEngine_GetEdgesBetweenKeepsSameTypeSetWithHead(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("edge-head-set-n1")
	n2 := testNode("edge-head-set-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	first := testEdge("edge-head-set-e1", n1.ID, n2.ID, "KNOWS")
	second := testEdge("edge-head-set-e2", n1.ID, n2.ID, "KNOWS")
	require.NoError(t, engine.CreateEdge(first))
	require.NoError(t, engine.CreateEdge(second))

	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n2.ID, "KNOWS", first.ID, true)
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n2.ID, "KNOWS", second.ID, true)
	requireEdgeBetweenHeadEntry(t, engine, n1.ID, n2.ID, "KNOWS", second.ID, true)
	edges, err := engine.GetEdgesBetween(n1.ID, n2.ID)
	require.NoError(t, err)
	require.Len(t, edges, 2)
	assert.NotNil(t, engine.GetEdgeBetween(n1.ID, n2.ID, "KNOWS"))
}

func TestBadgerEngine_GetEdgesBetweenSelfHealIsBounded(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("edge-heal-bound-n1")
	n2 := testNode("edge-heal-bound-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	edgeCount := edgeBetweenSelfHealMaxEdges + 5
	for i := 0; i < edgeCount; i++ {
		edge := testEdge(
			fmt.Sprintf("edge-heal-bound-e-%03d", i),
			n1.ID,
			n2.ID,
			fmt.Sprintf("REL_%03d", i),
		)
		require.NoError(t, engine.CreateEdge(edge))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			if err := txn.Delete(engine.edgeBetweenHeadKeyFromStringIDs(edge.StartNode, edge.EndNode, edge.Type)); err != nil {
				return err
			}
			return txn.Delete(engine.edgeBetweenIndexKeyFromStringIDs(edge.StartNode, edge.EndNode, edge.Type, edge.ID))
		}))
	}

	edges, err := engine.GetEdgesBetween(n1.ID, n2.ID)
	require.NoError(t, err)
	require.Len(t, edges, edgeCount)
	assert.Equal(t, edgeBetweenSelfHealMaxEdges, countEdgeBetweenSetEntries(t, engine, n1.ID, n2.ID))
}

func TestBadgerEngine_EdgeBetweenIndexBackfill(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("edge-index-backfill-n1")
	n2 := testNode("edge-index-backfill-n2")
	n3 := testNode("edge-index-backfill-n3")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	_, err = engine.CreateNode(n3)
	require.NoError(t, err)

	edge := testEdge("edge-index-backfill-e1", n1.ID, n2.ID, "KNOWS")
	staleEdgeID := EdgeID(prefixTestID("edge-index-backfill-stale"))
	require.NoError(t, engine.CreateEdge(edge))
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Delete(engine.edgeBetweenIndexKeyFromStringIDs(n1.ID, n2.ID, "KNOWS", edge.ID)); err != nil {
			return err
		}
		if err := txn.Delete(engine.edgeBetweenHeadKeyFromStringIDs(n1.ID, n2.ID, "KNOWS")); err != nil {
			return err
		}
		// Allocate num IDs for the stale entry so we can construct a
		// compact key for it (test simulates an orphan written under the
		// current numID scheme).
		n1Num, err := engine.idDict.resolveOrAllocateNodeNumIDInTxn(txn, n1.ID)
		if err != nil {
			return err
		}
		n3Num, err := engine.idDict.resolveOrAllocateNodeNumIDInTxn(txn, n3.ID)
		if err != nil {
			return err
		}
		staleEdgeNum, err := engine.idDict.resolveOrAllocateEdgeNumIDInTxn(txn, staleEdgeID)
		if err != nil {
			return err
		}
		if err := txn.Set(edgeBetweenIndexKey(n1Num, n3Num, "STALE", staleEdgeNum), []byte(staleEdgeID)); err != nil {
			return err
		}
		return txn.Delete(edgeBetweenIndexReadyKey)
	}))

	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, false)
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n3.ID, "STALE", staleEdgeID, true)
	requireEdgeBetweenHeadEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, false)
	require.NoError(t, engine.ensureEdgeBetweenIndex())
	require.Eventually(t, func() bool {
		return edgeBetweenIndexReady(t, engine) &&
			hasEdgeBetweenHeadEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID)
	}, 2*time.Second, 20*time.Millisecond)
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n2.ID, "KNOWS", edge.ID, true)
	requireEdgeBetweenIndexEntry(t, engine, n1.ID, n3.ID, "STALE", staleEdgeID, false)
	require.NotNil(t, engine.GetEdgeBetween(n1.ID, n2.ID, "KNOWS"))
}

func TestBadgerEngine_EdgeBetweenIndexBackfillRepairsStoreAsynchronously(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	n1 := testNode("edge-index-async-n1")
	n2 := testNode("edge-index-async-n2")
	_, err = engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	edge := testEdge("edge-index-async-e1", n1.ID, n2.ID, "KNOWS")
	require.NoError(t, engine.CreateEdge(edge))
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Delete(engine.edgeBetweenHeadKeyFromStringIDs(edge.StartNode, edge.EndNode, edge.Type)); err != nil {
			return err
		}
		if err := txn.Delete(engine.edgeBetweenIndexKeyFromStringIDs(edge.StartNode, edge.EndNode, edge.Type, edge.ID)); err != nil {
			return err
		}
		return txn.Delete(edgeBetweenIndexReadyKey)
	}))
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	got := reopened.GetEdgeBetween(n1.ID, n2.ID, "KNOWS")
	require.NotNil(t, got)
	assert.Equal(t, edge.ID, got.ID)
	require.Eventually(t, func() bool {
		return edgeBetweenIndexReady(t, reopened) &&
			hasEdgeBetweenHeadEntry(t, reopened, n1.ID, n2.ID, "KNOWS", edge.ID)
	}, 2*time.Second, 20*time.Millisecond)
}

// ============================================================================
// Bulk Operations Tests
// ============================================================================

func TestBadgerEngine_BulkCreateNodes(t *testing.T) {
	engine := createTestBadgerEngine(t)

	t.Run("creates multiple nodes", func(t *testing.T) {
		nodes := make([]*Node, 100)
		for i := 0; i < 100; i++ {
			// Use a better unique ID format
			nodes[i] = testNode("bulk-" + fmt.Sprintf("%03d", i))
		}

		err := engine.BulkCreateNodes(nodes)
		require.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.EqualValues(t, 100, count)
	})

	t.Run("is atomic - all or nothing", func(t *testing.T) {
		engine2 := createTestBadgerEngine(t)

		// First create a node
		node := testNode("existing")
		_, err := engine2.CreateNode(node)
		require.NoError(t, err)

		// Try to bulk create including a duplicate
		nodes := []*Node{
			testNode("new1"),
			testNode("existing"), // Duplicate!
			testNode("new2"),
		}

		err = engine2.BulkCreateNodes(nodes)
		assert.ErrorIs(t, err, ErrAlreadyExists)

		// Only the original node should exist
		count, _ := engine2.NodeCount()
		assert.EqualValues(t, 1, count)
	})

	t.Run("rejects invalid inputs and closed engine", func(t *testing.T) {
		err := engine.BulkCreateNodes([]*Node{nil})
		assert.ErrorIs(t, err, ErrInvalidData)

		err = engine.BulkCreateNodes([]*Node{{ID: ""}})
		assert.ErrorIs(t, err, ErrInvalidID)

		err = engine.BulkCreateNodes([]*Node{{ID: "plain-id", Labels: []string{"Test"}}})
		require.ErrorContains(t, err, "node ID must be prefixed with namespace")

		closed := createTestBadgerEngine(t)
		require.NoError(t, closed.Close())
		err = closed.BulkCreateNodes([]*Node{testNode("closed")})
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("notifies node created listeners", func(t *testing.T) {
		engine2 := createTestBadgerEngine(t)
		var created []NodeID
		engine2.OnNodeCreated(func(n *Node) {
			created = append(created, n.ID)
		})

		nodes := []*Node{
			testNode("notify-1"),
			testNode("notify-2"),
		}
		require.NoError(t, engine2.BulkCreateNodes(nodes))
		assert.ElementsMatch(t, []NodeID{nodes[0].ID, nodes[1].ID}, created)
	})
}

func TestBadgerEngine_BulkCreateEdges(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes first
	for i := 0; i < 10; i++ {
		_, err := engine.CreateNode(testNode(fmt.Sprintf("n%d", i)))
		require.NoError(t, err)
	}

	t.Run("creates multiple edges", func(t *testing.T) {
		edges := make([]*Edge, 20)
		for i := 0; i < 20; i++ {
			start := NodeID(prefixTestID(fmt.Sprintf("n%d", i%10)))
			end := NodeID(prefixTestID(fmt.Sprintf("n%d", (i+1)%10)))
			edges[i] = testEdge(fmt.Sprintf("e%02d", i), start, end, "CONNECTS")
		}

		err := engine.BulkCreateEdges(edges)
		require.NoError(t, err)

		count, _ := engine.EdgeCount()
		assert.EqualValues(t, 20, count)
	})

	t.Run("rejects invalid inputs duplicates and missing nodes", func(t *testing.T) {
		err := engine.BulkCreateEdges([]*Edge{nil})
		assert.ErrorIs(t, err, ErrInvalidData)

		err = engine.BulkCreateEdges([]*Edge{{ID: ""}})
		assert.ErrorIs(t, err, ErrInvalidID)

		err = engine.BulkCreateEdges([]*Edge{{
			ID:        EdgeID(prefixTestID("missing-edge")),
			StartNode: NodeID(prefixTestID("n0")),
			EndNode:   NodeID(prefixTestID("does-not-exist")),
			Type:      "CONNECTS",
		}})
		assert.ErrorIs(t, err, ErrNotFound)

		err = engine.BulkCreateEdges([]*Edge{{
			ID:        EdgeID(prefixTestID("e00")),
			StartNode: NodeID(prefixTestID("n0")),
			EndNode:   NodeID(prefixTestID("n1")),
			Type:      "CONNECTS",
		}})
		assert.ErrorIs(t, err, ErrAlreadyExists)

		closed := createTestBadgerEngine(t)
		require.NoError(t, closed.Close())
		err = closed.BulkCreateEdges([]*Edge{{
			ID:        EdgeID(prefixTestID("closed-edge")),
			StartNode: NodeID(prefixTestID("n0")),
			EndNode:   NodeID(prefixTestID("n1")),
			Type:      "CONNECTS",
		}})
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("notifies edge created listeners", func(t *testing.T) {
		engine2 := createTestBadgerEngine(t)
		_, err := engine2.CreateNode(testNode("edge-notify-1"))
		require.NoError(t, err)
		_, err = engine2.CreateNode(testNode("edge-notify-2"))
		require.NoError(t, err)

		var created []EdgeID
		engine2.OnEdgeCreated(func(e *Edge) {
			created = append(created, e.ID)
		})

		edges := []*Edge{testEdge("edge-notify", "edge-notify-1", "edge-notify-2", "LINK")}
		require.NoError(t, engine2.BulkCreateEdges(edges))
		assert.Equal(t, []EdgeID{edges[0].ID}, created)
	})
}

// ============================================================================
// Degree Functions Tests
// ============================================================================

func TestBadgerEngine_Degree(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create hub and spoke pattern
	hub := testNode("hub")
	_, err := engine.CreateNode(hub)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		spoke := testNode("spoke-" + string(rune('0'+i)))
		_, err := engine.CreateNode(spoke)
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(testEdge("out-"+string(rune('0'+i)), hub.ID, spoke.ID, "OUT")))
		require.NoError(t, engine.CreateEdge(testEdge("in-"+string(rune('0'+i)), spoke.ID, hub.ID, "IN")))
	}

	t.Run("GetOutDegree", func(t *testing.T) {
		degree := engine.GetOutDegree(NodeID(prefixTestID("hub")))
		assert.Equal(t, 5, degree)
	})

	t.Run("GetInDegree", func(t *testing.T) {
		degree := engine.GetInDegree(NodeID(prefixTestID("hub")))
		assert.Equal(t, 5, degree)
	})

	t.Run("returns 0 for non-existent node", func(t *testing.T) {
		assert.Equal(t, 0, engine.GetOutDegree(NodeID(prefixTestID("nonexistent"))))
		assert.Equal(t, 0, engine.GetInDegree(NodeID(prefixTestID("nonexistent"))))
	})
}

// ============================================================================
// Stats Tests
// ============================================================================

func TestBadgerEngine_Stats(t *testing.T) {
	engine := createTestBadgerEngine(t)

	t.Run("initial counts are zero", func(t *testing.T) {
		nodeCount, err := engine.NodeCount()
		require.NoError(t, err)
		assert.EqualValues(t, 0, nodeCount)

		edgeCount, err := engine.EdgeCount()
		require.NoError(t, err)
		assert.EqualValues(t, 0, edgeCount)
	})

	t.Run("counts increase after inserts", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			_, err := engine.CreateNode(testNode("n" + string(rune('0'+i))))
			require.NoError(t, err)
		}

		require.NoError(t, engine.CreateEdge(testEdge("e1", "n0", "n1", "A")))
		require.NoError(t, engine.CreateEdge(testEdge("e2", "n1", "n2", "B")))

		nodeCount, _ := engine.NodeCount()
		edgeCount, _ := engine.EdgeCount()

		assert.EqualValues(t, 10, nodeCount)
		assert.EqualValues(t, 2, edgeCount)
	})
}

// ============================================================================
// Persistence Tests
// ============================================================================

func TestBadgerEngine_Persistence(t *testing.T) {
	dir := t.TempDir()

	t.Run("data survives restart", func(t *testing.T) {
		// Create engine and add data
		engine1, err := NewBadgerEngine(dir)
		require.NoError(t, err)

		node := &Node{
			ID:         NodeID(prefixTestID("persistent")),
			Labels:     []string{"Test"},
			Properties: map[string]any{"value": "persisted"},
			CreatedAt:  time.Now(),
		}
		_, err = engine1.CreateNode(node)
		require.NoError(t, err)

		// Close
		require.NoError(t, engine1.Close())

		// Reopen
		engine2, err := NewBadgerEngine(dir)
		require.NoError(t, err)
		defer engine2.Close()

		// Verify data persisted
		retrieved, err := engine2.GetNode(NodeID(prefixTestID("persistent")))
		require.NoError(t, err)
		assert.Equal(t, "persisted", retrieved.Properties["value"])
	})

	t.Run("indexes persist", func(t *testing.T) {
		// Create fresh engine
		dir2 := t.TempDir()
		engine1, err := NewBadgerEngine(dir2)
		require.NoError(t, err)

		// Add nodes with labels
		for i := 0; i < 5; i++ {
			node := &Node{
				ID:     NodeID(prefixTestID("labeled-" + string(rune('0'+i)))),
				Labels: []string{"PersistLabel"},
			}
			_, err := engine1.CreateNode(node)
			require.NoError(t, err)
		}

		// Add edges
		_, err = engine1.CreateNode(&Node{ID: NodeID(prefixTestID("target")), Labels: []string{"Target"}})
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			edge := testEdge("persist-edge-"+string(rune('0'+i)), NodeID(prefixTestID("labeled-"+string(rune('0'+i)))), "target", "POINTS")
			require.NoError(t, engine1.CreateEdge(edge))
		}

		require.NoError(t, engine1.Close())

		// Reopen
		engine2, err := NewBadgerEngine(dir2)
		require.NoError(t, err)
		defer engine2.Close()

		// Verify label index works
		nodes, err := engine2.GetNodesByLabel("PersistLabel")
		require.NoError(t, err)
		assert.Len(t, nodes, 5)

		// Verify edge indexes work
		incoming, err := engine2.GetIncomingEdges(NodeID(prefixTestID("target")))
		require.NoError(t, err)
		assert.Len(t, incoming, 3)
	})

	t.Run("property index entries survive restart", func(t *testing.T) {
		dir3 := t.TempDir()
		engine1, err := NewBadgerEngine(dir3)
		require.NoError(t, err)

		nodes := []*Node{
			{ID: NodeID(prefixTestID("doc-1")), Labels: []string{"MongoDocument"}, Properties: map[string]any{"sourceId": "src-001"}},
			{ID: NodeID(prefixTestID("doc-2")), Labels: []string{"MongoDocument"}, Properties: map[string]any{"sourceId": "src-002"}},
			{ID: NodeID(prefixTestID("doc-3")), Labels: []string{"MongoDocument"}, Properties: map[string]any{"sourceId": "src-003"}},
		}
		for _, n := range nodes {
			_, err := engine1.CreateNode(n)
			require.NoError(t, err)
		}

		schema1 := engine1.GetSchemaForNamespace("test")
		require.NoError(t, schema1.AddPropertyIndex("idx_source_id", "MongoDocument", []string{"sourceId"}))
		for _, n := range nodes {
			require.NoError(t, schema1.PropertyIndexInsert("MongoDocument", "sourceId", n.ID, n.Properties["sourceId"]))
		}
		before := schema1.PropertyIndexLookup("MongoDocument", "sourceId", "src-002")
		require.Len(t, before, 1)
		require.Equal(t, NodeID(prefixTestID("doc-2")), before[0])

		require.NoError(t, engine1.Close())

		engine2, err := NewBadgerEngine(dir3)
		require.NoError(t, err)
		defer engine2.Close()

		schema2 := engine2.GetSchemaForNamespace("test")
		after := schema2.PropertyIndexLookup("MongoDocument", "sourceId", "src-002")
		require.Len(t, after, 1, "property index entries should be rebuilt on restart")
		require.Equal(t, NodeID(prefixTestID("doc-2")), after[0])
	})
}

// ============================================================================
// Concurrency Tests
// ============================================================================

func TestBadgerEngine_Concurrency(t *testing.T) {
	engine := createTestBadgerEngine(t)

	t.Run("concurrent reads", func(t *testing.T) {
		// Create some data with unique IDs
		for i := 0; i < 100; i++ {
			_, err := engine.CreateNode(testNode(fmt.Sprintf("conc-read-%03d", i)))
			require.NoError(t, err)
		}

		// Concurrent reads
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				nodes := engine.GetAllNodes()
				assert.GreaterOrEqual(t, len(nodes), 100)
			}(i)
		}
		wg.Wait()
	})

	t.Run("concurrent writes", func(t *testing.T) {
		engine2 := createTestBadgerEngine(t)

		var wg sync.WaitGroup
		errors := make(chan error, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				node := testNode(fmt.Sprintf("parallel-%03d", idx))

				_, err := engine2.CreateNode(node)
				require.NoError(t, err)
				if err != nil {
					errors <- err
				}
			}(i)
		}
		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("concurrent write error: %v", err)
		}

		count, _ := engine2.NodeCount()
		assert.EqualValues(t, 100, count)
	})
}

// ============================================================================
// Closed Engine Tests
// ============================================================================

func TestBadgerEngine_ClosedOperations(t *testing.T) {
	engine := createTestBadgerEngine(t)
	require.NoError(t, engine.Close())

	t.Run("CreateNode returns ErrStorageClosed", func(t *testing.T) {
		_, err := engine.CreateNode(testNode("test"))
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("GetNode returns ErrStorageClosed", func(t *testing.T) {
		_, err := engine.GetNode(NodeID(prefixTestID("test")))
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("GetNodesByLabel returns ErrStorageClosed", func(t *testing.T) {
		_, err := engine.GetNodesByLabel("Test")
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("NodeCount returns ErrStorageClosed", func(t *testing.T) {
		_, err := engine.NodeCount()
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("Close is idempotent", func(t *testing.T) {
		err := engine.Close()
		assert.NoError(t, err)
	})
}

// ============================================================================
// Utility Tests
// ============================================================================

func TestBadgerEngine_Size(t *testing.T) {
	engine, dir := createTestBadgerEngineOnDisk(t)
	defer engine.Close()

	// Add some data
	for i := 0; i < 100; i++ {
		node := &Node{
			ID:         NodeID(prefixTestID(fmt.Sprintf("size-%03d", i))),
			Labels:     []string{"SizeTest"},
			Properties: map[string]any{"data": "some test data to increase size"},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	// Force sync
	require.NoError(t, engine.Sync())

	// Check size (should be non-zero for disk engine)
	lsm, vlog := engine.Size()
	assert.True(t, lsm >= 0 || vlog >= 0, "Size should be trackable")

	// Check files exist
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	require.NoError(t, err)
	assert.True(t, len(files) > 0, "Data files should exist")
}

func TestBadgerEngine_Sync(t *testing.T) {
	engine, _ := createTestBadgerEngineOnDisk(t)
	defer engine.Close()

	// Add data
	_, err := engine.CreateNode(testNode("sync-test"))
	require.NoError(t, err)

	// Sync should not error
	err = engine.Sync()
	assert.NoError(t, err)
}

func TestBadgerEngine_RunGC(t *testing.T) {
	engine, _ := createTestBadgerEngineOnDisk(t)
	defer engine.Close()

	// Add and delete some data to create garbage
	for i := 0; i < 100; i++ {
		node := testNode(fmt.Sprintf("gc-%03d", i))
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	// Delete half
	for i := 0; i < 50; i++ {
		engine.DeleteNode(NodeID(prefixTestID(fmt.Sprintf("gc-%03d", i))))
	}

	// GC may or may not run depending on amount of garbage
	// Just ensure it doesn't error or panic
	_ = engine.RunGC()
}

// ============================================================================
// Constructor Tests
// ============================================================================

func TestNewBadgerEngine(t *testing.T) {
	t.Run("creates engine with valid path", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewBadgerEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		assert.NotNil(t, engine.db)
		assert.NotNil(t, engine.GetSchema())
	})

	t.Run("creates directory if not exists", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "subdir", "nested")
		engine, err := NewBadgerEngine(dir)
		require.NoError(t, err)
		defer engine.Close()

		// Verify directory was created
		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})
}

func TestNewBadgerEngineWithOptions(t *testing.T) {
	t.Run("respects InMemory option", func(t *testing.T) {
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{
			InMemory: true,
		})
		require.NoError(t, err)
		defer engine.Close()

		// Should work normally
		_, err = engine.CreateNode(testNode("test"))
		require.NoError(t, err)
	})

	t.Run("respects SyncWrites option", func(t *testing.T) {
		dir := t.TempDir()
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{
			DataDir:    dir,
			SyncWrites: true,
		})
		require.NoError(t, err)
		defer engine.Close()

		// Should work with sync writes
		_, err = engine.CreateNode(testNode("test"))
		require.NoError(t, err)
	})

	t.Run("rejects invalid serializer and encryption key", func(t *testing.T) {
		_, err := NewBadgerEngineWithOptions(BadgerOptions{
			InMemory:   true,
			Serializer: StorageSerializer("bogus"),
		})
		require.Error(t, err)

		_, err = NewBadgerEngineWithOptions(BadgerOptions{
			InMemory:      true,
			EncryptionKey: []byte("short"),
		})
		require.ErrorContains(t, err, "16, 24, or 32 bytes")
	})

	t.Run("applies cache defaults and overrides", func(t *testing.T) {
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{
			InMemory: true,
		})
		require.NoError(t, err)
		require.Equal(t, defaultBadgerNodeCacheMaxEntries, engine.nodeCacheMaxEntries)
		require.Equal(t, defaultBadgerEdgeTypeCacheMaxTypes, engine.edgeTypeCacheMaxTypes)
		require.Equal(t, defaultBadgerLabelFirstCacheMax, engine.labelFirstCacheMax)
		require.Equal(t, DefaultRetentionPolicyMaxVersionsPerKey, engine.retentionPolicy.MaxVersionsPerKey)
		require.Zero(t, engine.retentionPolicy.TTL)
		require.NoError(t, engine.Close())

		engine, err = NewBadgerEngineWithOptions(BadgerOptions{
			InMemory:                      true,
			HighPerformance:               true,
			NodeCacheMaxEntries:           7,
			EdgeTypeCacheMaxTypes:         9,
			LabelFirstNodeCacheMaxEntries: 11,
			EngineOptions: EngineOptions{
				RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 7, TTL: time.Hour},
			},
		})
		require.NoError(t, err)
		defer engine.Close()
		require.Equal(t, 7, engine.nodeCacheMaxEntries)
		require.Equal(t, 9, engine.edgeTypeCacheMaxTypes)
		require.Equal(t, 11, engine.labelFirstCacheMax)
		require.Equal(t, 7, engine.retentionPolicy.MaxVersionsPerKey)
		require.Equal(t, time.Hour, engine.retentionPolicy.TTL)
		_, err = engine.CreateNode(testNode("hp"))
		require.NoError(t, err)
	})

	t.Run("low memory mode also initializes successfully", func(t *testing.T) {
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{
			InMemory:  true,
			LowMemory: true,
		})
		require.NoError(t, err)
		defer engine.Close()

		_, err = engine.CreateNode(testNode("lm"))
		require.NoError(t, err)
	})
}

// ============================================================================
// Serialization Tests
// ============================================================================

func TestSerialization(t *testing.T) {
	t.Run("node round-trip", func(t *testing.T) {
		original := &Node{
			ID:              NodeID(prefixTestID("test-serialize")),
			Labels:          []string{"A", "B", "C"},
			Properties:      map[string]any{"string": "value", "number": float64(42), "bool": true},
			CreatedAt:       time.Now().Truncate(time.Second),
			UpdatedAt:       time.Now().Add(time.Hour).Truncate(time.Second),
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		}

		data, _, err := encodeNode(original)
		require.NoError(t, err)

		decoded, err := decodeNode(data)
		require.NoError(t, err)

		assert.Equal(t, original.ID, decoded.ID)
		assert.Equal(t, original.Labels, decoded.Labels)
		assert.Equal(t, original.Properties["string"], decoded.Properties["string"])
		assert.Equal(t, original.ChunkEmbeddings, decoded.ChunkEmbeddings)
	})

	t.Run("node with NamedEmbeddings round-trip", func(t *testing.T) {
		original := &Node{
			ID:     NodeID(prefixTestID("test-named-emb")),
			Labels: []string{"Document"},
			NamedEmbeddings: map[string][]float32{
				"default": {0.1, 0.2, 0.3},
				"title":   {0.4, 0.5, 0.6},
				"content": {0.7, 0.8, 0.9},
			},
		}

		data, _, err := encodeNode(original)
		require.NoError(t, err)

		decoded, err := decodeNode(data)
		require.NoError(t, err)

		assert.Equal(t, original.ID, decoded.ID)
		assert.Equal(t, len(original.NamedEmbeddings), len(decoded.NamedEmbeddings))
		assert.Equal(t, original.NamedEmbeddings["default"], decoded.NamedEmbeddings["default"])
		assert.Equal(t, original.NamedEmbeddings["title"], decoded.NamedEmbeddings["title"])
		assert.Equal(t, original.NamedEmbeddings["content"], decoded.NamedEmbeddings["content"])
	})

	t.Run("node with both NamedEmbeddings and ChunkEmbeddings", func(t *testing.T) {
		original := &Node{
			ID:     NodeID(prefixTestID("test-mixed-emb")),
			Labels: []string{"Document"},
			NamedEmbeddings: map[string][]float32{
				"default": {0.1, 0.2, 0.3},
			},
			ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}, {0.7, 0.8, 0.9}},
		}

		data, _, err := encodeNode(original)
		require.NoError(t, err)

		decoded, err := decodeNode(data)
		require.NoError(t, err)

		assert.Equal(t, original.NamedEmbeddings, decoded.NamedEmbeddings)
		assert.Equal(t, original.ChunkEmbeddings, decoded.ChunkEmbeddings)
	})

	t.Run("edge round-trip", func(t *testing.T) {
		original := &Edge{
			ID:            EdgeID(prefixTestID("test-edge")),
			StartNode:     NodeID(prefixTestID("start")),
			EndNode:       NodeID(prefixTestID("end")),
			Type:          "CONNECTS",
			Properties:    map[string]any{"weight": float64(1.5)},
			CreatedAt:     time.Now().Truncate(time.Second),
			Confidence:    0.95,
			AutoGenerated: true,
		}

		data, err := encodeEdge(original)
		require.NoError(t, err)

		decoded, err := decodeEdge(data)
		require.NoError(t, err)

		assert.Equal(t, original.ID, decoded.ID)
		assert.Equal(t, original.StartNode, decoded.StartNode)
		assert.Equal(t, original.EndNode, decoded.EndNode)
		assert.Equal(t, original.Type, decoded.Type)
		assert.InDelta(t, original.Confidence, decoded.Confidence, 0.001)
		assert.Equal(t, original.AutoGenerated, decoded.AutoGenerated)
	})
}

// ============================================================================
// Key Encoding Tests
// ============================================================================

func TestKeyEncoding(t *testing.T) {
	t.Run("nodeKey", func(t *testing.T) {
		key := nodeKey(NodeID(prefixTestID("test-node")))
		assert.Equal(t, prefixNode, key[0])
		assert.Equal(t, prefixTestID("test-node"), string(key[1:]))
	})

	t.Run("edgeKey", func(t *testing.T) {
		key := edgeKey(EdgeID(prefixTestID("test-edge")))
		assert.Equal(t, prefixEdge, key[0])
		assert.Equal(t, prefixTestID("test-edge"), string(key[1:]))
	})

	t.Run("labelIndexKey round-trips the numID suffix", func(t *testing.T) {
		key := labelIndexKey("Person", 123)
		assert.Equal(t, prefixLabelIndex, key[0])
		num, ok := extractNodeNumIDFromLabelIndex(key, len("person"))
		require.True(t, ok)
		assert.Equal(t, uint64(123), num)
	})

	t.Run("outgoingIndexKey is compact and round-trips edgeNumID", func(t *testing.T) {
		key := outgoingIndexKey(42, 100)
		assert.Equal(t, prefixOutgoingIndex, key[0])
		require.Len(t, key, 1+8+8)
		n, ok := extractEdgeNumIDFromOutgoingKey(key)
		require.True(t, ok)
		assert.Equal(t, uint64(100), n)
	})

	t.Run("incomingIndexKey is compact", func(t *testing.T) {
		key := incomingIndexKey(42, 100)
		assert.Equal(t, prefixIncomingIndex, key[0])
		require.Len(t, key, 1+8+8)
	})

	t.Run("edgeBetweenIndexKey is compact and prefix-scannable", func(t *testing.T) {
		// Post-dict-refactor: keys are (prefix + 8-byte start numID +
		// 8-byte end numID + type + \0 + 8-byte edge numID).
		key := edgeBetweenIndexKey(42, 43, "KNOWS", 100)
		assert.Equal(t, prefixEdgeBetweenIndex, key[0])
		assert.True(t, hasBytePrefix(key, edgeBetweenIndexPrefix(42, 43)))
		assert.True(t, hasBytePrefix(key, typedEdgeBetweenIndexPrefix(42, 43, "knows")))

		headKey := edgeBetweenHeadKey(42, 43, "KNOWS")
		assert.Equal(t, prefixEdgeBetweenHead, headKey[0])
		// Head key encodes only (start, end, type) with no edge ID — a
		// second edge of the same type reuses the same head key.
		require.Len(t, headKey, 1+8+8+len("knows"))
	})

	t.Run("extract helpers return empty on malformed keys", func(t *testing.T) {
		_, ok := extractEdgeNumIDFromOutgoingKey([]byte{prefixOutgoingIndex, 'x', 'y'})
		assert.False(t, ok)
		_, ok = extractNodeNumIDFromLabelIndex([]byte{prefixLabelIndex, 'P', 'e'}, len("Person"))
		assert.False(t, ok)
	})
}

// requireEdgeBetweenHeadEntry verifies the typed single-edge head index without
// using GetEdgeBetween, so fallback/self-heal logic cannot hide a missing head.
func requireEdgeBetweenHeadEntry(
	t *testing.T,
	engine *BadgerEngine,
	startID NodeID,
	endID NodeID,
	edgeType string,
	edgeID EdgeID,
	want bool,
) {
	t.Helper()

	startNum, _ := engine.idDict.lookupNodeNumID(startID)
	endNum, _ := engine.idDict.lookupNodeNumID(endID)
	key := edgeBetweenHeadKey(startNum, endNum, edgeType)
	var exists bool
	err := engine.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == nil {
			return item.Value(func(val []byte) error {
				exists = EdgeID(val) == edgeID
				return nil
			})
		}
		if err == badger.ErrKeyNotFound {
			exists = false
			return nil
		}
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, want, exists)
}

// hasEdgeBetweenHeadEntry reports whether the typed head points at edgeID.
func hasEdgeBetweenHeadEntry(
	t *testing.T,
	engine *BadgerEngine,
	startID NodeID,
	endID NodeID,
	edgeType string,
	edgeID EdgeID,
) bool {
	t.Helper()

	startNum, _ := engine.idDict.lookupNodeNumID(startID)
	endNum, _ := engine.idDict.lookupNodeNumID(endID)
	key := edgeBetweenHeadKey(startNum, endNum, edgeType)
	var exists bool
	err := engine.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == nil {
			return item.Value(func(val []byte) error {
				exists = EdgeID(val) == edgeID
				return nil
			})
		}
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
	require.NoError(t, err)
	return exists
}

// countEdgeBetweenSetEntries counts set-index entries for one node pair.
func countEdgeBetweenSetEntries(t *testing.T, engine *BadgerEngine, startID, endID NodeID) int {
	t.Helper()

	startNum, _ := engine.idDict.lookupNodeNumID(startID)
	endNum, _ := engine.idDict.lookupNodeNumID(endID)
	count := 0
	prefix := edgeBetweenIndexPrefix(startNum, endNum)
	err := engine.withView(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	return count
}

// edgeBetweenIndexReady reports whether the startup backfill marker is present.
func edgeBetweenIndexReady(t *testing.T, engine *BadgerEngine) bool {
	t.Helper()

	var ready bool
	err := engine.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(edgeBetweenIndexReadyKey)
		if err == nil {
			ready = true
			return nil
		}
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
	require.NoError(t, err)
	return ready
}

// requireEdgeBetweenIndexEntry verifies the direct relationship-existence index
// entry without using GetEdgeBetween, so the test catches accidental scan-only
// regressions in the storage layer.
func requireEdgeBetweenIndexEntry(
	t *testing.T,
	engine *BadgerEngine,
	startID NodeID,
	endID NodeID,
	edgeType string,
	edgeID EdgeID,
	want bool,
) {
	t.Helper()

	startNum, _ := engine.idDict.lookupNodeNumID(startID)
	endNum, _ := engine.idDict.lookupNodeNumID(endID)
	edgeNum, _ := engine.idDict.lookupEdgeNumID(edgeID)
	key := edgeBetweenIndexKey(startNum, endNum, edgeType, edgeNum)
	var exists bool
	err := engine.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			exists = true
			return nil
		}
		if err == badger.ErrKeyNotFound {
			exists = false
			return nil
		}
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, want, exists)
}

// hasBytePrefix keeps key-encoding assertions local to storage tests without
// pulling in bytes.HasPrefix just for one small invariant check.
func hasBytePrefix(value, prefix []byte) bool {
	if len(prefix) > len(value) {
		return false
	}
	for i := range prefix {
		if value[i] != prefix[i] {
			return false
		}
	}
	return true
}

// ============================================================================
// Interface Compliance Test
// ============================================================================

func TestBadgerEngine_ImplementsEngine(t *testing.T) {
	// This is a compile-time check
	var _ Engine = (*BadgerEngine)(nil)
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkBadgerEngine_CreateNode(b *testing.B) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node := &Node{
			ID:         NodeID(prefixTestID(fmt.Sprintf("bench-%06d", i))),
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}
		engine.CreateNode(node)
	}
}

func BenchmarkBadgerEngine_GetNode(b *testing.B) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID(fmt.Sprintf("bench-%06d", i))),
			Labels: []string{"Benchmark"},
		}
		engine.CreateNode(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 10000
		_, _ = engine.GetNode(NodeID(prefixTestID(fmt.Sprintf("bench-%06d", idx))))
	}
}

func BenchmarkBadgerEngine_BulkCreateNodes(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			engine, err := NewBadgerEngineInMemory()
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			nodes := make([]*Node, size)
			for i := 0; i < size; i++ {
				nodes[i] = &Node{
					ID:     NodeID(prefixTestID(fmt.Sprintf("bulk-%06d", i))),
					Labels: []string{"Bulk"},
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Reset engine between iterations
				engine.Close()
				engine, _ = NewBadgerEngineInMemory()
				engine.BulkCreateNodes(nodes)
			}
		})
	}
}

// ============================================================================
// Edge Update with Endpoint and Type Changes
// ============================================================================

func TestBadgerEngine_UpdateEdge_EndpointChange(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create three nodes
	n1 := testNode("ep-n1")
	n2 := testNode("ep-n2")
	n3 := testNode("ep-n3")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	_, err = engine.CreateNode(n3)
	require.NoError(t, err)

	// Create an edge from n1 -> n2
	edge := testEdge("ep-e1", "ep-n1", "ep-n2", "KNOWS")
	require.NoError(t, engine.CreateEdge(edge))

	// Verify edge exists
	got, err := engine.GetEdge(EdgeID(prefixTestID("ep-e1")))
	require.NoError(t, err)
	assert.Equal(t, NodeID(prefixTestID("ep-n1")), got.StartNode)
	assert.Equal(t, NodeID(prefixTestID("ep-n2")), got.EndNode)

	// Update edge to point n1 -> n3 (endpoint change)
	updated := &Edge{
		ID:         EdgeID(prefixTestID("ep-e1")),
		StartNode:  NodeID(prefixTestID("ep-n1")),
		EndNode:    NodeID(prefixTestID("ep-n3")),
		Type:       "KNOWS",
		Properties: map[string]interface{}{},
	}
	err = engine.UpdateEdge(updated)
	require.NoError(t, err)

	// Verify the edge was updated
	got, err = engine.GetEdge(EdgeID(prefixTestID("ep-e1")))
	require.NoError(t, err)
	assert.Equal(t, NodeID(prefixTestID("ep-n3")), got.EndNode)

	// Verify incoming edges updated: n3 should have the incoming edge, n2 should not
	incoming, err := engine.GetIncomingEdges(NodeID(prefixTestID("ep-n3")))
	require.NoError(t, err)
	assert.Len(t, incoming, 1)

	incoming, err = engine.GetIncomingEdges(NodeID(prefixTestID("ep-n2")))
	require.NoError(t, err)
	assert.Len(t, incoming, 0)
}

func TestBadgerEngine_UpdateEdge_TypeChange(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("tc-n1")
	n2 := testNode("tc-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	edge := testEdge("tc-e1", "tc-n1", "tc-n2", "KNOWS")
	require.NoError(t, engine.CreateEdge(edge))

	// Update edge type from KNOWS to LIKES
	updated := &Edge{
		ID:         EdgeID(prefixTestID("tc-e1")),
		StartNode:  NodeID(prefixTestID("tc-n1")),
		EndNode:    NodeID(prefixTestID("tc-n2")),
		Type:       "LIKES",
		Properties: map[string]interface{}{},
	}
	err = engine.UpdateEdge(updated)
	require.NoError(t, err)

	// Verify the type changed
	got, err := engine.GetEdge(EdgeID(prefixTestID("tc-e1")))
	require.NoError(t, err)
	assert.Equal(t, "LIKES", got.Type)

	// Verify edge type index: LIKES should have the edge, KNOWS should not
	likeEdges, err := engine.GetEdgesByType("LIKES")
	require.NoError(t, err)
	assert.Len(t, likeEdges, 1)

	knowEdges, err := engine.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	assert.Len(t, knowEdges, 0)
}

func TestBadgerEngine_UpdateEdge_NotFound(t *testing.T) {
	engine := createTestBadgerEngine(t)

	updated := &Edge{
		ID:         EdgeID(prefixTestID("nonexistent")),
		StartNode:  NodeID(prefixTestID("n1")),
		EndNode:    NodeID(prefixTestID("n2")),
		Type:       "REL",
		Properties: map[string]interface{}{},
	}
	err := engine.UpdateEdge(updated)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_UpdateEdge_NilAndEmptyID(t *testing.T) {
	engine := createTestBadgerEngine(t)

	err := engine.UpdateEdge(nil)
	assert.ErrorIs(t, err, ErrInvalidData)

	err = engine.UpdateEdge(&Edge{ID: ""})
	assert.ErrorIs(t, err, ErrInvalidID)
}

func TestBadgerEngine_ClosedEngineErrors(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	// All operations should return ErrStorageClosed on a closed engine
	_, err = engine.CreateNode(testNode("x"))
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = engine.CreateEdge(testEdge("x", "a", "b", "R"))
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, err = engine.GetNode(NodeID("x"))
	assert.ErrorIs(t, err, ErrStorageClosed)

	_, err = engine.GetEdge(EdgeID("x"))
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = engine.UpdateEdge(&Edge{ID: "x", StartNode: "a", EndNode: "b", Type: "R"})
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = engine.DeleteEdge(EdgeID("x"))
	assert.ErrorIs(t, err, ErrStorageClosed)

	err = engine.DeleteNode(NodeID("x"))
	assert.ErrorIs(t, err, ErrStorageClosed)
}

func TestBadgerEngine_GetEdge_EmptyID(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.GetEdge("")
	assert.ErrorIs(t, err, ErrInvalidID)
}

func TestBadgerEngine_UpdateEdge_MissingEndpoint(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes and edge
	n1 := testNode("me-n1")
	n2 := testNode("me-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	e := testEdge("me-e1", "me-n1", "me-n2", "KNOWS")
	require.NoError(t, engine.CreateEdge(e))

	// Try to update with a start node that doesn't exist
	updated := &Edge{
		ID:         EdgeID(prefixTestID("me-e1")),
		StartNode:  NodeID(prefixTestID("nonexistent")),
		EndNode:    NodeID(prefixTestID("me-n2")),
		Type:       "KNOWS",
		Properties: map[string]interface{}{},
	}
	err = engine.UpdateEdge(updated)
	assert.ErrorIs(t, err, ErrNotFound)
}
