package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMemoryEngine(t *testing.T) {
	engine := NewMemoryEngine()
	require.NotNil(t, engine)
	assert.NotNil(t, engine.nodes)
	assert.NotNil(t, engine.edges)
	assert.NotNil(t, engine.nodesByLabel)
	assert.NotNil(t, engine.outgoingEdges)
	assert.NotNil(t, engine.incomingEdges)
	assert.False(t, engine.closed)
}

// Node CRUD Tests

func TestMemoryEngine_CreateNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		node := &Node{
			ID:         "node-1",
			Labels:     []string{"Person", "Employee"},
			Properties: map[string]any{"name": "Alice", "age": 30},
		}

		err := engine.CreateNode(node)
		require.NoError(t, err)

		// Verify stored
		stored, err := engine.GetNode("node-1")
		require.NoError(t, err)
		assert.Equal(t, "node-1", string(stored.ID))
		assert.Equal(t, []string{"Person", "Employee"}, stored.Labels)
		assert.Equal(t, "Alice", stored.Properties["name"])
	})

	t.Run("nil node", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.CreateNode(nil)
		assert.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.CreateNode(&Node{ID: ""})
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("duplicate ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		node := &Node{ID: "node-1"}
		require.NoError(t, engine.CreateNode(node))

		err := engine.CreateNode(&Node{ID: "node-1"})
		assert.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("closed engine", func(t *testing.T) {
		engine := NewMemoryEngine()
		engine.Close()

		err := engine.CreateNode(&Node{ID: "node-1"})
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("deep copy prevents mutation", func(t *testing.T) {
		engine := NewMemoryEngine()
		props := map[string]any{"key": "original"}
		node := &Node{
			ID:         "node-1",
			Properties: props,
		}

		require.NoError(t, engine.CreateNode(node))

		// Mutate original
		props["key"] = "mutated"
		node.Properties["new"] = "value"

		// Verify stored value unchanged
		stored, _ := engine.GetNode("node-1")
		assert.Equal(t, "original", stored.Properties["key"])
		assert.Nil(t, stored.Properties["new"])
	})
}

func TestMemoryEngine_GetNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{
			ID:         "node-1",
			Labels:     []string{"Test"},
			Properties: map[string]any{"data": "value"},
		}))

		node, err := engine.GetNode("node-1")
		require.NoError(t, err)
		assert.Equal(t, "node-1", string(node.ID))
	})

	t.Run("not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.GetNode("nonexistent")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.GetNode("")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("closed engine", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "node-1"}))
		engine.Close()

		_, err := engine.GetNode("node-1")
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("returns copy not reference", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{
			ID:         "node-1",
			Properties: map[string]any{"key": "value"},
		}))

		node1, _ := engine.GetNode("node-1")
		node1.Properties["key"] = "mutated"

		node2, _ := engine.GetNode("node-1")
		assert.Equal(t, "value", node2.Properties["key"])
	})
}

func TestMemoryEngine_UpdateNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{
			ID:         "node-1",
			Labels:     []string{"Old"},
			Properties: map[string]any{"name": "Old Name"},
		}))

		err := engine.UpdateNode(&Node{
			ID:         "node-1",
			Labels:     []string{"New"},
			Properties: map[string]any{"name": "New Name"},
		})
		require.NoError(t, err)

		updated, _ := engine.GetNode("node-1")
		assert.Equal(t, []string{"New"}, updated.Labels)
		assert.Equal(t, "New Name", updated.Properties["name"])
	})

	t.Run("updates label index", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{
			ID:     "node-1",
			Labels: []string{"OldLabel"},
		}))

		require.NoError(t, engine.UpdateNode(&Node{
			ID:     "node-1",
			Labels: []string{"NewLabel"},
		}))

		// Old label should be empty
		oldNodes, _ := engine.GetNodesByLabel("OldLabel")
		assert.Empty(t, oldNodes)

		// New label should have node
		newNodes, _ := engine.GetNodesByLabel("NewLabel")
		require.Len(t, newNodes, 1)
		assert.Equal(t, "node-1", string(newNodes[0].ID))
	})

	t.Run("nil node", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.UpdateNode(nil)
		assert.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.UpdateNode(&Node{ID: "nonexistent"})
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestMemoryEngine_DeleteNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "node-1"}))

		err := engine.DeleteNode("node-1")
		require.NoError(t, err)

		_, err = engine.GetNode("node-1")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("removes from label index", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{
			ID:     "node-1",
			Labels: []string{"TestLabel"},
		}))

		require.NoError(t, engine.DeleteNode("node-1"))

		nodes, _ := engine.GetNodesByLabel("TestLabel")
		assert.Empty(t, nodes)
	})

	t.Run("cascades to outgoing edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "source"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "target"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "source",
			EndNode:   "target",
			Type:      "KNOWS",
		}))

		require.NoError(t, engine.DeleteNode("source"))

		_, err := engine.GetEdge("edge-1")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("cascades to incoming edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "source"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "target"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "source",
			EndNode:   "target",
			Type:      "KNOWS",
		}))

		require.NoError(t, engine.DeleteNode("target"))

		_, err := engine.GetEdge("edge-1")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.DeleteNode("nonexistent")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.DeleteNode("")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

// Edge CRUD Tests

func TestMemoryEngine_CreateEdge(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "node-1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "node-2"}))

		edge := &Edge{
			ID:         "edge-1",
			StartNode:  "node-1",
			EndNode:    "node-2",
			Type:       "KNOWS",
			Properties: map[string]any{"since": 2020},
		}

		err := engine.CreateEdge(edge)
		require.NoError(t, err)

		stored, err := engine.GetEdge("edge-1")
		require.NoError(t, err)
		assert.Equal(t, "edge-1", string(stored.ID))
		assert.Equal(t, NodeID("node-1"), stored.StartNode)
		assert.Equal(t, NodeID("node-2"), stored.EndNode)
		assert.Equal(t, "KNOWS", stored.Type)
	})

	t.Run("nil edge", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.CreateEdge(nil)
		assert.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.CreateEdge(&Edge{ID: ""})
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("duplicate ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "n1",
			EndNode:   "n2",
		}))

		err := engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "n1",
			EndNode:   "n2",
		})
		assert.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("start node not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "node-2"}))

		err := engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "nonexistent",
			EndNode:   "node-2",
		})
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("end node not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "node-1"}))

		err := engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "node-1",
			EndNode:   "nonexistent",
		})
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestMemoryEngine_GetEdge(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "n1",
			EndNode:   "n2",
			Type:      "REL",
		}))

		edge, err := engine.GetEdge("edge-1")
		require.NoError(t, err)
		assert.Equal(t, "edge-1", string(edge.ID))
	})

	t.Run("not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.GetEdge("nonexistent")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.GetEdge("")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestMemoryEngine_UpdateEdge(t *testing.T) {
	t.Run("success - update properties", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:         "edge-1",
			StartNode:  "n1",
			EndNode:    "n2",
			Type:       "KNOWS",
			Properties: map[string]any{"weight": 1},
		}))

		err := engine.UpdateEdge(&Edge{
			ID:         "edge-1",
			StartNode:  "n1",
			EndNode:    "n2",
			Type:       "KNOWS",
			Properties: map[string]any{"weight": 5},
		})
		require.NoError(t, err)

		updated, _ := engine.GetEdge("edge-1")
		assert.Equal(t, 5, updated.Properties["weight"])
	})

	t.Run("success - change endpoints", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n3"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "n1",
			EndNode:   "n2",
		}))

		err := engine.UpdateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "n2",
			EndNode:   "n3",
		})
		require.NoError(t, err)

		// Verify indexes updated
		outgoing, _ := engine.GetOutgoingEdges("n1")
		assert.Empty(t, outgoing)

		outgoing, _ = engine.GetOutgoingEdges("n2")
		require.Len(t, outgoing, 1)
		assert.Equal(t, "edge-1", string(outgoing[0].ID))
	})

	t.Run("not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.UpdateEdge(&Edge{ID: "nonexistent"})
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("new start node not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "n1",
			EndNode:   "n2",
		}))

		err := engine.UpdateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "nonexistent",
			EndNode:   "n2",
		})
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestMemoryEngine_DeleteEdge(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "n1",
			EndNode:   "n2",
		}))

		err := engine.DeleteEdge("edge-1")
		require.NoError(t, err)

		_, err = engine.GetEdge("edge-1")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("updates indexes", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "edge-1",
			StartNode: "n1",
			EndNode:   "n2",
		}))

		require.NoError(t, engine.DeleteEdge("edge-1"))

		outgoing, _ := engine.GetOutgoingEdges("n1")
		assert.Empty(t, outgoing)

		incoming, _ := engine.GetIncomingEdges("n2")
		assert.Empty(t, incoming)
	})

	t.Run("not found", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.DeleteEdge("nonexistent")
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		err := engine.DeleteEdge("")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

// Query Tests

func TestMemoryEngine_GetNodesByLabel(t *testing.T) {
	t.Run("returns matching nodes", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{
			ID:     "node-1",
			Labels: []string{"Person"},
		}))
		require.NoError(t, engine.CreateNode(&Node{
			ID:     "node-2",
			Labels: []string{"Person", "Employee"},
		}))
		require.NoError(t, engine.CreateNode(&Node{
			ID:     "node-3",
			Labels: []string{"Company"},
		}))

		persons, err := engine.GetNodesByLabel("Person")
		require.NoError(t, err)
		assert.Len(t, persons, 2)

		employees, err := engine.GetNodesByLabel("Employee")
		require.NoError(t, err)
		assert.Len(t, employees, 1)

		companies, err := engine.GetNodesByLabel("Company")
		require.NoError(t, err)
		assert.Len(t, companies, 1)
	})

	t.Run("returns empty for nonexistent label", func(t *testing.T) {
		engine := NewMemoryEngine()
		nodes, err := engine.GetNodesByLabel("NonexistentLabel")
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("closed engine", func(t *testing.T) {
		engine := NewMemoryEngine()
		engine.Close()
		_, err := engine.GetNodesByLabel("Test")
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("case insensitive matching (Neo4j compatible)", func(t *testing.T) {
		engine := NewMemoryEngine()
		// Create node with PascalCase label
		require.NoError(t, engine.CreateNode(&Node{
			ID:     "node-1",
			Labels: []string{"Person"},
		}))

		// Query with different cases - all should match
		lowercase, err := engine.GetNodesByLabel("person")
		require.NoError(t, err)
		assert.Len(t, lowercase, 1, "lowercase 'person' should match 'Person'")

		uppercase, err := engine.GetNodesByLabel("PERSON")
		require.NoError(t, err)
		assert.Len(t, uppercase, 1, "uppercase 'PERSON' should match 'Person'")

		mixedcase, err := engine.GetNodesByLabel("PeRsOn")
		require.NoError(t, err)
		assert.Len(t, mixedcase, 1, "mixed case 'PeRsOn' should match 'Person'")

		// Verify same node is returned
		assert.Equal(t, "node-1", string(lowercase[0].ID))
	})
}

func TestMemoryEngine_GetOutgoingEdges(t *testing.T) {
	t.Run("returns outgoing edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "center"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "target1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "target2"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e1",
			StartNode: "center",
			EndNode:   "target1",
		}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e2",
			StartNode: "center",
			EndNode:   "target2",
		}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e3",
			StartNode: "target1",
			EndNode:   "center",
		}))

		edges, err := engine.GetOutgoingEdges("center")
		require.NoError(t, err)
		assert.Len(t, edges, 2)
	})

	t.Run("returns empty for node with no outgoing", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "isolated"}))

		edges, err := engine.GetOutgoingEdges("isolated")
		require.NoError(t, err)
		assert.Empty(t, edges)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.GetOutgoingEdges("")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestMemoryEngine_GetIncomingEdges(t *testing.T) {
	t.Run("returns incoming edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "center"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "source1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "source2"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e1",
			StartNode: "source1",
			EndNode:   "center",
		}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e2",
			StartNode: "source2",
			EndNode:   "center",
		}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e3",
			StartNode: "center",
			EndNode:   "source1",
		}))

		edges, err := engine.GetIncomingEdges("center")
		require.NoError(t, err)
		assert.Len(t, edges, 2)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.GetIncomingEdges("")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestMemoryEngine_GetEdgesBetween(t *testing.T) {
	t.Run("returns edges between nodes", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n3"}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e1",
			StartNode: "n1",
			EndNode:   "n2",
			Type:      "KNOWS",
		}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e2",
			StartNode: "n1",
			EndNode:   "n2",
			Type:      "WORKS_WITH",
		}))
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        "e3",
			StartNode: "n1",
			EndNode:   "n3",
		}))

		edges, err := engine.GetEdgesBetween("n1", "n2")
		require.NoError(t, err)
		assert.Len(t, edges, 2)
	})

	t.Run("returns empty if no edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))

		edges, err := engine.GetEdgesBetween("n1", "n2")
		require.NoError(t, err)
		assert.Empty(t, edges)
	})

	t.Run("empty start ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.GetEdgesBetween("", "n2")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("empty end ID", func(t *testing.T) {
		engine := NewMemoryEngine()
		_, err := engine.GetEdgesBetween("n1", "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

// Bulk Operations Tests

func TestMemoryEngine_BulkCreateNodes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		nodes := []*Node{
			{ID: "node-1", Labels: []string{"A"}},
			{ID: "node-2", Labels: []string{"B"}},
			{ID: "node-3", Labels: []string{"A", "B"}},
		}

		err := engine.BulkCreateNodes(nodes)
		require.NoError(t, err)

		count, _ := engine.NodeCount()
		assert.Equal(t, int64(3), count)

		// Verify label indexes
		aNodes, _ := engine.GetNodesByLabel("A")
		assert.Len(t, aNodes, 2)
	})

	t.Run("atomic - fails on duplicate", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "existing"}))

		nodes := []*Node{
			{ID: "new-1"},
			{ID: "existing"}, // This should fail
			{ID: "new-2"},
		}

		err := engine.BulkCreateNodes(nodes)
		assert.ErrorIs(t, err, ErrAlreadyExists)

		// Verify none were created
		count, _ := engine.NodeCount()
		assert.Equal(t, int64(1), count) // Only "existing"
	})

	t.Run("fails on nil node", func(t *testing.T) {
		engine := NewMemoryEngine()
		nodes := []*Node{
			{ID: "node-1"},
			nil,
		}

		err := engine.BulkCreateNodes(nodes)
		assert.ErrorIs(t, err, ErrInvalidData)
	})
}

func TestMemoryEngine_BulkCreateEdges(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n3"}))

		edges := []*Edge{
			{ID: "e1", StartNode: "n1", EndNode: "n2"},
			{ID: "e2", StartNode: "n2", EndNode: "n3"},
			{ID: "e3", StartNode: "n1", EndNode: "n3"},
		}

		err := engine.BulkCreateEdges(edges)
		require.NoError(t, err)

		count, _ := engine.EdgeCount()
		assert.Equal(t, int64(3), count)
	})

	t.Run("atomic - fails on missing node", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
		require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))

		edges := []*Edge{
			{ID: "e1", StartNode: "n1", EndNode: "n2"},
			{ID: "e2", StartNode: "n2", EndNode: "nonexistent"},
		}

		err := engine.BulkCreateEdges(edges)
		assert.ErrorIs(t, err, ErrNotFound)

		// Verify none were created
		count, _ := engine.EdgeCount()
		assert.Equal(t, int64(0), count)
	})
}

// Count Tests

func TestMemoryEngine_NodeCount(t *testing.T) {
	engine := NewMemoryEngine()

	count, err := engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
	require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))

	count, err = engine.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestMemoryEngine_EdgeCount(t *testing.T) {
	engine := NewMemoryEngine()

	count, err := engine.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))
	require.NoError(t, engine.CreateNode(&Node{ID: "n2"}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "e1",
		StartNode: "n1",
		EndNode:   "n2",
	}))

	count, err = engine.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// Close Tests

func TestMemoryEngine_Close(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "n1"}))

		err := engine.Close()
		require.NoError(t, err)
		assert.True(t, engine.closed)
		assert.Nil(t, engine.nodes)
	})

	t.Run("all operations fail after close", func(t *testing.T) {
		engine := NewMemoryEngine()
		engine.Close()

		_, err := engine.NodeCount()
		assert.ErrorIs(t, err, ErrStorageClosed)

		_, err = engine.EdgeCount()
		assert.ErrorIs(t, err, ErrStorageClosed)
	})
}

// Concurrency Tests

func TestMemoryEngine_Concurrency(t *testing.T) {
	t.Run("concurrent reads", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "node-1", Labels: []string{"Test"}}))

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := engine.GetNode("node-1")
				assert.NoError(t, err)
			}()
		}
		wg.Wait()
	})

	t.Run("concurrent writes", func(t *testing.T) {
		engine := NewMemoryEngine()

		var wg sync.WaitGroup
		errors := make(chan error, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				// Use unique ID based on goroutine index
				nodeID := NodeID(fmt.Sprintf("node-%d", id))
				err := engine.CreateNode(&Node{ID: nodeID})
				if err != nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("Concurrent write error: %v", err)
		}
	})

	t.Run("concurrent read-write", func(t *testing.T) {
		engine := NewMemoryEngine()
		require.NoError(t, engine.CreateNode(&Node{ID: "shared"}))

		var wg sync.WaitGroup

		// Readers
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_, _ = engine.GetNode("shared")
				}
			}()
		}

		// Writers
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_ = engine.UpdateNode(&Node{
						ID:         "shared",
						Properties: map[string]any{"counter": id*10 + j},
					})
				}
			}(i)
		}

		wg.Wait()
	})
}

// Copy Tests

func TestMemoryEngine_copyNode(t *testing.T) {
	engine := NewMemoryEngine()

	original := &Node{
		ID:           "test",
		Labels:       []string{"A", "B"},
		Properties:   map[string]any{"key": "value"},
		Embedding:    []float32{0.1, 0.2, 0.3},
		DecayScore:   0.5,
		AccessCount:  10,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		LastAccessed: time.Now(),
	}

	copied := engine.copyNode(original)

	// Verify values copied
	assert.Equal(t, original.ID, copied.ID)
	assert.Equal(t, original.Labels, copied.Labels)
	assert.Equal(t, original.Properties["key"], copied.Properties["key"])
	assert.Equal(t, original.Embedding, copied.Embedding)

	// Verify independent copies
	original.Labels[0] = "X"
	original.Properties["key"] = "modified"
	original.Embedding[0] = 9.9

	assert.Equal(t, "A", copied.Labels[0])
	assert.Equal(t, "value", copied.Properties["key"])
	assert.Equal(t, float32(0.1), copied.Embedding[0])
}

func TestMemoryEngine_copyEdge(t *testing.T) {
	engine := NewMemoryEngine()

	original := &Edge{
		ID:            "test",
		StartNode:     "n1",
		EndNode:       "n2",
		Type:          "REL",
		Properties:    map[string]any{"weight": 5},
		Confidence:    0.9,
		AutoGenerated: true,
	}

	copied := engine.copyEdge(original)

	// Verify values copied
	assert.Equal(t, original.ID, copied.ID)
	assert.Equal(t, original.StartNode, copied.StartNode)
	assert.Equal(t, original.Type, copied.Type)
	assert.Equal(t, original.Properties["weight"], copied.Properties["weight"])

	// Verify independent
	original.Properties["weight"] = 999

	assert.Equal(t, 5, copied.Properties["weight"])
}

// Interface Compliance

func TestMemoryEngine_ImplementsEngine(t *testing.T) {
	var _ Engine = (*MemoryEngine)(nil)
}

// ========================================
// Tests for 0% coverage functions
// ========================================

func TestGetAllNodes(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Test empty storage
	t.Run("empty_storage", func(t *testing.T) {
		nodes := engine.GetAllNodes()
		if len(nodes) != 0 {
			t.Errorf("Expected 0 nodes, got %d", len(nodes))
		}
	})

	// Create some test nodes
	node1 := &Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	node2 := &Node{
		ID:         "node-2",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	node3 := &Node{
		ID:         "node-3",
		Labels:     []string{"Company"},
		Properties: map[string]interface{}{"name": "Acme"},
	}

	engine.CreateNode(node1)
	engine.CreateNode(node2)
	engine.CreateNode(node3)

	t.Run("all_nodes_returned", func(t *testing.T) {
		nodes := engine.GetAllNodes()
		if len(nodes) != 3 {
			t.Errorf("Expected 3 nodes, got %d", len(nodes))
		}

		// Verify all nodes are present
		foundIDs := make(map[NodeID]bool)
		for _, n := range nodes {
			foundIDs[n.ID] = true
		}
		if !foundIDs["node-1"] || !foundIDs["node-2"] || !foundIDs["node-3"] {
			t.Error("Not all nodes were returned")
		}
	})

	t.Run("returns_copies", func(t *testing.T) {
		nodes := engine.GetAllNodes()
		// Modify returned node
		nodes[0].Properties["modified"] = true

		// Original should be unchanged
		original, _ := engine.GetNode(nodes[0].ID)
		if _, exists := original.Properties["modified"]; exists {
			t.Error("Modification affected original node - not a copy")
		}
	})

	t.Run("closed_engine", func(t *testing.T) {
		closedEngine := NewMemoryEngine()
		closedEngine.CreateNode(&Node{ID: "test", Labels: []string{"Test"}})
		closedEngine.Close()

		nodes := closedEngine.GetAllNodes()
		if len(nodes) != 0 {
			t.Errorf("Closed engine should return empty slice, got %d nodes", len(nodes))
		}
	})
}

func TestGetEdgeBetween(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Create nodes
	engine.CreateNode(&Node{ID: "alice", Labels: []string{"Person"}})
	engine.CreateNode(&Node{ID: "bob", Labels: []string{"Person"}})
	engine.CreateNode(&Node{ID: "charlie", Labels: []string{"Person"}})

	// Create edges
	engine.CreateEdge(&Edge{
		ID:        "edge-1",
		Type:      "KNOWS",
		StartNode: "alice",
		EndNode:   "bob",
	})
	engine.CreateEdge(&Edge{
		ID:        "edge-2",
		Type:      "WORKS_WITH",
		StartNode: "alice",
		EndNode:   "bob",
	})
	engine.CreateEdge(&Edge{
		ID:        "edge-3",
		Type:      "KNOWS",
		StartNode: "bob",
		EndNode:   "charlie",
	})

	t.Run("find_existing_edge", func(t *testing.T) {
		edge := engine.GetEdgeBetween("alice", "bob", "KNOWS")
		if edge == nil {
			t.Fatal("Expected to find KNOWS edge between alice and bob")
		}
		if edge.Type != "KNOWS" {
			t.Errorf("Expected type KNOWS, got %s", edge.Type)
		}
	})

	t.Run("find_any_edge_type", func(t *testing.T) {
		// Empty type should match any
		edge := engine.GetEdgeBetween("alice", "bob", "")
		if edge == nil {
			t.Fatal("Expected to find edge between alice and bob")
		}
	})

	t.Run("no_edge_wrong_type", func(t *testing.T) {
		edge := engine.GetEdgeBetween("alice", "bob", "MARRIED_TO")
		if edge != nil {
			t.Error("Should not find MARRIED_TO edge")
		}
	})

	t.Run("no_edge_between_nodes", func(t *testing.T) {
		edge := engine.GetEdgeBetween("alice", "charlie", "KNOWS")
		if edge != nil {
			t.Error("Should not find edge between alice and charlie")
		}
	})

	t.Run("no_edge_nonexistent_source", func(t *testing.T) {
		edge := engine.GetEdgeBetween("unknown", "bob", "")
		if edge != nil {
			t.Error("Should not find edge from nonexistent node")
		}
	})

	t.Run("returns_copy", func(t *testing.T) {
		edge := engine.GetEdgeBetween("alice", "bob", "KNOWS")
		edge.Properties = map[string]interface{}{"modified": true}

		original := engine.GetEdgeBetween("alice", "bob", "KNOWS")
		if _, exists := original.Properties["modified"]; exists {
			t.Error("Modification affected original edge - not a copy")
		}
	})

	t.Run("closed_engine", func(t *testing.T) {
		closedEngine := NewMemoryEngine()
		closedEngine.CreateNode(&Node{ID: "a", Labels: []string{"X"}})
		closedEngine.CreateNode(&Node{ID: "b", Labels: []string{"X"}})
		closedEngine.CreateEdge(&Edge{ID: "e", Type: "T", StartNode: "a", EndNode: "b"})
		closedEngine.Close()

		edge := closedEngine.GetEdgeBetween("a", "b", "T")
		if edge != nil {
			t.Error("Closed engine should return nil")
		}
	})
}

func TestGetInDegree(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Create nodes
	engine.CreateNode(&Node{ID: "center", Labels: []string{"Node"}})
	engine.CreateNode(&Node{ID: "n1", Labels: []string{"Node"}})
	engine.CreateNode(&Node{ID: "n2", Labels: []string{"Node"}})
	engine.CreateNode(&Node{ID: "n3", Labels: []string{"Node"}})

	t.Run("zero_incoming", func(t *testing.T) {
		degree := engine.GetInDegree("center")
		if degree != 0 {
			t.Errorf("Expected 0 incoming edges, got %d", degree)
		}
	})

	// Add incoming edges to center
	engine.CreateEdge(&Edge{ID: "e1", Type: "POINTS_TO", StartNode: "n1", EndNode: "center"})
	engine.CreateEdge(&Edge{ID: "e2", Type: "POINTS_TO", StartNode: "n2", EndNode: "center"})
	engine.CreateEdge(&Edge{ID: "e3", Type: "LINKS", StartNode: "n3", EndNode: "center"})

	t.Run("three_incoming", func(t *testing.T) {
		degree := engine.GetInDegree("center")
		if degree != 3 {
			t.Errorf("Expected 3 incoming edges, got %d", degree)
		}
	})

	t.Run("nonexistent_node", func(t *testing.T) {
		degree := engine.GetInDegree("nonexistent")
		if degree != 0 {
			t.Errorf("Expected 0 for nonexistent node, got %d", degree)
		}
	})

	t.Run("closed_engine", func(t *testing.T) {
		closedEngine := NewMemoryEngine()
		closedEngine.CreateNode(&Node{ID: "x", Labels: []string{"X"}})
		closedEngine.CreateNode(&Node{ID: "y", Labels: []string{"X"}})
		closedEngine.CreateEdge(&Edge{ID: "e", Type: "T", StartNode: "y", EndNode: "x"})
		closedEngine.Close()

		degree := closedEngine.GetInDegree("x")
		if degree != 0 {
			t.Errorf("Closed engine should return 0, got %d", degree)
		}
	})
}

func TestGetOutDegree(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Create nodes
	engine.CreateNode(&Node{ID: "center", Labels: []string{"Node"}})
	engine.CreateNode(&Node{ID: "n1", Labels: []string{"Node"}})
	engine.CreateNode(&Node{ID: "n2", Labels: []string{"Node"}})

	t.Run("zero_outgoing", func(t *testing.T) {
		degree := engine.GetOutDegree("center")
		if degree != 0 {
			t.Errorf("Expected 0 outgoing edges, got %d", degree)
		}
	})

	// Add outgoing edges from center
	engine.CreateEdge(&Edge{ID: "e1", Type: "POINTS_TO", StartNode: "center", EndNode: "n1"})
	engine.CreateEdge(&Edge{ID: "e2", Type: "LINKS", StartNode: "center", EndNode: "n2"})

	t.Run("two_outgoing", func(t *testing.T) {
		degree := engine.GetOutDegree("center")
		if degree != 2 {
			t.Errorf("Expected 2 outgoing edges, got %d", degree)
		}
	})

	t.Run("nonexistent_node", func(t *testing.T) {
		degree := engine.GetOutDegree("nonexistent")
		if degree != 0 {
			t.Errorf("Expected 0 for nonexistent node, got %d", degree)
		}
	})

	t.Run("closed_engine", func(t *testing.T) {
		closedEngine := NewMemoryEngine()
		closedEngine.CreateNode(&Node{ID: "x", Labels: []string{"X"}})
		closedEngine.CreateNode(&Node{ID: "y", Labels: []string{"X"}})
		closedEngine.CreateEdge(&Edge{ID: "e", Type: "T", StartNode: "x", EndNode: "y"})
		closedEngine.Close()

		degree := closedEngine.GetOutDegree("x")
		if degree != 0 {
			t.Errorf("Closed engine should return 0, got %d", degree)
		}
	})
}
