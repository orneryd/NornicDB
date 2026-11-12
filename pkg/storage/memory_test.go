package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMemoryEngine(t *testing.T) {
	engine := newTestMemoryEngine(t)
	require.NotNil(t, engine)
	defer engine.Close()

	// Verify engine is functional through public interface
	nodeCount, err := engine.NodeCount(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(0), nodeCount)

	edgeCount, err := engine.EdgeCount(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(0), edgeCount)
}

// Node CRUD Tests

func TestMemoryEngine_CreateNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		node := &Node{
			ID:         NodeID(prefixTestID("node-1")),
			Labels:     []string{"Person", "Employee"},
			Properties: map[string]any{"name": "Alice", "age": 30},
		}

		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)

		// Verify stored
		stored, err := engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
		require.NoError(t, err)
		assert.Equal(t, prefixTestID("node-1"), string(stored.ID))
		assert.Equal(t, []string{"Person", "Employee"}, stored.Labels)
		assert.Equal(t, "Alice", stored.Properties["name"])
	})

	t.Run("nil node", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), nil)
		assert.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: ""})
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("duplicate ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		node := &Node{ID: NodeID(prefixTestID("node-1"))}
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)

		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-1"))})
		assert.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("closed engine", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		engine.Close()

		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-1"))})
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("deep copy prevents mutation", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		props := map[string]any{"key": "original"}
		node := &Node{
			ID:         NodeID(prefixTestID("node-1")),
			Properties: props,
		}

		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)

		// Mutate original
		props["key"] = "mutated"
		node.Properties["new"] = "value"

		// Verify stored value unchanged
		stored, _ := engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
		assert.Equal(t, "original", stored.Properties["key"])
		assert.Nil(t, stored.Properties["new"])
	})
}

func TestMemoryEngine_GetNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:         NodeID(prefixTestID("node-1")),
			Labels:     []string{"Test"},
			Properties: map[string]any{"data": "value"},
		})
		require.NoError(t, err)

		node, err := engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
		require.NoError(t, err)
		assert.Equal(t, prefixTestID("node-1"), string(node.ID))
	})

	t.Run("not found", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.GetNode(t.Context(), NodeID(prefixTestID("nonexistent")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.GetNode(t.Context(), "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("closed engine", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-1"))})
		require.NoError(t, err)
		engine.Close()

		_, err = engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("returns copy not reference", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:         NodeID(prefixTestID("node-1")),
			Properties: map[string]any{"key": "value"},
		})
		require.NoError(t, err)

		node1, _ := engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
		node1.Properties["key"] = "mutated"

		node2, _ := engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
		assert.Equal(t, "value", node2.Properties["key"])
	})
}

func TestMemoryEngine_UpdateNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:         NodeID(prefixTestID("node-1")),
			Labels:     []string{"Old"},
			Properties: map[string]any{"name": "Old Name"},
		})
		require.NoError(t, err)

		err = engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{
			ID:         NodeID(prefixTestID("node-1")),
			Labels:     []string{"New"},
			Properties: map[string]any{"name": "New Name"},
		}})
		require.NoError(t, err)

		updated, _ := engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
		assert.Equal(t, []string{"New"}, updated.Labels)
		assert.Equal(t, "New Name", updated.Properties["name"])
	})

	t.Run("updates label index", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:     NodeID(prefixTestID("node-1")),
			Labels: []string{"OldLabel"},
		})
		require.NoError(t, err)

		require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{
			ID:     NodeID(prefixTestID("node-1")),
			Labels: []string{"NewLabel"},
		}}))

		// Old label should be empty
		oldNodes, _ := engine.GetNodesByLabel(context.Background(), "OldLabel")
		assert.Empty(t, oldNodes)

		// New label should have node
		newNodes, _ := engine.GetNodesByLabel(context.Background(), "NewLabel")
		require.Len(t, newNodes, 1)
		assert.Equal(t, prefixTestID("node-1"), string(newNodes[0].ID))
	})

	t.Run("nil node", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		err := engine.UpdateNode(t.Context(), NodeUpdateRequest{After: nil})
		assert.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("upsert creates if not exists", func(t *testing.T) {
		// UpdateNode now has upsert behavior - creates if not exists
		engine := newTestMemoryEngine(t)
		err := engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{
			ID:         NodeID(prefixTestID("new-node")),
			Labels:     []string{"Created"},
			Properties: map[string]any{"via": "UpdateNode"},
		}})
		assert.NoError(t, err)

		// Verify the node was created
		node, err := engine.GetNode(t.Context(), NodeID(prefixTestID("new-node")))
		assert.NoError(t, err)
		assert.Equal(t, []string{"Created"}, node.Labels)
		assert.Equal(t, "UpdateNode", node.Properties["via"])
	})
}

func TestMemoryEngine_DeleteNode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-1"))})
		require.NoError(t, err)

		err = engine.DeleteNode(t.Context(), NodeID(prefixTestID("node-1")))
		require.NoError(t, err)

		_, err = engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("removes from label index", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:     NodeID(prefixTestID("node-1")),
			Labels: []string{"TestLabel"},
		})
		require.NoError(t, err)

		require.NoError(t, engine.DeleteNode(t.Context(), NodeID(prefixTestID("node-1"))))

		nodes, _ := engine.GetNodesByLabel(context.Background(), "TestLabel")
		assert.Empty(t, nodes)
	})

	t.Run("cascades to outgoing edges", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("source"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("target"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("source")),
			EndNode:   NodeID(prefixTestID("target")),
			Type:      "KNOWS",
		}))

		require.NoError(t, engine.DeleteNode(t.Context(), NodeID(prefixTestID("source"))))

		_, err = engine.GetEdge(t.Context(), EdgeID(prefixTestID("edge-1")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("cascades to incoming edges", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("source"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("target"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("source")),
			EndNode:   NodeID(prefixTestID("target")),
			Type:      "KNOWS",
		}))

		require.NoError(t, engine.DeleteNode(t.Context(), NodeID(prefixTestID("target"))))

		_, err = engine.GetEdge(t.Context(), EdgeID(prefixTestID("edge-1")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("not found", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		err := engine.DeleteNode(t.Context(), NodeID(prefixTestID("nonexistent")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		err := engine.DeleteNode(t.Context(), "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

// Edge CRUD Tests

func TestMemoryEngine_CreateEdge(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-2"))})
		require.NoError(t, err)

		edge := &Edge{
			ID:         EdgeID(prefixTestID("edge-1")),
			StartNode:  NodeID(prefixTestID("node-1")),
			EndNode:    NodeID(prefixTestID("node-2")),
			Type:       "KNOWS",
			Properties: map[string]any{"since": 2020},
		}

		err = engine.CreateEdge(t.Context(), edge)
		require.NoError(t, err)

		stored, err := engine.GetEdge(t.Context(), EdgeID(prefixTestID("edge-1")))
		require.NoError(t, err)
		assert.Equal(t, prefixTestID("edge-1"), string(stored.ID))
		assert.Equal(t, NodeID(prefixTestID("node-1")), stored.StartNode)
		assert.Equal(t, NodeID(prefixTestID("node-2")), stored.EndNode)
		assert.Equal(t, "KNOWS", stored.Type)
	})

	t.Run("nil edge", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		err := engine.CreateEdge(t.Context(), nil)
		assert.ErrorIs(t, err, ErrInvalidData)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		err := engine.CreateEdge(t.Context(), &Edge{ID: ""})
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("duplicate ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
		}))

		err = engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
		})
		assert.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("start node not found", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-2"))})
		require.NoError(t, err)

		err = engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("nonexistent")),
			EndNode:   NodeID(prefixTestID("node-2")),
		})
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("end node not found", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-1"))})
		require.NoError(t, err)

		err = engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("node-1")),
			EndNode:   NodeID(prefixTestID("nonexistent")),
		})
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestMemoryEngine_GetEdge(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
			Type:      "REL",
		}))

		edge, err := engine.GetEdge(t.Context(), EdgeID(prefixTestID("edge-1")))
		require.NoError(t, err)
		assert.Equal(t, prefixTestID("edge-1"), string(edge.ID))
	})

	t.Run("not found", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.GetEdge(t.Context(), EdgeID(prefixTestID("nonexistent")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.GetEdge(t.Context(), "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

func TestMemoryEngine_UpdateEdge(t *testing.T) {
	t.Run("success - update properties", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:         EdgeID(prefixTestID("edge-1")),
			StartNode:  NodeID(prefixTestID("n1")),
			EndNode:    NodeID(prefixTestID("n2")),
			Type:       "KNOWS",
			Properties: map[string]any{"weight": 1},
		}))

		err = engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{
			ID:         EdgeID(prefixTestID("edge-1")),
			StartNode:  NodeID(prefixTestID("n1")),
			EndNode:    NodeID(prefixTestID("n2")),
			Type:       "KNOWS",
			Properties: map[string]any{"weight": 5},
		}})
		require.NoError(t, err)

		updated, _ := engine.GetEdge(t.Context(), EdgeID(prefixTestID("edge-1")))
		// Gob may decode integers as different types (int, int8, int16, int32, int64, etc.)
		// Normalize to int for comparison
		var weightVal int
		switch v := updated.Properties["weight"].(type) {
		case int:
			weightVal = v
		case int8:
			weightVal = int(v)
		case int16:
			weightVal = int(v)
		case int32:
			weightVal = int(v)
		case int64:
			weightVal = int(v)
		case uint8:
			weightVal = int(v)
		case uint16:
			weightVal = int(v)
		case uint32:
			weightVal = int(v)
		case uint64:
			weightVal = int(v)
		default:
			t.Fatalf("unexpected type for 'weight': %T", v)
		}
		assert.Equal(t, 5, weightVal)
	})

	t.Run("success - change endpoints", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n3"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
		}))

		err = engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("n2")),
			EndNode:   NodeID(prefixTestID("n3")),
		}})
		require.NoError(t, err)

		// Verify indexes updated
		outgoing, _ := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: NodeID(prefixTestID("n1"))})
		assert.Empty(t, outgoing)

		outgoing, _ = engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: NodeID(prefixTestID("n2"))})
		require.Len(t, outgoing, 1)
		assert.Equal(t, prefixTestID("edge-1"), string(outgoing[0].ID))
	})

	t.Run("not found", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		err := engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: EdgeID(prefixTestID("nonexistent"))}})
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("new start node not found", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
		}))

		err = engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("nonexistent")),
			EndNode:   NodeID(prefixTestID("n2")),
		}})
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestMemoryEngine_DeleteEdge(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
		}))

		err = engine.DeleteEdge(t.Context(), EdgeID(prefixTestID("edge-1")))
		require.NoError(t, err)

		_, err = engine.GetEdge(t.Context(), EdgeID(prefixTestID("edge-1")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("updates indexes", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("edge-1")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
		}))

		require.NoError(t, engine.DeleteEdge(t.Context(), EdgeID(prefixTestID("edge-1"))))

		outgoing, _ := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: NodeID(prefixTestID("n1"))})
		assert.Empty(t, outgoing)

		incoming, _ := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: NodeID(prefixTestID("n2"))})
		assert.Empty(t, incoming)
	})

	t.Run("not found", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		err := engine.DeleteEdge(t.Context(), EdgeID(prefixTestID("nonexistent")))
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		err := engine.DeleteEdge(t.Context(), "")
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

// Query Tests

func TestMemoryEngine_GetNodesByLabel(t *testing.T) {
	t.Run("returns matching nodes", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:     NodeID(prefixTestID("node-1")),
			Labels: []string{"Person"},
		})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{
			ID:     NodeID(prefixTestID("node-2")),
			Labels: []string{"Person", "Employee"},
		})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{
			ID:     NodeID(prefixTestID("node-3")),
			Labels: []string{"Company"},
		})
		require.NoError(t, err)

		persons, err := engine.GetNodesByLabel(context.Background(), "Person")
		require.NoError(t, err)
		assert.Len(t, persons, 2)

		employees, err := engine.GetNodesByLabel(context.Background(), "Employee")
		require.NoError(t, err)
		assert.Len(t, employees, 1)

		companies, err := engine.GetNodesByLabel(context.Background(), "Company")
		require.NoError(t, err)
		assert.Len(t, companies, 1)
	})

	t.Run("returns empty for nonexistent label", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		nodes, err := engine.GetNodesByLabel(context.Background(), "NonexistentLabel")
		require.NoError(t, err)
		assert.Empty(t, nodes)
	})

	t.Run("closed engine", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		engine.Close()
		_, err := engine.GetNodesByLabel(context.Background(), "Test")
		assert.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("labels are case-sensitive, as in Neo4j (#862)", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:     NodeID(prefixTestID("node-1")),
			Labels: []string{"Person"},
		})
		require.NoError(t, err)

		exact, err := engine.GetNodesByLabel(context.Background(), "Person")
		require.NoError(t, err)
		require.Len(t, exact, 1)
		assert.Equal(t, prefixTestID("node-1"), string(exact[0].ID))

		for _, other := range []string{"person", "PERSON", "PeRsOn"} {
			nodes, err := engine.GetNodesByLabel(context.Background(), other)
			require.NoError(t, err)
			assert.Empty(t, nodes, "%q is another label", other)
		}
	})
}

// memoryDirectionalEdgeCase is one directional edge-lookup case. A case with
// partners builds the shared two-partner fixture, an isolated case builds only
// that node, and a case with neither runs against the empty engine.
type memoryDirectionalEdgeCase struct {
	name      string
	partners  [2]string
	towardHub bool
	isolated  string
	query     string
	wantCount int
	wantErr   error
}

// memoryDirectionalEdgeFixture builds the graph the outgoing and incoming
// edge-lookup cases share: a "center" hub, two partner nodes, e1/e2 joining the
// hub to one partner each in the queried direction and e3 joining it the other
// way. towardHub selects the incoming shape; otherwise the edges point away.
func memoryDirectionalEdgeFixture(t *testing.T, engine *MemoryEngine, partners [2]string, towardHub bool) {
	t.Helper()
	for _, id := range []string{"center", partners[0], partners[1]} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID(id))})
		require.NoError(t, err)
	}
	for _, spec := range [][3]string{
		{"e1", partners[0], "center"},
		{"e2", partners[1], "center"},
		{"e3", "center", partners[0]},
	} {
		start, end := spec[1], spec[2]
		if !towardHub {
			start, end = end, start
		}
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID(spec[0])),
			StartNode: NodeID(prefixTestID(start)),
			EndNode:   NodeID(prefixTestID(end)),
		}))
	}
}

// runMemoryDirectionalEdgeCases owns the fixture and the assertions for both
// lookup directions; each caller supplies only its own lookup so the outgoing
// and incoming contracts stay distinct while the graph and expectations live
// once.
func runMemoryDirectionalEdgeCases(t *testing.T, lookup func(*MemoryEngine, NodeID) ([]*Edge, error), cases []memoryDirectionalEdgeCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			if tc.partners[0] != "" {
				memoryDirectionalEdgeFixture(t, engine, tc.partners, tc.towardHub)
			}
			if tc.isolated != "" {
				_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID(tc.isolated))})
				require.NoError(t, err)
			}
			id := NodeID(tc.query)
			if tc.query != "" {
				id = NodeID(prefixTestID(tc.query))
			}
			edges, err := lookup(engine, id)
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.wantCount == 0 {
				assert.Empty(t, edges)
				return
			}
			assert.Len(t, edges, tc.wantCount)
		})
	}
}

func TestMemoryEngine_GetOutgoingEdges(t *testing.T) {
	runMemoryDirectionalEdgeCases(t, func(engine *MemoryEngine, id NodeID) ([]*Edge, error) {
		return engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: id})
	}, []memoryDirectionalEdgeCase{
		{name: "returns outgoing edges", partners: [2]string{"target1", "target2"}, query: "center", wantCount: 2},
		{name: "returns empty for node with no outgoing", isolated: "isolated", query: "isolated"},
		{name: "empty ID", wantErr: ErrInvalidID},
	})
}

func TestMemoryEngine_GetIncomingEdges(t *testing.T) {
	runMemoryDirectionalEdgeCases(t, func(engine *MemoryEngine, id NodeID) ([]*Edge, error) {
		return engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: id})
	}, []memoryDirectionalEdgeCase{
		{name: "returns incoming edges", partners: [2]string{"source1", "source2"}, towardHub: true, query: "center", wantCount: 2},
		{name: "empty ID", wantErr: ErrInvalidID},
	})
}

func TestMemoryEngine_GetEdgesBetween(t *testing.T) {
	t.Run("returns edges between nodes", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n3"))})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("e1")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
			Type:      "KNOWS",
		}))
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("e2")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n2")),
			Type:      "WORKS_WITH",
		}))
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID:        EdgeID(prefixTestID("e3")),
			StartNode: NodeID(prefixTestID("n1")),
			EndNode:   NodeID(prefixTestID("n3")),
		}))

		edges, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: NodeID(prefixTestID("n1")), Other: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		assert.Len(t, edges, 2)
	})

	t.Run("returns empty if no edges", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)

		edges, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: NodeID(prefixTestID("n1")), Other: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		assert.Empty(t, edges)
	})

	t.Run("empty start ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "", Other: "n2"})
		assert.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("empty end ID", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "n1", Other: ""})
		assert.ErrorIs(t, err, ErrInvalidID)
	})
}

// Bulk Operations Tests

func TestMemoryEngine_BulkCreateNodes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		nodes := []*Node{
			{ID: NodeID(prefixTestID("node-1")), Labels: []string{"A"}},
			{ID: NodeID(prefixTestID("node-2")), Labels: []string{"B"}},
			{ID: NodeID(prefixTestID("node-3")), Labels: []string{"A", "B"}},
		}

		err := engine.BulkCreateNodes(t.Context(), nodes)
		require.NoError(t, err)

		count, _ := engine.NodeCount(t.Context())
		assert.Equal(t, int64(3), count)

		// Verify label indexes
		aNodes, _ := engine.GetNodesByLabel(context.Background(), "A")
		assert.Len(t, aNodes, 2)
	})

	t.Run("atomic - fails on duplicate", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("existing"))})
		require.NoError(t, err)

		nodes := []*Node{
			{ID: NodeID(prefixTestID("new-1"))},
			{ID: NodeID(prefixTestID("existing"))}, // This should fail
			{ID: NodeID(prefixTestID("new-2"))},
		}

		err = engine.BulkCreateNodes(t.Context(), nodes)
		assert.ErrorIs(t, err, ErrAlreadyExists)

		// Verify none were created
		count, _ := engine.NodeCount(t.Context())
		assert.Equal(t, int64(1), count) // Only "existing"
	})

	t.Run("fails on nil node", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		nodes := []*Node{
			{ID: NodeID(prefixTestID("node-1"))},
			nil,
		}

		err := engine.BulkCreateNodes(t.Context(), nodes)
		assert.ErrorIs(t, err, ErrInvalidData)
	})
}

func TestMemoryEngine_BulkCreateEdges(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n3"))})
		require.NoError(t, err)

		edges := []*Edge{
			{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n2"))},
			{ID: EdgeID(prefixTestID("e2")), StartNode: NodeID(prefixTestID("n2")), EndNode: NodeID(prefixTestID("n3"))},
			{ID: EdgeID(prefixTestID("e3")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n3"))},
		}

		err = engine.BulkCreateEdges(t.Context(), edges)
		require.NoError(t, err)

		count, _ := engine.EdgeCount(t.Context())
		assert.Equal(t, int64(3), count)
	})

	t.Run("atomic - fails on missing node", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
		require.NoError(t, err)

		edges := []*Edge{
			{ID: EdgeID(prefixTestID("e1")), StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("n2"))},
			{ID: EdgeID(prefixTestID("e2")), StartNode: NodeID(prefixTestID("n2")), EndNode: NodeID(prefixTestID("nonexistent"))},
		}

		err = engine.BulkCreateEdges(t.Context(), edges)
		assert.ErrorIs(t, err, ErrNotFound)

		// Verify none were created
		count, _ := engine.EdgeCount(t.Context())
		assert.Equal(t, int64(0), count)
	})
}

// Count Tests

func TestMemoryEngine_NodeCount(t *testing.T) {
	engine := newTestMemoryEngine(t)

	count, err := engine.NodeCount(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
	require.NoError(t, err)

	count, err = engine.NodeCount(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestMemoryEngine_EdgeCount(t *testing.T) {
	engine := newTestMemoryEngine(t)

	count, err := engine.EdgeCount(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2"))})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
		ID:        EdgeID(prefixTestID("e1")),
		StartNode: NodeID(prefixTestID("n1")),
		EndNode:   NodeID(prefixTestID("n2")),
	}))

	count, err = engine.EdgeCount(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// Close Tests

func TestMemoryEngine_Close(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1"))})
		require.NoError(t, err)

		err = engine.Close()
		require.NoError(t, err)
		// After close, operations should fail
		_, err = engine.GetNode(t.Context(), NodeID(prefixTestID("n1")))
		assert.Error(t, err)
	})

	t.Run("all operations fail after close", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		engine.Close()

		_, err := engine.NodeCount(t.Context())
		assert.Error(t, err) // BadgerDB returns different error type

		_, err = engine.EdgeCount(t.Context())
		assert.Error(t, err)
	})
}

// Concurrency Tests

func TestMemoryEngine_Concurrency(t *testing.T) {
	t.Run("concurrent reads", func(t *testing.T) {
		engine := newTestMemoryEngine(t)

		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("node-1")), Labels: []string{"Test"}})
		require.NoError(t, err)

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := engine.GetNode(t.Context(), NodeID(prefixTestID("node-1")))
				assert.NoError(t, err)
			}()
		}
		wg.Wait()
	})

	t.Run("concurrent writes", func(t *testing.T) {
		engine := newTestMemoryEngine(t)

		var wg sync.WaitGroup
		errors := make(chan error, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				// Use unique ID based on goroutine index
				nodeID := NodeID(prefixTestID(fmt.Sprintf("node-%d", id)))
				_, err := engine.CreateNode(t.Context(), &Node{ID: nodeID})
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
		engine := newTestMemoryEngine(t)

		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("shared"))})
		require.NoError(t, err)

		var wg sync.WaitGroup

		// Readers
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_, _ = engine.GetNode(t.Context(), NodeID(prefixTestID("shared")))
				}
			}()
		}

		// Writers
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_ = engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{
						ID:         NodeID(prefixTestID("shared")),
						Properties: map[string]any{"counter": id*10 + j},
					}})
				}
			}(i)
		}

		wg.Wait()
	})
}

// Copy Tests

func TestMemoryEngine_copyNode(t *testing.T) {
	original := &Node{
		ID:              NodeID(prefixTestID("test")),
		Labels:          []string{"A", "B"},
		Properties:      map[string]any{"key": "value"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	copied := CopyNode(original)

	// Verify values copied
	assert.Equal(t, original.ID, copied.ID)
	assert.Equal(t, original.Labels, copied.Labels)
	assert.Equal(t, original.Properties["key"], copied.Properties["key"])
	assert.Equal(t, original.ChunkEmbeddings, copied.ChunkEmbeddings)

	// Verify independent copies
	original.Labels[0] = "X"
	original.Properties["key"] = "modified"
	original.ChunkEmbeddings[0][0] = 9.9

	assert.Equal(t, "A", copied.Labels[0])
	assert.Equal(t, "value", copied.Properties["key"])
	assert.Equal(t, float32(0.1), copied.ChunkEmbeddings[0][0])
}

func TestMemoryEngine_CopyNodeWithNamedEmbeddings(t *testing.T) {
	original := &Node{
		ID:     NodeID(prefixTestID("test-named")),
		Labels: []string{"A", "B"},
		Properties: map[string]any{
			"key": "value",
		},
		NamedEmbeddings: map[string][]float32{
			"default": {0.1, 0.2, 0.3},
			"title":   {0.4, 0.5, 0.6},
		},
	}

	copied := CopyNode(original)

	// Verify values copied
	assert.Equal(t, original.ID, copied.ID)
	assert.Equal(t, original.Labels, copied.Labels)
	assert.Equal(t, len(original.NamedEmbeddings), len(copied.NamedEmbeddings))
	assert.Equal(t, original.NamedEmbeddings["default"], copied.NamedEmbeddings["default"])
	assert.Equal(t, original.NamedEmbeddings["title"], copied.NamedEmbeddings["title"])

	// Verify independent copies
	original.NamedEmbeddings["default"][0] = 9.9
	assert.Equal(t, float32(0.1), copied.NamedEmbeddings["default"][0])
}

func TestMemoryEngine_CopyNodeWithMixedEmbeddings(t *testing.T) {
	original := &Node{
		ID:     NodeID(prefixTestID("test-mixed")),
		Labels: []string{"Document"},
		NamedEmbeddings: map[string][]float32{
			"default": {0.1, 0.2, 0.3},
		},
		ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}},
	}

	copied := CopyNode(original)

	// Verify both types of embeddings are copied
	assert.Equal(t, original.NamedEmbeddings, copied.NamedEmbeddings)
	assert.Equal(t, original.ChunkEmbeddings, copied.ChunkEmbeddings)

	// Verify independent copies
	original.NamedEmbeddings["default"][0] = 9.9
	original.ChunkEmbeddings[0][0] = 9.9

	assert.Equal(t, float32(0.1), copied.NamedEmbeddings["default"][0])
	assert.Equal(t, float32(0.4), copied.ChunkEmbeddings[0][0])
}

func TestMemoryEngine_copyEdge(t *testing.T) {
	original := &Edge{
		ID:                   EdgeID(prefixTestID("test")),
		StartNode:            NodeID(prefixTestID("n1")),
		EndNode:              NodeID(prefixTestID("n2")),
		Type:                 "REL",
		Properties:           map[string]any{"weight": 5},
		Confidence:           0.9,
		AutoGenerated:        true,
		VisibilitySuppressed: true,
	}

	copied := CopyEdge(original)

	// Verify values copied
	assert.Equal(t, original.ID, copied.ID)
	assert.Equal(t, original.StartNode, copied.StartNode)
	assert.Equal(t, original.Type, copied.Type)
	assert.Equal(t, original.Properties["weight"], copied.Properties["weight"])
	assert.Equal(t, original.VisibilitySuppressed, copied.VisibilitySuppressed)

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

func TestAllNodes(t *testing.T) {
	engine := newTestMemoryEngine(t)
	defer engine.Close()

	// Test empty storage
	t.Run("empty_storage", func(t *testing.T) {
		nodes, err := engine.AllNodes(context.Background())
		if err != nil {
			t.Fatalf("AllNodes failed: %v", err)
		}
		if len(nodes) != 0 {
			t.Errorf("Expected 0 nodes, got %d", len(nodes))
		}
	})

	// Create some test nodes
	node1 := &Node{
		ID:         NodeID(prefixTestID("node-1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	node2 := &Node{
		ID:         NodeID(prefixTestID("node-2")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	node3 := &Node{
		ID:         NodeID(prefixTestID("node-3")),
		Labels:     []string{"Company"},
		Properties: map[string]interface{}{"name": "Acme"},
	}

	engine.CreateNode(t.Context(), node1)
	engine.CreateNode(t.Context(), node2)
	engine.CreateNode(t.Context(), node3)

	t.Run("all_nodes_returned", func(t *testing.T) {
		nodes, err := engine.AllNodes(context.Background())
		if err != nil {
			t.Fatalf("AllNodes failed: %v", err)
		}
		if len(nodes) != 3 {
			t.Errorf("Expected 3 nodes, got %d", len(nodes))
		}

		// Verify all nodes are present
		foundIDs := make(map[NodeID]bool)
		for _, n := range nodes {
			foundIDs[n.ID] = true
		}
		if !foundIDs[NodeID(prefixTestID("node-1"))] || !foundIDs[NodeID(prefixTestID("node-2"))] || !foundIDs[NodeID(prefixTestID("node-3"))] {
			t.Error("Not all nodes were returned")
		}
	})

	t.Run("returns_copies", func(t *testing.T) {
		nodes, err := engine.AllNodes(context.Background())
		if err != nil {
			t.Fatalf("AllNodes failed: %v", err)
		}
		// Modify returned node
		nodes[0].Properties["modified"] = true

		// Original should be unchanged
		original, _ := engine.GetNode(t.Context(), nodes[0].ID)
		if _, exists := original.Properties["modified"]; exists {
			t.Error("Modification affected original node - not a copy")
		}
	})

	t.Run("closed_engine", func(t *testing.T) {
		closedEngine := newTestMemoryEngine(t)
		closedEngine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("test")), Labels: []string{"Test"}})
		closedEngine.Close()

		if nodes, err := closedEngine.AllNodes(context.Background()); err == nil || len(nodes) != 0 {
			t.Errorf("Closed engine must report the failed read: len=%d err=%v", len(nodes), err)
		}
	})
}

func TestGetEdgeBetween(t *testing.T) {
	engine := newTestMemoryEngine(t)
	defer engine.Close()

	// Create nodes
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("alice")), Labels: []string{"Person"}})
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("bob")), Labels: []string{"Person"}})
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("charlie")), Labels: []string{"Person"}})

	// Create edges
	engine.CreateEdge(t.Context(), &Edge{
		ID:        EdgeID(prefixTestID("edge-1")),
		Type:      "KNOWS",
		StartNode: NodeID(prefixTestID("alice")),
		EndNode:   NodeID(prefixTestID("bob")),
	})
	engine.CreateEdge(t.Context(), &Edge{
		ID:        EdgeID(prefixTestID("edge-2")),
		Type:      "WORKS_WITH",
		StartNode: NodeID(prefixTestID("alice")),
		EndNode:   NodeID(prefixTestID("bob")),
	})
	engine.CreateEdge(t.Context(), &Edge{
		ID:        EdgeID(prefixTestID("edge-3")),
		Type:      "KNOWS",
		StartNode: NodeID(prefixTestID("bob")),
		EndNode:   NodeID(prefixTestID("charlie")),
	})

	t.Run("find_existing_edge", func(t *testing.T) {
		edge, err := engine.GetEdgeBetween(t.Context(), NodeID(prefixTestID("alice")), NodeID(prefixTestID("bob")), "KNOWS")
		require.NoError(t, err)
		if edge == nil {
			t.Fatal("Expected to find KNOWS edge between alice and bob")
		}
		if edge.Type != "KNOWS" {
			t.Errorf("Expected type KNOWS, got %s", edge.Type)
		}
	})

	t.Run("find_any_edge_type", func(t *testing.T) {
		// Empty type should match any
		edge, err := engine.GetEdgeBetween(t.Context(), NodeID(prefixTestID("alice")), NodeID(prefixTestID("bob")), "")
		require.NoError(t, err)
		if edge == nil {
			t.Fatal("Expected to find edge between alice and bob")
		}
	})

	t.Run("no_edge_wrong_type", func(t *testing.T) {
		edge, err := engine.GetEdgeBetween(t.Context(), NodeID(prefixTestID("alice")), NodeID(prefixTestID("bob")), "MARRIED_TO")
		require.NoError(t, err)
		if edge != nil {
			t.Error("Should not find MARRIED_TO edge")
		}
	})

	t.Run("no_edge_between_nodes", func(t *testing.T) {
		edge, err := engine.GetEdgeBetween(t.Context(), NodeID(prefixTestID("alice")), NodeID(prefixTestID("charlie")), "KNOWS")
		require.NoError(t, err)
		if edge != nil {
			t.Error("Should not find edge between alice and charlie")
		}
	})

	t.Run("no_edge_nonexistent_source", func(t *testing.T) {
		edge, err := engine.GetEdgeBetween(t.Context(), NodeID(prefixTestID("unknown")), NodeID(prefixTestID("bob")), "")
		require.NoError(t, err)
		if edge != nil {
			t.Error("Should not find edge from nonexistent node")
		}
	})

	t.Run("returns_copy", func(t *testing.T) {
		edge, err := engine.GetEdgeBetween(t.Context(), NodeID(prefixTestID("alice")), NodeID(prefixTestID("bob")), "KNOWS")
		require.NoError(t, err)
		edge.Properties = map[string]interface{}{"modified": true}

		original, err := engine.GetEdgeBetween(t.Context(), NodeID(prefixTestID("alice")), NodeID(prefixTestID("bob")), "KNOWS")
		require.NoError(t, err)
		if _, exists := original.Properties["modified"]; exists {
			t.Error("Modification affected original edge - not a copy")
		}
	})

	t.Run("closed_engine", func(t *testing.T) {
		closedEngine := newTestMemoryEngine(t)
		closedEngine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("a")), Labels: []string{"X"}})
		closedEngine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("b")), Labels: []string{"X"}})
		closedEngine.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e")), Type: "T", StartNode: NodeID(prefixTestID("a")), EndNode: NodeID(prefixTestID("b"))})
		closedEngine.Close()

		edge, err := closedEngine.GetEdgeBetween(t.Context(), NodeID(prefixTestID("a")), NodeID(prefixTestID("b")), "T")
		require.ErrorIs(t, err, ErrStorageClosed)
		if edge != nil {
			t.Error("Closed engine should not return an edge")
		}
	})
}

func TestGetInDegree(t *testing.T) {
	engine := newTestMemoryEngine(t)
	defer engine.Close()

	// Create nodes
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("center")), Labels: []string{"Node"}})
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"Node"}})
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"Node"}})
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n3")), Labels: []string{"Node"}})

	t.Run("zero_incoming", func(t *testing.T) {
		degree, err := engine.GetInDegree(t.Context(), NodeID(prefixTestID("center")))
		require.NoError(t, err)
		if degree != 0 {
			t.Errorf("Expected 0 incoming edges, got %d", degree)
		}
	})

	// Add incoming edges to center
	engine.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e1")), Type: "POINTS_TO", StartNode: NodeID(prefixTestID("n1")), EndNode: NodeID(prefixTestID("center"))})
	engine.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e2")), Type: "POINTS_TO", StartNode: NodeID(prefixTestID("n2")), EndNode: NodeID(prefixTestID("center"))})
	engine.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e3")), Type: "LINKS", StartNode: NodeID(prefixTestID("n3")), EndNode: NodeID(prefixTestID("center"))})

	t.Run("three_incoming", func(t *testing.T) {
		degree, err := engine.GetInDegree(t.Context(), NodeID(prefixTestID("center")))
		require.NoError(t, err)
		if degree != 3 {
			t.Errorf("Expected 3 incoming edges, got %d", degree)
		}
	})

	t.Run("nonexistent_node", func(t *testing.T) {
		degree, err := engine.GetInDegree(t.Context(), NodeID(prefixTestID("nonexistent")))
		require.NoError(t, err)
		if degree != 0 {
			t.Errorf("Expected 0 for nonexistent node, got %d", degree)
		}
	})

	t.Run("closed_engine", func(t *testing.T) {
		closedEngine := newTestMemoryEngine(t)
		closedEngine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("x")), Labels: []string{"X"}})
		closedEngine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("y")), Labels: []string{"X"}})
		closedEngine.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e")), Type: "T", StartNode: NodeID(prefixTestID("y")), EndNode: NodeID(prefixTestID("x"))})
		closedEngine.Close()

		degree, err := closedEngine.GetInDegree(t.Context(), NodeID(prefixTestID("x")))
		require.ErrorIs(t, err, ErrStorageClosed)
		if degree != 0 {
			t.Errorf("Closed engine should return 0, got %d", degree)
		}
	})
}

func TestGetOutDegree(t *testing.T) {
	engine := newTestMemoryEngine(t)
	defer engine.Close()

	// Create nodes
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("center")), Labels: []string{"Node"}})
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n1")), Labels: []string{"Node"}})
	engine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("n2")), Labels: []string{"Node"}})

	t.Run("zero_outgoing", func(t *testing.T) {
		degree, err := engine.GetOutDegree(t.Context(), NodeID(prefixTestID("center")))
		require.NoError(t, err)
		if degree != 0 {
			t.Errorf("Expected 0 outgoing edges, got %d", degree)
		}
	})

	// Add outgoing edges from center
	engine.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e1")), Type: "POINTS_TO", StartNode: NodeID(prefixTestID("center")), EndNode: NodeID(prefixTestID("n1"))})
	engine.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e2")), Type: "LINKS", StartNode: NodeID(prefixTestID("center")), EndNode: NodeID(prefixTestID("n2"))})

	t.Run("two_outgoing", func(t *testing.T) {
		degree, err := engine.GetOutDegree(t.Context(), NodeID(prefixTestID("center")))
		require.NoError(t, err)
		if degree != 2 {
			t.Errorf("Expected 2 outgoing edges, got %d", degree)
		}
	})

	t.Run("nonexistent_node", func(t *testing.T) {
		degree, err := engine.GetOutDegree(t.Context(), NodeID(prefixTestID("nonexistent")))
		require.NoError(t, err)
		if degree != 0 {
			t.Errorf("Expected 0 for nonexistent node, got %d", degree)
		}
	})

	t.Run("closed_engine", func(t *testing.T) {
		closedEngine := newTestMemoryEngine(t)
		closedEngine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("x")), Labels: []string{"X"}})
		closedEngine.CreateNode(t.Context(), &Node{ID: NodeID(prefixTestID("y")), Labels: []string{"X"}})
		closedEngine.CreateEdge(t.Context(), &Edge{ID: EdgeID(prefixTestID("e")), Type: "T", StartNode: NodeID(prefixTestID("x")), EndNode: NodeID(prefixTestID("y"))})
		closedEngine.Close()

		degree, err := closedEngine.GetOutDegree(t.Context(), NodeID(prefixTestID("x")))
		require.ErrorIs(t, err, ErrStorageClosed)
		if degree != 0 {
			t.Errorf("Closed engine should return 0, got %d", degree)
		}
	})
}
