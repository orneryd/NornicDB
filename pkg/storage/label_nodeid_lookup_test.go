package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type labelLookupFallbackEngine struct {
	Engine
	nodes []*Node
	err   error
}

type firstNodeOnlyEngine struct {
	Engine
	node *Node
	err  error
}

func (e *labelLookupFallbackEngine) GetNodesByLabel(label string) ([]*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.nodes, nil
}

func (e *firstNodeOnlyEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.node, nil
}

func TestFirstNodeIDByLabel_NamespaceFiltering(t *testing.T) {
	base := newTestMemoryEngine(t)
	engineA := NewNamespacedEngine(base, "a")
	engineB := NewNamespacedEngine(base, "b")

	_, err := engineA.CreateNode(&Node{
		ID:     NodeID("node-a"),
		Labels: []string{"Person"},
	})
	require.NoError(t, err)

	_, err = engineB.CreateNode(&Node{
		ID:     NodeID("node-b"),
		Labels: []string{"Person"},
	})
	require.NoError(t, err)

	id, err := FirstNodeIDByLabel(engineA, "Person")
	require.NoError(t, err)
	require.Equal(t, NodeID("node-a"), id)
}

func TestFirstNodeIDByLabel_InvalidatesOnDelete(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := NewNamespacedEngine(base, "test")

	_, err := engine.CreateNode(&Node{
		ID:     NodeID("node-1"),
		Labels: []string{"Person"},
	})
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{
		ID:     NodeID("node-2"),
		Labels: []string{"Person"},
	})
	require.NoError(t, err)

	id1, err := FirstNodeIDByLabel(engine, "Person")
	require.NoError(t, err)

	require.NoError(t, engine.DeleteNode(id1))

	id2, err := FirstNodeIDByLabel(engine, "Person")
	require.NoError(t, err)

	if id1 == "node-1" {
		require.Equal(t, NodeID("node-2"), id2)
	} else {
		require.Equal(t, NodeID("node-1"), id2)
	}
}

func TestNodeIDsByLabel_FallbackBranches(t *testing.T) {
	engine := &labelLookupFallbackEngine{
		Engine: newTestMemoryEngine(t),
		nodes: []*Node{
			nil,
			{ID: "n1"},
			{ID: "n2"},
		},
	}

	ids, err := NodeIDsByLabel(engine, "Person", 0)
	require.NoError(t, err)
	require.Equal(t, []NodeID{"n1", "n2"}, ids)

	ids, err = NodeIDsByLabel(engine, "Person", 1)
	require.NoError(t, err)
	require.Equal(t, []NodeID{"n1"}, ids)

	engine.err = ErrNotFound
	_, err = NodeIDsByLabel(engine, "Person", 1)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestFirstNodeIDByLabel_FallbackBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := &firstNodeOnlyEngine{Engine: base, node: &Node{ID: "n1"}}

	id, err := FirstNodeIDByLabel(engine, "Person")
	require.NoError(t, err)
	require.Equal(t, NodeID("n1"), id)

	engine.node = nil
	id, err = FirstNodeIDByLabel(engine, "Person")
	require.ErrorIs(t, err, ErrNotFound)
	require.Empty(t, id)

	engine.err = errors.New("first lookup failed")
	id, err = FirstNodeIDByLabel(engine, "Person")
	require.ErrorContains(t, err, "first lookup failed")
	require.Empty(t, id)
}
