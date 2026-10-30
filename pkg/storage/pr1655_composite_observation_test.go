package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPR1655CompositeObservedDeletedTarget(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		t.Run(kind, func(t *testing.T) {
			backing := newTestEngine(t)
			constituent := NewNamespacedEngine(backing, "one")
			for _, id := range []NodeID{"a", "b"} {
				_, err := constituent.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, constituent.CreateEdge(&Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R"}))
			composite := NewCompositeEngine(map[string]Engine{"one": constituent}, map[string]string{"one": "one"}, map[string]string{"one": "read_write"})
			var updateErr error
			if kind == "node" {
				before, err := composite.GetNode("4:one:a")
				require.NoError(t, err)
				require.NoError(t, constituent.DeleteNode("a"))
				after := *before
				after.Properties = map[string]any{"v": "new"}
				updateErr = composite.UpdateNode(NodeUpdateRequest{Before: before, After: &after})
			} else {
				before, err := composite.GetEdge("5:one:r")
				require.NoError(t, err)
				require.NoError(t, constituent.DeleteEdge("r"))
				after := *before
				after.Properties = map[string]any{"v": "new"}
				updateErr = composite.UpdateEdge(EdgeUpdateRequest{Before: before, After: &after})
			}
			require.ErrorIs(t, updateErr, ErrConflict, "an observed target deleted by a peer remains a retryable write conflict through composite routing")
		})
	}
}

func TestPR1655CompositeNilMutationInput(t *testing.T) {
	composite := NewCompositeEngine(nil, nil, nil)
	require.ErrorIs(t, composite.UpdateNode(NodeUpdateRequest{}), ErrInvalidData)
	require.ErrorIs(t, composite.UpdateEdge(EdgeUpdateRequest{}), ErrInvalidData)
}

func TestPR1655CompositeObservationKeepsSource(t *testing.T) {
	engines := map[string]Engine{}
	for _, name := range []string{"one", "two"} {
		engine := NewNamespacedEngine(newTestEngine(t), name)
		engines[name] = engine
		for _, id := range []NodeID{"a", "b"} {
			_, err := engine.CreateNode(&Node{ID: id, Properties: map[string]any{"source": name}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"source": name}}))
	}
	composite := NewCompositeEngine(engines, map[string]string{"one": "one", "two": "two"}, map[string]string{"one": "read_write", "two": "read_write"})
	node, err := composite.GetNode("4:one:a")
	require.NoError(t, err)
	edge, err := composite.GetEdge("5:one:r")
	require.NoError(t, err)
	require.NoError(t, engines["one"].DeleteEdge("r"))
	require.NoError(t, engines["one"].DeleteNode("a"))
	// A caller can construct a replacement without carrying runtime metadata;
	// the actual observed predecessor still identifies its original source.
	require.ErrorIs(t, composite.UpdateNode(NodeUpdateRequest{Before: node, After: &Node{ID: node.ID, Properties: map[string]any{"source": "wrong"}}}), ErrConflict)
	require.ErrorIs(t, composite.UpdateEdge(EdgeUpdateRequest{Before: edge, After: &Edge{ID: edge.ID, StartNode: edge.StartNode, EndNode: edge.EndNode, Type: edge.Type, Properties: map[string]any{"source": "wrong"}}}), ErrConflict)
	remainingNode, err := engines["two"].GetNode("a")
	require.NoError(t, err)
	require.Equal(t, "two", remainingNode.Properties["source"])
	remainingEdge, err := engines["two"].GetEdge("r")
	require.NoError(t, err)
	require.Equal(t, "two", remainingEdge.Properties["source"])
}

func TestPR1655CompositeUnqualifiedMissingObservation(t *testing.T) {
	engine := NewNamespacedEngine(newTestEngine(t), "one")
	composite := NewCompositeEngine(map[string]Engine{"one": engine}, map[string]string{"one": "one"}, map[string]string{"one": "read_write"})
	require.ErrorIs(t, composite.UpdateNode(NodeUpdateRequest{Before: &Node{ID: "gone"}, After: &Node{ID: "gone"}}), ErrConflict)
	require.ErrorIs(t, composite.UpdateEdge(EdgeUpdateRequest{Before: &Edge{ID: "gone"}, After: &Edge{ID: "gone"}}), ErrConflict)
	require.ErrorIs(t, composite.UpdateNode(NodeUpdateRequest{After: &Node{ID: "gone"}}), ErrNotFound)
	require.ErrorIs(t, composite.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "gone"}}), ErrNotFound)
}
