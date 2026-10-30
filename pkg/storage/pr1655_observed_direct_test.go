package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Observed updates never turn a peer's deletion into an upsert. Namespace
// adapters must preserve this proof while binding the replacement's identity.
func TestPR1655ObservedDirectDeletion(t *testing.T) {
	for _, namespaced := range []bool{false, true} {
		t.Run(boolString(namespaced), func(t *testing.T) {
			b := openPR1655AcceptanceEngine(t, true)
			seedPR1655AcceptanceGraph(t, b, [2]NodeID{"test:pr1655-a", "test:pr1655-b"})
			var target Engine = b
			nodeID, edgeID := NodeID("test:pr1655-a"), EdgeID("test:pr1655-r")
			if namespaced {
				target = NewNamespacedEngine(b, "test")
				nodeID = "pr1655-a"
				edgeID = "pr1655-r"
			}
			before, err := target.GetNode(nodeID)
			require.NoError(t, err)
			edgeBefore, err := target.GetEdge(edgeID)
			require.NoError(t, err)
			require.NoError(t, target.DeleteEdge(edgeID))
			require.NoError(t, target.DeleteNode(nodeID))
			after := *before
			after.Properties = map[string]any{"note": "rejected"}
			edgeAfter := *edgeBefore
			edgeAfter.Properties = map[string]any{"note": "rejected"}
			require.ErrorIs(t, target.UpdateNode(NodeUpdateRequest{Before: before, After: &after}), ErrConflict)
			require.ErrorIs(t, target.UpdateEdge(EdgeUpdateRequest{Before: edgeBefore, After: &edgeAfter}), ErrConflict)
			_, err = target.GetNode(nodeID)
			require.ErrorIs(t, err, ErrNotFound)
			_, err = target.GetEdge(edgeID)
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}
