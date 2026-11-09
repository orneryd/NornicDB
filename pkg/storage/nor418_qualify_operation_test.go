package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// QualifyOperation is the one owner of prepared-operation qualification:
// a relationship update's observation is scoped with its replacement, and the
// caller's records are not changed.
func TestNamespacedQualifyOperationScopesObservation(t *testing.T) {
	base := newTestMemoryEngine(t)
	scope := NewNamespacedEngine(base, "tenant")
	ctx := t.Context()
	before := &Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "T"}
	after := &Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "T", Properties: map[string]any{"n": int64(1)}}
	op := EdgeUpdateRequest{Before: before, After: after}.Operation()
	require.NoError(t, scope.QualifyOperation(ctx, &op))
	for _, edge := range []*Edge{op.Edge, op.OldEdge} {
		require.Equal(t, EdgeID("tenant:r"), edge.ID)
		require.Equal(t, NodeID("tenant:a"), edge.StartNode)
		require.Equal(t, NodeID("tenant:b"), edge.EndNode)
	}
	require.Equal(t, EdgeID("r"), before.ID, "the caller's observation is not modified")
	require.Equal(t, NodeID("a"), after.StartNode, "the caller's replacement is not modified")

	op = Operation{Type: OpDeleteEdge, EdgeID: "r"}
	require.NoError(t, scope.QualifyOperation(ctx, &op))
	require.Equal(t, EdgeID("tenant:r"), op.EdgeID)
	op = Operation{Type: OpDeleteNode, NodeID: "n"}
	require.NoError(t, scope.QualifyOperation(ctx, &op))
	require.Equal(t, NodeID("tenant:n"), op.NodeID)
	op = Operation{Type: OpCreateNode, Node: &Node{ID: "n", Labels: []string{"L"}}}
	require.NoError(t, scope.QualifyOperation(ctx, &op))
	require.Equal(t, NodeID("tenant:n"), op.Node.ID)
}
