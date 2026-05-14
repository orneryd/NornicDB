package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReconcileDecaySuppression_NoOpWhenDisabled(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	// Decay disabled by default → reconciliation is a no-op.
	require.NoError(t, e.ReconcileDecaySuppression("test"))

	changes, err := e.ReconcileDecaySuppressionWithChanges("test")
	require.NoError(t, err)
	require.Empty(t, changes)
}

func TestReconcileDecaySuppression_NoOpWhenEmptyNamespace(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
	e.SetDecayEnabled(true)

	// Empty-namespace argument is rejected (no work to do).
	require.NoError(t, e.ReconcileDecaySuppression(""))
	changes, err := e.ReconcileDecaySuppressionWithChanges("")
	require.NoError(t, err)
	require.Empty(t, changes)
}

func TestReconcileDecaySuppression_WithEntities(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
	e.SetDecayEnabled(true)

	// Stage two nodes and one edge in namespace "test".
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := e.CreateNode(&Node{
			ID: id, Labels: []string{"L"}, Properties: map[string]any{},
		})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(&Edge{
		ID: "test:e", StartNode: "test:a", EndNode: "test:b",
		Type: "REL", Properties: map[string]any{},
	}))

	// Reconciliation walks every node and edge in the namespace
	// without error. Without an active scorer or schema, no entity
	// transitions to suppressed → no changes returned.
	require.NoError(t, e.ReconcileDecaySuppression("test"))
	changes, err := e.ReconcileDecaySuppressionWithChanges("test")
	require.NoError(t, err)
	require.Empty(t, changes, "no scorer ⇒ no suppression transitions")
}

func TestReadNodeSuppressionState_RoundTrip(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	_, err := e.CreateNode(&Node{
		ID: "test:n", Labels: []string{"L1", "L2"}, Properties: map[string]any{},
	})
	require.NoError(t, err)

	suppressed, labels, err := e.readNodeSuppressionState("test:n")
	require.NoError(t, err)
	require.False(t, suppressed, "freshly-created node is not suppressed")
	require.ElementsMatch(t, []string{"L1", "L2"}, labels)

	// Missing node → error from the underlying Get.
	_, _, err = e.readNodeSuppressionState("test:nope")
	require.Error(t, err)
}

func TestReadEdgeSuppressionState_RoundTrip(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := e.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(&Edge{
		ID: "test:e", StartNode: "test:a", EndNode: "test:b",
		Type: "REL_TYPE", Properties: map[string]any{},
	}))

	suppressed, edgeType, err := e.readEdgeSuppressionState("test:e")
	require.NoError(t, err)
	require.False(t, suppressed)
	require.Equal(t, "REL_TYPE", edgeType)

	_, _, err = e.readEdgeSuppressionState("test:nope")
	require.Error(t, err)
}

func TestCollectNamespaceNodeAndEdgeIDs(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	for _, id := range []NodeID{"test:a", "test:b", "other:c"} {
		_, err := e.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{},
	}))
	require.NoError(t, e.CreateEdge(&Edge{
		ID: "other:e2", StartNode: "other:c", EndNode: "test:a", Type: "R", Properties: map[string]any{},
	}))

	nodeIDs, err := e.collectNamespaceNodeIDs("test")
	require.NoError(t, err)
	require.Len(t, nodeIDs, 2)
	for _, id := range nodeIDs {
		require.Contains(t, []NodeID{"test:a", "test:b"}, id)
	}

	edgeIDs, err := e.collectNamespaceEdgeIDs("test")
	require.NoError(t, err)
	require.Equal(t, []EdgeID{"test:e1"}, edgeIDs)

	// Unknown namespace → empty.
	nodeIDs, err = e.collectNamespaceNodeIDs("nosuch")
	require.NoError(t, err)
	require.Empty(t, nodeIDs)
}
