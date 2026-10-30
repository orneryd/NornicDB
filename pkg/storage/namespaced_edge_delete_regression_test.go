package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespacedEngine_BulkDeleteEdgesInvalidatesPreloadedTraversalCaches(t *testing.T) {
	inner := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = inner.Close() })

	tenant := NewNamespacedEngine(inner, "test")
	_, err := tenant.CreateNode(&Node{ID: "a", Labels: []string{"Entity"}})
	require.NoError(t, err)
	_, err = tenant.CreateNode(&Node{ID: "b", Labels: []string{"Entity"}})
	require.NoError(t, err)
	_, err = tenant.CreateNode(&Node{ID: "ep", Labels: []string{"Episodic"}})
	require.NoError(t, err)

	rel := &Edge{ID: "r1", StartNode: "a", EndNode: "b", Type: "RELATES_TO", Properties: map[string]interface{}{"uuid": "r1"}}
	mention := &Edge{ID: "m1", StartNode: "ep", EndNode: "a", Type: "MENTIONS", Properties: map[string]interface{}{"uuid": "m1"}}
	require.NoError(t, tenant.CreateEdge(rel))
	require.NoError(t, tenant.CreateEdge(mention))

	// Preload both edge-type and adjacency caches through the namespaced wrapper.
	byType, err := tenant.GetEdgesByType("RELATES_TO")
	require.NoError(t, err)
	require.Len(t, byType, 1)
	outgoingA, err := tenant.GetOutgoingEdges("a")
	require.NoError(t, err)
	require.Len(t, outgoingA, 1)
	outgoingEP, err := tenant.GetOutgoingEdges("ep")
	require.NoError(t, err)
	require.Len(t, outgoingEP, 1)

	require.NoError(t, tenant.BulkDeleteEdges([]EdgeID{"r1", "m1"}))

	byType, err = tenant.GetEdgesByType("RELATES_TO")
	require.NoError(t, err)
	require.Empty(t, byType)
	outgoingA, err = tenant.GetOutgoingEdges("a")
	require.NoError(t, err)
	require.Empty(t, outgoingA)
	outgoingEP, err = tenant.GetOutgoingEdges("ep")
	require.NoError(t, err)
	require.Empty(t, outgoingEP)
}
