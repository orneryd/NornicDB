package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// This forwards Err from a real context and cancels only if an admitted node
// deletion incorrectly asks its caller to admit a child deletion again.
type cancelNestedDeleteAdmission struct {
	context.Context
	cancel       context.CancelFunc
	observations int
}

func (c *cancelNestedDeleteAdmission) Err() error {
	c.observations++
	if c.observations == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func acceptedDeleteGraph(t *testing.T) (*InMemoryStorage, *Node) {
	t.Helper()
	graph := NewInMemoryStorage()
	root, err := graph.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	for range 2 {
		neighbor, err := graph.CreateNode(t.Context(), nil, nil)
		require.NoError(t, err)
		_, err = graph.CreateRelationship(t.Context(), root.ID, neighbor.ID, "R", nil)
		require.NoError(t, err)
	}
	_, err = graph.CreateRelationship(t.Context(), 2, root.ID, "Incoming", nil)
	require.NoError(t, err)
	_, err = graph.CreateRelationship(t.Context(), root.ID, root.ID, "Self", nil)
	require.NoError(t, err)
	return graph, root
}
func TestDeleteNodeCascadeCompletesWithoutReadmittingIncidentEdges(t *testing.T) {
	graph, root := acceptedDeleteGraph(t)
	lifetime, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := &cancelNestedDeleteAdmission{Context: lifetime, cancel: cancel}
	require.NoError(t, graph.DeleteNode(request, root.ID))
	t.Logf("request admission observations: %d", request.observations)
	// The healthy baseline left orphan relationships even with one admission.
	// The probe also protects against introducing cancelable child admissions;
	// it does not inject cancellation inside the corrected accepted mutation.
	cancel()
	require.ErrorIs(t, lifetime.Err(), context.Canceled)
	_, err := graph.GetNode(t.Context(), root.ID)
	require.ErrorIs(t, err, ErrNodeNotFound)
	relationships, err := graph.AllRelationships(t.Context())
	require.NoError(t, err)
	require.Empty(t, relationships, "node deletion mirrors the native cascade and leaves no orphan relationships")
	nodes, err := graph.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, nodes, 2)
}
func TestDeleteNodeCanceledBeforeAdmissionPreservesGraph(t *testing.T) {
	graph, root := acceptedDeleteGraph(t)
	request, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, graph.DeleteNode(request, root.ID), context.Canceled)
	nodes, err := graph.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, nodes, 3)
	relationships, err := graph.AllRelationships(t.Context())
	require.NoError(t, err)
	require.Len(t, relationships, 4)
	require.NoError(t, graph.DeleteNode(t.Context(), root.ID))
	relationships, err = graph.AllRelationships(t.Context())
	require.NoError(t, err)
	require.Empty(t, relationships)
}
