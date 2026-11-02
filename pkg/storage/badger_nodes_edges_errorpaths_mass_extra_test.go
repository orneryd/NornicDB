package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_NodeAndEdgeInputValidationBranches(t *testing.T) {
	engine := newTestMemoryEngine(t)

	_, err := engine.CreateNode(t.Context(), nil)
	require.ErrorIs(t, err, ErrInvalidData)
	_, err = engine.CreateNode(t.Context(), &Node{})
	require.ErrorIs(t, err, ErrInvalidID)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "unprefixed"})
	require.Error(t, err)

	require.ErrorIs(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: nil}), ErrInvalidData)
	require.ErrorIs(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{}}), ErrInvalidID)
	require.Error(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "unprefixed"}}))

	_, err = engine.GetNode(t.Context(), "")
	require.ErrorIs(t, err, ErrInvalidID)

	require.ErrorIs(t, engine.UpdateNodeEmbeddingSidecar(context.Background(), nil), ErrInvalidData)
	require.ErrorIs(t, engine.UpdateNodeEmbeddingSidecar(context.Background(), &Node{}), ErrInvalidID)
	require.Error(t, engine.UpdateNodeEmbeddingSidecar(context.Background(), &Node{ID: "unprefixed"}))
	require.ErrorIs(t, engine.UpdateNodeEmbeddingSidecar(context.Background(), &Node{ID: "test:missing"}), ErrNotFound)

	require.ErrorIs(t, engine.DeleteNode(t.Context(), ""), ErrInvalidID)
	require.NoError(t, engine.BulkDeleteNodes(t.Context(), nil))
	require.NoError(t, engine.BulkDeleteEdges(t.Context(), nil))
}

func TestBadgerEngine_EdgeCRUDErrorAndBranchPaths(t *testing.T) {
	engine := newTestMemoryEngine(t)

	require.ErrorIs(t, engine.CreateEdge(t.Context(), nil), ErrInvalidData)
	require.ErrorIs(t, engine.CreateEdge(t.Context(), &Edge{}), ErrInvalidID)
	require.ErrorIs(t, engine.CreateEdge(t.Context(), &Edge{
		ID: "test:e-missing-endpoints", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	}), ErrNotFound)

	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	// Endpoint-change path where new endpoint is missing.
	require.ErrorIs(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:missing", Type: "REL",
	}}), ErrNotFound)

	// Type-change path.
	require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL2",
	}}))

	// Endpoint+type change path.
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:c", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{
		ID: "test:e1", StartNode: "test:b", EndNode: "test:c", Type: "REL3",
	}}))

	// Not found / invalid branches.
	require.ErrorIs(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: nil}), ErrInvalidData)
	require.ErrorIs(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{}}), ErrInvalidID)
	require.ErrorIs(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: "test:missing", StartNode: "test:a", EndNode: "test:b", Type: "REL"}}), ErrNotFound)

	require.ErrorIs(t, engine.DeleteEdge(t.Context(), ""), ErrInvalidID)
	require.ErrorIs(t, engine.DeleteEdge(t.Context(), "test:missing"), ErrNotFound)
	require.NoError(t, engine.DeleteEdge(t.Context(), "test:e1"))
	require.NoError(t, engine.BulkDeleteEdges(t.Context(), []EdgeID{"test:missing", ""}))
}
