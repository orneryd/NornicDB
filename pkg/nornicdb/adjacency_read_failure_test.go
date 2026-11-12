package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type adjacencyReadFailureEngine struct {
	storage.Engine
	direction storage.AdjacentEdgeDirection
	failure   error
}

func (e adjacencyReadFailureEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	edges, err := e.Engine.ReadAdjacentEdges(ctx, request)
	if err == nil && request.Direction == e.direction {
		return edges, e.failure
	}
	return edges, err
}

func TestGetEdgesForNodeDoesNotReturnPartialSuccessAfterReadFailure(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(base, "adjacency_errors")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: id})
		require.NoError(t, err)
	}
	for _, edge := range []*storage.Edge{
		{ID: "out", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"v": int64(1)}},
		{ID: "in", StartNode: "b", EndNode: "a", Type: "R", Properties: map[string]any{"v": int64(2)}},
	} {
		require.NoError(t, store.CreateEdge(t.Context(), edge))
	}
	db := &DB{storage: store}
	healthy, err := db.GetEdgesForNode(t.Context(), "a")
	require.NoError(t, err)
	require.Len(t, healthy, 2)
	require.Equal(t, "out", healthy[0].ID)
	require.Equal(t, "in", healthy[1].ID)
	require.Equal(t, int64(1), healthy[0].Properties["v"])
	listed, err := db.ListEdges(t.Context(), "R", 10, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, healthy, listed, "listed and adjacent results share the complete stored-image projection")
	for _, edge := range healthy {
		point, err := db.GetEdge(t.Context(), edge.ID)
		require.NoError(t, err)
		require.Equal(t, edge, point)
	}

	failure := errors.New("adjacency unavailable")
	for _, direction := range []storage.AdjacentEdgeDirection{storage.AdjacentOutgoing, storage.AdjacentIncoming} {
		for _, cause := range []error{failure, context.Canceled} {
			t.Run(fmt.Sprintf("direction=%d/cause=%s", direction, cause), func(t *testing.T) {
				db := &DB{storage: adjacencyReadFailureEngine{Engine: store, direction: direction, failure: cause}}
				edges, err := db.GetEdgesForNode(t.Context(), "a")
				require.ErrorIs(t, err, cause)
				require.Nil(t, edges, "a failed directional read cannot be a complete result")
			})
		}
	}
}
