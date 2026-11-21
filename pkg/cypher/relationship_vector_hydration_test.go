package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cancelRelationshipHydration struct {
	*storage.NamespacedEngine
	cancel   context.CancelFunc
	hydrated int
}

func (s *cancelRelationshipHydration) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	s.hydrated++
	edge, err := s.NamespacedEngine.GetEdge(ctx, id)
	if s.cancel != nil {
		s.cancel()
	}
	return edge, err
}

func TestIndexedRelationshipVectorHydrationStopsOnCancellation(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		store := newVectorInputExecutor(t, 2).storage.(*storage.NamespacedEngine)
		service := search.NewService(store, 2, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		service.SetIndexFlags(false, true)
		require.NoError(t, service.BuildIndexes(t.Context()))

		probe := &cancelRelationshipHydration{NamespacedEngine: store}
		exec := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0})
		exec.SetSearchService(service)
		query := vectorInputQuery("db.index.vector.queryRelationships", "link_idx", "2", "[1.0, 0.0]")

		healthy, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		requireVectorInputRows(t, healthy, "relationship", []string{"r1", "r2"})
		// Only the indexed path hydrates hits through direct GetEdge calls.
		require.Equal(t, 2, probe.hydrated)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		probe.cancel = cancel
		probe.hydrated = 0
		result, err := exec.Execute(ctx, query, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		require.Equal(t, 1, probe.hydrated, "cancellation during the first read must stop the remaining hydration")
	})
}
