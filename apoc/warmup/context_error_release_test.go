package warmup

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

type stoppingNeighborhoodStorage struct {
	*storage.InMemoryStorage
	cancel context.CancelFunc
}

func (s *stoppingNeighborhoodStorage) GetNodeNeighbors(ctx context.Context, id int64, kind string, direction storage.Direction) ([]*storage.Node, error) {
	nodes, err := s.InMemoryStorage.GetNodeNeighbors(ctx, id, kind, direction)
	if cancel := s.cancel; cancel != nil {
		s.cancel = nil
		cancel()
	}
	return nodes, err
}
func TestSubgraphCancellationReleasesCacheAndRetries(t *testing.T) {
	backend := &stoppingNeighborhoodStorage{InMemoryStorage: storage.NewInMemoryStorage()}
	node, err := backend.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	neighbor, err := backend.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	_, err = backend.CreateRelationship(t.Context(), node.ID, neighbor.ID, "R", nil)
	require.NoError(t, err)
	functions := New(backend)
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend.cancel = cancel
	result, err := functions.Subgraph(request, node, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.True(t, functions.cache.mu.TryRLock(), "an error exit releases the cache write lock")
	functions.cache.mu.RUnlock()
	result, err = functions.Subgraph(t.Context(), node, 1)
	require.NoError(t, err)
	require.Equal(t, 2, result["nodesLoaded"])
	require.Equal(t, 1, result["relationshipsLoaded"])
}
