package nornicgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamLifecycleNativeGRPC(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	svc := search.NewService(store)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	registry, err := resultstream.NewRegistry(resultstream.Config{MaxStreams: 1, MaxStreamsPerOwner: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	svc.SetContinuationRegistry(registry)
	dropped := false
	api, err := NewService(Config{
		ResolveContinuationDatabase: registry.ResolveDatabase,
		DiscardContinuation: func(owner, qid, database string) error {
			return registry.Discard(resultstream.Scope{Owner: owner, Database: database}, qid)
		},
		ResolveDatabase: func(context.Context, string) (DatabaseDependencies, error) {
			if dropped {
				return DatabaseDependencies{}, errors.New("database dropped")
			}
			return DatabaseDependencies{Searcher: svc}, nil
		},
	}, nil, nil, svc)
	require.NoError(t, err)
	var qid string
	for i := 0; i < 4; i++ {
		first, err := api.SearchText(context.Background(), &gen.SearchTextRequest{Mode: "id", N: 1})
		require.NoError(t, err)
		qid = first.Qid
		last, err := api.SearchText(context.Background(), &gen.SearchTextRequest{Qid: qid, N: 1})
		require.NoError(t, err)
		require.False(t, last.HasMore)
	}
	dropped = true
	for i := 0; i < 2; i++ {
		released, err := api.SearchText(context.Background(), &gen.SearchTextRequest{Qid: qid, Discard: true})
		require.NoError(t, err)
		require.True(t, released.Released)
	}
}
