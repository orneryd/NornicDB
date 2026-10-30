package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamLifecycleInvalidation(t *testing.T) {
	for _, end := range []string{"write", "backfill", "policy", "abandoned_write"} {
		t.Run(end, func(t *testing.T) {
			base := newTestMemoryEngine(t)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			store := storage.NewNamespacedEngine(base, "test")
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			svc := NewService(store, DefaultVectorDimensions, nil)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			registry, err := resultstream.NewRegistry(resultstream.Config{MaxStreams: 1, MaxStreamsPerOwner: 1})
			require.NoError(t, err)
			t.Cleanup(registry.Close)
			svc.SetContinuationRegistry(registry)
			start := func() *SearchContinuationPage {
				page, err := svc.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), SearchContinuationRequest{Owner: "alice", Mode: SearchContinuationID, N: 1}, nil, nil, nil, ChunkedSearchErrorPolicy{})
				require.NoError(t, err)
				require.True(t, page.HasMore)
				return page
			}
			first := start()
			switch end {
			case "write", "abandoned_write":
				_, err = store.CreateNode(&storage.Node{ID: "c"})
				require.NoError(t, err)
			case "backfill":
				node, err := store.GetNode("b")
				require.NoError(t, err)
				node.ChunkEmbeddings = [][]float32{{1, 0}}
				require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: node}))
			case "policy":
				svc.SetCompleteContinuationPolicy(CompleteContinuationPolicy{MaxMembers: 10})
			}
			if end != "abandoned_write" {
				_, err = svc.SearchTextContinuation(context.Background(), "", nil, SearchContinuationRequest{Owner: "alice", QID: first.QID, N: 1}, nil, nil, nil, ChunkedSearchErrorPolicy{})
				require.ErrorIs(t, err, resultstream.ErrInvalidated)
			}
			// Admission itself collects invalidated abandoned cursors.
			_ = start()
			_, err = svc.SearchTextContinuation(context.Background(), "", nil, SearchContinuationRequest{Owner: "alice", QID: first.QID, Discard: true}, nil, nil, nil, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
		})
	}
}
