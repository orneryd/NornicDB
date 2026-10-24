package nornicdb

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// State retirement and continuation retirement must share the creation fence
// on every reset route, including a reset that preserves persisted files.
func TestRebaseSearchResetRetiresStateAndContinuation(t *testing.T) {
	for _, route := range []string{"reset", "prefix", "preserve_files"} {
		t.Run(route, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Memory.SearchCursorMax = 1
			cfg.Database.AsyncWritesEnabled = false
			db, err := Open(t.TempDir(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			const database = "reset_composition"
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			store := storage.NewNamespacedEngine(base, database)
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			original, err := db.GetOrCreateSearchService(database, store)
			require.NoError(t, err)
			start := search.SearchContinuationRequest{Owner: "alice", Mode: search.SearchContinuationID, N: 1}
			page, err := original.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), start, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.NotEmpty(t, page.QID)
			switch route {
			case "reset":
				db.ResetSearchService(database)
			case "prefix":
				db.dropSearchForPrefix(database + ":")
			case "preserve_files":
				db.invalidateSearchState(func(name string) bool { return name == database }, false)
			}
			require.True(t, original.Closed(), "the previous service must be retired")
			fresh, err := db.GetOrCreateSearchService(database, store)
			require.NoError(t, err)
			require.NotSame(t, original, fresh)
			_, err = fresh.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "alice", QID: page.QID, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
			require.ErrorIs(t, err, resultstream.ErrGoneQID)
			_, err = fresh.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), start, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
			require.NoError(t, err, "reset must release the old cursor's capacity and bind the fresh epoch")
		})
	}
}
