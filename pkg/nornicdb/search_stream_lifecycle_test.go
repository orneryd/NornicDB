package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamLifecycleDatabaseDrop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.SearchCursorMax = 1
	cfg.Database.AsyncWritesEnabled = false
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := db.storage.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	database := db.defaultDatabaseName()
	svc, err := db.GetOrCreateSearchService(database, db.storage)
	require.NoError(t, err)
	request := search.SearchContinuationRequest{Owner: "alice", Mode: search.SearchContinuationID, N: 1}
	first, err := svc.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), request, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	db.DropSearchServiceState(database)
	svc, err = db.GetOrCreateSearchService(database, db.storage)
	require.NoError(t, err)
	_, err = svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "alice", QID: first.QID, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrGoneQID)
	require.NoError(t, db.DiscardSearchContinuation("alice", first.QID, database))
	svc, err = db.GetOrCreateSearchService(database, db.storage)
	require.NoError(t, err)
	_, err = svc.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), request, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err, "drop must free the only registry slot")
}
