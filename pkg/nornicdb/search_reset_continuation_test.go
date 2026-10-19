package nornicdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Pause public service creation after it binds the registry epoch, before it
// publishes the service. Reset must retire that creation before a replacement
// is cached; otherwise every later START uses a permanently stale epoch.
func TestSearchStreamResetSerializesInFlightServiceCreation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.SearchCursorMax = 1
	cfg.Database.AsyncWritesEnabled = false
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	const database = "stream_reset_creation"
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, database)
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var resolverOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	db.SetRerankerResolver(func(name string) search.Reranker {
		if name == database {
			resolverOnce.Do(func() { close(entered); <-release })
		}
		return nil
	})
	type creation struct {
		svc *search.Service
		err error
	}
	created := make(chan creation, 1)
	go func() {
		svc, err := db.GetOrCreateSearchService(database, store)
		created <- creation{svc, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("service creation did not reach the publication gate")
	}
	started, reset := make(chan struct{}), make(chan struct{})
	go func() { close(started); db.ResetSearchService(database); close(reset) }()
	<-started
	select {
	case <-reset:
	case <-time.After(time.Second):
		// A serialized reset waits for the gated creation to publish first.
	}
	unblock()
	var original creation
	select {
	case original = <-created:
	case <-time.After(5 * time.Second):
		t.Fatal("gated service creation did not complete")
	}
	require.NoError(t, original.err)
	t.Cleanup(func() { require.NoError(t, original.svc.Close()) })
	select {
	case <-reset:
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not complete after service publication")
	}
	fresh, err := db.GetOrCreateSearchService(database, store)
	require.NoError(t, err)
	request := search.SearchContinuationRequest{Owner: "alice", Mode: search.SearchContinuationID, N: 1}
	page, err := fresh.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), request, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err, "the cached replacement must bind the epoch after reset")
	require.NotSame(t, original.svc, fresh, "reset retires an already in-flight creation")
	_, err = fresh.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "alice", QID: page.QID, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	_, err = fresh.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), request, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err, "reading to the end releases the only owner slot")
}
