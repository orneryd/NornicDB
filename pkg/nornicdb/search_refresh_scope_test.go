package nornicdb

import (
	"context"
	"sync"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/inference"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// dbRefreshScopeTestDB builds a DB whose default database has no search
// service entry, so resolving one reports the resolver's own error.
func dbRefreshScopeTestDB(t *testing.T) (*DB, storage.Engine) {
	t.Helper()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	ns := storage.NewNamespacedEngine(base, "nornic")
	db := &DB{
		storage:           ns,
		baseStorage:       base,
		config:            DefaultConfig(),
		searchServices:    map[string]*dbSearchService{},
		inferenceServices: map[string]*inference.Engine{},
		buildCtx:          context.Background(),
	}
	return db, ns
}

// A resolver failure is the operation's error: it is not treated as a nil
// service, which would silently drop the refresh.
func TestDBRefreshScopePropagatesResolverError(t *testing.T) {
	ctx := context.Background()
	db, ns := dbRefreshScopeTestDB(t)
	_, err := ns.CreateNode(&storage.Node{ID: "node", Labels: []string{"Doc"}, Properties: map[string]any{"content": "kept"}})
	require.NoError(t, err)

	// No service entry and no base storage: the DB reports the error its own
	// resolver produces for a database it cannot resolve.
	db.baseStorage = nil
	scope := dbRefreshScope{db: db, dbName: "tenant_without_service"}
	_, deferred, err := scope.PrepareSearchRefresh(ctx, search.RefreshRequest{Node: "node"})
	require.Error(t, err, "the scope swallowed its resolver error")
	require.False(t, deferred)
	require.ErrorContains(t, search.RefreshIndexes(ctx, scope, search.RefreshRequest{Node: "node"}), "base storage is nil")
}

// A system database resolution failure reaches the operation unchanged.
func TestDBRefreshScopeSystemDatabaseErrorReachesOperation(t *testing.T) {
	ctx := context.Background()
	db, ns := dbRefreshScopeTestDB(t)
	scope := dbRefreshScope{db: db, dbName: "system", storage: ns}
	err := search.RefreshIndexes(ctx, scope, search.RefreshRequest{Node: "node"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "system database")
}

// A closed database's nil service is not an error the scope invents; the
// resolver's own closed error is returned.
func TestDBRefreshScopeClosedDBError(t *testing.T) {
	ctx := context.Background()
	db, ns := dbRefreshScopeTestDB(t)
	db.closed = true
	scope := dbRefreshScope{db: db, dbName: "nornic", storage: ns}
	require.ErrorIs(t, search.RefreshIndexes(ctx, scope, search.RefreshRequest{Node: "node"}), ErrClosed)
}

// A cancelled context ends the build wait with its own error, unchanged.
func TestDBRefreshScopeContextCancellationReachesOperation(t *testing.T) {
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	ns := storage.NewNamespacedEngine(engine, "nornic")
	svc := search.NewService(ns, 3, nil)
	entry := &dbSearchService{dbName: "nornic", engine: ns, svc: svc, buildDone: make(chan struct{})}
	entry.buildOnce.Do(func() {})
	db := &DB{
		config:            DefaultConfig(),
		storage:           ns,
		baseStorage:       engine,
		searchServices:    map[string]*dbSearchService{"nornic": entry},
		inferenceServices: map[string]*inference.Engine{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	defer close(entry.buildDone)

	scope := dbRefreshScope{db: db, dbName: "nornic", storage: ns}
	require.ErrorIs(t, search.RefreshIndexes(ctx, scope, search.RefreshRequest{Node: "node"}), context.Canceled)
}

// The DB scope never defers: a deleted node's refresh is applied now.
func TestDBRefreshScopeNeverDefers(t *testing.T) {
	ctx := context.Background()
	db, ns := dbRefreshScopeTestDB(t)
	svc, err := db.EnsureSearchIndexes(ctx, "nornic", ns, SearchIndexBuildWait)
	require.NoError(t, err)
	require.NotNil(t, svc)
	_, err = ns.CreateNode(&storage.Node{ID: "node", Labels: []string{"Doc"}, Properties: map[string]any{"content": "deleteword"}})
	require.NoError(t, err)
	require.NoError(t, svc.RefreshNode("node", storage.CaptureVectorWorkOrigin()))
	require.Equal(t, 1, svc.FulltextDocCount())
	require.NoError(t, ns.DeleteNode("node"))
	require.NoError(t, search.RefreshIndexes(ctx, dbRefreshScope{db: db, dbName: "nornic", storage: ns}, search.RefreshRequest{Node: "node"}))
	require.Zero(t, svc.FulltextDocCount(), "the DB scope deferred its refresh")
}

// Concurrent DB refreshes are serialized by their own resolution, so the
// operation applies each request against the service it prepared.
func TestDBRefreshScopeConcurrentRefreshesApplyEachRequest(t *testing.T) {
	ctx := context.Background()
	db, ns := dbRefreshScopeTestDB(t)
	svc, err := db.EnsureSearchIndexes(ctx, "nornic", ns, SearchIndexBuildWait)
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"one", "two"} {
		_, err := ns.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"content": string(id) + "word"}})
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, id := range []storage.NodeID{"one", "two"} {
		wg.Add(1)
		go func(id storage.NodeID) {
			defer wg.Done()
			errs <- search.RefreshIndexes(ctx, dbRefreshScope{db: db, dbName: "nornic", storage: ns}, search.RefreshRequest{Node: id})
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 2, svc.FulltextDocCount())
}
