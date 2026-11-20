package cypher

// Batch-3 probes: final-build (2dbc67e96) adapters.

import (
	"context"
	"testing"
	"time"

	apoccoll "github.com/orneryd/nornicdb/apoc/coll"
	"github.com/orneryd/nornicdb/apoc/registry"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const ei3HasServerTxTimeout = true

func ei3InstallLookup(t *testing.T, fns map[string]interface{}, bundled bool) {
	t.Helper()
	handlers := map[string]*PluginFunctionHandler{}
	for name, fn := range fns {
		handlers[name] = mustCallableForTest(fn)
	}
	if bundled {
		toSet, err := registry.Compile(apoccoll.ToSet, registry.CanonicalReads)
		if err != nil {
			t.Fatal(err)
		}
		contains, err := registry.Compile(apoccoll.Contains, registry.CanonicalReads)
		if err != nil {
			t.Fatal(err)
		}
		handlers["apoc.coll.toSet"], handlers["apoc.coll.contains"] = toSet, contains
	}
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
		if h, ok := handlers[name]; ok {
			return h, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
}

func ei3BadgerBase(t *testing.T, timeout time.Duration) storage.Engine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		InMemory:      true,
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: timeout}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return fixtureowner.Own(t, fixtureowner.Track(t, &storage.MemoryEngine{BadgerEngine: engine}, storage.ErrStorageClosed))
}

func ei3TxStore(t *testing.T, timeout time.Duration) storage.Engine {
	t.Helper()
	store := storage.NewNamespacedEngine(ei3BadgerBase(t, timeout), "test")
	if _, err := NewStorageExecutor(store).Execute(t.Context(), "CREATE CONSTRAINT u_k FOR (n:U) REQUIRE n.k IS UNIQUE", nil); err != nil {
		t.Fatal(err)
	}
	return store
}

func ei3TxManager(t *testing.T, timeout time.Duration) (*multidb.DatabaseManager, storage.Engine) {
	t.Helper()
	mgr, err := multidb.NewDatabaseManager(t.Context(), ei3BadgerBase(t, timeout), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	store, err := mgr.GetStorage("nornic")
	if err != nil {
		t.Fatal(err)
	}
	return mgr, store
}

func ei3NoCacheExecutor(store storage.Engine) *StorageExecutor {
	return NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

func ei3GetNode(ctx context.Context, store storage.Engine, id storage.NodeID) {
	_, _ = store.GetNode(ctx, id)
}

func ei3CreateNode(ctx context.Context, store storage.Engine, n *storage.Node) error {
	_, err := store.CreateNode(ctx, n)
	return err
}
