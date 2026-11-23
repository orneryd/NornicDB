package cypher

// Batch-3 probe L537 (research only). Same file on both builds.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/storage/lifecycle"
)

func ei3LifecycleEngine(t *testing.T) (*storage.BadgerEngine, *lifecycle.MVCCLifecycleManager) {
	t.Helper()
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, EngineOptions: storage.EngineOptions{
		RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: 100},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	config := lifecycle.DefaultLifecycleConfig()
	config.Enabled = true
	config.MaxVersionsPerKey = 2
	manager := lifecycle.NewMVCCLifecycleManager(config, engine)
	engine.SetLifecycleController(manager)
	return engine, manager
}

func ei3Readers(m *lifecycle.MVCCLifecycleManager) string {
	reg := m.ReaderRegistry()
	out := fmt.Sprintf("active=%d", reg.ActiveCount())
	for _, r := range reg.Snapshot() {
		out += fmt.Sprintf(" [id=%s ns=%s]", r.ReaderID, r.Namespace)
	}
	return out
}

// (a) One reader ID registered through a composite over two databases of one engine.
func TestEI3L537CompositeReaderRegistration(t *testing.T) {
	engine, manager := ei3LifecycleEngine(t)
	a, b := storage.NewNamespacedEngine(engine, "dba"), storage.NewNamespacedEngine(engine, "dbb")
	composite := storage.NewCompositeEngine(map[string]storage.Engine{"a": a, "b": b}, map[string]string{"a": "dba", "b": "dbb"}, map[string]string{"a": "read_write", "b": "read_write"})
	release := composite.RegisterSnapshotReader(storage.SnapshotReaderInfo{ReaderID: "ei3-reader", SnapshotVersion: storage.MVCCVersion{CommitTimestamp: time.Now(), CommitSequence: 1}, StartTime: time.Now()})
	t.Logf("EI3 L537a after composite registration of one reader over 2 databases: %s", ei3Readers(manager))
	if manager.ReaderRegistry().ActiveCount() != 2 {
		t.Errorf("EI3 L537a: composite registration kept %d entries for 2 databases", manager.ReaderRegistry().ActiveCount())
	}
	release()
	t.Logf("EI3 L537a after release: %s", ei3Readers(manager))
}

// (b) An explicit transaction's MERGE on a key another session changed after
// BEGIN advances its snapshot; is it still listed as an active reader, and
// does a prune then remove what it reads?
func TestEI3L537RepinDropsOwnEntry(t *testing.T) {
	engine, manager := ei3LifecycleEngine(t)
	store := storage.NewNamespacedEngine(engine, "test")
	run := func(e *StorageExecutor, q string) string {
		r, err := e.Execute(context.Background(), q, nil)
		return ei3Rows(r, err)
	}
	setup := NewStorageExecutor(store)
	t.Logf("constraint: %s", run(setup, "CREATE CONSTRAINT u_k FOR (n:U) REQUIRE n.k IS UNIQUE"))
	t.Logf("seed: %s", run(setup, "CREATE (:U {k: 1, v: 0}), (:X {id: 'x', v: 0})"))
	holder := NewStorageExecutor(store)
	t.Logf("BEGIN: %s readers: %s", run(holder, "BEGIN"), ei3Readers(manager))
	t.Logf("other session update k=1: %s readers: %s", run(setup, "MATCH (u:U {k: 1}) SET u.v = 1"), ei3Readers(manager))
	t.Logf("holder MERGE: %s readers: %s", run(holder, "MERGE (u:U {k: 1}) RETURN u.v"), ei3Readers(manager))
	inTx := manager.ReaderRegistry().ActiveCount()
	t.Logf("holder read x: %s", run(holder, "MATCH (x:X {id: 'x'}) RETURN x.v"))
	for v := 1; v <= 5; v++ {
		run(setup, fmt.Sprintf("MATCH (x:X {id: 'x'}) SET x.v = %d", v))
	}
	pruneErr := engine.TriggerPruneNow(context.Background())
	t.Logf("prune: err=%v readers: %s", pruneErr, ei3Readers(manager))
	t.Logf("holder read x after other writes and prune (snapshot value 0 expected): %s", run(holder, "MATCH (x:X {id: 'x'}) RETURN x.v"))
	t.Logf("holder COMMIT: %s readers: %s", run(holder, "COMMIT"), ei3Readers(manager))
	if inTx != 1 {
		t.Errorf("EI3 L537b: the open transaction is listed as %d active readers after its MERGE re-pin (want 1)", inTx)
	}
}
