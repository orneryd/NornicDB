package storage

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// A running MVCC prune must never hold back a new transaction. Before this
// fix the prune held mvccPruneMu for its whole walk and every transaction took
// the same mutex to register its snapshot when it pinned its database, so
// every BEGIN, write and login waited for the whole walk. Regression of
// 7b14b9133 (fix for GitHub #732); no NOR issue, recorded on NOR-405.

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// seedPruneHistory creates nodes with history beyond MaxVersionsPerKey 1.
func seedPruneHistory(t *testing.T, engine *BadgerEngine, prefix string, nodes, updates int) []NodeID {
	t.Helper()
	ids := make([]NodeID, 0, nodes)
	for i := 0; i < nodes; i++ {
		id := NodeID(fmt.Sprintf("%s-%d", prefix, i))
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"v": 0}})
		require.NoError(t, err)
		for v := 1; v <= updates; v++ {
			require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"v": v}}}))
		}
		ids = append(ids, id)
	}
	return ids
}

// parkPruneInsideDelete starts a prune and returns once it is waiting to
// commit its first delete while holding its pass: the commit gate is held,
// so the prune stays mid-walk until release is called.
func parkPruneInsideDelete(t *testing.T, engine *BadgerEngine, opts MVCCPruneOptions) (release func(), done <-chan error) {
	t.Helper()
	waiting := make(chan struct{}, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &engine.db.commitGate {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	}))
	engine.db.commitGate.Lock()
	released := false
	release = func() {
		if !released {
			released = true
			engine.db.commitGate.Unlock()
		}
	}
	t.Cleanup(release)
	result := make(chan error, 1)
	go func() {
		_, err := engine.PruneMVCCVersions(context.Background(), opts)
		result <- err
	}()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-waiting:
			// The pass is running once its pass lock is held.
			if engine.mvccPruneMu.TryLock() {
				engine.mvccPruneMu.Unlock()
				continue
			}
			return release, result
		case <-deadline:
			release()
			t.Fatal("the prune never reached its first delete")
		}
	}
}

func TestMVCCPruneNeverBlocksTransactionSnapshotRegistration(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	ids := seedPruneHistory(t, engine, prefixTestID("prune-admission"), 4, 4)
	release, pruned := parkPruneInsideDelete(t, engine, MVCCPruneOptions{MaxVersionsPerKey: 1})

	// A new transaction pins its database, which registers its snapshot,
	// and reads through it while the prune is in the middle of its walk.
	admitted := make(chan error, 1)
	go func() {
		tx, err := engine.BeginTransaction(context.Background())
		if err != nil {
			admitted <- err
			return
		}
		defer func() { _ = tx.Rollback() }()
		if err := tx.SetNamespace("test"); err != nil {
			admitted <- err
			return
		}
		node, err := tx.GetNode(context.Background(), ids[len(ids)-1])
		if err == nil && node.Properties["v"] != int64(4) && node.Properties["v"] != 4 {
			err = fmt.Errorf("transaction read v=%v, want 4", node.Properties["v"])
		}
		admitted <- err
	}()
	select {
	case err := <-admitted:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 2)
		release()
		<-admitted
		<-pruned
		t.Fatal("a new transaction waited for the running prune")
	}
	release()
	require.NoError(t, <-pruned)
}

// A transaction that registers while a prune is mid-walk keeps reading its
// own snapshot after the prune has deleted the versions it started from.
func TestMVCCPruneKeepsTransactionSnapshotRegisteredDuringPrune(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	ids := seedPruneHistory(t, engine, prefixTestID("prune-during"), 3, 3)
	tx, err := engine.BeginTransaction(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	// Newer versions after the transaction's cut.
	for _, id := range ids {
		for v := 10; v <= 13; v++ {
			require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"v": v}}}))
		}
	}
	release, pruned := parkPruneInsideDelete(t, engine, MVCCPruneOptions{MaxVersionsPerKey: 1})
	registered := make(chan error, 1)
	go func() { registered <- tx.SetNamespace("test") }()
	select {
	case err := <-registered:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		release()
		<-registered
		<-pruned
		t.Fatal("snapshot registration waited for the running prune")
	}
	release()
	require.NoError(t, <-pruned)
	for _, id := range ids {
		node, err := tx.GetNode(context.Background(), id)
		require.NoError(t, err)
		require.EqualValues(t, 3, node.Properties["v"], "the transaction keeps its own snapshot")
	}
}

// An as-of reader registered before a prune keeps its versions.
func TestMVCCPruneKeepsAsOfReaderVersions(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	id := NodeID(prefixTestID("prune-asof"))
	_, err := engine.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"v": 0}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"v": 1}}}))
	head, err := engine.GetNodeCurrentHead(context.Background(), id)
	require.NoError(t, err)
	for v := 2; v <= 8; v++ {
		require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"v": v}}}))
	}
	release, err := engine.acquireSnapshotReader(SnapshotReaderInfo{Namespace: "test", SnapshotVersion: head.Version, StartTime: time.Now()})
	require.NoError(t, err)
	defer release()
	_, err = engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	node, err := engine.GetNodeVisibleAt(t.Context(), id, head.Version)
	require.NoError(t, err)
	require.EqualValues(t, 1, node.Properties["v"])
}

// A snapshot keeps its old view across a database DROP and recreate, also
// when a prune runs in between (NOR-178).
func TestMVCCPruneKeepsSnapshotAcrossDropAndRecreate(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	id := NodeID("d7:n")
	_, err := engine.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"body": "Old"}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("d7"))
	node, err := tx.GetNode(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, "Old", node.Properties["body"])

	_, _, err = engine.DeleteByPrefix("d7:")
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"body": "New"}})
	require.NoError(t, err)
	for v := 0; v < 3; v++ {
		require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"body": fmt.Sprintf("New%d", v)}}}))
	}
	_, err = engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)

	node, err = tx.GetNode(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, "Old", node.Properties["body"], "the snapshot keeps its view across DROP and recreate")
}
