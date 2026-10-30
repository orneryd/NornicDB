// Author: #1683 lane (Claude), 2026-10-30 (NOR-148). Diagnostic only, scratch
// branch, never on the PR branch. It logs the inputs of the maintained corpus's
// incremental-refresh decision around one UpdateNode, for a DatabaseManager
// database and, as the control, a bare namespaced engine.
package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNOR148DiagManagerRevision(t *testing.T) {
	for _, viaManager := range []bool{false, true} {
		name := "bare_namespaced"
		if viaManager {
			name = "database_manager"
		}
		t.Run(name, func(t *testing.T) { nor148DiagRevision(t, viaManager) })
	}
}

func nor148DiagRevision(t *testing.T, viaManager bool) {
	base := storage.NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { _ = base.Close() })
	var store storage.Engine = storage.NewNamespacedEngine(base, "diag")
	if viaManager {
		manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = manager.Close() })
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "diagdb"}))
		store, err = manager.GetStorage("diagdb")
		require.NoError(t, err)
	}
	t.Logf("engine type %T", store)
	var events atomic.Int64
	count := func(*storage.Node) { events.Add(1) }
	require.NoError(t, storage.SubscribeEvents(store, storage.EventHandlers{NodeCreated: count, NodeUpdated: count}))
	nodes := make([]*storage.Node, 8)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Source"}, Properties: map[string]any{"text": "hotel"}}
	}
	require.NoError(t, store.BulkCreateNodes(nodes))
	settle := func() bool {
		for i := 0; i < 200; i++ {
			if storage.EventNotificationsSettled(store) {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return false
	}
	capture := func(label string) (storage.SnapshotReadRevision, bool, int64) {
		settled := settle()
		// The same reader request the full-text acquisition makes for nodes.
		view, err := storage.OpenReadSnapshot(storage.WithSnapshotReader(context.Background(), storage.SnapshotAllNodes), store)
		require.NoError(t, err)
		defer view.Close()
		snap := view.(storage.QueryReadSnapshot)
		rev, ok := snap.CapturedRevision()
		end, endOK := snap.CurrentRevision()
		n := events.Load()
		var scoped uint64
		if versioned, has := store.(interface{ GraphMutationVersion() (uint64, bool) }); has {
			scoped, _ = versioned.GraphMutationVersion()
		}
		t.Logf("%s: settled=%v capturedOK=%v stable=%v graph=%d storeVersion=%d events=%d reader=%v revision=%+v", label, settled, ok, endOK && end == rev, rev.GraphMutationVersion(), scoped, n, snap.Reader(), rev)
		return rev, settled, n
	}
	before, _, eventsBefore := capture("before")
	node, err := store.GetNode("n3")
	require.NoError(t, err)
	node.Properties["text"] = "changed"
	require.NoError(t, store.UpdateNode(node))
	after, settled, eventsAfter := capture("after")
	t.Logf("DECISION settled=%v graphDelta=%d eventDelta=%d singleReaderCommitAfter=%v sameReaderView=%v",
		settled, after.GraphMutationVersion()-before.GraphMutationVersion(), eventsAfter-eventsBefore,
		after.SingleReaderCommitAfter(before), after.SameReaderView(before))
}

// r3: the public test's own window. Revisions are captured around the first
// ranked query (the population) and around the one UpdateNode, with the
// executor's maintained corpus in place, so every commit in between shows.
func TestNOR148DiagManagerPublicWindow(t *testing.T) {
	for _, viaManager := range []bool{false, true} {
		name := "bare_namespaced"
		if viaManager {
			name = "database_manager"
		}
		t.Run(name, func(t *testing.T) { nor148DiagPublicWindow(t, viaManager) })
	}
}

func nor148DiagPublicWindow(t *testing.T, viaManager bool) {
	base := storage.NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { _ = base.Close() })
	publisher := &fulltextManagerPublisher{BadgerEngine: base}
	var store storage.Engine = storage.NewNamespacedEngine(publisher, "diag")
	if viaManager {
		manager, err := multidb.NewDatabaseManager(t.Context(), publisher, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = manager.Close() })
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "diagdb"}))
		store, err = manager.GetStorage("diagdb")
		require.NoError(t, err)
	}
	var events atomic.Int64
	count := func(*storage.Node) { events.Add(1) }
	require.NoError(t, storage.SubscribeEvents(store, storage.EventHandlers{NodeCreated: count, NodeUpdated: count}))
	nodes := make([]*storage.Node, 16)
	for i := range nodes {
		id := fmt.Sprintf("n%06d", i)
		nodes[i] = &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source"}, Properties: map[string]any{"id": id, "text": "hotel services"}}
	}
	require.NoError(t, store.BulkCreateNodes(nodes))
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text]", nil)
	require.NoError(t, err)
	query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score ORDER BY score DESC, node.id"
	capture := func(label string) storage.SnapshotReadRevision {
		view, err := storage.OpenReadSnapshot(storage.WithSnapshotReader(context.Background(), storage.SnapshotAllNodes), store)
		require.NoError(t, err)
		defer view.Close()
		rev, ok := view.(storage.QueryReadSnapshot).CapturedRevision()
		t.Logf("%s: settled=%v capturedOK=%v events=%d scans=%d revision=%+v", label, storage.EventNotificationsSettled(store), ok, events.Load(), publisher.scans.Load(), rev)
		return rev
	}
	capture("before-first-query")
	_, err = exec.Execute(ctx, query, map[string]any{"query": "hotel"})
	require.NoError(t, err)
	populated := capture("after-first-query")
	time.Sleep(50 * time.Millisecond)
	idle := capture("after-50ms-idle")
	node, err := store.GetNode("n000008")
	require.NoError(t, err)
	node.Properties["text"] = "newunique hotel services"
	require.NoError(t, store.UpdateNode(node))
	updated := capture("after-update")
	_, err = exec.Execute(ctx, query, map[string]any{"query": "newunique"})
	require.NoError(t, err)
	capture("after-second-query")
	t.Logf("WINDOW idle_vs_populated singleReaderCommitAfter=%v sameReaderView=%v; update_vs_populated graphDelta=%d singleReaderCommitAfter=%v",
		idle.SingleReaderCommitAfter(populated), idle.SameReaderView(populated),
		updated.GraphMutationVersion()-populated.GraphMutationVersion(), updated.SingleReaderCommitAfter(populated))
}

// r4: which setup step makes one UpdateNode publish two reader-relevant
// commits on a DatabaseManager database, and whether it recurs per update.
func TestNOR148DiagManagerUpdateCommits(t *testing.T) {
	for _, variant := range []struct {
		name                string
		publisher, fulltext bool
	}{
		{"raw_engine_no_executor", false, false},
		{"publisher_no_executor", true, false},
		{"raw_engine_with_fulltext", false, true},
		{"publisher_with_fulltext", true, true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			base := storage.NewMemoryEngine().BadgerEngine
			t.Cleanup(func() { _ = base.Close() })
			var inner storage.Engine = base
			if variant.publisher {
				inner = &fulltextManagerPublisher{BadgerEngine: base}
			}
			manager, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "diagdb"}))
			store, err := manager.GetStorage("diagdb")
			require.NoError(t, err)
			nodes := make([]*storage.Node, 16)
			for i := range nodes {
				id := fmt.Sprintf("n%06d", i)
				nodes[i] = &storage.Node{ID: storage.NodeID(id), Labels: []string{"Source"}, Properties: map[string]any{"id": id, "text": "hotel services"}}
			}
			require.NoError(t, store.BulkCreateNodes(nodes))
			if variant.fulltext {
				exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
				_, err := exec.Execute(context.Background(), "CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text]", nil)
				require.NoError(t, err)
				_, err = exec.Execute(context.Background(), "CALL db.index.fulltext.queryNodes('source_text', 'hotel') YIELD node RETURN node.id", nil)
				require.NoError(t, err)
			}
			latest := func() (uint64, uint64) {
				view, err := storage.OpenReadSnapshot(storage.WithSnapshotReader(context.Background(), storage.SnapshotAllNodes), store)
				require.NoError(t, err)
				defer view.Close()
				rev, _ := view.(storage.QueryReadSnapshot).CapturedRevision()
				// readerLatest is unexported; read it from the value printed with %+v.
				var ts uint64
				if m := nor148ReaderLatest.FindStringSubmatch(fmt.Sprintf("%+v", rev)); m != nil {
					ts, _ = strconv.ParseUint(m[1], 10, 64)
				}
				return ts, rev.GraphMutationVersion()
			}
			for update := 1; update <= 3; update++ {
				beforeTs, beforeGraph := latest()
				node, err := store.GetNode("n000008")
				require.NoError(t, err)
				node.Properties["text"] = fmt.Sprintf("update %d hotel", update)
				require.NoError(t, store.UpdateNode(node))
				afterTs, afterGraph := latest()
				t.Logf("COMMITS %s update=%d readerCommits=%d graphDelta=%d", variant.name, update, afterTs-beforeTs, afterGraph-beforeGraph)
			}
		})
	}
}

var nor148ReaderLatest = regexp.MustCompile(`readerLatest:(\d+)`)
