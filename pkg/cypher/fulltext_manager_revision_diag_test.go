// Author: #1683 lane (Claude), 2026-10-30 (NOR-148). Diagnostic only, scratch
// branch, never on the PR branch. It logs the inputs of the maintained corpus's
// incremental-refresh decision around one UpdateNode, for a DatabaseManager
// database and, as the control, a bare namespaced engine.
package cypher

import (
	"context"
	"fmt"
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
