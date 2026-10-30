// Author: #1683 lane (Claude), 2026-10-30. Scratch diagnostic, never on the PR
// branch: reports each full-text incremental admission condition across one
// write, for small and large writes through the public Cypher and storage
// paths, on the large-transaction fixture.
package cypher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func diag1683Capture(t *testing.T, engine storage.Engine) (storage.SnapshotReadRevision, bool) {
	t.Helper()
	snapshot, err := storage.OpenReadSnapshot(storage.WithSnapshotReader(context.Background(), storage.SnapshotAllNodes), engine)
	require.NoError(t, err)
	defer snapshot.Close()
	q, ok := snapshot.(storage.QueryReadSnapshot)
	if !ok {
		return storage.SnapshotReadRevision{}, false
	}
	captured, capturedOK := q.CapturedRevision()
	current, currentOK := q.CurrentRevision()
	return captured, capturedOK && currentOK && captured == current
}

func TestDiag1683Admission(t *testing.T) {
	for _, shape := range []struct {
		name  string
		write func(t *testing.T, exec *StorageExecutor, store *fulltextCountingStore)
	}{
		{"store_update_one", func(t *testing.T, exec *StorageExecutor, store *fulltextCountingStore) {
			node, err := store.GetNode("n000040")
			require.NoError(t, err)
			node.Properties["title"] = "newunique"
			require.NoError(t, store.UpdateNode(node))
		}},
		{"cypher_explicit_one", func(t *testing.T, exec *StorageExecutor, store *fulltextCountingStore) {
			for _, s := range []string{"BEGIN", "MATCH (n {id: 'n000040'}) SET n.title = 'newunique'", "COMMIT"} {
				_, err := exec.Execute(context.Background(), s, nil)
				require.NoError(t, err, s)
			}
		}},
		{"cypher_implicit_one", func(t *testing.T, exec *StorageExecutor, store *fulltextCountingStore) {
			_, err := exec.Execute(context.Background(), "MATCH (n {id: 'n000040'}) SET n.title = 'newunique'", nil)
			require.NoError(t, err)
		}},
		{"cypher_explicit_all", func(t *testing.T, exec *StorageExecutor, store *fulltextCountingStore) {
			for _, s := range []string{"BEGIN", "MATCH (n) SET n.title = CASE n.id WHEN 'n000040' THEN 'newunique' ELSE 'Correspondence revised' END", "COMMIT"} {
				_, err := exec.Execute(context.Background(), s, nil)
				require.NoError(t, err, s)
			}
		}},
		{"cypher_explicit_sources", func(t *testing.T, exec *StorageExecutor, store *fulltextCountingStore) {
			for _, s := range []string{"BEGIN", "MATCH (n:Source) SET n.title = CASE n.id WHEN 'n000040' THEN 'newunique' ELSE 'Correspondence revised' END", "COMMIT"} {
				_, err := exec.Execute(context.Background(), s, nil)
				require.NoError(t, err, s)
			}
		}},
	} {
		for _, engineKind := range []string{"memory", "disk_lowmem"} {
			t.Run(shape.name+"/"+engineKind, func(t *testing.T) {
				var base *storage.BadgerEngine
				if engineKind == "memory" {
					base = storage.NewMemoryEngine().BadgerEngine
				} else {
					var err error
					base, err = storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
					require.NoError(t, err)
				}
				exec, store := fulltextMaintainedFixtureOn(t, base, 4000)
				ctx := context.Background()
				query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score ORDER BY score DESC, node.id"
				_, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
				require.NoError(t, err)
				var events, resets atomic.Int64
				node := func(*storage.Node) { events.Add(1) }
				require.NoError(t, storage.SubscribeEvents(store, storage.EventHandlers{
					NodeCreated: node, NodeUpdated: node, NodeDeleted: func(storage.NodeID) { events.Add(1) },
					EdgeCreated: func(*storage.Edge) { events.Add(1) }, EdgeUpdated: func(*storage.Edge) { events.Add(1) }, EdgeDeleted: func(storage.EdgeID) { events.Add(1) },
					NamespaceReset: func(string) { resets.Add(1) },
				}))
				before, beforeOK := diag1683Capture(t, store)
				shape.write(t, exec, store)
				settledNow := storage.EventNotificationsSettled(store)
				immediate, immediateOK := diag1683Capture(t, store)
				eventsNow := events.Load()
				waited := time.Duration(0)
				for !storage.EventNotificationsSettled(store) && waited < 5*time.Second {
					time.Sleep(5 * time.Millisecond)
					waited += 5 * time.Millisecond
				}
				after, afterOK := diag1683Capture(t, store)
				time.Sleep(200 * time.Millisecond)
				late, lateOK := diag1683Capture(t, store)
				scans := store.scans.Load()
				_, err = exec.Execute(ctx, query, map[string]any{"query": "newunique"})
				require.NoError(t, err)
				t.Logf("DIAG1683 %s/%s before_ok=%v immediate_ok=%v after_ok=%v late_ok=%v settled_immediately=%v settle_wait=%s events_immediately=%d events=%d resets=%d graph_delta_immediate=%d graph_delta=%d single_after=%v single_immediate=%v same_after_late=%v late_single=%v query_rereads=%d",
					shape.name, engineKind, beforeOK, immediateOK, afterOK, lateOK, settledNow, waited, eventsNow, events.Load(), resets.Load(),
					int64(immediate.GraphMutationVersion())-int64(before.GraphMutationVersion()), int64(after.GraphMutationVersion())-int64(before.GraphMutationVersion()),
					after.SingleReaderCommitAfter(before), immediate.SingleReaderCommitAfter(before), late.SameReaderView(after), late.SingleReaderCommitAfter(before),
					store.scans.Load()-scans)
			})
		}
	}
}
