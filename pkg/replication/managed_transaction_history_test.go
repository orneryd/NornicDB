package replication

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedPendingTransactionPreservesVersionHistory(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, entity := range []string{"node", "edge", "endpoint", "adjacency"} {
			for _, restore := range []bool{false, true} {
				t.Run(fmt.Sprintf("buffered=%v/%s/snapshot=%v", buffered, entity, restore), func(t *testing.T) {
					base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					defer base.Close()
					peer := storage.NewMemoryEngine()
					defer peer.Close()
					adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
					require.NoError(t, err)
					defer adapter.Close()
					remote, err := NewStorageAdapterWithWAL(peer, t.TempDir())
					require.NoError(t, err)
					defer remote.Close()
					local := NewStandaloneReplicator(DefaultConfig(), adapter)
					require.NoError(t, local.Start(context.Background()))
					defer local.Shutdown()
					r := &managedRouteReplicator{Replicator: local, peer: remote}
					manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, r, time.Second), nil)
					require.NoError(t, err)
					defer manager.Close()
					require.NoError(t, manager.CreateDatabase("history_command"))
					view, err := manager.GetStorage("history_command")
					require.NoError(t, err)
					require.NoError(t, view.BulkCreateNodes([]*storage.Node{{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"key": "a", "payload": "initial"}}, {ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"key": "b"}}}))
					require.NoError(t, view.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"key": "e", "payload": "initial"}}))
					manager.GetStorageSize("history_command")
					exec := cypher.NewStorageExecutor(view)
					ctx := context.Background()
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					query := "MATCH (n:Doc {key:'a'}) SET n.payload='queued'"
					if entity == "edge" || entity == "endpoint" {
						query = "MATCH ()-[r:LINK {key:'e'}]->() SET r.payload='queued'"
					}
					if entity == "adjacency" {
						query = "MATCH (n:Doc {key:'a'}) DETACH DELETE n"
					}
					_, err = exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					r.delay = true
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.ErrorIs(t, err, context.DeadlineExceeded)
					pending := r.pending
					require.NotNil(t, pending)
					r.delay = false
					r.pending = nil
					// Restore every public field, including timestamps: identity,
					// rather than value comparison, must detect the two writes.
					if entity == "node" || entity == "endpoint" {
						original, err := view.GetNode("a")
						require.NoError(t, err)
						changed, err := view.GetNode("a")
						require.NoError(t, err)
						changed.Properties["payload"] = "intermediate"
						require.NoError(t, view.UpdateNode(changed))
						require.NoError(t, view.UpdateNode(original))
					} else {
						original, err := view.GetEdge("e")
						require.NoError(t, err)
						changed, err := view.GetEdge("e")
						require.NoError(t, err)
						changed.Properties["payload"] = "intermediate"
						require.NoError(t, view.UpdateEdge(changed))
						require.NoError(t, view.UpdateEdge(original))
					}
					if restore {
						var snapshot bytes.Buffer
						require.NoError(t, adapter.WriteSnapshot(&snapshot))
						restored := storage.NewMemoryEngine()
						defer restored.Close()
						target, err := NewStorageAdapterWithWAL(restored, t.TempDir())
						require.NoError(t, err)
						defer target.Close()
						require.NoError(t, target.RestoreSnapshot(&snapshot))
						r.peer = target
					}
					require.ErrorIs(t, adapter.ApplyCommand(pending), storage.ErrConflict)
					require.ErrorIs(t, r.peer.ApplyCommand(&Command{Type: pending.Type, Data: pending.Data, Timestamp: pending.Timestamp, RequestID: pending.RequestID}), storage.ErrConflict)
					// A following current-state command must still succeed after
					// late rejection, including against the restored peer.
					current, err := view.GetNode("a")
					require.NoError(t, err)
					current.Properties["payload"] = "following"
					require.NoError(t, view.UpdateNode(current))
					remoteView := storage.NewNamespacedEngine(r.peer.Engine(), "history_command")
					node, err := remoteView.GetNode("a")
					require.NoError(t, err)
					require.Equal(t, "following", node.Properties["payload"])
					tracked, _, _ := manager.GetStorageSize("history_command")
					recount, err := multidb.NewDatabaseManager(base, nil)
					require.NoError(t, err)
					defer recount.Close()
					exact, _, _ := recount.GetStorageSize("history_command")
					require.Equal(t, exact, tracked)
				})
			}
		}
	}
}
