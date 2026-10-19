package replication

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Negative deltas become capacity only when their stored change publishes.
// An uncertain delete/shrink cannot admit growth using its hoped-for result.
func TestManagedPendingReductionDoesNotDonateCapacity(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, entity := range []string{"node", "edge"} {
			for _, mode := range []string{"delete-count", "delete-bytes", "shrink-bytes"} {
				t.Run(fmt.Sprintf("buffered=%v/%s/%s", buffered, entity, mode), func(t *testing.T) {
					base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					defer base.Close()
					adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
					require.NoError(t, err)
					defer adapter.Close()
					local := NewStandaloneReplicator(DefaultConfig(), adapter)
					require.NoError(t, local.Start(context.Background()))
					defer local.Shutdown()
					router := &commandMatrixReplicator{Replicator: local, adapter: adapter}
					manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, router, time.Second), nil)
					require.NoError(t, err)
					defer manager.Close()
					require.NoError(t, manager.CreateDatabase("pending_reduction"))
					view, err := manager.GetStorage("pending_reduction")
					require.NoError(t, err)
					require.NoError(t, view.BulkCreateNodes([]*storage.Node{{ID: "a"}, {ID: "b"}}))
					write := func(id, payload string) error {
						if entity == "node" {
							_, err := view.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"payload": payload}})
							return err
						}
						return view.CreateEdge(&storage.Edge{ID: storage.EdgeID(id), StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": payload}})
					}
					require.NoError(t, write("old", strings.Repeat("p", 4096)))
					total, _, _ := manager.GetStorageSize("pending_reduction")
					limits := &multidb.Limits{}
					if mode == "delete-count" {
						if entity == "node" {
							limits.Storage.MaxNodes = 3
						} else {
							limits.Storage.MaxEdges = 1
						}
					} else {
						limits.Storage.MaxBytes = total
					}
					require.NoError(t, manager.SetDatabaseLimits("pending_reduction", limits))
					router.outcome = "pending"
					if entity == "node" {
						if mode == "shrink-bytes" {
							node, getErr := view.GetNode("old")
							require.NoError(t, getErr)
							node.Properties["payload"] = "small"
							err = view.UpdateNode(node)
						} else {
							err = view.DeleteNode("old")
						}
					} else {
						if mode == "shrink-bytes" {
							edge, getErr := view.GetEdge("old")
							require.NoError(t, getErr)
							edge.Properties["payload"] = "small"
							err = view.UpdateEdge(edge)
						} else {
							err = view.DeleteEdge("old")
						}
					}
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.ErrorIs(t, write("new", strings.Repeat("q", 1024)), multidb.ErrStorageLimitExceeded)
					require.NotNil(t, router.pending)
					pending := router.pending
					router.pending = nil
					require.NoError(t, adapter.ApplyCommand(pending))
					require.NoError(t, write("new", strings.Repeat("q", 1024)))
					tracked, _, _ := manager.GetStorageSize("pending_reduction")
					recount, err := multidb.NewDatabaseManager(base, nil)
					require.NoError(t, err)
					defer recount.Close()
					exact, _, _ := recount.GetStorageSize("pending_reduction")
					require.Equal(t, exact, tracked)
				})
			}
		}
	}
}
