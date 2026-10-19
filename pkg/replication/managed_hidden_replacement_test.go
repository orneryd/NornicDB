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

func TestManagedHiddenReplacementPreservesPeerImage(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, route := range []string{"bulk", "transaction"} {
			for _, quota := range []string{"count", "bytes"} {
				t.Run(fmt.Sprintf("buffered=%v/%s/%s", buffered, route, quota), func(t *testing.T) {
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
					router := &managedRouteReplicator{Replicator: local, peer: remote}
					manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, router, time.Second), nil)
					require.NoError(t, err)
					defer manager.Close()
					const db = "hidden_command"
					require.NoError(t, manager.CreateDatabase(db))
					view, err := manager.GetStorage(db)
					require.NoError(t, err)
					require.NoError(t, view.BulkCreateNodes([]*storage.Node{{ID: "a"}, {ID: "b"}}))
					stamp := time.Now().UTC()
					edge := func(payload string) *storage.Edge {
						return &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": payload}, CreatedAt: stamp, UpdatedAt: stamp}
					}
					require.NoError(t, view.CreateEdge(edge(strings.Repeat("old", 1024))))
					// Visibility is local policy; replay carries the accepted
					// physical replacement rather than repeating that policy.
					require.NoError(t, base.SetEdgeManuallySuppressed(db+":e", true))
					_, err = view.GetEdge("e")
					require.ErrorIs(t, err, storage.ErrNotFound)
					size, _, _ := manager.GetStorageSize(db)
					limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 2, MaxEdges: 1}}
					if quota == "bytes" {
						limits.Storage.MaxBytes = size
					}
					require.NoError(t, manager.SetDatabaseLimits(db, limits))
					for attempt := 0; attempt < 2; attempt++ {
						replacement := edge("replacement")
						if route == "bulk" {
							err = view.BulkCreateEdges([]*storage.Edge{replacement})
						} else {
							tx, beginErr := base.BeginTransaction()
							require.NoError(t, beginErr)
							require.NoError(t, tx.SetNamespace(db))
							replacement.ID, replacement.StartNode, replacement.EndNode = db+":e", db+":a", db+":b"
							require.NoError(t, tx.BulkCreateEdges([]*storage.Edge{replacement}))
							err = view.(interface {
								CommitTransaction(context.Context, *storage.BadgerTransaction) error
							}).CommitTransaction(context.Background(), tx)
						}
						require.NoError(t, err)
						for _, engine := range []storage.Engine{view, storage.NewNamespacedEngine(peer, db)} {
							current, readErr := storage.GetStoredEdge(engine, "e")
							require.NoError(t, readErr)
							require.Equal(t, "replacement", current.Properties["payload"])
							stored, countErr := storage.CollectStoredEdges(context.Background(), engine)
							require.NoError(t, countErr)
							require.Len(t, stored, 1, "one physical relationship regardless of local visibility policy")
						}
						if quota == "bytes" {
							size, _, _ = manager.GetStorageSize(db)
							limits.Storage.MaxBytes = size
							require.NoError(t, manager.SetDatabaseLimits(db, limits))
						}
					}
				})
			}
		}
	}
}
