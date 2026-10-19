package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedTransactionPreservesTypedAndHiddenImages(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, mode := range []string{"unlimited", "count", "tracked", "bytes"} {
			for _, route := range []string{"native", "implicit", "explicit"} {
				t.Run(fmt.Sprintf("buffered=%v/%s/%s", buffered, mode, route), func(t *testing.T) {
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
					require.NoError(t, manager.CreateDatabase("typed_command"))
					view, err := manager.GetStorage("typed_command")
					require.NoError(t, err)
					if mode == "count" {
						require.NoError(t, manager.SetDatabaseLimits("typed_command", &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}))
					}
					if mode == "bytes" {
						require.NoError(t, manager.SetDatabaseLimits("typed_command", &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: 1 << 20}}))
					}
					if mode == "tracked" {
						manager.GetStorageSize("typed_command")
					}
					exec := cypher.NewStorageExecutor(view)
					ctx := context.Background()
					values, err := exec.Execute(ctx, "RETURN date('2026-10-19') AS d, time('12:30:00+01:00') AS t, localtime('12:30:00') AS lt, datetime('2026-10-19T12:30:00Z') AS dt, localdatetime('2026-10-19T12:30:00') AS ldt, duration('P1D') AS dur, point({x:1.0,y:2.0}) AS p", nil)
					require.NoError(t, err)
					payload := make(map[string]any)
					for i, name := range values.Columns {
						payload[name] = values.Rows[0][i]
					}
					payload["dates"] = []any{payload["d"], payload["d"]}
					props := map[string]any{"key": "typed"}
					for key, value := range payload {
						props[key] = value
					}
					if route == "native" {
						_, err = view.CreateNode(&storage.Node{ID: "typed", Labels: []string{"Doc"}, Properties: props, VisibilitySuppressed: true, EmbedMeta: map[string]any{"model": "typed"}})
					} else {
						if route == "explicit" {
							_, err = exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						_, err = exec.Execute(ctx, "CREATE (n:Doc {key:'typed', d:$d, t:$t, lt:$lt, dt:$dt, ldt:$ldt, dur:$dur, p:$p, dates:$dates}) RETURN n.key", payload)
						if err == nil && route == "explicit" {
							_, err = exec.Execute(ctx, "COMMIT", nil)
						}
					}
					require.NoError(t, err)
					leader, err := storage.CollectStoredNodes(ctx, view)
					require.NoError(t, err)
					require.Len(t, leader, 1)
					copy, err := storage.CollectStoredNodes(ctx, storage.NewNamespacedEngine(peer, "typed_command"))
					require.NoError(t, err)
					require.Len(t, copy, 1)
					require.Len(t, copy[0].Properties, len(leader[0].Properties))
					for key, value := range leader[0].Properties {
						other := copy[0].Properties[key]
						require.IsType(t, value, other, key)
						// time.Time's location pointer is codec-specific; typed
						// values preserve their Cypher kind and exact public value.
						require.Equal(t, fmt.Sprint(value), fmt.Sprint(other), key)
					}
					require.Equal(t, leader[0].EmbedMeta, copy[0].EmbedMeta)
					require.Equal(t, leader[0].VisibilitySuppressed, copy[0].VisibilitySuppressed)
					require.True(t, leader[0].CreatedAt.Equal(copy[0].CreatedAt))
					require.True(t, leader[0].UpdatedAt.Equal(copy[0].UpdatedAt))
					tracked, _, _ := manager.GetStorageSize("typed_command")
					recount, err := multidb.NewDatabaseManager(base, nil)
					require.NoError(t, err)
					defer recount.Close()
					exact, _, _ := recount.GetStorageSize("typed_command")
					require.Equal(t, exact, tracked)
					require.NoError(t, manager.SetDatabaseLimits("typed_command", &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: exact, MaxNodes: 1}}))
					for retry := 0; retry < 3; retry++ {
						images, readErr := storage.CollectStoredNodes(ctx, view)
						require.NoError(t, readErr)
						require.Len(t, images, 1)
						unchanged := *images[0]
						require.NoError(t, view.UpdateNode(&unchanged), "unchanged typed payload at exact quota")
						current, _, _ := manager.GetStorageSize("typed_command")
						// Storage may invalidate embedding metadata while staging.
						// Accounting must describe the actual stored final image.
						oracle, oracleErr := multidb.NewDatabaseManager(base, nil)
						require.NoError(t, oracleErr)
						defer oracle.Close()
						observed, _, _ := oracle.GetStorageSize("typed_command")
						require.Equal(t, observed, current, "incremental accounting matches a fresh stored-image recount")
						require.LessOrEqual(t, current, exact)
						exact = current
						require.NoError(t, manager.SetDatabaseLimits("typed_command", &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: exact, MaxNodes: 1}}))
					}
				})
			}
		}
	}
}
