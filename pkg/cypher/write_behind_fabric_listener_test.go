package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestWriteBehindFabricListenerWriteBack(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit", true: "explicit"}[explicit], func(t *testing.T) {
			b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			m, err := multidb.NewDatabaseManager(b, nil)
			require.NoError(t, err)
			defer m.Close()
			require.NoError(t, m.CreateDatabase("reviewshard"))
			require.NoError(t, m.CreateCompositeDatabase("reviewfabric", []multidb.ConstituentRef{{Alias: "local", DatabaseName: "reviewshard", Type: "local", AccessMode: "read_write"}}))
			require.NoError(t, m.SetDatabaseLimits("reviewshard", &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 20}}))
			shard, err := m.GetStorage("reviewshard")
			require.NoError(t, err)
			cmp, err := m.GetStorage("reviewfabric")
			require.NoError(t, err)
			executor := NewStorageExecutor(cmp)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: m})
			listenerResult := make(chan error, 1)
			b.OnNodeCreated(func(n *storage.Node) {
				if n.ID == "reviewshard:prior" {
					writer := NewStorageExecutor(shard)
					ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
					defer cancel()
					_, err := writer.Execute(ctx, "CREATE (:ListenerWrite {v: 1})", nil)
					listenerResult <- err
				}
			})
			prior, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, prior.SetImplicit(true))
			_, err = prior.CreateNode(&storage.Node{ID: "reviewshard:prior"})
			require.NoError(t, err)
			require.NoError(t, prior.Commit())
			require.True(t, prior.AcknowledgedBuffered())
			ctx := context.Background()
			if explicit {
				_, err = executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err = executor.Execute(ctx, "CALL { USE reviewfabric.local CREATE (n:FabricWrite {v: 2}) RETURN count(n) AS c } RETURN c", nil)
			require.NoError(t, err)
			if explicit {
				_, err = executor.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			require.NoError(t, b.FlushWriteBehind())
			select {
			case err := <-listenerResult:
				require.NoError(t, err, "Fabric participant released listeners before database admission")
			case <-time.After(3 * time.Second):
				t.Fatal("Fabric listener never delivered")
			}
		})
	}
}
