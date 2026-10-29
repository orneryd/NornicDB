package nornicdb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestSol1482R4ReplicatedEventConsumption(t *testing.T) {
	for _, role := range []string{"primary", "standby"} {
		t.Run(role, func(t *testing.T) {
			config := DefaultConfig()
			config.Database.DataDir = t.TempDir()
			config.Memory.EmbeddingEnabled = false
			config.Memory.DecayEnabled = false
			config.Memory.AutoLinksEnabled = false
			t.Setenv("NORNICDB_CLUSTER_MODE", "ha_standby")
			t.Setenv("NORNICDB_CLUSTER_DATA_DIR", filepath.Join(t.TempDir(), "replication"))
			t.Setenv("NORNICDB_CLUSTER_HA_ROLE", role)
			t.Setenv("NORNICDB_CLUSTER_HA_PEER_ADDR", "127.0.0.1:65534")
			t.Setenv("NORNICDB_CLUSTER_BIND_ADDR", "127.0.0.1:0")
			t.Setenv("NORNICDB_CLUSTER_HA_AUTO_FAILOVER", "false")
			db, err := Open(config.Database.DataDir, config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			db.bgWg.Wait()
			require.NotNil(t, db.replicationAdapter)
			apply := func(kind replication.CommandType, payload any) {
				data := searchConsumerCommandData(t, kind, payload)
				require.NoError(t, db.replicationAdapter.ApplyCommand(&replication.Command{Type: kind, Data: data}))
			}
			apply(replication.CmdCreateNode, &storage.Node{ID: "tenant:a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "oldword"}})
			tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant")
			svc, err := db.EnsureSearchIndexes(context.Background(), "tenant", tenant, SearchIndexBuildWait)
			require.NoError(t, err)
			require.Equal(t, 1, consumerDBHits(t, svc, "oldword"))
			apply(replication.CmdUpdateNode, &storage.Node{ID: "tenant:a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "newword"}})
			settleQueuedRefreshes()
			assert.Equal(t, 1, consumerDBHits(t, svc, "newword"), "replicated update did not reach search")
			apply(replication.CmdDeleteByPrefix, "tenant:")
			settleQueuedRefreshes()
			fresh, err := db.EnsureSearchIndexes(context.Background(), "tenant", tenant, SearchIndexBuildWait)
			require.NoError(t, err)
			assert.NotSame(t, svc, fresh, "replicated namespace reset did not drop search")
			assert.Zero(t, fresh.FulltextDocCount(), "replicated prefix delete kept stale search documents")
		})
	}
}
