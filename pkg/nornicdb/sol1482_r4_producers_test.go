package nornicdb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestSol1482R4ColdPersistedNamespaceReset(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(map[bool]string{false: "prefix", true: "restore"}[restore], func(t *testing.T) {
			ctx := context.Background()
			config := DefaultConfig()
			config.Memory.EmbeddingEnabled = false
			config.Memory.DecayEnabled = false
			config.Memory.AutoLinksEnabled = false
			config.Database.DataDir = t.TempDir()
			config.Database.PersistSearchIndexes = true
			db, err := Open(config.Database.DataDir, config)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			waitDBWorkIdle(t, db)
			tenant := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "tenant")
			dims := config.Memory.EmbeddingDimensions
			if dims <= 0 {
				dims = 1024
			}
			vector := make([]float32, dims)
			vector[0] = 1
			_, err = tenant.CreateNode(&storage.Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "backupword"}, ChunkEmbeddings: [][]float32{vector}})
			require.NoError(t, err)
			svc, err := db.EnsureSearchIndexes(ctx, "tenant", tenant, SearchIndexBuildWait)
			require.NoError(t, err)
			requireIndexed(t, svc, 1, "seed")
			backup := filepath.Join(t.TempDir(), "backup.bin")
			require.NoError(t, db.Backup(ctx, backup))
			node, err := tenant.GetNode("a")
			require.NoError(t, err)
			node.Properties["content"] = "laterword"
			require.NoError(t, tenant.UpdateNode(node))
			settleQueuedRefreshes()
			svc.PersistIndexesToDisk()
			path, err := searchPersistenceBasePath(config.Database.DataDir, "tenant")
			require.NoError(t, err)
			require.DirExists(t, path)
			// A lazy/unopened search service retains its persisted state on disk.
			db.ResetSearchService("tenant")
			if restore {
				require.NoError(t, db.Restore(ctx, backup))
			} else {
				_, _, err = db.GetBaseStorageForManager().DeleteByPrefix("tenant:")
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "freshword"}, ChunkEmbeddings: [][]float32{vector}})
				require.NoError(t, err)
			}
			fresh, err := db.EnsureSearchIndexes(ctx, "tenant", tenant, SearchIndexBuildWait)
			require.NoError(t, err)
			if !restore {
				settleQueuedRefreshes()
				require.Equal(t, 1, fresh.FulltextDocCount(), "a recreated tenant retained obsolete persisted documents")
			}
			require.Zero(t, consumerDBHits(t, fresh, "laterword"), "reset loaded an obsolete persisted tenant index")
			if restore {
				require.Equal(t, 1, consumerDBHits(t, fresh, "backupword"), "restored tenant was not reindexed")
			} else {
				require.Equal(t, 1, fresh.FulltextDocCount(), "recreated tenant retained persisted documents")
			}
		})
	}
}
