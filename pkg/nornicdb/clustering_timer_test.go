package nornicdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestClusteringTimer_StopDoesNotPanic(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Memory.KmeansClusterInterval = 5 * time.Millisecond
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	db.startClusteringTimer(5 * time.Millisecond)

	time.Sleep(10 * time.Millisecond)
	db.stopClusteringTimer()
	db.stopClusteringTimer() // idempotent

	// Give the goroutine a moment to observe stop.
	time.Sleep(10 * time.Millisecond)
}

// The growth rule clusters a database once its vectors grew by the ratio since
// its last clustering and then held still for one check.
func TestClusteringGrowthDue(t *testing.T) {
	due := clusteringGrowthDue(0.2)
	entry := &dbSearchService{lastClusteredEmbedCount: 1000}
	require.False(t, due(entry, 1100), "grew 10%")
	require.False(t, due(entry, 1300), "grew 30%, still loading")
	require.True(t, due(entry, 1300), "grew 30%, then still")

	entry = &dbSearchService{lastClusteredEmbedCount: 1000}
	require.False(t, due(entry, 1100))
	require.False(t, due(entry, 1100), "still, but grew only 10%")

	entry = &dbSearchService{}
	require.False(t, due(entry, 0))
	require.False(t, due(entry, 0), "nothing loaded")
	require.False(t, due(entry, 500))
	require.True(t, due(entry, 500), "first load, then still")

	entry = &dbSearchService{lastClusteredEmbedCount: 1000}
	require.False(t, due(entry, 600))
	require.False(t, due(entry, 600), "shrank")
}

// A bulk load into a running database is clustered once it stops, long
// before the clustering interval.
func TestClusteringTimer_ClustersBulkLoadWhenItStops(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)
	t.Setenv("NORNICDB_KMEANS_GROWTH_CHECK_INTERVAL", "20ms")

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "bulk")
	svc := search.NewServiceWithDimensions(engine, 3)
	svc.EnableClustering(nil, 4)
	svc.SetMinEmbeddingsForClustering(10)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	entry := &dbSearchService{dbName: "bulk", engine: engine, svc: svc}
	db.searchServicesMu.Lock()
	db.searchServices["bulk"] = entry
	db.searchServicesMu.Unlock()

	db.startClusteringTimer(time.Hour)
	t.Cleanup(db.stopClusteringTimer)

	for i := 0; i < 200; i++ {
		node := &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("v%d", i)),
			ChunkEmbeddings: [][]float32{{float32(i%7) + 1, float32(i%5) + 1, float32(i%3) + 1}},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}

	require.Eventually(t, func() bool {
		entry.clusterMu.Lock()
		defer entry.clusterMu.Unlock()
		return entry.lastClusteredEmbedCount == 200
	}, 5*time.Second, 10*time.Millisecond, "clustered after the load, not at the 1h tick")
	require.True(t, svc.IsClusteringEnabled())
	require.NotNil(t, svc.ClusterStats())
	require.Greater(t, svc.ClusterStats().NumClusters, 0)
}
