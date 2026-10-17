package search

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A vector stored, or one removed, while the HNSW index is first built from
// its snapshot reaches the installed graph: the
// build records such changes and replays them once the graph is installed,
// as a rebuild does.
func TestHNSWInitialBuildKeepsChangesThatRaceIt(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0") // HNSW at any size
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	svc := NewServiceWithDimensions(engine, 3)
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "before", ChunkEmbeddings: [][]float32{{0, 1, 0}}}))

	hnswBeforeInstallHook = func() {
		require.NoError(t, svc.IndexNode(&storage.Node{ID: "during", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
		require.NoError(t, svc.RemoveNode("before"))
	}
	t.Cleanup(func() { hnswBeforeInstallHook = nil })
	index, err := svc.getOrCreateHNSWIndex(context.Background(), 3, svc.pipelineBuildSeeds().hnswHints)
	require.NoError(t, err)
	hnswBeforeInstallHook = nil

	require.Eventually(t, func() bool { return !svc.hnswRebuildInFlight.Load() }, 5*time.Second, time.Millisecond)
	require.Equal(t, 1, index.Size())
	results, err := index.Search(context.Background(), []float32{1, 0, 0}, 5, 0)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "during", results[0].ID)

	// After the replay, changes reach the graph directly again.
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "after", ChunkEmbeddings: [][]float32{{0, 0, 1}}}))
	require.Equal(t, 2, index.Size())
}

// A build that fails stops recording the changes that race it, so a later
// build or rebuild can record its own.
func TestHNSWFailedInitialBuildStopsRecording(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	svc := NewServiceWithDimensions(engine, 3)
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		require.NoError(t, svc.IndexNode(&storage.Node{ID: id, ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.getOrCreateHNSWIndex(ctx, 3, svc.pipelineBuildSeeds().hnswHints)
	require.Error(t, err)
	require.False(t, svc.hnswRebuildInFlight.Load())
	index, err := svc.getOrCreateHNSWIndex(context.Background(), 3, svc.pipelineBuildSeeds().hnswHints)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !svc.hnswRebuildInFlight.Load() }, 5*time.Second, time.Millisecond)
	require.Equal(t, 3, index.Size())
}
