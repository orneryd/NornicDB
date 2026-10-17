package search

import (
	"context"
	"fmt"
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

	hnswBeforeInstallHook = func(context.Context) {
		require.NoError(t, svc.IndexNode(&storage.Node{ID: "during", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
		require.NoError(t, svc.RemoveNode("before"))
	}
	t.Cleanup(func() { hnswBeforeInstallHook = nil })
	index, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
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
	_, err := svc.getOrCreateHNSWIndex(ctx, 3)
	require.Error(t, err)
	require.False(t, svc.hnswRebuildInFlight.Load())
	index, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !svc.hnswRebuildInFlight.Load() }, 5*time.Second, time.Millisecond)
	require.Equal(t, 3, index.Size())
}

// A build whose source no longer resolves a vector it was handed (the vector
// was removed after the build's snapshot) still links that node. As the
// entry point, an unresolvable node used to leave every later insertion
// without neighbours, so searches found only one vector. Both with and
// without the build's vector cache.
func TestHNSWBuildLinksVectorTheSourceNoLongerResolves(t *testing.T) {
	for _, cached := range []bool{true, false} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {
			source := NewVectorIndex(2)
			pairs := make([]hnswBuildPair, 0, 6)
			for i := 0; i < 6; i++ {
				id := fmt.Sprintf("v-%d", i)
				require.NoError(t, source.Add(id, []float32{1, float32(i) / 10}))
				stored, ok := source.getVectorRef(id)
				require.True(t, ok)
				pairs = append(pairs, hnswBuildPair{id: id, vec: stored})
			}
			source.Remove("v-0")
			built := newHNSWBuildIndex(2, DefaultHNSWConfig(), source.getVectorRef, len(pairs))
			if !cached {
				built.buildVectorLimit = 0
			}
			for _, pair := range pairs {
				require.NoError(t, built.Add(pair.id, pair.vec))
			}
			built.finishBuild()
			built.Remove("v-0")

			results, err := built.Search(context.Background(), []float32{1, 0}, 10, -1)
			require.NoError(t, err)
			ids := make([]string, 0, len(results))
			for _, result := range results {
				ids = append(ids, result.ID)
			}
			require.ElementsMatch(t, []string{"v-1", "v-2", "v-3", "v-4", "v-5"}, ids)
		})
	}
}
