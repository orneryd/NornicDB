package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHNSWMutationsRemainImmediatelySearchableAtAnyIndexSize(t *testing.T) {
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	// Cross the former live-update cutoff before indexing the new node. Keeping
	// the large corpus in the canonical vector store makes this regression test
	// cheap while exercising the same Service decision path as production.
	for i := 0; i < 50_001; i++ {
		if err := svc.vectorIndex.Add(fmt.Sprintf("seed-%05d", i), []float32{0, 1}); err != nil {
			t.Fatalf("seed vector %d: %v", i, err)
		}
	}

	idx := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, idx.Add("seed-00000", []float32{0, 1}))
	svc.hnswMu.Lock()
	svc.hnswIndex = idx
	svc.hnswMu.Unlock()
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              "new",
		ChunkEmbeddings: [][]float32{{1, 0}},
	}))

	pipeline := svc.buildPipelineForMode(strategyModeHNSW, svc.vectorIndex, nil)
	require.NotNil(t, pipeline)
	results, err := pipeline.Search(context.Background(), []float32{1, 0}, 1, -1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "new", results[0].ID)

	svc.vectorIndex.Remove("seed-00000")
	svc.hnswRemoveLive("seed-00000")
	results, err = pipeline.Search(context.Background(), []float32{0, 1}, 2, -1)
	require.NoError(t, err)
	for _, result := range results {
		require.NotEqual(t, "seed-00000", result.ID)
	}
}

func BenchmarkHNSWLiveAdditionAtScale(b *testing.B) {
	const dimensions = 32
	for _, baseSize := range []int{1_000, 10_000, 50_000} {
		b.Run(fmt.Sprintf("vectors_%d", baseSize), func(b *testing.B) {
			index := NewHNSWIndex(dimensions, DefaultHNSWConfig())
			for i := 0; i < baseSize; i++ {
				vec := make([]float32, dimensions)
				vec[i%dimensions] = 1
				if err := index.Add(fmt.Sprintf("base-%07d", i), vec); err != nil {
					b.Fatal(err)
				}
			}
			vec := make([]float32, dimensions)
			vec[0] = 1
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := index.Update(fmt.Sprintf("live-%07d", i), vec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPersistedHNSWIncludesLatestLiveMutations(t *testing.T) {
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	require.NoError(t, svc.vectorIndex.Add("existing", []float32{0, 1}))
	require.NoError(t, svc.vectorIndex.Add("new", []float32{1, 0}))

	idx := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, idx.Add("existing", []float32{0, 1}))
	svc.hnswMu.Lock()
	svc.hnswIndex = idx
	svc.hnswMu.Unlock()
	svc.hnswUpdateLive("new", []float32{1, 0})

	path := filepath.Join(t.TempDir(), "hnsw")
	svc.persistHNSWBackground(path)
	reloaded, err := LoadHNSWIndex(path, svc.getVectorLookup())
	require.NoError(t, err)
	require.NotNil(t, reloaded)
	results, err := reloaded.Search(context.Background(), []float32{1, 0}, 1, -1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "new", results[0].ID)
}

func TestHNSWRebuildPreservesMutationsArrivingBeforeSwap(t *testing.T) {
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	require.NoError(t, svc.vectorIndex.Add("existing", []float32{0, 1}))
	require.NoError(t, svc.vectorIndex.Add("trigger", []float32{0.5, 0.5}))

	old := NewHNSWIndex(2, DefaultHNSWConfig())
	require.NoError(t, old.Add("existing", []float32{0, 1}))
	require.NoError(t, old.Add("trigger", []float32{0.5, 0.5}))
	old.Remove("trigger") // Force a maintenance rebuild through tombstone ratio.
	svc.hnswMu.Lock()
	svc.hnswIndex = old
	svc.hnswMu.Unlock()

	// Hold the mutation boundary so the rebuild can finish its store snapshot
	// but cannot swap before the simulated late mutation is recorded.
	svc.indexMu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- svc.maybeRebuildHNSW(context.Background(), 0, 1, 0)
	}()
	deadline := time.Now().Add(time.Second)
	for !svc.hnswRebuildInFlight.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !svc.hnswRebuildInFlight.Load() {
		svc.indexMu.Unlock()
		t.Fatal("HNSW rebuild did not start")
	}
	require.NoError(t, svc.vectorIndex.Add("late", []float32{1, 0}))
	svc.hnswUpdateLive("late", []float32{1, 0})
	svc.indexMu.Unlock()
	require.NoError(t, <-done)

	svc.hnswMu.RLock()
	rebuilt := svc.hnswIndex
	svc.hnswMu.RUnlock()
	results, err := rebuilt.Search(context.Background(), []float32{1, 0}, 1, -1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "late", results[0].ID)
}
