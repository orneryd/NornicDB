package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type xhighThresholdGenerator interface {
	searchCandidatesWithExhaustion(context.Context, []float32, int, float64) ([]Candidate, bool, error)
}

// Eight qualifying vectors and one below the threshold: the full exact scan
// has returned every eligible result even though its stored population is nine.
func TestXhighExactThresholdPopulationBoundary(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	ctx := context.Background()
	query := []float32{1, 0}
	vectorAt := func(i int) []float32 {
		if i == 8 {
			return []float32{-1, 0}
		}
		return []float32{1, 0}
	}
	index := NewVectorIndex(2)
	store, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	cfg := gpu.DefaultEmbeddingIndexConfig(2)
	cfg.GPUEnabled, cfg.AutoSync = false, false
	embeddings := gpu.NewEmbeddingIndex(nil, cfg)
	for i := range 9 {
		id := fmt.Sprintf("eligible-%02d", i)
		require.NoError(t, index.Add(id, vectorAt(i)))
		require.NoError(t, store.Add(id, vectorAt(i)))
		require.NoError(t, embeddings.Add(id, vectorAt(i)))
	}
	for name, generator := range map[string]xhighThresholdGenerator{
		"cpu":              NewBruteForceCandidateGen(index),
		"file":             NewFileStoreBruteForceCandidateGen(store),
		"gpu_cpu_fallback": NewGPUBruteForceCandidateGen(embeddings),
	} {
		t.Run(name, func(t *testing.T) {
			rows, exhausted, err := generator.searchCandidatesWithExhaustion(ctx, query, 8, 0)
			require.NoError(t, err)
			require.Len(t, rows, 8)
			t.Logf("stored=9 eligible=8 limit=8 returned=%d exhausted=%t", len(rows), exhausted)
			assert.True(t, exhausted, "all qualifying vectors were returned by the exact scan")
		})
	}
	t.Run("public_start_pull", func(t *testing.T) {
		base := newTestMemoryEngine(t)
		t.Cleanup(func() { _ = base.Close() })
		engine := storage.NewNamespacedEngine(base, "threshold")
		service := NewServiceWithDimensions(engine, 2)
		t.Cleanup(func() { _ = service.Close() })
		service.SetIndexFlags(false, true)
		for i := range 9 {
			node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("eligible-%02d", i)), ChunkEmbeddings: [][]float32{vectorAt(i)}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, service.IndexNode(node))
		}
		require.Equal(t, 9, service.EmbeddingCount())
		threshold := 0.0
		options := &SearchOptions{Limit: 8, MaxCandidateLimit: 8, InitialOverfetchRatio: 1, MinSimilarity: &threshold}
		embed := func(context.Context, string) ([]float32, error) { return query, nil }
		first, err := service.SearchTextContinuation(ctx, "threshold", options,
			SearchContinuationRequest{Owner: "xhigh", Database: "threshold", N: 1}, nil, embed, service.Search, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		require.Len(t, first.Results, 1)
		require.True(t, first.HasMore)
		last, err := service.SearchTextContinuation(ctx, "", nil,
			SearchContinuationRequest{Owner: "xhigh", Database: "threshold", QID: first.QID, N: 16}, nil, nil, nil, ChunkedSearchErrorPolicy{})
		t.Logf("public PULL after all eight eligible rows fit candidate cap: error=%v", err)
		require.NoError(t, err, "a ninth nonmatching vector cannot make the complete eligible population exceed capacity")
		require.Equal(t, 8, len(first.Results)+len(last.Results))
		require.False(t, last.HasMore)
		assert.True(t, last.CollectionExhausted)
		assert.Equal(t, SearchContinuationCollectionComplete, last.Completion)
	})
}

// The scan receipt and pipeline must agree for threshold equality and for
// eligible populations on either side of the cap, including parallel scans.
func TestExactThresholdQualifiedPopulationMatrix(t *testing.T) {
	for _, eligible := range []int{0, 7, 8, 9, 4097} {
		for _, minimum := range []float64{0, 1} {
			t.Run(fmt.Sprintf("eligible_%d/min_%g", eligible, minimum), func(t *testing.T) {
				index := NewVectorIndex(2)
				store, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				cfg := gpu.DefaultEmbeddingIndexConfig(2)
				cfg.GPUEnabled, cfg.AutoSync = false, false
				embeddings := gpu.NewEmbeddingIndex(nil, cfg)
				for i := 0; i < eligible+3; i++ {
					id := fmt.Sprintf("v-%05d", i)
					v := []float32{1, 0}
					if i >= eligible {
						v = []float32{-1, 0}
					}
					require.NoError(t, index.Add(id, v))
					require.NoError(t, store.Add(id, v))
					require.NoError(t, embeddings.Add(id, v))
				}
				for name, gen := range map[string]xhighThresholdGenerator{"cpu": NewBruteForceCandidateGen(index), "file": NewFileStoreBruteForceCandidateGen(store), "gpu_cpu_fallback": NewGPUBruteForceCandidateGen(embeddings)} {
					t.Run(name, func(t *testing.T) {
						for _, depth := range []int{7, 8, 9, 4096} {
							rows, complete, err := gen.searchCandidatesWithExhaustion(context.Background(), []float32{1, 0}, depth, minimum)
							require.NoError(t, err)
							require.Len(t, rows, min(depth, eligible))
							require.Equal(t, depth >= eligible, complete)
							pipeline := NewVectorSearchPipeline(gen.(CandidateGenerator), &IdentityExactScorer{})
							scored, complete, err := pipeline.searchWithExhaustion(context.Background(), []float32{1, 0}, depth, minimum)
							require.NoError(t, err)
							require.Len(t, scored, min(depth, eligible))
							require.Equal(t, depth >= eligible, complete)
						}
					})
				}
			})
		}
	}
}
