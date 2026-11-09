package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

// Exercise real scans, rather than scripted exhaustion reports: equality of
// the issued limit and the scanned population must be positive coverage proof.
func TestExactCandidatePopulationBoundary(t *testing.T) {
	for _, population := range []int{0, 1, 8, 9} {
		t.Run(fmt.Sprintf("population_%d", population), func(t *testing.T) {
			index := NewVectorIndex(2)
			store, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			cfg := gpu.DefaultEmbeddingIndexConfig(2)
			cfg.GPUEnabled, cfg.AutoSync = false, false
			embeddings := gpu.NewEmbeddingIndex(nil, cfg)
			for i := 0; i < population; i++ {
				id := fmt.Sprintf("vector-%02d", i)
				require.NoError(t, index.Add(id, exhaustionQuery))
				require.NoError(t, store.Add(id, exhaustionQuery))
				require.NoError(t, embeddings.Add(id, exhaustionQuery))
			}
			for name, gen := range map[string]exhaustionGenerator{
				"cpu":              NewBruteForceCandidateGen(index),
				"file":             NewFileStoreBruteForceCandidateGen(store),
				"gpu_cpu_fallback": NewGPUBruteForceCandidateGen(embeddings),
			} {
				t.Run(name, func(t *testing.T) {
					if population > 0 {
						ctx, cancel := context.WithCancel(context.Background())
						cancel()
						rows, exhausted, err := gen.searchCandidatesWithExhaustion(ctx, exhaustionQuery, population, -1)
						require.ErrorIs(t, err, context.Canceled)
						require.Empty(t, rows)
						require.False(t, exhausted, "failed full-depth scans cannot supply coverage proof")
					}
					for _, depth := range []int{1, 7, 8, 9} {
						t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
							requireExhaustion(t, gen, depth, -1, min(depth, population), depth >= population)
							requireExhaustion(t, gen, depth, 2, 0, true)
							pipeline := NewVectorSearchPipeline(gen.(CandidateGenerator), &IdentityExactScorer{})
							results, exhausted, err := pipeline.searchWithExhaustion(context.Background(), exhaustionQuery, depth, -1)
							require.NoError(t, err)
							require.Len(t, results, min(depth, population))
							require.Equal(t, depth >= population, exhausted, "pipeline must preserve the scan's coverage proof")
						})
					}
				})
			}
		})
	}
}

func TestExactCandidatePopulationLargeScan(t *testing.T) {
	index := NewVectorIndex(2)
	// Release scans this population on parallel workers; develop retains its
	// existing exact implementation. The evidence contract is the same.
	const population = 4097
	for i := 0; i < population; i++ {
		require.NoError(t, index.Add(fmt.Sprintf("parallel-%04d", i), exhaustionQuery))
	}
	gen := NewBruteForceCandidateGen(index)
	requireExhaustion(t, gen, population-1, -1, population-1, false)
	requireExhaustion(t, gen, population, -1, population, true)
	requireExhaustion(t, gen, population+1, -1, population, true)
}
