package search

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
	"testing"
)

// Every receipt consumer must resolve final qualification, including more
// rejected near duplicates than a stage batch and a genuine overflow.
func TestExactReceiptFinalQualificationAcrossProviders(t *testing.T) {
	for _, eligible := range []int{0, 7, 8, 9} {
		for _, rejected := range []int{1, 65} {
			t.Run(fmt.Sprintf("eligible_%d/rejected_%d", eligible, rejected), func(t *testing.T) {
				index := NewVectorIndex(2)
				store, err := NewVectorFileStore(t.TempDir()+"/vectors", 2)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				cfg := gpu.DefaultEmbeddingIndexConfig(2)
				cfg.GPUEnabled, cfg.AutoSync = false, false
				embeddings := gpu.NewEmbeddingIndex(nil, cfg)
				for i := 0; i < eligible+rejected; i++ {
					v := []float32{1, 0}
					if i >= eligible {
						v = []float32{1, 0.001}
					}
					id := fmt.Sprintf("v-%03d", i)
					require.NoError(t, index.Add(id, v))
					require.NoError(t, store.Add(id, v))
					require.NoError(t, embeddings.Add(id, v))
				}
				for name, p := range map[string]*VectorSearchPipeline{
					"cpu":              NewVectorSearchPipeline(NewBruteForceCandidateGen(index), NewCPUExactScorer(index)),
					"file":             NewVectorSearchPipeline(NewFileStoreBruteForceCandidateGen(store), NewCPUExactScorer(store)),
					"gpu_cpu_fallback": NewVectorSearchPipeline(NewGPUBruteForceCandidateGen(embeddings), NewCPUExactScorer(index)),
				} {
					t.Run(name, func(t *testing.T) {
						rows, complete, err := p.searchWithExhaustion(context.Background(), []float32{1, 0}, 8, 1)
						require.NoError(t, err)
						require.Len(t, rows, min(8, eligible))
						require.Equal(t, eligible <= 8, complete, "completion uses final qualifying nodes, not widened matches")
						for _, r := range rows {
							require.Equal(t, 1.0, r.Score)
							require.Less(t, r.ID, fmt.Sprintf("v-%03d", eligible))
						}
					})
				}
			})
		}
	}
}

// A final routine may round a cosine a few float32 bits higher than the
// generator. Removing the safety margin would lose this qualifying result.
func TestExactReceiptKeepsFloatSafetyMargin(t *testing.T) {
	index := NewVectorIndex(2)
	store, err := NewVectorFileStore(t.TempDir()+"/vectors", 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	cfg := gpu.DefaultEmbeddingIndexConfig(2)
	cfg.GPUEnabled, cfg.AutoSync = false, false
	embeddings := gpu.NewEmbeddingIndex(nil, cfg)
	for _, id := range []string{"kept", "rejected"} {
		require.NoError(t, index.Add(id, []float32{1, 0.001}))
		require.NoError(t, store.Add(id, []float32{1, 0.001}))
		require.NoError(t, embeddings.Add(id, []float32{1, 0.001}))
	}
	for name, gen := range map[string]CandidateGenerator{"cpu": NewBruteForceCandidateGen(index), "file": NewFileStoreBruteForceCandidateGen(store), "gpu_cpu_fallback": NewGPUBruteForceCandidateGen(embeddings)} {
		t.Run(name, func(t *testing.T) {
			raw, err := gen.SearchCandidates(context.Background(), []float32{1, 0}, 2, candidatePrefilter(1, 2))
			require.NoError(t, err)
			require.Len(t, raw, 2)
			for _, r := range raw {
				require.Less(t, r.Score, 1.0)
				require.GreaterOrEqual(t, r.Score+exactScoreSlack(2), 1.0)
			}
			p := NewVectorSearchPipeline(gen, slackFinalScoreProbe{})
			rows, complete, err := p.searchWithExhaustion(context.Background(), []float32{1, 0}, 1, 1)
			require.NoError(t, err)
			require.True(t, complete)
			require.Equal(t, []ScoredCandidate{{ID: "kept", Score: 1}}, rows)
		})
	}
}

type slackFinalScoreProbe struct{}

func (slackFinalScoreProbe) ScoreCandidates(_ context.Context, _ []float32, pool []Candidate) ([]ScoredCandidate, error) {
	out := make([]ScoredCandidate, len(pool))
	for i, c := range pool {
		score := c.Score
		if c.ID == "kept" {
			score = 1
		}
		out[i] = ScoredCandidate{ID: c.ID, Score: score}
	}
	return out, nil
}
