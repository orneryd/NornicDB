package search

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHybridSearchEvaluatesCandidatePredicateOncePerRequest(t *testing.T) {
	for _, strategy := range []struct {
		name, threshold string
		want            strategyMode
	}{
		{"lexical-hnsw", "0", strategyModeHNSW},
		{"parallel-cpu", "100", strategyModeBruteCPU},
	} {
		t.Run(strategy.name, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", strategy.threshold)

			engine := newFilterTestEngine(t)
			for i := 0; i < 24; i++ {
				_, err := engine.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:doc-%02d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"content": "walrus notes", "slot": int64(i)}, ChunkEmbeddings: [][]float32{{1, 0, 0}}})
				require.NoError(t, err)
			}
			service := NewService(engine, 3, nil)
			t.Cleanup(func() { _ = service.Close() })
			require.NoError(t, service.BuildIndexes(t.Context()))
			require.Equal(t, strategy.want, service.currentPipelineStrategy())
			service.SetSearchResultCachePolicy(0, 0)
			for _, selective := range []bool{false, true} {
				t.Run(fmt.Sprintf("selective-%t", selective), func(t *testing.T) {
					var mu sync.Mutex
					calls := make(map[storage.NodeID]int)
					opts := DefaultSearchOptions()
					opts.Limit = 3
					minimum := 0.0
					opts.MinSimilarity = &minimum
					opts.MinRRFScore = 0
					opts.AdaptiveOverfetch = true
					opts.CandidateTarget = 6
					opts.MaxCandidateLimit = 100
					opts.Where = func(_ context.Context, node *storage.Node) (bool, error) {
						mu.Lock()
						calls[node.ID]++
						mu.Unlock()
						return !selective || node.Properties["slot"].(int64)%8 == 0, nil
					}
					for request := 0; request < 2; request++ {
						mu.Lock()
						clear(calls)
						mu.Unlock()
						response, err := service.Search(t.Context(), "walrus", []float32{1, 0, 0}, opts)
						require.NoError(t, err)
						require.Len(t, response.Results, 3)
						if selective {
							var ids []string
							for _, hit := range response.Results {
								ids = append(ids, hit.ID)
							}
							require.ElementsMatch(t, []string{"nornic:doc-00", "nornic:doc-08", "nornic:doc-16"}, ids)
						}
						mu.Lock()
						require.NotEmpty(t, calls)
						for id, count := range calls {
							require.Equal(t, 1, count, "request%d candidate%s", request, id)
						}
						mu.Unlock()
					}
				})
			}
		})
	}
}
