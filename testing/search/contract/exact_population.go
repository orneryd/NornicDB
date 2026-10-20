package contract

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// RunExactPopulation connects actual candidate evidence to the continuation
// owner and public PULL. No response rows or exhaustion flags are scripted.
// Both branches support this exact path; release's compressed policies remain
// covered by Run's existing matrix.
func RunExactPopulation(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	for _, population := range []int{CandidateCap - 1, CandidateCap, CandidateCap + 1} {
		for _, initial := range []int{2, CandidateCap, 2 * CandidateCap} {
			t.Run(fmt.Sprintf("population_%d/start_%d", population, initial), func(t *testing.T) {
				f := factory(t, population, Policy{})
				f.Service.SetIndexFlags(false, true)
				query := make([]float32, f.Service.VectorIndexDimensions())
				query[0] = 1
				for i := 0; i < population; i++ {
					node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("exact-%02d", i)), ChunkEmbeddings: [][]float32{query}}
					_, err := f.Engine.CreateNode(node)
					require.NoError(t, err)
					require.NoError(t, f.Service.IndexNode(node))
				}
				require.Equal(t, population, f.Service.EmbeddingCount())
				require.Zero(t, f.Service.FulltextDocCount())
				var issued []int
				producer := func(ctx context.Context, _ string, _ []float32, options *search.SearchOptions) (*search.SearchResponse, error) {
					issued = append(issued, options.Limit)
					response, err := f.Service.Search(ctx, "", query, options)
					if err == nil {
						require.Equal(t, "vector_brute", response.SearchMethod)
						require.Equal(t, options.Limit >= population, response.RetrievalExhausted)
					}
					return response, err
				}
				zero := 0.0
				options := &search.SearchOptions{Limit: initial, MaxCandidateLimit: CandidateCap, InitialOverfetchRatio: 1, MinSimilarity: &zero}
				first, err := f.Service.SearchTextContinuation(context.Background(), "exact", options, search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
				require.NoError(t, err)
				require.True(t, first.HasMore)
				last, err := f.Pull(context.Background(), first.QID, 32)
				for _, depth := range issued {
					require.LessOrEqual(t, depth, CandidateCap)
				}
				require.Equal(t, min(initial, CandidateCap), issued[0])
				if population > CandidateCap {
					require.ErrorIs(t, err, resultstream.ErrCapacity, "unretrieved live vectors must not be reported complete")
					return
				}
				require.NoError(t, err, "a population-wide exact scan must not fail capacity on the next pull")
				require.False(t, last.More)
				complete := population <= CandidateCap
				reason := search.SearchContinuationCandidateComplete
				if complete {
					reason = search.SearchContinuationCollectionComplete
				}
				require.Equal(t, reason, last.Completion)
				require.Equal(t, complete, last.Collection)
				require.Equal(t, complete, last.Ranked)
				all := append([]string{first.Results[0].ID}, last.IDs...)
				require.Len(t, all, min(population, CandidateCap))
				seen := make(map[string]bool)
				for _, id := range all {
					require.False(t, seen[id], "duplicate %s", id)
					seen[id] = true
				}
				require.LessOrEqual(t, issued[len(issued)-1], max(population, CandidateCap), "no redundant deeper scan after coverage")
			})
		}
	}
}
