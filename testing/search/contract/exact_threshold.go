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

// Use real exact scans and the exported service START, followed by the public
// transport PULL. The wire models do not expose MaxCandidateLimit. Below-cutoff
// vectors must not consume that budget; another qualifying vector still must.
func RunExactThresholdPopulation(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	for _, eligible := range []int{CandidateCap - 1, CandidateCap, CandidateCap + 1} {
		for _, initial := range []int{2, CandidateCap, 2 * CandidateCap} {
			t.Run(fmt.Sprintf("eligible_%d/start_%d", eligible, initial), func(t *testing.T) {
				f := factory(t, eligible+3, Policy{})
				f.Service.SetIndexFlags(false, true)
				query := make([]float32, f.Service.VectorIndexDimensions())
				query[0] = 1
				opposite := make([]float32, len(query))
				opposite[0] = -1
				for i := 0; i < eligible+3; i++ {
					v := query
					if i >= eligible {
						v = opposite
					}
					node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("threshold-%02d", i)), ChunkEmbeddings: [][]float32{v}}
					_, err := f.Engine.CreateNode(node)
					require.NoError(t, err)
					require.NoError(t, f.Service.IndexNode(node))
				}
				var issued []int
				producer := func(ctx context.Context, _ string, _ []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
					issued = append(issued, opts.Limit)
					response, err := f.Service.Search(ctx, "", query, opts)
					if err == nil {
						require.Equal(t, "vector_brute", response.SearchMethod)
						require.Equal(t, opts.Limit >= eligible, response.RetrievalExhausted, "coverage counts qualifying vectors in the successful scan")
					}
					return response, err
				}
				zero := 0.0
				opts := &search.SearchOptions{Limit: initial, MaxCandidateLimit: CandidateCap, InitialOverfetchRatio: 1, MinSimilarity: &zero}
				first, err := f.Service.SearchTextContinuation(context.Background(), "threshold", opts, search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
				require.NoError(t, err)
				require.Len(t, first.Results, 1)
				require.True(t, first.HasMore)
				last, err := f.Pull(context.Background(), first.QID, 16)
				for _, depth := range issued {
					require.LessOrEqual(t, depth, CandidateCap)
				}
				if eligible > CandidateCap {
					require.ErrorIs(t, err, resultstream.ErrCapacity)
					return
				}
				require.NoError(t, err, "all qualifying rows fit the candidate budget")
				require.False(t, last.More)
				require.True(t, last.Collection)
				require.True(t, last.Ranked)
				require.Equal(t, search.SearchContinuationCollectionComplete, last.Completion)
				all := append([]string{first.Results[0].ID}, last.IDs...)
				expected := make([]string, eligible)
				for i := range expected {
					expected[i] = fmt.Sprintf("threshold-%02d", i)
				}
				require.ElementsMatch(t, expected, all, "every qualifying row exactly once, no rejected vector")
			})
		}
	}
}
