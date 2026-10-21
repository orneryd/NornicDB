package contract

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// The producer's widened prefilter must not consume final-qualified capacity.
// Service START owns the bounded cursor; each fixture PULL uses its real route.
func RunExactThresholdSlack(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	for _, tc := range []struct {
		name  string
		other float32
		cap   int
	}{
		{"reviewer_001_offset", 0.01, 8},
		{"outside_prefilter_control", 1, 8},
		{"inside_slack_at_eligible_cap", 0.001, 8},
		{"inside_slack_at_stored_cap_control", 0.001, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := factory(t, 9, Policy{})
			f.Service.SetIndexFlags(false, true)
			query := make([]float32, f.Service.VectorIndexDimensions())
			query[0] = 1
			for i := 0; i < 9; i++ {
				v := append([]float32(nil), query...)
				if i == 8 {
					v[1] = tc.other
				}
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("slack-%02d", i)), ChunkEmbeddings: [][]float32{v}}
				_, err := f.Engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, f.Service.IndexNode(node))
			}
			minimum := 1.0
			opts := &search.SearchOptions{Limit: 8, MaxCandidateLimit: tc.cap, InitialOverfetchRatio: 1, MinSimilarity: &minimum}
			response, err := f.Service.Search(context.Background(), "", query, opts)
			require.NoError(t, err)
			require.Len(t, response.Results, 8)
			producer := func(ctx context.Context, _ string, _ []float32, o *search.SearchOptions) (*search.SearchResponse, error) {
				return f.Service.Search(ctx, "", query, o)
			}
			first, err := f.Service.SearchTextContinuation(context.Background(), "threshold-slack", opts, search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.Len(t, first.Results, 1)
			last, err := f.Pull(context.Background(), first.QID, 16)
			require.NoError(t, err, "the rejected near duplicate cannot consume eligible capacity")
			require.False(t, last.More)
			require.True(t, last.Collection)
			require.True(t, last.Ranked)
			require.Equal(t, search.SearchContinuationCollectionComplete, last.Completion)
			all := append([]string{first.Results[0].ID}, last.IDs...)
			expected := make([]string, 8)
			for i := range expected {
				expected[i] = fmt.Sprintf("slack-%02d", i)
			}
			require.True(t, response.RetrievalExhausted, "only eight vectors meet the final threshold")
			require.ElementsMatch(t, expected, all)
		})
	}
}
