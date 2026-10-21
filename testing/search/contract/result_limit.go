package contract

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type LimitRequest struct {
	Limit, N, MaxResults int
	Mode                 search.SearchContinuationMode
}
type LimitFixture struct {
	Fixture
	Start func(context.Context, LimitRequest) (Page, error)
}

// RunResultLimit drives real public START and PULL, with actual indexed rows
// and exhaustion evidence. Limit is retrieval depth; MaxResults caps the whole
// stream, independently of N and the completion proof of the last search.
func RunResultLimit(t *testing.T, factory func(*testing.T, bool) LimitFixture) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	for _, vector := range []bool{false, true} {
		t.Run(fmt.Sprintf("vector_%t", vector), func(t *testing.T) {
			f := factory(t, vector)
			f.Service.SetIndexFlags(!vector, vector)
			query := make([]float32, f.Service.VectorIndexDimensions())
			query[0] = 1
			for i := 0; i < 8; i++ {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("ceiling-%02d", i))}
				if vector {
					node.ChunkEmbeddings = [][]float32{query}
				} else {
					node.Properties = map[string]any{"content": "ceilingtoken corpus"}
				}
				_, err := f.Engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, f.Service.IndexNode(node))
			}
			t.Run("one_shot_limit", func(t *testing.T) {
				page, err := f.Start(context.Background(), LimitRequest{Limit: 2})
				require.NoError(t, err)
				require.Len(t, page.IDs, 2, "one-shot limit remains an output ceiling")
				require.False(t, page.More)
			})
			for _, mode := range []search.SearchContinuationMode{search.SearchContinuationRanked, search.SearchContinuationRankedThenID, search.SearchContinuationID} {
				for _, tc := range []struct {
					name              string
					depth, n, ceiling int
				}{
					{"initial_overfetch", 8, 16, 2}, {"initial_buffer", 8, 1, 2},
					{"expanded", 1, 1, 5}, {"expanded_page", 1, 3, 5},
					{"population_equals_ceiling", 8, 16, 8}, {"population_below_ceiling", 8, 16, 9}, {"unbounded", 8, 16, 0},
				} {
					t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
						page, err := f.Start(context.Background(), LimitRequest{tc.depth, tc.n, tc.ceiling, mode})
						require.NoError(t, err)
						var all []string
						pulls := 0
						for {
							require.LessOrEqual(t, len(page.IDs), tc.n, "page size")
							all = append(all, page.IDs...)
							if tc.ceiling > 0 {
								require.LessOrEqual(t, len(all), tc.ceiling, "lifetime ceiling")
							}
							if !page.More {
								break
							}
							require.NotEmpty(t, page.QID)
							require.Equal(t, search.SearchContinuationMoreResults, page.Completion)
							require.False(t, page.Collection)
							pulls++
							require.Less(t, pulls, 16)
							page, err = f.Pull(context.Background(), page.QID, tc.n)
							require.NoError(t, err)
						}
						want := 8
						if tc.ceiling > 0 {
							want = min(want, tc.ceiling)
						}
						require.Len(t, all, want)
						expected := make([]string, want)
						for i := range expected {
							expected[i] = fmt.Sprintf("ceiling-%02d", i)
						}
						require.Equal(t, expected, all, "retain the ranked or ID prefix")
						seen := map[string]bool{}
						for _, id := range all {
							require.False(t, seen[id], "duplicate %s", id)
							seen[id] = true
						}
						// A cap equal to the genuinely exhausted population did not omit rows.
						if want < 8 {
							require.Equal(t, search.SearchContinuationMaxResultsComplete, page.Completion)
							require.False(t, page.Collection)
						} else {
							require.Equal(t, search.SearchContinuationCollectionComplete, page.Completion)
							require.True(t, page.Collection)
						}
					})
				}
			}
		})
	}
}
