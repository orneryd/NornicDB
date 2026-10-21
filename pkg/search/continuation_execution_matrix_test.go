package search

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The oracle is independent of depth-planning implementation: an issued
// request covering a known indexed population cannot later be a capacity
// failure. Declared retrieval and candidate budgets remain distinct evidence.
func TestContinuationExecutionGeneratedDepthMatrix(t *testing.T) {
	const cap = 8
	for _, population := range []int{cap - 1, cap, cap + 1} {
		t.Run(fmt.Sprintf("population_%d", population), func(t *testing.T) {
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "matrix")
			service := NewService(engine)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			for i := 0; i < population; i++ {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%02d", i)), Properties: map[string]any{"content": "matrix corpus"}}
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, service.IndexNode(node))
			}
			require.Equal(t, population, service.FulltextDocCount())
			for _, initial := range []int{1, 4, 8, 16} {
				for _, pattern := range []string{"growing", "plateau", "delayed"} {
					for _, evidence := range []string{"unknown", "retrieval", "budget"} {
						for _, mode := range []SearchContinuationMode{SearchContinuationRanked, SearchContinuationRankedThenID} {
							name := fmt.Sprintf("start_%d/%s/%s/%s", initial, pattern, evidence, mode)
							t.Run(name, func(t *testing.T) {
								var issued []int
								producer := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
									issued = append(issued, opts.Limit)
									count := min(opts.Limit, population)
									if pattern == "plateau" || (pattern == "delayed" && opts.Limit < population) {
										count = 1
									}
									rows := make([]SearchResult, count)
									for i := range rows {
										id := fmt.Sprintf("doc-%02d", i)
										rows[i] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(population - i)}
									}
									return &SearchResponse{Results: rows, SearchMethod: "unreported", RetrievalExhausted: evidence == "retrieval", CandidateBudgetReached: evidence == "budget"}, nil
								}
								request := SearchContinuationRequest{Owner: name, Mode: mode, N: 1}
								page, err := service.SearchTextContinuation(context.Background(), "matrix", &SearchOptions{Limit: initial, MaxCandidateLimit: cap}, request, nil, nil, producer, ChunkedSearchErrorPolicy{})
								seen := map[string]bool{}
								for pulls := 0; err == nil; pulls++ {
									require.Less(t, pulls, 32, "continuation did not terminate")
									for _, r := range page.Results {
										require.False(t, seen[r.ID], "duplicate result %s", r.ID)
										seen[r.ID] = true
									}
									if !page.HasMore {
										break
									}
									request.QID = page.QID
									request.N = 16
									page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
								}
								for _, depth := range issued {
									require.LessOrEqual(t, depth, cap, "every issued step respects MaxCandidateLimit")
								}
								if evidence == "unknown" && population > cap {
									require.True(t, errors.Is(err, resultstream.ErrCapacity), "unknown population beyond the cap must remain incomplete: %v", err)
									return
								}
								require.NoError(t, err, "a population already covered by an issued request cannot become capacity-exceeded")
								require.False(t, page.HasMore)
								if mode == SearchContinuationRankedThenID {
									require.Equal(t, population, len(seen), "the catalog completes the eligible population")
									require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
								} else {
									if evidence == "retrieval" {
										require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
									} else {
										require.Equal(t, SearchContinuationCandidateComplete, page.Completion)
										require.False(t, page.CollectionExhausted)
									}
									if evidence == "unknown" {
										want := population
										if pattern == "plateau" {
											want = 1
										}
										require.Equal(t, want, len(seen))
									}
								}
							})
						}
					}
				}
			}
		})
	}
}
