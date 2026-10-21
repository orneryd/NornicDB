package contract

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// The wire APIs do not expose candidate evidence or a quantization-rescore
// override. Use supported Service START with real scored rows, then public
// PULL, to cross caller ceilings with every supported scoring/evidence policy.
func RunResultLimitEvidence(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	for _, policy := range Policies() {
		t.Run(policy.Name, func(t *testing.T) {
			f := factory(t, 8, policy)
			f.Service.SetIndexFlags(false, true)
			query := make([]float32, f.Service.VectorIndexDimensions())
			query[0] = 1
			for i := 0; i < 8; i++ {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("ceiling-%02d", i)), ChunkEmbeddings: [][]float32{query}}
				_, err := f.Engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, f.Service.IndexNode(node))
			}
			policy.Prepare(t, f.Service)
			for _, initial := range []int{2, 8} {
				for _, ceiling := range []int{2, 5, 8} {
					for _, evidence := range []string{"actual", "unknown", "budget"} {
						t.Run(fmt.Sprintf("start_%d/ceiling_%d/%s", initial, ceiling, evidence), func(t *testing.T) {
							producer := func(ctx context.Context, _ string, _ []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
								actual := *opts
								policy.Options(&actual)
								zero := 0.0
								actual.MinSimilarity = &zero
								response, err := f.Service.Search(ctx, "", query, &actual)
								if err != nil {
									return nil, err
								}
								owned := *response
								if evidence != "actual" {
									owned.RetrievalExhausted = false
								}
								if evidence == "budget" && opts.Limit >= 8 {
									owned.CandidateBudgetReached = true
								}
								return &owned, nil
							}
							first, err := f.Service.SearchTextContinuation(context.Background(), "ceiling", &search.SearchOptions{Limit: initial, MaxCandidateLimit: 8}, search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1, MaxResults: ceiling}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
							require.NoError(t, err)
							all := []string{first.Results[0].ID}
							last := Page{More: first.HasMore, Completion: first.Completion, Collection: first.CollectionExhausted, Ranked: first.RankedPoolExhausted}
							if first.HasMore {
								last, err = f.Pull(context.Background(), first.QID, 32)
								require.NoError(t, err)
								all = append(all, last.IDs...)
							}
							require.Len(t, all, ceiling)
							require.False(t, last.More)
							complete := ceiling == 8 && evidence == "actual"
							reason := search.SearchContinuationMaxResultsComplete
							if complete {
								reason = search.SearchContinuationCollectionComplete
							}
							require.Equal(t, reason, last.Completion)
							require.Equal(t, complete, last.Collection)
							require.Equal(t, complete, last.Ranked)
							seen := map[string]bool{}
							for _, id := range all {
								require.False(t, seen[id])
								seen[id] = true
							}
						})
					}
				}
			}
		})
	}
}
