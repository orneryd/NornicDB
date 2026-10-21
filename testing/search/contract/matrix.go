// Package contract holds the generated, protocol-independent oracle
// used by the public search continuation tests. It is not a production adapter.
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

const CandidateCap = 8

type Page struct {
	QID        string
	More       bool
	IDs        []string
	Completion string
	Collection bool
	Ranked     bool
}

type Fixture struct {
	Service         *search.Service
	Engine          storage.Engine
	Owner, Database string
	Pull            func(context.Context, string, int) (Page, error)
}

// Run exercises the actual transport PULL with a service-owned cursor. The
// supported service START permits MaxCandidateLimit and scripted evidence,
// which the HTTP/gRPC wire models do not expose. Each scripted step first runs
// real Service.Search, with the branch's supported scoring policy; only the
// result-growth/evidence pattern is controlled by the test.
func Run(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	t.Run("threshold_slack", func(t *testing.T) { RunExactThresholdSlack(t, factory) })
	t.Run("threshold_population", func(t *testing.T) { RunExactThresholdPopulation(t, factory) })
	t.Run("exact_population", func(t *testing.T) { RunExactPopulation(t, factory) })
	t.Run("result_limits", func(t *testing.T) { RunResultLimitEvidence(t, factory) })
	RunRetry(t, factory)
	RunCancellation(t, factory)
	for _, policy := range Policies() {
		for _, population := range []int{CandidateCap - 1, CandidateCap, CandidateCap + 1} {
			t.Run(fmt.Sprintf("%s/population_%d", policy.Name, population), func(t *testing.T) {
				f := factory(t, population, policy)
				f.Service.SetIndexFlags(false, true)
				query := make([]float32, f.Service.VectorIndexDimensions())
				query[0] = 1
				for i := 0; i < population; i++ {
					node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("matrix-%02d", i)), Labels: []string{"Matrix"}, Properties: map[string]any{"eligible": "yes"}, ChunkEmbeddings: [][]float32{query}}
					_, err := f.Engine.CreateNode(node)
					require.NoError(t, err)
					require.NoError(t, f.Service.IndexNode(node))
				}
				require.Equal(t, population, f.Service.EmbeddingCount())
				require.Zero(t, f.Service.FulltextDocCount())
				policy.Prepare(t, f.Service)
				for _, initial := range []int{2, 4, 8, 16} {
					for _, pattern := range []string{"growing", "plateau", "delayed"} {
						for _, evidence := range []string{"unknown", "retrieval", "budget"} {
							t.Run(fmt.Sprintf("start_%d/%s/%s", initial, pattern, evidence), func(t *testing.T) {
								var issued []int
								producer := func(ctx context.Context, _ string, _ []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
									issued = append(issued, opts.Limit)
									actualOptions := *opts
									policy.Options(&actualOptions)
									zero := 0.0
									actualOptions.MinSimilarity = &zero
									response, err := f.Service.Search(ctx, "", query, &actualOptions)
									if err != nil {
										return nil, err
									}
									count := min(opts.Limit, population)
									if pattern == "plateau" || (pattern == "delayed" && opts.Limit < population) {
										count = 2
									}
									require.GreaterOrEqual(t, len(response.Results), count)
									owned := *response
									response = &owned
									response.Results = response.Results[:count]
									response.SearchMethod = "unreported"
									response.RetrievalExhausted = evidence == "retrieval"
									response.CandidateBudgetReached = evidence == "budget"
									return response, nil
								}
								first, err := f.Service.SearchTextContinuation(context.Background(), "matrix", &search.SearchOptions{Limit: initial, MaxCandidateLimit: CandidateCap}, search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
								require.NoError(t, err)
								require.True(t, first.HasMore, "the public pull must consume an actual cursor")
								last, err := f.Pull(context.Background(), first.QID, 32)
								for _, depth := range issued {
									require.LessOrEqual(t, depth, CandidateCap)
								}
								if population > CandidateCap && evidence == "unknown" {
									require.ErrorIs(t, err, resultstream.ErrCapacity)
									return
								}
								require.NoError(t, err)
								require.False(t, last.More)
								wantCompletion := search.SearchContinuationCandidateComplete
								if evidence == "retrieval" {
									wantCompletion = search.SearchContinuationCollectionComplete
								}
								require.Equal(t, wantCompletion, last.Completion)
								require.Equal(t, evidence == "retrieval", last.Collection)
								require.Equal(t, evidence == "retrieval", last.Ranked)
								all := append([]string{first.Results[0].ID}, last.IDs...)
								unique := map[string]bool{}
								for _, id := range all {
									require.False(t, unique[id])
									unique[id] = true
								}
								want := min(min(initial, CandidateCap), population)
								if evidence == "unknown" {
									want = population
								}
								if pattern == "plateau" || (pattern == "delayed" && evidence != "unknown" && min(initial, CandidateCap) < population) {
									want = 2
								}
								require.Len(t, all, want)
							})
						}
					}
				}
			})
		}
	}
}
