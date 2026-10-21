package contract

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A canceled full-depth attempt has not retrieved the allowed population. A
// retry through the same public cursor must actually issue that step again,
// then complete, rather than converting the failed attempt into ErrCapacity.
func RunRetry(t *testing.T, factory func(*testing.T, int, Policy) Fixture) {
	for _, policy := range Policies() {
		t.Run(policy.Name+"/retry_cancelled_full_step", func(t *testing.T) {
			f := factory(t, CandidateCap, policy)
			f.Service.SetIndexFlags(false, true)
			query := make([]float32, f.Service.VectorIndexDimensions())
			query[0] = 1
			for i := 0; i < CandidateCap; i++ {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("retry-%02d", i)), ChunkEmbeddings: [][]float32{query}}
				_, err := f.Engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, f.Service.IndexNode(node))
			}
			policy.Prepare(t, f.Service)
			var issued []int
			var signal *boundaryContext
			armed := false
			producer := func(ctx context.Context, _ string, _ []float32, options *search.SearchOptions) (*search.SearchResponse, error) {
				issued = append(issued, options.Limit)
				if armed {
					signal.Context = ctx
					ctx = signal
				}
				opts := *options
				policy.Options(&opts)
				response, err := f.Service.Search(ctx, "", query, &opts)
				if err != nil {
					return nil, err
				}
				owned := *response
				owned.RetrievalExhausted = false
				owned.CandidateBudgetReached = false
				return &owned, nil
			}
			request := search.SearchContinuationRequest{Owner: f.Owner, Database: f.Database, N: 1}
			first, err := f.Service.SearchTextContinuation(context.Background(), "matrix", &search.SearchOptions{Limit: 4, MaxCandidateLimit: CandidateCap}, request, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.True(t, first.HasMore)
			root, cancel := context.WithCancel(context.Background())
			defer cancel()
			signal = &boundaryContext{Context: root, cancel: cancel, target: ".withVectorScoreReader"}
			armed = true
			_, err = f.Pull(root, first.QID, 16)
			require.True(t, signal.reached)
			require.ErrorIs(t, err, context.Canceled)
			armed = false
			last, err := f.Pull(context.Background(), first.QID, 16)
			require.NoError(t, err, "a canceled attempt must not consume the successful retrieval depth")
			require.Equal(t, []int{4, 8, 8}, issued)
			require.False(t, last.More)
			require.Equal(t, search.SearchContinuationCandidateComplete, last.Completion)
			require.Len(t, last.IDs, CandidateCap-1)
			for _, id := range last.IDs {
				require.NotEqual(t, first.Results[0].ID, id)
			}
		})
	}
}
