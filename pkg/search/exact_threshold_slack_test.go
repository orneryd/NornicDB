package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// The public stage deliberately widens the generator threshold by float32
// slack. A near duplicate below the requested minimum must not become an
// extra qualifying match in the completion receipt at the caller's cap.
func TestXhigh014fPublicThresholdSlackProbe(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	for _, tc := range []struct {
		name  string
		other []float32
		cap   int
	}{
		{"below_slack_control", []float32{1, 0.01}, 8},
		{"inside_slack_at_eligible_cap", []float32{1, 0.001}, 8},
		{"inside_slack_at_stored_cap_control", []float32{1, 0.001}, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			t.Cleanup(func() { _ = base.Close() })
			engine := storage.NewNamespacedEngine(base, "threshold")
			service := NewService(engine, 2, nil)
			t.Cleanup(func() { _ = service.Close() })
			service.SetIndexFlags(false, true)
			for i := 0; i < 9; i++ {
				v := []float32{1, 0}
				if i == 8 {
					v = tc.other
				}
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("v%02d", i)), ChunkEmbeddings: [][]float32{v}}
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, service.IndexNode(node))
			}
			minimum := 1.0
			opts := &SearchOptions{Limit: 8, MaxCandidateLimit: tc.cap, InitialOverfetchRatio: 1, MinSimilarity: &minimum}
			response, err := service.Search(ctx, "", []float32{1, 0}, opts)
			require.NoError(t, err)
			require.Len(t, response.Results, 8)
			t.Logf("stored=9 actual_threshold=1 eligible=8 cap=%d returned=%d complete=%t", tc.cap, len(response.Results), response.RetrievalExhausted)
			embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
			first, err := service.SearchTextContinuation(ctx, "query", opts, SearchContinuationRequest{Owner: "review", Database: "threshold", N: 1}, nil, embed, service.Search, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			last, err := service.SearchTextContinuation(ctx, "", nil, SearchContinuationRequest{Owner: "review", Database: "threshold", QID: first.QID, N: 16}, nil, nil, nil, ChunkedSearchErrorPolicy{})
			t.Logf("public pull error=%v", err)
			if assert.NoError(t, err, "the rejected near duplicate must not consume eligible-population capacity") {
				require.Equal(t, 8, len(first.Results)+len(last.Results))
				require.True(t, last.CollectionExhausted)
				require.Equal(t, SearchContinuationCollectionComplete, last.Completion)
			}
		})
	}
}
