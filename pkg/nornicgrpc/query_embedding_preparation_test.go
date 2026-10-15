package nornicgrpc

import (
	"context"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

func TestNativeQueryEmbeddingPreparation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request *gen.SearchTextRequest
		want    int
	}{
		{"text", &gen.SearchTextRequest{Query: "alpha"}, 1},
		{"ranked start", &gen.SearchTextRequest{Query: "alpha", N: 1}, 1},
		{"ranked then ID start", &gen.SearchTextRequest{Query: "alpha", Mode: "ranked_then_id", N: 1}, 1},
		{"ID start", &gen.SearchTextRequest{Mode: "id", N: 1}, 0},
		{"ID start ignores text", &gen.SearchTextRequest{Mode: "id", Query: "alpha", N: 1}, 0},
		{"pull", &gen.SearchTextRequest{Qid: "existing", N: 1}, 0},
		{"discard", &gen.SearchTextRequest{Qid: "existing", Discard: true}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			searcher := &continuationStubSearcher{
				stubSearcher: stubSearcher{resp: &search.SearchResponse{SearchMethod: "rrf_hybrid"}},
				responses:    []*search.SearchContinuationPage{{}},
			}
			prepared := 0
			service, err := NewService(Config{
				PrepareQueryEmbedding: func(_ context.Context, database string) (EmbedQueryFunc, ChunkQueryFunc) {
					require.Equal(t, "nornic", database)
					prepared++
					return func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil },
						func(_ context.Context, text string) ([]string, error) { return []string{text}, nil }
				},
			}, nil, nil, searcher)
			require.NoError(t, err)
			_, err = service.SearchText(context.Background(), tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.want, prepared)
			if tc.name == "text" {
				require.Equal(t, []float32{1, 0}, searcher.lastEmbedding)
			}
		})
	}
}
