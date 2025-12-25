package nornicgrpc

import (
	"context"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type stubSearcher struct {
	lastQuery     string
	lastEmbedding []float32
	lastOpts      *search.SearchOptions

	resp *search.SearchResponse
	err  error
}

func (s *stubSearcher) Search(ctx context.Context, query string, embedding []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
	s.lastQuery = query
	s.lastEmbedding = embedding
	s.lastOpts = opts
	return s.resp, s.err
}

func TestService_SearchText_EmbedsAndConvertsResults(t *testing.T) {
	st := &stubSearcher{
		resp: &search.SearchResponse{
			SearchMethod:      "rrf_hybrid",
			FallbackTriggered: false,
			Message:           "",
			Results: []search.SearchResult{
				{
					NodeID:     storage.NodeID("nornic:node1"),
					Labels:     []string{"Doc"},
					Properties: map[string]any{"title": "hello"},
					Score:      0.9,
					RRFScore:   0.8,
					VectorRank: 1,
					BM25Rank:   2,
				},
			},
		},
	}

	svc, err := NewService(
		Config{DefaultDatabase: "nornic", MaxLimit: 100},
		func(ctx context.Context, query string) ([]float32, error) {
			require.Equal(t, "database performance", query)
			return []float32{0.1, 0.2}, nil
		},
		st,
	)
	require.NoError(t, err)

	resp, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{
		Query:  "database performance",
		Limit:  10,
		Labels: []string{"Doc"},
	})
	require.NoError(t, err)
	require.Equal(t, "rrf_hybrid", resp.SearchMethod)
	require.Len(t, resp.Hits, 1)
	require.Equal(t, "nornic:node1", resp.Hits[0].NodeId)
	require.Equal(t, []string{"Doc"}, resp.Hits[0].Labels)
	require.NotNil(t, resp.Hits[0].Properties)
	require.Equal(t, float32(0.9), resp.Hits[0].Score)
	require.Equal(t, float32(0.8), resp.Hits[0].RrfScore)
	require.Equal(t, int32(1), resp.Hits[0].VectorRank)
	require.Equal(t, int32(2), resp.Hits[0].Bm25Rank)

	require.Equal(t, "database performance", st.lastQuery)
	require.Equal(t, []float32{0.1, 0.2}, st.lastEmbedding)
	require.NotNil(t, st.lastOpts)
	require.Equal(t, 10, st.lastOpts.Limit)
	require.Equal(t, []string{"Doc"}, st.lastOpts.Types)
}
