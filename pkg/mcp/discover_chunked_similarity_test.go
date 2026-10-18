package mcp

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// A chunked discover hit whose best chunk matched only by keyword, but which
// another chunk matched by vector, reports that vector similarity rather
// than its keyword score clamped to 1.
func TestDiscoverResultSimilarityOfChunkedMixedHit(t *testing.T) {
	response, err := search.SearchTextChunks(
		context.Background(),
		"complete query",
		&search.SearchOptions{Limit: 10},
		func(context.Context, string) ([]string, error) {
			return []string{"keyword chunk", "vector chunk"}, nil
		},
		func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil },
		func(_ context.Context, query string, _ []float32, _ *search.SearchOptions) (*search.SearchResponse, error) {
			if query == "keyword chunk" {
				return &search.SearchResponse{Results: []search.SearchResult{{NodeID: "mixed", Score: 0.9, Similarity: 7.5, BM25Rank: 1}}}, nil
			}
			return &search.SearchResponse{Results: []search.SearchResult{{NodeID: "mixed", Score: 0.3, Similarity: 0.62, VectorRank: 1}}}, nil
		},
	)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.InDelta(t, 0.62, discoverResultSimilarity(response.Results[0]), 1e-12)
}
