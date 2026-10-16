package mcp

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
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

// chunkVectorEmbedder chunks every discover query into fixed chunks and
// embeds each chunk as its own vector.
type chunkVectorEmbedder struct {
	chunks  []string
	vectors map[string][]float32
}

func (e *chunkVectorEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	return e.vectors[text], nil
}
func (e *chunkVectorEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i], _ = e.Embed(ctx, text)
	}
	return out, nil
}
func (e *chunkVectorEmbedder) ChunkText(string, int, int) ([]string, error) {
	return append([]string(nil), e.chunks...), nil
}
func (*chunkVectorEmbedder) Dimensions() int { return 1024 }
func (*chunkVectorEmbedder) Model() string   { return "chunk-vector-test" }
func (*chunkVectorEmbedder) Backend() string { return "cpu" }

// The same mixed hit through the public discover tool: handleDiscover chunks
// the query, fuses the chunk searches, and the DiscoverResult it returns
// carries the vector chunk's cosine, not the keyword chunk's normalized score.
func TestHandleDiscover_ChunkedMixedHitReportsVectorSimilarity(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	unit := func(x, y float32) []float32 {
		vector := make([]float32, 1024)
		vector[0], vector[1] = x, y
		return vector
	}
	_, err = db.GetStorage().CreateNode(&storage.Node{
		ID:     "mixed",
		Labels: []string{"Memory"},
		Properties: map[string]interface{}{
			"title":   "Mixed",
			"content": "zebrafinch notes about migration",
		},
		ChunkEmbeddings: [][]float32{unit(0.6, 0.8)},
	})
	require.NoError(t, err)
	require.NoError(t, db.BuildSearchIndexes(context.Background()))

	// "zebrafinch" matches the content by keyword, with a vector orthogonal
	// to the document's; "quiet harbor" matches nothing by keyword, and its
	// vector has cosine 0.6 with the document's.
	embedder := &chunkVectorEmbedder{
		chunks: []string{"zebrafinch", "quiet harbor"},
		vectors: map[string][]float32{
			"zebrafinch":   unit(-0.8, 0.6),
			"quiet harbor": unit(1, 0),
		},
	}
	serverCfg := DefaultServerConfig()
	serverCfg.Embedder = embedder
	serverCfg.EmbeddingEnabled = true
	server := NewServer(db, serverCfg)

	result, err := server.handleDiscover(context.Background(), map[string]interface{}{
		"query": "zebrafinch quiet harbor",
		"limit": 5,
		"depth": 1,
	})
	require.NoError(t, err)
	discover := result.(DiscoverResult)
	require.Equal(t, "vector", discover.Method)
	require.Len(t, discover.Results, 1)
	require.Equal(t, "Mixed", discover.Results[0].Title)
	require.InDelta(t, 0.6, discover.Results[0].Similarity, 1e-3, "the vector chunk's cosine, not a keyword score")

	// min_similarity filters on that value.
	result, err = server.handleDiscover(context.Background(), map[string]interface{}{
		"query":          "zebrafinch quiet harbor",
		"limit":          5,
		"depth":          1,
		"min_similarity": 0.7,
	})
	require.NoError(t, err)
	require.Empty(t, result.(DiscoverResult).Results)
}
