package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A chunked result keeps SearchResult's rank contract: a positive rank names
// the best rank any searched chunk gave it by that method, and 0 means no
// searched chunk found it by that method.
func TestSearchTextChunksKeepsRankProvenance(t *testing.T) {
	t.Run("vector-only hit in every chunk", func(t *testing.T) {
		for _, graph := range []bool{false, true} {
			name := "nodes"
			if graph {
				name = "graph"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				store := newNamespacedEngine(t)
				t.Cleanup(func() { _ = store.Close() })
				vector := []float32{1, 0}
				node := &storage.Node{
					ID:              "source",
					Labels:          []string{"SearchSource"},
					Properties:      map[string]interface{}{"text": "bravo filler"},
					ChunkEmbeddings: [][]float32{vector},
				}
				_, err := store.CreateNode(node)
				require.NoError(t, err)
				svc := NewServiceWithDimensions(store, 2)
				t.Cleanup(func() { _ = svc.Close() })
				require.NoError(t, svc.IndexNode(node))
				opts := DefaultSearchOptions()
				opts.Limit, opts.MinRRFScore, opts.Graph = 100, 0, graph
				chunks := make([]string, 69)
				for index := range chunks {
					chunks[index] = "zulu"
				}
				response, err := SearchTextChunks(ctx, "long query", opts,
					func(context.Context, string) ([]string, error) { return chunks, nil },
					func(context.Context, string) ([]float32, error) { return vector, nil }, svc.Search)
				require.NoError(t, err)
				require.Equal(t, "chunked_rrf_hybrid", response.SearchMethod)
				require.Len(t, response.Results, 1)
				hit := response.Results[0]
				require.InDelta(t, 32.0/61, hit.Score, 1e-12)
				require.Equal(t, 1, hit.VectorRank, "every searched chunk ranked the source first by vector")
				require.Equal(t, 0, hit.BM25Rank, "no searched chunk matched the source by keyword")
			})
		}
	})

	t.Run("vector-only, keyword-only and mixed chunks", func(t *testing.T) {
		chunkResults := map[string][]SearchResult{
			// "both" is found by vector only here, with its highest score.
			"vector chunk": {
				{NodeID: "vector-only", Score: 0.9, VectorRank: 1},
				{NodeID: "both", Score: 0.95, VectorRank: 3},
			},
			// ... and by keyword only here.
			"keyword chunk": {
				{NodeID: "keyword-only", Score: 0.8, BM25Rank: 1},
				{NodeID: "both", Score: 0.2, BM25Rank: 4},
			},
			// ... and by both methods here, with better ranks than above.
			"mixed chunk": {
				{NodeID: "both", Score: 0.5, VectorRank: 2, BM25Rank: 2},
				{NodeID: "keyword-only", Score: 0.1, BM25Rank: 5},
			},
		}
		response, err := SearchTextChunks(
			context.Background(),
			"complete query",
			&SearchOptions{Limit: 10},
			func(context.Context, string) ([]string, error) {
				return []string{"vector chunk", "keyword chunk", "mixed chunk"}, nil
			},
			func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil },
			func(_ context.Context, query string, _ []float32, _ *SearchOptions) (*SearchResponse, error) {
				results, ok := chunkResults[query]
				require.True(t, ok, "unexpected search query %q", query)
				return &SearchResponse{Results: append([]SearchResult(nil), results...)}, nil
			},
		)
		require.NoError(t, err)
		ranks := make(map[storage.NodeID][2]int, len(response.Results))
		for _, result := range response.Results {
			ranks[result.NodeID] = [2]int{result.VectorRank, result.BM25Rank}
		}
		require.Equal(t, map[storage.NodeID][2]int{
			"vector-only":  {1, 0},
			"keyword-only": {0, 1},
			"both":         {2, 2},
		}, ranks)
		for _, result := range response.Results {
			if result.NodeID == "both" {
				// The representative remains the highest-scoring chunk result;
				// only its ranks come from every chunk.
				require.InDelta(t, 1.0/62+1.0/62+1.0/61, result.Score, 1e-12)
			}
		}
	})
}
