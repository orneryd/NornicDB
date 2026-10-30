package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkSearchTextChunks(b *testing.B) {
	ctx := context.Background()
	embedding := []float32{1, 0}

	b.Run("single_chunk", func(b *testing.B) {
		chunks := []string{"query"}
		response := &SearchResponse{Results: []SearchResult{{NodeID: "node-1", Score: 1}}}
		opts := &SearchOptions{Limit: 10}
		b.ReportAllocs()
		for b.Loop() {
			result, err := SearchTextChunks(
				ctx,
				"query",
				opts,
				func(context.Context, string) ([]string, error) { return chunks, nil },
				func(context.Context, string) ([]float32, error) { return embedding, nil },
				func(context.Context, string, []float32, *SearchOptions) (*SearchResponse, error) {
					return response, nil
				},
			)
			if err != nil || result != response {
				b.Fatalf("unexpected result: response=%p err=%v", result, err)
			}
		}
	})

	b.Run("eight_chunks_100_candidates", func(b *testing.B) {
		const (
			chunkCount     = 8
			candidateCount = 100
		)
		chunks := make([]string, chunkCount)
		responses := make(map[string]*SearchResponse, chunkCount)
		for chunkIndex := range chunkCount {
			chunk := fmt.Sprintf("chunk-%d", chunkIndex)
			chunks[chunkIndex] = chunk
			results := make([]SearchResult, candidateCount)
			for rank := range candidateCount {
				results[rank] = SearchResult{
					NodeID: storage.NodeID(fmt.Sprintf("node-%03d", (rank+chunkIndex*25)%275)),
					Score:  float64(candidateCount - rank),
				}
			}
			responses[chunk] = &SearchResponse{Results: results}
		}
		opts := &SearchOptions{Limit: 10}
		b.ReportAllocs()
		for b.Loop() {
			result, err := SearchTextChunks(
				ctx,
				"query",
				opts,
				func(context.Context, string) ([]string, error) { return chunks, nil },
				func(context.Context, string) ([]float32, error) { return embedding, nil },
				func(_ context.Context, query string, _ []float32, _ *SearchOptions) (*SearchResponse, error) {
					return responses[query], nil
				},
			)
			if err != nil || len(result.Results) != opts.Limit {
				b.Fatalf("unexpected result count=%d err=%v", len(result.Results), err)
			}
		}
	})
}
