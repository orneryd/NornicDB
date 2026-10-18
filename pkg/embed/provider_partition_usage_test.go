package embed_test

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestSolReviewStructuredSingletonPartitionUsage(t *testing.T) {
	mock := &batchHTTPMock{}
	e := embed.NewTracedEmbedder(embed.NewCachedEmbedder(newBatchProvider(t, "voyage", "multimodal", mock), 32))
	texts, properties := make([]string, 11), make([]map[string]any, 11)
	for i := range properties {
		properties[i] = map[string]any{"_embedding_content": `[{"type":"image_url","image_url":"https://example.test/mock.png"}]`}
	}
	results, err := e.EmbedDocumentPropertyBatchChunks(context.Background(), texts, properties, 512, 0)
	require.NoError(t, err)
	require.Equal(t, []int{10, 1}, mock.sizes)
	require.Zero(t, results[0].TotalTokens)
	require.Equal(t, 7, results[10].TotalTokens, "last request belongs wholly to the final document")
}

func TestProviderHTTPDocumentPartitionUsage(t *testing.T) {
	for _, mode := range []string{"text", "multimodal", "contextualized"} {
		for _, path := range []string{"documents", "properties"} {
			t.Run(mode+"/"+path, func(t *testing.T) {
				mock := &batchHTTPMock{}
				e := embed.NewTracedEmbedder(embed.NewCachedEmbedder(newBatchProvider(t, "voyage", mode, mock), 2048))
				texts := batchTexts(1001)
				var results []*embed.DocumentChunkResult
				var err error
				if path == "properties" {
					results, err = e.EmbedDocumentPropertyBatchChunks(context.Background(), texts, make([]map[string]any, len(texts)), 512, 0)
				} else {
					results, err = e.EmbedDocumentBatchChunks(context.Background(), texts, 512, 0)
				}
				require.NoError(t, err)
				require.Equal(t, []int{1000, 1}, mock.sizes)
				for i, result := range results {
					require.Equal(t, [][]float32{{float32(i), 1}}, result.Embeddings)
					want := 0
					if i == 1000 { want = 7 }
					require.Equal(t, want, result.TotalTokens, "document %d", i)
				}
			})
		}
	}
}

func TestProviderHTTPTokenBudgetPartitionUsage(t *testing.T) {
	for _, tc := range []struct { mode string; docs int; sizes []int }{
		{"text", 4, []int{3, 1}},
		{"contextualized", 4, []int{3, 1}},
		{"multimodal", 11, []int{10, 1}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			mock := &batchHTTPMock{}
			e := embed.NewTracedEmbedder(embed.NewCachedEmbedder(newBatchProvider(t, "voyage", tc.mode, mock), 32))
			texts := make([]string, tc.docs)
			for i := range texts { texts[i] = strings.Repeat("x", 30000) }
			results, err := e.EmbedDocumentBatchChunks(context.Background(), texts, 0, 0)
			require.NoError(t, err)
			require.Equal(t, tc.sizes, mock.sizes)
			for i, result := range results {
				want := 0
				if i == len(results)-1 { want = 7 }
				require.Equal(t, want, result.TotalTokens, "document %d", i)
			}
		})
	}
}

func TestProviderHTTPChunkPartitionUsage(t *testing.T) {
	for _, mode := range []string{"text", "multimodal"} {
		for _, tc := range []struct { name string; texts []string; sizes, tokens []int }{
			{"one-document", []string{strings.Repeat("x ", 1001)}, []int{1000, 1}, []int{7007}},
			{"pure-and-shared", []string{strings.Repeat("x ", 1001), "1"}, []int{1000, 2}, []int{7000, 0}},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				mock := &batchHTTPMock{}
				e := embed.NewTracedEmbedder(embed.NewCachedEmbedder(newBatchProvider(t, "voyage", mode, mock), 32))
				results, err := e.EmbedDocumentBatchChunks(context.Background(), tc.texts, 1, 0)
				require.NoError(t, err)
				require.Equal(t, tc.sizes, mock.sizes)
				for i, result := range results { require.Equal(t, tc.tokens[i], result.TotalTokens) }
			})
		}
	}
}
