package embed_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestProviderHTTPOllamaCustomBatchRoute(t *testing.T) {
	for _, tc := range []struct{ path, want string }{
		{"/api/embeddings", "/api/embed"},
		{"/proxy/api/embeddings", "/proxy/api/embed"},
		{"/api/embeddings?tenant=mock", "/api/embed?tenant=mock"},
		{"/proxy/api/embeddings?tenant=mock", "/proxy/api/embed?tenant=mock"},
		{"/proxy/api/embed?tenant=mock", "/proxy/api/embed?tenant=mock"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.URL.RequestURI() != tc.want {
					http.Error(w, "wrong batch route", http.StatusNotFound)
					return
				}
				var body struct {
					Input []string `json:"input"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				require.Equal(t, []string{"0", "1"}, body.Input)
				_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": [][]float32{{0, 1}, {1, 1}}, "prompt_eval_count": 14})
			}))
			t.Cleanup(server.Close)
			base, err := embed.NewEmbedder(context.Background(), &embed.Config{Provider: "ollama", APIURL: server.URL, APIPath: tc.path, Model: "mock", Dimensions: 2})
			require.NoError(t, err)
			e := embed.NewTracedEmbedder(embed.NewCachedEmbedder(base, 16))
			results, err := e.EmbedDocumentBatchChunks(context.Background(), []string{"0", "1"}, 512, 0)
			require.NoError(t, err)
			require.Equal(t, 1, requests)
			require.Equal(t, [][]float32{{0, 1}}, results[0].Embeddings)
			require.Equal(t, [][]float32{{1, 1}}, results[1].Embeddings)
		})
	}
}

func TestProviderHTTPMultimodalDocumentChunkAlignment(t *testing.T) {
	for _, path := range []string{"single", "batch"} {
		t.Run(path, func(t *testing.T) {
			mock := &batchHTTPMock{}
			e := embed.NewTracedEmbedder(embed.NewCachedEmbedder(newBatchProvider(t, "voyage", "multimodal", mock), 16))
			properties := map[string]any{"_embedding_content": `[{"type":"text","text":"first passage"},{"type":"image_url","image_url":"https://example.test/mock.png"},{"type":"text","text":"later passage"}]`}
			var result *embed.DocumentChunkResult
			if path == "single" {
				var err error
				result, err = e.EmbedDocumentPropertyChunks(context.Background(), "fallback", properties, 512, 0)
				require.NoError(t, err)
			} else {
				results, err := e.EmbedDocumentPropertyBatchChunks(context.Background(), []string{"fallback"}, []map[string]any{properties}, 512, 0)
				require.NoError(t, err)
				result = results[0]
			}
			require.Len(t, result.Embeddings, 1)
			require.Equal(t, []string{"first passage\nlater passage"}, result.Chunks)
			require.Equal(t, 7, result.TotalTokens)
			require.Equal(t, []int{1}, mock.sizes)
		})
	}
}
