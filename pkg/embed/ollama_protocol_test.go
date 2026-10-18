package embed_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Both protocols are strict: accepting the other request shape would hide the
// regression in custom legacy endpoints and explicitly configured modern ones.
func TestOllamaEndpointProtocol(t *testing.T) {
	for _, endpoint := range []struct {
		path, batchPath string
		legacy          bool
	}{
		{"/legacy-embedding", "/legacy-embedding", true},
		{"/proxy/legacy-embedding?tenant=mock", "/proxy/legacy-embedding?tenant=mock", true},
		{"/api/embeddings", "/api/embed", true},
		{"/proxy/api/embeddings?tenant=mock", "/proxy/api/embed?tenant=mock", true},
		{"/api/embed", "/api/embed", false},
		{"/proxy/api/embed?tenant=mock", "/proxy/api/embed?tenant=mock", false},
	} {
		for _, operation := range []string{"single", "batch", "documents"} {
			t.Run(endpoint.path+"/"+operation, func(t *testing.T) {
				requests := 0
				wantPath := endpoint.path
				if operation != "single" {
					wantPath = endpoint.batchPath
				}
				promptProtocol := endpoint.legacy && wantPath == endpoint.path
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					if r.URL.RequestURI() != wantPath {
						http.Error(w, "wrong endpoint", http.StatusNotFound)
						return
					}
					var body struct {
						Model  string   `json:"model"`
						Prompt *string  `json:"prompt"`
						Input  []string `json:"input"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						return
					}
					require.Equal(t, "mock", body.Model)
					w.Header().Set("Content-Type", "application/json")
					if promptProtocol {
						if body.Prompt == nil || body.Input != nil {
							http.Error(w, "legacy endpoint requires prompt", http.StatusBadRequest)
							return
						}
						i, err := strconv.Atoi(*body.Prompt)
						require.NoError(t, err)
						_ = json.NewEncoder(w).Encode(map[string]any{"embedding": []float32{float32(i), 1}, "prompt_eval_count": 7})
						return
					}
					if body.Prompt != nil || len(body.Input) == 0 {
						http.Error(w, "modern endpoint requires input", http.StatusBadRequest)
						return
					}
					vectors := make([][]float32, len(body.Input))
					for i, text := range body.Input {
						n, err := strconv.Atoi(text)
						require.NoError(t, err)
						vectors[i] = []float32{float32(n), 1}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": vectors, "prompt_eval_count": 7 * len(vectors)})
				}))
				t.Cleanup(server.Close)
				base := embed.NewOllama(&embed.Config{APIURL: server.URL, APIPath: endpoint.path, Model: "mock", Dimensions: 2})
				ctx := context.Background()
				want := [][]float32{{0, 1}, {1, 1}}
				var vectors [][]float32
				if operation == "single" {
					vector, err := base.Embed(ctx, "0")
					require.NoError(t, err)
					vectors, want = [][]float32{vector}, want[:1]
				} else if operation == "batch" {
					var err error
					vectors, err = base.EmbedBatch(ctx, []string{"0", "1"})
					require.NoError(t, err)
				} else {
					wrapped := embed.NewTracedEmbedder(embed.NewCachedEmbedder(base, 16))
					results, err := wrapped.EmbedDocumentBatchChunks(ctx, []string{"0", "1"}, 512, 0)
					require.NoError(t, err)
					for _, result := range results {
						vectors = append(vectors, result.Embeddings...)
					}
				}
				require.Equal(t, want, vectors)
				wantRequests := 1
				if promptProtocol {
					wantRequests = len(want)
				}
				require.Equal(t, wantRequests, requests)
			})
		}
	}
}
