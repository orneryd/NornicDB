package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestR73ReviewEmbeddingAPIURLQueryCredentialsReachProviders(t *testing.T) {
	const rawQuery = "api_key=first%2Bvalue&region=eu&api_key=second%20value"
	tests := []struct {
		name         string
		provider     string
		basePath     string
		apiPath      string
		expectedPath string
	}{
		{name: "ollama", provider: "ollama", basePath: "/proxy", apiPath: "/api/embeddings", expectedPath: "/proxy/api/embeddings"},
		{name: "openai", provider: "openai", basePath: "/proxy", apiPath: "/v1/embeddings", expectedPath: "/proxy/v1/embeddings"},
		{name: "orca", provider: "orca", basePath: "/proxy/v1", expectedPath: "/proxy/v1/embeddings"},
		{name: "voyage", provider: "voyage", basePath: "/proxy", expectedPath: "/proxy/v1/embeddings"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() != test.expectedPath {
					t.Error("provider request used the wrong path")
				}
				if r.URL.RawQuery != rawQuery {
					t.Error("provider request did not preserve the ordered query")
				}
				switch test.provider {
				case "ollama":
					_ = json.NewEncoder(w).Encode(map[string]any{"embedding": []float32{1}, "prompt_eval_count": 1})
				case "voyage":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"data":  []map[string]any{{"index": 0, "embedding": []float32{1}}},
						"model": "route-test",
						"usage": map[string]any{"total_tokens": 1},
					})
				default:
					_ = json.NewEncoder(w).Encode(map[string]any{
						"data":  []map[string]any{{"index": 0, "embedding": []float32{1}}},
						"usage": map[string]any{"total_tokens": 1},
					})
				}
			}))
			t.Cleanup(server.Close)

			embedder, err := NewEmbedder(&Config{
				Provider:   test.provider,
				APIURL:     server.URL + test.basePath + "?" + rawQuery,
				APIPath:    test.apiPath,
				APIKey:     "header-value",
				Model:      "route-test",
				Dimensions: 1,
				Timeout:    time.Second,
			})
			if err != nil {
				t.Fatal("provider construction failed")
			}
			if _, err := embedder.Embed(context.Background(), "probe"); err != nil {
				t.Fatal("provider request failed")
			}
		})
	}
}
