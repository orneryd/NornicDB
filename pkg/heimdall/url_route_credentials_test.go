package heimdall

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestR73ReviewHeimdallAPIURLQueryCredentialsReachProviders(t *testing.T) {
	const rawQuery = "api_key=first%2Bvalue&region=eu&api_key=second%20value"
	tests := []struct {
		provider     string
		basePath     string
		expectedPath string
	}{
		{provider: "ollama", basePath: "/proxy", expectedPath: "/proxy/api/chat"},
		{provider: "openai", basePath: "/proxy", expectedPath: "/proxy/v1/chat/completions"},
		{provider: "orca", basePath: "/proxy/v1", expectedPath: "/proxy/v1/chat/completions"},
		{provider: "vllm", basePath: "/proxy", expectedPath: "/proxy/v1/chat/completions"},
		{provider: "litellm", basePath: "/proxy", expectedPath: "/proxy/v1/chat/completions"},
	}
	for _, test := range tests {
		t.Run(test.provider, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() != test.expectedPath {
					t.Error("provider request used the wrong path")
				}
				if r.URL.RawQuery != rawQuery {
					t.Error("provider request did not preserve the ordered query")
				}
				if test.provider == "ollama" {
					_ = json.NewEncoder(w).Encode(map[string]any{"message": map[string]any{"content": "ok"}, "done": true})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"choices": []map[string]any{{"message": map[string]any{"content": "ok"}, "finish_reason": "stop"}},
				})
			}))
			t.Cleanup(server.Close)

			manager, err := NewManager(Config{
				Enabled:  true,
				Provider: test.provider,
				APIURL:   server.URL + test.basePath + "?" + rawQuery,
				APIKey:   "header-value",
				Model:    "route-test",
			})
			if err != nil {
				t.Fatal("provider construction failed")
			}
			t.Cleanup(func() { _ = manager.Close() })
			if _, err := manager.Generate(context.Background(), "probe", GenerateParams{}); err != nil {
				t.Fatal("provider request failed")
			}
		})
	}
}
