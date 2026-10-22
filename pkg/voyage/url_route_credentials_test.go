package voyage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestR73ReviewVoyageBaseURLQueryCredentialsReachReranker(t *testing.T) {
	const rawQuery = "api_key=first%2Bvalue&region=eu&api_key=second%20value"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/proxy/v1/rerank" {
			t.Error("rerank request used the wrong path")
		}
		if r.URL.RawQuery != rawQuery {
			t.Error("rerank request did not preserve the ordered query")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data":  []map[string]any{{"index": 0, "relevance_score": 0.9}},
			"model": "route-test",
			"usage": map[string]any{"total_tokens": 1},
		})
	}))
	t.Cleanup(server.Close)

	reranker, err := NewReranker(&RerankerConfig{
		Enabled: true,
		APIURL:  server.URL + "/proxy?" + rawQuery,
		APIKey:  "header-value",
		Model:   "route-test",
		TopK:    1,
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatal("reranker construction failed")
	}
	result, err := reranker.Rerank(context.Background(), "probe", []Candidate{{ID: "one", Content: "document"}})
	if err != nil {
		t.Fatal("rerank request failed")
	}
	if len(result) != 1 {
		t.Fatal("rerank response was not returned")
	}
}
