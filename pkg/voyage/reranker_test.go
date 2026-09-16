package voyage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRerankerRequestsNativeEndpoint(t *testing.T) {
	var got struct {
		Query      string   `json:"query"`
		Documents  []string `json:"documents"`
		Model      string   `json:"model"`
		TopK       int      `json:"top_k"`
		Truncation bool     `json:"truncation"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/rerank", r.URL.Path)
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 1, "relevance_score": 0.98}, {"index": 0, "relevance_score": 0.25}},
		}))
	}))
	t.Cleanup(server.Close)

	reranker, err := NewReranker(&RerankerConfig{Enabled: true, APIURL: server.URL, APIKey: "test-key", Model: "rerank-test", TopK: 2, Timeout: time.Second})
	require.NoError(t, err)
	results, err := reranker.Rerank(context.Background(), "query", []Candidate{
		{ID: "a", Content: "alpha", Score: 0.4},
		{ID: "b", Content: "beta", Score: 0.3},
		{ID: "c", Content: "gamma", Score: 0.2},
	})
	require.NoError(t, err)
	require.Equal(t, "query", got.Query)
	require.Equal(t, []string{"alpha", "beta"}, got.Documents)
	require.Equal(t, "rerank-test", got.Model)
	require.Equal(t, 2, got.TopK)
	require.True(t, got.Truncation)
	require.Equal(t, []string{"b", "a"}, []string{results[0].ID, results[1].ID})
}

func TestRerankerFailsOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)
	reranker, err := NewReranker(&RerankerConfig{Enabled: true, APIURL: server.URL, APIKey: "test-key", TopK: 2, Timeout: time.Second})
	require.NoError(t, err)
	candidates := []Candidate{{ID: "a", Content: "alpha", Score: 0.4}, {ID: "b", Content: "beta", Score: 0.3}}
	results, err := reranker.Rerank(context.Background(), "query", candidates)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, []string{results[0].ID, results[1].ID})
}

func TestRerankerRequiresExplicitAPIKey(t *testing.T) {
	_, err := NewReranker(&RerankerConfig{Enabled: true})
	require.ErrorContains(t, err, "requires an API key")
}

func TestRerankerMinScoreCanReturnEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 0, "relevance_score": 0.2}},
		}))
	}))
	t.Cleanup(server.Close)
	reranker, err := NewReranker(&RerankerConfig{Enabled: true, APIURL: server.URL, APIKey: "test-key", MinScore: 0.5})
	require.NoError(t, err)
	results, err := reranker.Rerank(context.Background(), "query", []Candidate{{ID: "a", Content: "alpha", Score: 0.4}})
	require.NoError(t, err)
	require.Empty(t, results)
}
