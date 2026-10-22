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

// DisableTruncation sends truncation: false, so the provider fails an
// over-long query or pair instead of shortening it (#907, I27).
func TestRerankerDisableTruncation(t *testing.T) {
	var truncation *bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Truncation *bool `json:"truncation"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		truncation = body.Truncation
		http.Error(w, `{"detail":"Request to model 'rerank-2.5' failed. The query has 8577 tokens, the max allowed is 8000."}`, http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	reranker, err := NewReranker(&RerankerConfig{Enabled: true, APIURL: server.URL, APIKey: "test-key", Timeout: time.Second, DisableTruncation: true})
	require.NoError(t, err)
	results, err := reranker.Rerank(context.Background(), "a long query", []Candidate{{ID: "a", Content: "alpha", Score: 0.4}})
	require.Error(t, err)
	require.Nil(t, results)
	require.NotNil(t, truncation)
	require.False(t, *truncation)
}

// A failed request is an error, so the caller can fall back and say so.
func TestRerankerReportsProviderFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)
	reranker, err := NewReranker(&RerankerConfig{Enabled: true, APIURL: server.URL, APIKey: "test-key", TopK: 2, Timeout: time.Second})
	require.NoError(t, err)
	candidates := []Candidate{{ID: "a", Content: "alpha", Score: 0.4}, {ID: "b", Content: "beta", Score: 0.3}}
	results, err := reranker.Rerank(context.Background(), "query", candidates)
	require.ErrorContains(t, err, "429")
	require.Nil(t, results)
}

// Invalid provider indices fail at the shared wire owner before projection.
func TestRerankerRejectsUnusableResponseAtWireOwner(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 7, "relevance_score": 0.9}},
		}))
	}))
	t.Cleanup(server.Close)
	reranker, err := NewReranker(&RerankerConfig{Enabled: true, APIURL: server.URL, APIKey: "test-key", Timeout: time.Second})
	require.NoError(t, err)
	results, err := reranker.Rerank(context.Background(), "query", []Candidate{{ID: "a", Content: "alpha", Score: 0.4}})
	require.ErrorContains(t, err, "invalid results")
	require.Nil(t, results)
}

// A disabled reranker passes candidates through in their order.
func TestDisabledRerankerPassesThrough(t *testing.T) {
	var reranker *Reranker
	candidates := []Candidate{{ID: "a", Content: "alpha", Score: 0.4}, {ID: "b", Content: "beta", Score: 0.3}}
	results, err := reranker.Rerank(context.Background(), "query", candidates)
	require.NoError(t, err)
	require.Equal(t, []RankedCandidate{
		{ID: "a", Content: "alpha", OriginalRank: 1, NewRank: 1, BiScore: 0.4, CrossScore: 0.4, FinalScore: 0.4},
		{ID: "b", Content: "beta", OriginalRank: 2, NewRank: 2, BiScore: 0.3, CrossScore: 0.3, FinalScore: 0.3},
	}, results)
}

func TestRerankerRequiresExplicitAPIKey(t *testing.T) {
	_, err := NewReranker(&RerankerConfig{Enabled: true})
	require.ErrorContains(t, err, "requires an API key")
}

func TestRerankerMinScoreMarksRawResult(t *testing.T) {
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
	require.Len(t, results, 1)
	require.True(t, results[0].Filtered)
}
