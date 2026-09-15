package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVoyageRerankerRequestsNativeEndpoint(t *testing.T) {
	var got struct {
		Query           string   `json:"query"`
		Documents       []string `json:"documents"`
		Model           string   `json:"model"`
		TopK            int      `json:"top_k"`
		ReturnDocuments bool     `json:"return_documents"`
		Truncation      bool     `json:"truncation"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/rerank", r.URL.Path)
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 1, "relevance_score": 0.98},
				{"index": 0, "relevance_score": 0.25},
			},
			"model": "rerank-test",
			"usage": map[string]any{"total_tokens": 12},
		}))
	}))
	t.Cleanup(server.Close)

	reranker, err := NewVoyageReranker(&VoyageRerankConfig{
		Enabled: true,
		APIURL:  server.URL,
		APIKey:  "test-key",
		Model:   "rerank-test",
		TopK:    2,
		Timeout: time.Second,
	})
	require.NoError(t, err)

	results, err := reranker.Rerank(context.Background(), "query", []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.4},
		{ID: "b", Content: "beta", Score: 0.3},
		{ID: "c", Content: "gamma", Score: 0.2},
	})
	require.NoError(t, err)
	require.Equal(t, "query", got.Query)
	require.Equal(t, []string{"alpha", "beta"}, got.Documents)
	require.Equal(t, "rerank-test", got.Model)
	require.Equal(t, 2, got.TopK)
	require.False(t, got.ReturnDocuments)
	require.True(t, got.Truncation)
	require.Equal(t, []RerankResult{
		{ID: "b", Content: "beta", OriginalRank: 2, NewRank: 1, BiScore: 0.3, CrossScore: 0.98, FinalScore: 0.98},
		{ID: "a", Content: "alpha", OriginalRank: 1, NewRank: 2, BiScore: 0.4, CrossScore: 0.25, FinalScore: 0.25},
	}, results)
}

func TestVoyageRerankerFailsOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	reranker, err := NewVoyageReranker(&VoyageRerankConfig{
		Enabled: true,
		APIURL:  server.URL,
		APIKey:  "test-key",
		TopK:    2,
		Timeout: time.Second,
	})
	require.NoError(t, err)

	results, err := reranker.Rerank(context.Background(), "query", []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.4},
		{ID: "b", Content: "beta", Score: 0.3},
	})
	require.NoError(t, err)
	require.Equal(t, []RerankResult{
		{ID: "a", Content: "alpha", OriginalRank: 1, NewRank: 1, BiScore: 0.4, CrossScore: 0.4, FinalScore: 0.4},
		{ID: "b", Content: "beta", OriginalRank: 2, NewRank: 2, BiScore: 0.3, CrossScore: 0.3, FinalScore: 0.3},
	}, results)
}

func TestVoyageRerankerZeroTopKUsesAllCandidates(t *testing.T) {
	var got struct {
		Documents []string `json:"documents"`
		TopK      int      `json:"top_k"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 2, "relevance_score": 0.9},
				{"index": 1, "relevance_score": 0.8},
				{"index": 0, "relevance_score": 0.7},
			},
		}))
	}))
	t.Cleanup(server.Close)

	reranker, err := NewVoyageReranker(&VoyageRerankConfig{
		Enabled: true,
		APIURL:  server.URL,
		APIKey:  "test-key",
		TopK:    0,
		Timeout: time.Second,
	})
	require.NoError(t, err)

	results, err := reranker.Rerank(context.Background(), "query", []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.4},
		{ID: "b", Content: "beta", Score: 0.3},
		{ID: "c", Content: "gamma", Score: 0.2},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta", "gamma"}, got.Documents)
	require.Equal(t, 3, got.TopK)
	require.Equal(t, []string{"c", "b", "a"}, []string{results[0].ID, results[1].ID, results[2].ID})
}

func TestVoyageRerankerMinScoreCanReturnEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 0, "relevance_score": 0.2},
				{"index": 1, "relevance_score": 0.1},
			},
			"model": "rerank-test",
		}))
	}))
	t.Cleanup(server.Close)

	reranker, err := NewVoyageReranker(&VoyageRerankConfig{
		Enabled:  true,
		APIURL:   server.URL,
		APIKey:   "test-key",
		Model:    "rerank-test",
		TopK:     2,
		Timeout:  time.Second,
		MinScore: 0.5,
	})
	require.NoError(t, err)

	results, err := reranker.Rerank(context.Background(), "query", []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.4},
		{ID: "b", Content: "beta", Score: 0.3},
	})
	require.NoError(t, err)
	require.Empty(t, results)
}
