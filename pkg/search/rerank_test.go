package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCrossEncoderDisabled(t *testing.T) {
	ce := NewCrossEncoder(&CrossEncoderConfig{
		Enabled: false,
	})

	candidates := []RerankCandidate{
		{ID: "1", Content: "First document", Score: 0.9},
		{ID: "2", Content: "Second document", Score: 0.8},
		{ID: "3", Content: "Third document", Score: 0.7},
	}

	results, err := ce.Rerank(context.Background(), "test query", candidates)
	require.NoError(t, err)

	// Should pass through without reranking
	assert.Len(t, results, 3)
	assert.Equal(t, "1", results[0].ID)
	assert.Equal(t, "2", results[1].ID)
	assert.Equal(t, "3", results[2].ID)

	// Scores should be preserved
	assert.Equal(t, 0.9, results[0].BiScore)
	assert.Equal(t, 0.9, results[0].FinalScore)
}

func TestCrossEncoderRerank(t *testing.T) {
	// Create mock reranking server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return Cohere-style response that reverses the ranking
		response := map[string]interface{}{
			"results": []map[string]interface{}{
				{"index": 2, "relevance_score": 0.95}, // Third doc is most relevant
				{"index": 0, "relevance_score": 0.80}, // First doc second
				{"index": 1, "relevance_score": 0.60}, // Second doc last
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	ce := NewCrossEncoder(&CrossEncoderConfig{
		Enabled: true,
		APIURL:  server.URL,
		Model:   "test-model",
		TopK:    100,
		Timeout: 5 * time.Second,
	})

	candidates := []RerankCandidate{
		{ID: "1", Content: "First document", Score: 0.9},
		{ID: "2", Content: "Second document", Score: 0.8},
		{ID: "3", Content: "Third document", Score: 0.7},
	}

	results, err := ce.Rerank(context.Background(), "test query", candidates)
	require.NoError(t, err)

	// Reranked order should be: 3, 1, 2
	assert.Len(t, results, 3)
	assert.Equal(t, "3", results[0].ID, "Third doc should be first after rerank")
	assert.Equal(t, "1", results[1].ID, "First doc should be second after rerank")
	assert.Equal(t, "2", results[2].ID, "Second doc should be third after rerank")

	// Check scores
	assert.Equal(t, 0.95, results[0].CrossScore)
	assert.Equal(t, 0.80, results[1].CrossScore)
	assert.Equal(t, 0.60, results[2].CrossScore)

	// Check rank tracking
	assert.Equal(t, 3, results[0].OriginalRank, "Third doc was originally rank 3")
	assert.Equal(t, 1, results[0].NewRank, "Third doc is now rank 1")
}

func TestCrossEncoderMinScore(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"results": []map[string]interface{}{
				{"index": 0, "relevance_score": 0.9},
				{"index": 1, "relevance_score": 0.5},
				{"index": 2, "relevance_score": 0.2}, // Below threshold
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	ce := NewCrossEncoder(&CrossEncoderConfig{
		Enabled:  true,
		APIURL:   server.URL,
		MinScore: 0.3, // Filter out scores below 0.3
		Timeout:  5 * time.Second,
	})

	candidates := []RerankCandidate{
		{ID: "1", Content: "First", Score: 0.9},
		{ID: "2", Content: "Second", Score: 0.8},
		{ID: "3", Content: "Third", Score: 0.7},
	}

	results, err := ce.Rerank(context.Background(), "query", candidates)
	require.NoError(t, err)

	// Only 2 results should pass MinScore filter
	assert.Len(t, results, 2)
	assert.Equal(t, "1", results[0].ID)
	assert.Equal(t, "2", results[1].ID)
}

func TestCrossEncoderHuggingFaceFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// HuggingFace TEI format - scores in order
		response := map[string]interface{}{
			"scores": []float64{0.3, 0.9, 0.6},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	ce := NewCrossEncoder(&CrossEncoderConfig{
		Enabled: true,
		APIURL:  server.URL,
		Timeout: 5 * time.Second,
	})

	candidates := []RerankCandidate{
		{ID: "1", Content: "First", Score: 0.9},
		{ID: "2", Content: "Second", Score: 0.8},
		{ID: "3", Content: "Third", Score: 0.7},
	}

	results, err := ce.Rerank(context.Background(), "query", candidates)
	require.NoError(t, err)

	// Should rerank based on HF scores: 2 (0.9) > 3 (0.6) > 1 (0.3)
	assert.Equal(t, "2", results[0].ID)
	assert.Equal(t, "3", results[1].ID)
	assert.Equal(t, "1", results[2].ID)
}

func TestCrossEncoderTopK(t *testing.T) {
	callCount := 0
	var receivedDocs []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		if docs, ok := req["documents"].([]interface{}); ok {
			receivedDocs = make([]string, len(docs))
			for i, d := range docs {
				receivedDocs[i] = d.(string)
			}
		}

		// Return scores for whatever we received
		scores := make([]float64, len(receivedDocs))
		for i := range scores {
			scores[i] = float64(len(receivedDocs)-i) / float64(len(receivedDocs))
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"scores": scores})
	}))
	defer server.Close()

	ce := NewCrossEncoder(&CrossEncoderConfig{
		Enabled: true,
		APIURL:  server.URL,
		TopK:    3, // Only rerank top 3
		Timeout: 5 * time.Second,
	})

	// Send 5 candidates
	candidates := []RerankCandidate{
		{ID: "1", Content: "Doc 1", Score: 0.9},
		{ID: "2", Content: "Doc 2", Score: 0.8},
		{ID: "3", Content: "Doc 3", Score: 0.7},
		{ID: "4", Content: "Doc 4", Score: 0.6},
		{ID: "5", Content: "Doc 5", Score: 0.5},
	}

	_, err := ce.Rerank(context.Background(), "query", candidates)
	require.NoError(t, err)

	// Only 3 documents should be sent to the API
	assert.Len(t, receivedDocs, 3)
}

func TestCrossEncoderAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ce := NewCrossEncoder(&CrossEncoderConfig{
		Enabled: true,
		APIURL:  server.URL,
		Timeout: 5 * time.Second,
	})

	candidates := []RerankCandidate{
		{ID: "1", Content: "First", Score: 0.9},
		{ID: "2", Content: "Second", Score: 0.8},
	}

	// The API error is reported; the search Service owns the fallback.
	results, err := ce.Rerank(context.Background(), "query", candidates)
	require.Error(t, err)
	assert.Nil(t, results)
}

func TestCrossEncoderEmptyCandidates(t *testing.T) {
	ce := NewCrossEncoder(&CrossEncoderConfig{
		Enabled: true,
	})

	results, err := ce.Rerank(context.Background(), "query", []RerankCandidate{})
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestDefaultCrossEncoderConfig(t *testing.T) {
	config := DefaultCrossEncoderConfig()

	assert.False(t, config.Enabled)
	assert.Equal(t, "http://localhost:8081/rerank", config.APIURL)
	assert.Equal(t, "cross-encoder/ms-marco-MiniLM-L-6-v2", config.Model)
	assert.Equal(t, 100, config.TopK)
	assert.Equal(t, 30*time.Second, config.Timeout)
	assert.Equal(t, 0.0, config.MinScore)
}

func TestCrossEncoderWithAuth(t *testing.T) {
	var receivedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"scores": []float64{0.9, 0.8},
		})
	}))
	defer server.Close()

	ce := NewCrossEncoder(&CrossEncoderConfig{
		Enabled: true,
		APIURL:  server.URL,
		APIKey:  "test-api-key",
		Timeout: 5 * time.Second,
	})

	candidates := []RerankCandidate{
		{ID: "1", Content: "First", Score: 0.9},
		{ID: "2", Content: "Second", Score: 0.8},
	}

	_, err := ce.Rerank(context.Background(), "query", candidates)
	require.NoError(t, err)

	assert.Equal(t, "Bearer test-api-key", receivedAuth)
}

func TestCrossEncoderAdditionalAPIResponseBranches(t *testing.T) {
	t.Run("simple rankings response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"rankings": []map[string]interface{}{
					{"index": 1, "score": 0.95},
					{"index": 0, "score": 0.25},
				},
			})
		}))
		defer server.Close()

		ce := NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: server.URL, Timeout: time.Second})
		out, err := ce.Rerank(context.Background(), "query", []RerankCandidate{
			{ID: "a", Content: "alpha", Score: 0.1},
			{ID: "b", Content: "beta", Score: 0.2},
		})
		require.NoError(t, err)
		require.Len(t, out, 2)
		assert.Equal(t, "b", out[0].ID)
		assert.Equal(t, 0.95, out[0].CrossScore)
	})

	t.Run("malformed response is an error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("not-json"))
		}))
		defer server.Close()

		ce := NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: server.URL, Timeout: time.Second})
		out, err := ce.Rerank(context.Background(), "query", []RerankCandidate{
			{ID: "a", Content: "alpha", Score: 0.9},
			{ID: "b", Content: "beta", Score: 0.8},
		})
		require.ErrorContains(t, err, "failed to parse response")
		assert.Nil(t, out)
	})

	t.Run("unrecognized response and invalid request url are errors", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
		}))
		defer server.Close()

		candidates := []RerankCandidate{{ID: "a", Content: "alpha", Score: 0.7}}
		ce := NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: server.URL, Timeout: time.Second})
		out, err := ce.Rerank(context.Background(), "query", candidates)
		require.ErrorContains(t, err, "unable to parse rerank response")
		assert.Nil(t, out)

		badURL := NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: "://bad-url", Timeout: time.Second})
		out, err = badURL.Rerank(context.Background(), "query", candidates)
		require.ErrorContains(t, err, "failed to create request")
		assert.Nil(t, out)
	})
}

func TestCrossEncoder_NameEnabledConfigAndAvailability(t *testing.T) {
	ce := NewCrossEncoder(nil)
	assert.Equal(t, "cross_encoder", ce.Name())
	assert.False(t, ce.Enabled())
	require.NotNil(t, ce.Config())

	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer health.Close()

	ce = NewCrossEncoder(&CrossEncoderConfig{
		Enabled: true,
		APIURL:  health.URL + "/rerank",
		Timeout: 5 * time.Second,
	})
	assert.True(t, ce.Enabled())
	assert.True(t, ce.IsAvailable(context.Background()))

	ce.config.Enabled = false
	assert.False(t, ce.IsAvailable(context.Background()))
}

func TestLLMReranker_DefaultsAndPassThrough(t *testing.T) {
	cfg := DefaultLLMRerankerConfig()
	require.NotNil(t, cfg)
	assert.False(t, cfg.Enabled)
	assert.Equal(t, 25, cfg.MaxCandidates)

	r := NewLLMReranker(nil, nil)
	assert.Equal(t, "heimdall_llm", r.Name())
	assert.False(t, r.Enabled())
	assert.False(t, r.IsAvailable(context.Background()))

	cands := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.8},
		{ID: "b", Content: "beta", Score: 0.6},
	}
	out, err := r.Rerank(context.Background(), "q", cands)
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, "a", out[0].ID)
	assert.Equal(t, "b", out[1].ID)
}

func TestLLMReranker_BuildPromptAndParseResponse(t *testing.T) {
	r := NewLLMReranker(&LLMRerankerConfig{
		Enabled:       true,
		Timeout:       time.Second,
		MaxCandidates: 3,
		MaxDocChars:   5,
		MaxQueryChars: 5,
	}, func(ctx context.Context, prompt string) (string, error) {
		return `{"ranked":[{"index":1,"score":0.9},{"index":0,"score":0.4}]}`, nil
	})

	prompt := r.buildPrompt("query-long", []RerankCandidate{
		{ID: "a", Content: "abcdefg", Score: 0.1},
	})
	assert.Contains(t, prompt, "id=a")
	assert.Contains(t, prompt, "abcde")
	assert.False(t, strings.Contains(prompt, "abcdefg"))

	order, scores := parseLLMRerankResponse(`{"ranked":[{"index":2,"score":0.7},{"index":1,"score":0.5}]}`, 3)
	require.Equal(t, []int{2, 1}, order)
	require.InDelta(t, 0.7, scores[2], 1e-9)

	order, scores = parseLLMRerankResponse(`{"order":[1,0,1],"scores":[0.8,0.4,0.1]}`, 3)
	require.Equal(t, []int{1, 0}, order)
	require.InDelta(t, 0.8, scores[1], 1e-9)
	require.InDelta(t, 0.4, scores[0], 1e-9)

	order, scores = parseLLMRerankResponse("ranks 2 then 0 then 2", 3)
	require.Equal(t, []int{2, 0}, order)
	require.Nil(t, scores)

	order, scores = parseLLMRerankResponse("no ranking", 3)
	require.Nil(t, order)
	require.Nil(t, scores)
}

// When the LLM scores every candidate below MinScore, they keep their order:
// the rerank ran, and found nothing to promote.
func TestLLMReranker_AllBelowMinScoreKeepsOrder(t *testing.T) {
	r := NewLLMReranker(&LLMRerankerConfig{Enabled: true, Timeout: time.Second, MinScore: 0.5}, func(context.Context, string) (string, error) {
		return `{"ranked":[{"index":1,"score":0.2},{"index":0,"score":0.1}]}`, nil
	})
	out, err := r.Rerank(context.Background(), "q", []RerankCandidate{{ID: "a", Content: "alpha", Score: 0.8}, {ID: "b", Content: "beta", Score: 0.6}})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, []string{out[0].ID, out[1].ID})
	assert.Equal(t, 0.8, out[0].FinalScore)
}

func TestLLMReranker_RerankScoredFilteredAndFallbacks(t *testing.T) {
	cfg := &LLMRerankerConfig{
		Enabled:       true,
		Timeout:       time.Second,
		MaxCandidates: 2,
		MaxDocChars:   100,
		MaxQueryChars: 4,
		MinScore:      0.5,
	}
	var gotPrompt string
	r := NewLLMReranker(cfg, func(ctx context.Context, prompt string) (string, error) {
		gotPrompt = prompt
		return `{"ranked":[{"index":1,"score":0.9},{"index":0,"score":0.2}]}`, nil
	})

	cands := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.8},
		{ID: "b", Content: "beta", Score: 0.6},
		{ID: "c", Content: "gamma", Score: 0.4},
	}
	out, err := r.Rerank(context.Background(), "query-long", cands)
	require.NoError(t, err)
	require.NotEmpty(t, gotPrompt)
	require.Len(t, out, 1)
	assert.Equal(t, "b", out[0].ID)
	assert.Equal(t, 1, out[0].NewRank)

	// A malformed response is an error.
	rBad := NewLLMReranker(cfg, func(ctx context.Context, prompt string) (string, error) {
		return "{}", nil
	})
	out, err = rBad.Rerank(context.Background(), "q", cands[:2])
	require.ErrorContains(t, err, "unable to parse rerank response")
	assert.Nil(t, out)

	// So is an order that names no candidate.
	rOutOfRange := NewLLMReranker(cfg, func(ctx context.Context, prompt string) (string, error) {
		return `{"order":[7,9]}`, nil
	})
	out, err = rOutOfRange.Rerank(context.Background(), "q", cands[:2])
	require.ErrorContains(t, err, "unable to parse rerank response")
	assert.Nil(t, out)
}

func TestLLMReranker_RerankOrderFillsOmittedAndErrorFallback(t *testing.T) {
	cfg := &LLMRerankerConfig{
		Enabled:       true,
		Timeout:       time.Second,
		MaxCandidates: 0,
		MaxDocChars:   0,
	}
	cands := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.8},
		{ID: "b", Content: "beta", Score: 0.6},
		{ID: "c", Content: "gamma", Score: 0.4},
	}

	r := NewLLMReranker(cfg, func(ctx context.Context, prompt string) (string, error) {
		return `{"order":[2,0]}`, nil
	})
	var nilCtx context.Context
	out, err := r.Rerank(nilCtx, " query ", cands)
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, "c", out[0].ID)
	assert.Equal(t, "a", out[1].ID)
	assert.Equal(t, "b", out[2].ID)
	assert.Equal(t, 3, out[2].NewRank)
	assert.Equal(t, cands[1].Score, out[2].FinalScore)
	assert.Greater(t, out[0].FinalScore, out[1].FinalScore)

	out, err = r.Rerank(context.Background(), "query", nil)
	require.NoError(t, err)
	assert.Empty(t, out)

	rErr := NewLLMReranker(cfg, func(ctx context.Context, prompt string) (string, error) {
		return "", context.Canceled
	})
	out, err = rErr.Rerank(context.Background(), "query", cands[:2])
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, out)
}
