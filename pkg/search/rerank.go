// Package search provides cross-encoder reranking for improved search quality.
//
// Cross-encoder reranking is a two-stage retrieval approach:
//
//  1. Stage 1 (Fast): Bi-encoder retrieval - vector similarity + BM25
//     - Uses pre-computed embeddings for O(log N) search
//     - Returns top-K candidates (e.g., 100)
//
//  2. Stage 2 (Accurate): Cross-encoder reranking
//     - Passes (query, document) pairs through a cross-encoder model
//     - Model sees both query and document together → more accurate
//     - Re-scores and re-ranks the top-K candidates
//     - Returns final top-N results (e.g., 10)
//
// Why Cross-Encoder?
//
// Bi-encoders (like embeddings) encode query and document separately:
//
//	query_emb  = encode(query)
//	doc_emb    = encode(document)
//	score      = cosine(query_emb, doc_emb)
//
// Cross-encoders encode them together:
//
//	score = cross_encode(query, document)  // sees interaction!
//
// The cross-encoder can capture fine-grained semantic relationships that
// bi-encoders miss, but it's slower (O(N) vs O(log N)).
//
// ELI12 (Explain Like I'm 12):
//
// Imagine finding a book in a library:
//
//   - Bi-encoder (Stage 1): Like using the card catalog.
//     Fast lookup by category/keywords, but might miss nuances.
//
//   - Cross-encoder (Stage 2): Like actually reading each book's summary.
//     More accurate, but takes longer. So we only do it for the
//     top candidates from Stage 1.
//
// Reference: Nogueira & Cho (2019)
// "Passage Re-ranking with BERT"
package search

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Reranker is a Stage-2 reranking component.
//
// Implementations MUST be fail-open: if reranking cannot be performed (service
// unavailable, parse error, timeout), they should return a pass-through ranking
// rather than failing the overall search request.
type Reranker interface {
	// Name identifies the reranker implementation for observability.
	// Examples: "cross_encoder", "heimdall_llm".
	Name() string

	// Enabled indicates whether reranking is configured/enabled.
	Enabled() bool

	// IsAvailable is an optional health check. Implementations should keep this
	// cheap; Search() should not call it on the hot path.
	IsAvailable(ctx context.Context) bool

	// Rerank reorders candidates for a query.
	Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error)
}

// CrossEncoderConfig configures the cross-encoder reranker.
type CrossEncoderConfig struct {
	// Enabled turns on cross-encoder reranking
	Enabled bool

	// APIURL is the reranking service endpoint
	// Supports: Cohere, HuggingFace TEI, local models
	APIURL string

	// APIKey for authentication (if required)
	APIKey string

	// Model name (e.g., "cross-encoder/ms-marco-MiniLM-L-6-v2")
	Model string

	// TopK is how many candidates to rerank (default: 100)
	TopK int

	// Timeout for reranking requests
	Timeout time.Duration

	// MinScore is the minimum rerank score to include (0-1)
	MinScore float64
}

// DefaultCrossEncoderConfig returns sensible defaults.
func DefaultCrossEncoderConfig() *CrossEncoderConfig {
	return &CrossEncoderConfig{
		Enabled:  false,
		APIURL:   "http://localhost:8081/rerank",
		Model:    "cross-encoder/ms-marco-MiniLM-L-6-v2",
		TopK:     100,
		Timeout:  30 * time.Second,
		MinScore: 0.0,
	}
}

// CrossEncoder performs cross-encoder reranking.
type CrossEncoder struct {
	config *CrossEncoderConfig
	client *http.Client
}

func (ce *CrossEncoder) Name() string { return "cross_encoder" }

func (ce *CrossEncoder) Enabled() bool {
	return ce != nil && ce.config != nil && ce.config.Enabled
}

// NewCrossEncoder creates a new cross-encoder reranker.
func NewCrossEncoder(config *CrossEncoderConfig) *CrossEncoder {
	if config == nil {
		config = DefaultCrossEncoderConfig()
	}

	return &CrossEncoder{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

// RerankCandidate represents a document to be reranked.
type RerankCandidate struct {
	ID      string
	Content string
	Score   float64 // Original score (from bi-encoder)
}

// RerankResult is a reranked document with new score.
type RerankResult struct {
	ID           string
	Content      string
	OriginalRank int
	NewRank      int
	BiScore      float64 // Original bi-encoder score
	CrossScore   float64 // Cross-encoder score
	FinalScore   float64 // Combined or cross-encoder score
}

// Rerank takes a query and candidates, returns reranked results.
func (ce *CrossEncoder) Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error) {
	if ce == nil || ce.config == nil || !ce.config.Enabled {
		// Pass through without reranking
		results := make([]RerankResult, len(candidates))
		for i, c := range candidates {
			results[i] = RerankResult{
				ID:           c.ID,
				Content:      c.Content,
				OriginalRank: i + 1,
				NewRank:      i + 1,
				BiScore:      c.Score,
				CrossScore:   c.Score,
				FinalScore:   c.Score,
			}
		}
		return results, nil
	}

	if len(candidates) == 0 {
		return []RerankResult{}, nil
	}

	// Limit candidates to TopK (if configured)
	topK := ce.config.TopK
	if topK <= 0 {
		topK = 100 // Default
	}
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}

	// Call reranking API
	scores, err := ce.callRerankAPI(ctx, query, candidates)
	if err != nil {
		// Fallback to original ranking on error
		return ce.passThrough(candidates), nil
	}

	// Build results with new scores
	results := make([]RerankResult, len(candidates))
	for i, c := range candidates {
		results[i] = RerankResult{
			ID:           c.ID,
			Content:      c.Content,
			OriginalRank: i + 1,
			BiScore:      c.Score,
			CrossScore:   scores[i],
			FinalScore:   scores[i], // Use cross-encoder score as final
		}
	}

	// Sort by cross-encoder score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].CrossScore > results[j].CrossScore
	})

	// Assign new ranks and filter by MinScore
	filtered := make([]RerankResult, 0, len(results))
	for i := range results {
		results[i].NewRank = i + 1
		if results[i].CrossScore >= ce.config.MinScore {
			filtered = append(filtered, results[i])
		}
	}

	return filtered, nil
}

// passThrough returns results without reranking.
func (ce *CrossEncoder) passThrough(candidates []RerankCandidate) []RerankResult {
	results := make([]RerankResult, len(candidates))
	for i, c := range candidates {
		results[i] = RerankResult{
			ID:           c.ID,
			Content:      c.Content,
			OriginalRank: i + 1,
			NewRank:      i + 1,
			BiScore:      c.Score,
			CrossScore:   c.Score,
			FinalScore:   c.Score,
		}
	}
	return results
}

// callRerankAPI calls the cross-encoder service.
func (ce *CrossEncoder) callRerankAPI(ctx context.Context, query string, candidates []RerankCandidate) ([]float64, error) {
	// Build request body - supports multiple formats
	// Format 1: Cohere-style
	// Format 2: HuggingFace TEI-style
	// Format 3: Simple pairs format

	documents := make([]string, len(candidates))
	for i, c := range candidates {
		documents[i] = c.Content
	}

	// Try Cohere format first (most common)
	reqBody := map[string]interface{}{
		"query":     query,
		"documents": documents,
		"model":     ce.config.Model,
		"top_n":     len(documents),
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, localizedError(localization.SearchRerankRequestMarshalFailed(err), err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ce.config.APIURL, bytes.NewReader(body))
	if err != nil {
		return nil, localizedError(localization.SearchRerankRequestCreationFailed(err), err)
	}

	req.Header.Set("Content-Type", "application/json")
	if ce.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+ce.config.APIKey)
	}

	resp, err := ce.client.Do(req)
	if err != nil {
		return nil, localizedError(localization.SearchRerankRequestFailed(err), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, localizedError(localization.SearchRerankAPIStatus(resp.StatusCode), nil)
	}

	// Parse response - handle multiple formats
	var result struct {
		// Cohere format
		Results []struct {
			Index          int     `json:"index"`
			RelevanceScore float64 `json:"relevance_score"`
		} `json:"results"`

		// HuggingFace TEI format
		Scores []float64 `json:"scores"`

		// Simple format
		Rankings []struct {
			Index int     `json:"index"`
			Score float64 `json:"score"`
		} `json:"rankings"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, localizedError(localization.SearchRerankResponseParseFailed(err), err)
	}

	scores := make([]float64, len(candidates))

	// Handle Cohere format
	if len(result.Results) > 0 {
		for _, r := range result.Results {
			if r.Index < len(scores) {
				scores[r.Index] = r.RelevanceScore
			}
		}
		return scores, nil
	}

	// Handle HuggingFace TEI format
	if len(result.Scores) > 0 {
		copy(scores, result.Scores)
		return scores, nil
	}

	// Handle simple format
	if len(result.Rankings) > 0 {
		for _, r := range result.Rankings {
			if r.Index < len(scores) {
				scores[r.Index] = r.Score
			}
		}
		return scores, nil
	}

	return nil, localizedError(localization.SearchRerankResponseUnrecognized(), nil)
}

// IsAvailable checks if the reranking service is available.
func (ce *CrossEncoder) IsAvailable(ctx context.Context) bool {
	if !ce.config.Enabled {
		return false
	}

	// Try a simple health check or minimal rerank request
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimSuffix(ce.config.APIURL, "/rerank")+"/health", nil)
	if err != nil {
		return false
	}

	resp, err := ce.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// Config returns the current configuration.
func (ce *CrossEncoder) Config() *CrossEncoderConfig {
	return ce.config
}
