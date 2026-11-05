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
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/rerankwire"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/voyage"
)

// Reranker is a Stage-2 reranking component.
//
// When reranking cannot be performed (service unavailable, unusable
// response, timeout), Rerank returns an error instead of a pass-through
// ranking. The Service owns the fail-open: a search keeps its fused order and
// reports the fallback (SearchFallbackRerankFailed), and RerankCandidates
// returns that canonical failure through its existing error result. A disabled
// reranker passes candidates through unchanged.
type Reranker interface {
	// Name identifies the reranker implementation for observability.
	// Examples: "cross_encoder", "heimdall_llm".
	Name() string

	// Enabled indicates whether reranking is configured/enabled.
	Enabled() bool

	// IsAvailable is an optional health check. Implementations should keep this
	// cheap; Search() should not call it on the hot path.
	IsAvailable(ctx context.Context) bool

	// Rerank returns the raw provider-selected candidate indices in provider
	// order. A unique in-range subset is valid. Implementations preserve every
	// returned entry; search.Service alone validates it and completes a valid
	// subset with omitted candidates in their original order.
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
type RerankCandidate = voyage.Candidate

// RerankResult is a reranked document with new score.
type RerankResult = voyage.RankedCandidate

func rerankResultForIndex(candidates []RerankCandidate, index, newRank int, score float64) RerankResult {
	result := RerankResult{
		OriginalRank: index + 1,
		NewRank:      newRank,
		CrossScore:   score,
		FinalScore:   score,
	}
	if index >= 0 && index < len(candidates) {
		candidate := candidates[index]
		result.ID = candidate.ID
		result.Content = candidate.Content
		result.BiScore = candidate.Score
	}
	return result
}

// Rerank takes a query and candidates, returns reranked results.
func (ce *CrossEncoder) Rerank(ctx context.Context, query string, candidates []RerankCandidate) (_ []RerankResult, err error) {
	if ce == nil || ce.config == nil || !ce.config.Enabled {
		return voyage.PassThrough(candidates), nil
	}

	if len(candidates) == 0 {
		return []RerankResult{}, nil
	}

	defer func() {
		err = observability.RedactClientError(err, ce.config.APIURL, ce.config.APIKey)
	}()

	// Limit candidates to TopK (if configured)
	topK := ce.config.TopK
	if topK <= 0 {
		topK = 100 // Default
	}
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}

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

	endpoint, err := observability.ComposeRequestURL(ce.config.APIURL, "", observability.AppendRequestRoute)
	if err != nil {
		return nil, localizedError(localization.SearchRerankRequestCreationFailed(err), err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
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
		Results []rerankwire.IndexedResult `json:"results"`

		// HuggingFace TEI format
		Scores []rerankwire.Required[float64] `json:"scores"`

		// Simple format
		Rankings []rerankwire.IndexedResult `json:"rankings"`
	}

	if err := rerankwire.DecodeResponse(resp.Body, &result); err != nil {
		return nil, localizedError(localization.SearchRerankResponseParseFailed(err), err)
	}

	scores, err := acceptIndexedRerankScores(result.Results, result.Scores, result.Rankings, len(candidates))
	if err != nil {
		return nil, err
	}

	// Preserve every provider entry, including duplicates and invalid indices,
	// for the shared Service validator.
	results := make([]RerankResult, len(scores))
	for i, score := range scores {
		results[i] = rerankResultForIndex(candidates, score.index, i+1, score.score)
	}

	// Sort by cross-encoder score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].CrossScore > results[j].CrossScore
	})

	// Assign new ranks and mark provider filtering. Service validates before it
	// removes filtered entries, so malformed low-score entries cannot disappear.
	for i := range results {
		results[i].NewRank = i + 1
		results[i].Filtered = results[i].CrossScore < ce.config.MinScore
	}

	return results, nil
}

type indexedRerankScore struct {
	index int
	score float64
}

func acceptIndexedRerankScores(results []rerankwire.IndexedResult, scores []rerankwire.Required[float64], rankings []rerankwire.IndexedResult, population int) ([]indexedRerankScore, error) {
	accepted, err := rerankwire.AcceptCrossEncoder(results, scores, rankings, population)
	if err != nil {
		return nil, err
	}
	normalized := make([]indexedRerankScore, len(accepted))
	for index, result := range accepted {
		normalized[index] = indexedRerankScore{index: result.Index, score: result.Score}
	}
	return normalized, nil
}

// IsAvailable checks if the reranking service is available.
func (ce *CrossEncoder) IsAvailable(ctx context.Context) bool {
	if !ce.config.Enabled {
		return false
	}

	// Try a simple health check or minimal rerank request
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	configured, err := url.Parse(ce.config.APIURL)
	if err != nil {
		return false
	}
	healthPath := strings.TrimSuffix(configured.EscapedPath(), "/rerank") + "/health"
	healthURL, err := observability.ComposeRequestURL(ce.config.APIURL, healthPath, observability.MergeRequestFollowup)
	if err != nil {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
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
