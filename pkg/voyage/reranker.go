package voyage

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Candidate is a document submitted for provider-native reranking.
type Candidate struct {
	ID      string
	Content string
	Score   float64
}

// RankedCandidate is a candidate with its provider-assigned rank and score.
type RankedCandidate struct {
	ID           string
	Content      string
	OriginalRank int
	NewRank      int
	BiScore      float64
	CrossScore   float64
	FinalScore   float64
	// Filtered preserves provider output until search.Service has validated the
	// complete batch. It is exported only because the Voyage adapter and the
	// search owner are separate packages; no protocol projects it.
	Filtered bool `json:"-"`
}

// RerankerConfig configures Voyage's native rerank provider.
type RerankerConfig struct {
	Enabled  bool
	APIURL   string
	APIKey   string
	Model    string
	TopK     int
	Timeout  time.Duration
	MinScore float64
	// DisableTruncation sends truncation: false, so a query or a
	// query-document pair over the model's token limit fails the rerank
	// instead of being shortened by the provider (NORNICDB_SEARCH_RERANK_TRUNCATION=false).
	DisableTruncation bool
}

// DefaultRerankerConfig returns Voyage reranker defaults. APIKey must be
// supplied explicitly through the generic rerank configuration.
func DefaultRerankerConfig() *RerankerConfig {
	return &RerankerConfig{
		Enabled: true,
		APIURL:  DefaultBaseURL,
		Model:   DefaultRerankModel,
		TopK:    100,
		Timeout: 30 * time.Second,
	}
}

// Reranker reranks candidates with Voyage's native rerank API.
type Reranker struct {
	config *RerankerConfig
	client *Client
}

// NewReranker creates a Voyage reranker.
func NewReranker(config *RerankerConfig) (*Reranker, error) {
	if config == nil {
		config = DefaultRerankerConfig()
	}
	cfg := *config
	if strings.TrimSpace(cfg.APIURL) == "" {
		cfg.APIURL = DefaultBaseURL
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("Voyage rerank requires an API key")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = DefaultRerankModel
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	client, err := NewClient(Config{APIKey: cfg.APIKey, BaseURL: cfg.APIURL, Timeout: cfg.Timeout})
	if err != nil {
		return nil, err
	}
	return &Reranker{config: &cfg, client: client}, nil
}

// Name identifies the provider for observability.
func (r *Reranker) Name() string { return "voyage" }

// Enabled reports whether reranking is configured and ready.
func (r *Reranker) Enabled() bool {
	return r != nil && r.config != nil && r.config.Enabled && r.client != nil
}

// IsAvailable performs the cheap local availability check required by search.
func (r *Reranker) IsAvailable(context.Context) bool { return r.Enabled() }

// Rerank applies Voyage's native reranking. It preserves the provider's raw
// indexed result set so search.Service can validate the batch before filtering.
func (r *Reranker) Rerank(ctx context.Context, query string, candidates []Candidate) ([]RankedCandidate, error) {
	if !r.Enabled() {
		return PassThrough(candidates), nil
	}
	if len(candidates) == 0 {
		return []RankedCandidate{}, nil
	}
	topK := len(candidates)
	if r.config.TopK > 0 && r.config.TopK < topK {
		topK = r.config.TopK
	}
	head := candidates[:topK]
	documents := make([]string, len(head))
	for i, candidate := range head {
		documents[i] = candidate.Content
	}
	resp, err := r.client.Rerank(ctx, query, documents, RerankOptions{
		Model: r.config.Model, TopK: topK, ReturnDocuments: false, Truncation: !r.config.DisableTruncation,
	})
	if err != nil {
		return nil, err
	}
	results := make([]RankedCandidate, 0, len(resp.Data))
	for rank, item := range resp.Data {
		result := rankedCandidateForIndex(head, item.Index, rank+1, item.RelevanceScore)
		result.Filtered = r.config.MinScore > 0 && item.RelevanceScore < r.config.MinScore
		results = append(results, result)
	}
	return results, nil
}

// rankedCandidateForIndex preserves the provider's raw candidate index.
// Invalid indices intentionally produce an unmapped result; search.Service is
// the single owner that validates the complete provider batch.
func rankedCandidateForIndex(candidates []Candidate, index, newRank int, score float64) RankedCandidate {
	result := RankedCandidate{
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

// PassThrough ranks candidates in their given order with their given scores:
// the ranking when nothing reranked them.
func PassThrough(candidates []Candidate) []RankedCandidate {
	results := make([]RankedCandidate, len(candidates))
	for i, candidate := range candidates {
		results[i] = RankedCandidate{
			ID: candidate.ID, Content: candidate.Content,
			OriginalRank: i + 1, NewRank: i + 1,
			BiScore: candidate.Score, CrossScore: candidate.Score, FinalScore: candidate.Score,
		}
	}
	return results
}
