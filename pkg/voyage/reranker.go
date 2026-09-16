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

// Rerank applies Voyage's native reranking while failing open on provider errors.
func (r *Reranker) Rerank(ctx context.Context, query string, candidates []Candidate) ([]RankedCandidate, error) {
	if !r.Enabled() {
		return passThrough(candidates), nil
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
		Model: r.config.Model, TopK: topK, ReturnDocuments: false, Truncation: true,
	})
	if err != nil {
		return passThrough(head), nil
	}
	results := make([]RankedCandidate, 0, len(resp.Data))
	seenValidResult := false
	for rank, item := range resp.Data {
		if item.Index < 0 || item.Index >= len(head) {
			continue
		}
		seenValidResult = true
		candidate := head[item.Index]
		if r.config.MinScore > 0 && item.RelevanceScore < r.config.MinScore {
			continue
		}
		results = append(results, RankedCandidate{
			ID: candidate.ID, Content: candidate.Content,
			OriginalRank: item.Index + 1, NewRank: rank + 1,
			BiScore: candidate.Score, CrossScore: item.RelevanceScore, FinalScore: item.RelevanceScore,
		})
	}
	if !seenValidResult {
		return passThrough(head), nil
	}
	return results, nil
}

func passThrough(candidates []Candidate) []RankedCandidate {
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
