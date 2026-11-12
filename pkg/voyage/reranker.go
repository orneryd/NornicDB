package voyage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/providerbatch"
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

// rerankCapabilities is what Voyage publishes for one reranker model: the
// documents in one request, the aggregate request budget, and the query and
// query-plus-document limits those totals are counted under. The published
// amounts are tokens; text is measured in UTF-8 bytes here, a conservative
// upper bound on tokens, so a request stays inside the published budget without
// a provider tokenizer.
//
// A zero field means Voyage publishes no such limit for the configured model.
type rerankCapabilities struct {
	documentsPerRequest int
	requestBudget       int
	pairLimit           int
	queryLimit          int
}

// Limits: https://docs.voyageai.com/docs/reranker (verified 2026-11-10).
// rerankCapabilitiesFor combines the API's 1000-document request cap with
// published model limits. An undocumented model keeps that API cap, with no
// invented token budget; its provider remains authoritative for token limits.
func rerankCapabilitiesFor(model string) rerankCapabilities {
	limits := rerankCapabilities{documentsPerRequest: 1000}
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "rerank-3", "rerank-3-lite", "rerank-2.5", "rerank-2.5-lite":
		limits.requestBudget, limits.pairLimit, limits.queryLimit = 600_000, 32_000, 8000
	case "rerank-2":
		limits.requestBudget, limits.pairLimit, limits.queryLimit = 600_000, 16_000, 4000
	case "rerank-2-lite":
		limits.requestBudget, limits.pairLimit, limits.queryLimit = 600_000, 8000, 2000
	case "rerank-1":
		limits.requestBudget, limits.pairLimit, limits.queryLimit = 300_000, 8000, 2000
	case "rerank-lite-1":
		limits.requestBudget, limits.pairLimit, limits.queryLimit = 300_000, 4000, 1000
	}
	return limits
}

// Rerank applies Voyage's native reranking. It preserves the provider's raw
// indexed result set so search.Service can validate the batch before filtering.
//
// The provider bounds one rerank request by a document count and by an
// aggregate budget in which the query is counted once per document. A window
// larger than that budget is split into consecutive requests that each stay
// inside the model's own published limits, and the answers are merged by score.
// A single request keeps the provider's own order.
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
	limits := rerankCapabilitiesFor(r.config.Model)
	truncating := !r.config.DisableTruncation
	// One document costs the query plus that document. With truncation enabled
	// the provider shortens an over-limit query or pair, so the effective cost
	// is the model's limit; with truncation disabled the real size is counted
	// and the provider's refusal of an over-limit input stays visible instead of
	// being masked or shortened here.
	cost := func(index int) int {
		queryBytes := len(query)
		if truncating && limits.queryLimit > 0 && queryBytes > limits.queryLimit {
			queryBytes = limits.queryLimit
		}
		size := queryBytes + len(head[index].Content)
		if truncating && limits.pairLimit > 0 && size > limits.pairLimit {
			size = limits.pairLimit
		}
		return size
	}
	var results []RankedCandidate
	requests := 0
	err := providerbatch.ForBatches(ctx, "rerank documents", len(head), limits.documentsPerRequest, limits.requestBudget, cost, func(start, end int) error {
		documents := make([]string, end-start)
		for index := start; index < end; index++ {
			documents[index-start] = head[index].Content
		}
		// Every document of the request is scored: the window is already
		// bounded by TopK, and a merged answer needs each request's complete
		// answer to rank the window as one request would.
		resp, err := r.client.Rerank(ctx, query, documents, RerankOptions{
			Model: r.config.Model, TopK: end - start, ReturnDocuments: false, Truncation: truncating,
		})
		if err != nil {
			return err
		}
		requests++
		for rank, item := range resp.Data {
			result := rankedCandidateForIndex(head, start, end-start, item.Index, rank+1, item.RelevanceScore)
			result.Filtered = r.config.MinScore > 0 && item.RelevanceScore < r.config.MinScore
			results = append(results, result)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if requests > 1 {
		// Separate requests return separate provider orders, so the merged
		// answer is ordered by provider score. Equal scores keep the order the
		// requests produced them in, which is the candidate window's own order.
		sort.SliceStable(results, func(i, j int) bool { return results[i].CrossScore > results[j].CrossScore })
		for index := range results {
			results[index].NewRank = index + 1
		}
	}
	return results, nil
}

// rankedCandidateForIndex maps one provider result of the request that started
// at offset onto the candidate window. The provider's index is local to that
// request, so a valid index is remapped to the candidate's position in the
// window before any merge.
//
// An index the request could not have produced keeps an unmappable rank instead
// of being offset onto another request's candidate: search.Service is the single
// owner that validates the complete provider batch, and a provider error must
// stay visible there.
func rankedCandidateForIndex(candidates []Candidate, offset, count, index, newRank int, score float64) RankedCandidate {
	result := RankedCandidate{
		OriginalRank: len(candidates) + 1,
		NewRank:      newRank,
		CrossScore:   score,
		FinalScore:   score,
	}
	if index < 0 || index >= count {
		return result
	}
	candidate := candidates[offset+index]
	result.OriginalRank = offset + index + 1
	result.ID = candidate.ID
	result.Content = candidate.Content
	result.BiScore = candidate.Score
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
