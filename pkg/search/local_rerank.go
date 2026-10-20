// Package search provides local GGUF-based Stage-2 reranking.
//
// LocalReranker uses a BGE-style reranker model (e.g. BGE-Reranker-v2-m3) loaded
// via localllm, scoring (query, document) pairs and reordering candidates by
// relevance. It is independent of Heimdall and dedicated to search like the
// embedding model.

package search

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/rerankwire"
	"github.com/orneryd/nornicdb/pkg/voyage"
)

// RerankScorer scores a (query, document) pair for relevance.
// Implementations are typically *localllm.RerankerModel (loaded from GGUF).
type RerankScorer interface {
	Score(ctx context.Context, query, document string) (float32, error)
}

// LocalRerankerConfig configures local GGUF reranker behavior.
type LocalRerankerConfig struct {
	Enabled bool

	Timeout time.Duration

	// MaxCandidates caps how many candidates are scored per query.
	MaxCandidates int

	// MaxDocChars truncates each candidate's content before scoring.
	MaxDocChars int

	// MinScore filters out candidates below this score (0 = no filter).
	MinScore float64
}

// DefaultLocalRerankerConfig returns sensible defaults for BGE-style reranking.
func DefaultLocalRerankerConfig() *LocalRerankerConfig {
	return &LocalRerankerConfig{
		Enabled:       true,
		Timeout:       15 * time.Second,
		MaxCandidates: 50,
		MaxDocChars:   512,
		MinScore:      0.0,
	}
}

// LocalReranker performs Stage-2 reranking using a local GGUF reranker model.
type LocalReranker struct {
	scorer RerankScorer
	cfg    *LocalRerankerConfig
}

// NewLocalReranker creates a reranker that uses the given scorer (e.g. *localllm.RerankerModel).
func NewLocalReranker(scorer RerankScorer, cfg *LocalRerankerConfig) *LocalReranker {
	if cfg == nil {
		cfg = DefaultLocalRerankerConfig()
	}
	return &LocalReranker{scorer: scorer, cfg: cfg}
}

func (r *LocalReranker) Name() string { return "local_gguf" }

func (r *LocalReranker) Enabled() bool {
	return r != nil && r.scorer != nil && r.cfg != nil && r.cfg.Enabled
}

func (r *LocalReranker) IsAvailable(ctx context.Context) bool {
	if !r.Enabled() {
		return false
	}
	_, err := r.scorer.Score(ctx, "health", "check")
	return err == nil
}

// Rerank scores each candidate with the scorer and returns results sorted by
// score (desc). A scorer error is an error (see Reranker). The raw scored
// batch, including entries below MinScore, is retained for Service validation.
func (r *LocalReranker) Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error) {
	if !r.Enabled() {
		return voyage.PassThrough(candidates), nil
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	maxCandidates := r.cfg.MaxCandidates
	if maxCandidates <= 0 {
		maxCandidates = 50
	}
	if len(candidates) > maxCandidates {
		candidates = candidates[:maxCandidates]
	}

	maxDocChars := r.cfg.MaxDocChars
	if maxDocChars <= 0 {
		maxDocChars = 512
	}

	callCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()

	q := strings.TrimSpace(query)
	type scored struct {
		idx   int
		score float64
	}
	scores := make([]scored, 0, len(candidates))

	for i, c := range candidates {
		doc := strings.TrimSpace(c.Content)
		if len(doc) > maxDocChars {
			doc = doc[:maxDocChars]
		}
		score, err := r.scorer.Score(callCtx, q, doc)
		if err != nil {
			return nil, err
		}
		s := float64(score)
		scores = append(scores, scored{idx: i, score: s})
	}

	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })

	wireResults := make([]rerankwire.TypedResult, len(scores))
	for index, score := range scores {
		wireResults[index] = rerankwire.TypedResult{Index: score.idx, Score: score.score}
	}
	accepted, err := rerankwire.AcceptTyped(wireResults, len(candidates))
	if err != nil {
		return nil, err
	}
	results := make([]RerankResult, 0, len(accepted))
	for rank, item := range accepted {
		result := rerankResultForIndex(candidates, item.Index, rank+1, item.Score)
		result.Filtered = r.cfg.MinScore > 0 && item.Score < r.cfg.MinScore
		results = append(results, result)
	}
	return results, nil
}
