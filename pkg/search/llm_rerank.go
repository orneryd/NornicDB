package search

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/rerankwire"
	"github.com/orneryd/nornicdb/pkg/voyage"
)

// LLMFunc is a minimal, dependency-free function signature for calling an LLM.
// It is intentionally generic so the search package does not depend on Heimdall.
//
// Implementations should be safe for concurrent use.
type LLMFunc func(ctx context.Context, prompt string) (string, error)

// LLMRerankerConfig controls LLM-based reranking behavior.
//
// A failed LLM call is reported as an error. search.Service validates the
// complete parsed result set and owns fallback reporting.
type LLMRerankerConfig struct {
	Enabled bool

	// Timeout bounds how long a single LLM rerank call can take.
	Timeout time.Duration

	// MaxCandidates caps how many candidates are included in one prompt.
	// This protects against long prompts on large candidate sets.
	MaxCandidates int

	// MaxDocChars truncates each candidate's text content to this many characters.
	MaxDocChars int

	// MaxQueryChars truncates the query string included in the prompt.
	MaxQueryChars int

	// MinScore filters candidates whose LLM score is below this value.
	// A value of 0 means "no filtering".
	MinScore float64
}

// DefaultLLMRerankerConfig returns conservative defaults intended for small local models.
func DefaultLLMRerankerConfig() *LLMRerankerConfig {
	return &LLMRerankerConfig{
		Enabled:       false,
		Timeout:       8 * time.Second,
		MaxCandidates: 25,
		MaxDocChars:   700,
		MaxQueryChars: 800,
		MinScore:      0.0,
	}
}

// LLMReranker performs Stage-2 reranking using an LLM (e.g., Heimdall).
//
// The model is prompted to output JSON ranking information using candidate indices
// (not IDs) to keep responses short and parsing robust.
type LLMReranker struct {
	cfg *LLMRerankerConfig
	llm LLMFunc
}

// NewLLMReranker creates a new LLM reranker.
func NewLLMReranker(cfg *LLMRerankerConfig, llm LLMFunc) *LLMReranker {
	if cfg == nil {
		cfg = DefaultLLMRerankerConfig()
	}
	return &LLMReranker{cfg: cfg, llm: llm}
}

func (r *LLMReranker) Name() string { return "heimdall_llm" }

func (r *LLMReranker) Enabled() bool {
	return r != nil && r.cfg != nil && r.cfg.Enabled && r.llm != nil
}

func (r *LLMReranker) IsAvailable(ctx context.Context) bool {
	// No external health-check. If enabled and LLM func exists, we treat it as available.
	return r.Enabled()
}

// Rerank takes a query and candidates, returns reranked results.
//
// An LLM call error is an error (see Reranker). Parsed output is preserved in
// full for search.Service to validate before applying MinScore filtering.
func (r *LLMReranker) Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error) {
	if !r.Enabled() {
		return voyage.PassThrough(candidates), nil
	}
	if len(candidates) == 0 {
		return []RerankResult{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Limit candidates defensively.
	maxCandidates := r.cfg.MaxCandidates
	if maxCandidates <= 0 {
		maxCandidates = 25
	}
	if len(candidates) > maxCandidates {
		candidates = candidates[:maxCandidates]
	}

	q := strings.TrimSpace(query)
	if max := r.cfg.MaxQueryChars; max > 0 && len(q) > max {
		q = q[:max]
	}

	// Build prompt.
	prompt := r.buildPrompt(q, candidates)

	// Call LLM with a timeout.
	callCtx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()

	raw, err := r.llm(callCtx, prompt)
	if err != nil {
		return nil, err
	}

	accepted, err := rerankwire.AcceptLLM(raw, len(candidates))
	if err != nil {
		return nil, err
	}
	if len(accepted) == 0 {
		return []RerankResult{}, nil
	}

	// Build reranked results.
	results := make([]RerankResult, 0, len(accepted))
	for newRank, item := range accepted {
		result := rerankResultForIndex(candidates, item.Index, newRank+1, item.Score)
		result.Filtered = item.ScorePresent && r.cfg.MinScore > 0 && item.Score < r.cfg.MinScore
		results = append(results, result)
	}

	return results, nil
}

func (r *LLMReranker) buildPrompt(query string, candidates []RerankCandidate) string {
	var sb strings.Builder

	sb.WriteString("You are a search reranker.\n")
	sb.WriteString("Given a QUERY and CANDIDATES, reorder candidates by relevance to the query.\n")
	sb.WriteString("Output JSON only. Do not include markdown or explanations.\n")
	sb.WriteString("\n")
	sb.WriteString(`Output format:
{"ranked":[{"index":0,"score":0.0}],"reason":"optional short reason"}

Rules:
- index is the 0-based candidate index shown below.
- score is a number from 0.0 to 1.0 (higher = more relevant).
- include each candidate index at most once.
`)
	sb.WriteString("\n")

	sb.WriteString("QUERY:\n")
	sb.WriteString(query)
	sb.WriteString("\n\n")

	sb.WriteString("CANDIDATES:\n")
	maxDoc := r.cfg.MaxDocChars
	if maxDoc <= 0 {
		maxDoc = 700
	}
	for i, c := range candidates {
		sb.WriteString(fmt.Sprintf("%d) id=%s bi_score=%.6f\n", i, c.ID, c.Score))
		doc := strings.TrimSpace(c.Content)
		if len(doc) > maxDoc {
			doc = doc[:maxDoc]
		}
		sb.WriteString(doc)
		sb.WriteString("\n---\n")
	}
	return sb.String()
}

func parseLLMRerankResponse(raw string, n int) (order []int, scores map[int]float64, err error) {
	accepted, err := rerankwire.AcceptLLM(raw, n)
	if err != nil {
		return nil, nil, err
	}
	if len(accepted) == 0 {
		return nil, nil, nil
	}
	order = make([]int, len(accepted))
	for index, result := range accepted {
		order[index] = result.Index
		if result.ScorePresent {
			if scores == nil {
				scores = make(map[int]float64, len(accepted))
			}
			scores[result.Index] = result.Score
		}
	}
	return order, scores, nil
}
