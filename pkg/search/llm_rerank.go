package search

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/voyage"
)

// LLMFunc is a minimal, dependency-free function signature for calling an LLM.
// It is intentionally generic so the search package does not depend on Heimdall.
//
// Implementations should be safe for concurrent use.
type LLMFunc func(ctx context.Context, prompt string) (string, error)

// LLMRerankerConfig controls LLM-based reranking behavior.
//
// A failed LLM call or unusable output is reported as an error; the search
// Service falls back to the original order and reports that it did.
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
// An LLM error, or output that ranks none of the candidates, is an error (see
// Reranker). When the LLM scores every candidate below MinScore, the
// candidates keep their order.
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

	order, scores := parseLLMRerankResponse(strings.TrimSpace(raw), len(candidates))
	if len(order) == 0 {
		return nil, localizedError(localization.SearchRerankResponseUnrecognized(), nil)
	}

	// Build reranked results.
	results := make([]RerankResult, 0, len(candidates))

	seen := make(map[int]struct{}, len(candidates))
	hasScores := len(scores) > 0
	for newRank, idx := range order {
		if idx < 0 || idx >= len(candidates) {
			continue
		}
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		c := candidates[idx]

		var score float64
		if hasScores {
			// If scores are provided, use them (fallback to original score if missing).
			score = c.Score
			if s, ok := scores[idx]; ok {
				score = s
			}
			if r.cfg.MinScore > 0 && score < r.cfg.MinScore {
				// Intentionally drop low-confidence candidates when scored.
				continue
			}
		} else {
			// If the model did not provide numeric scores, synthesize a monotonic score
			// from the rank order so downstream code can still use Score as a proxy.
			// (Ordering is still preserved by returning the slice in rank order.)
			denom := float64(len(candidates))
			if denom <= 0 {
				denom = 1
			}
			score = 1.0 - (float64(newRank) / denom)
		}

		results = append(results, RerankResult{
			ID:           c.ID,
			Content:      c.Content,
			OriginalRank: idx + 1,
			NewRank:      newRank + 1,
			BiScore:      c.Score,
			CrossScore:   score,
			FinalScore:   score,
		})
	}

	if len(results) == 0 {
		return voyage.PassThrough(candidates), nil
	}

	// Fill any candidates the model omitted (fail-open), preserving original order.
	for i := range candidates {
		if _, ok := seen[i]; ok {
			continue
		}
		c := candidates[i]
		results = append(results, RerankResult{
			ID:           c.ID,
			Content:      c.Content,
			OriginalRank: i + 1,
			NewRank:      len(results) + 1,
			BiScore:      c.Score,
			CrossScore:   c.Score,
			FinalScore:   c.Score,
		})
	}

	// Re-assign NewRank sequentially in final output order.
	for i := range results {
		results[i].NewRank = i + 1
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

func parseLLMRerankResponse(raw string, n int) (order []int, scores map[int]float64) {
	type rankedItem struct {
		Index int     `json:"index"`
		Score float64 `json:"score"`
	}
	type resp struct {
		Ranked []rankedItem `json:"ranked"`
		Order  []int        `json:"order"`
		Scores []float64    `json:"scores"`
	}

	extractJSON := func(s string) string {
		// Try object first.
		start := strings.Index(s, "{")
		end := strings.LastIndex(s, "}")
		if start >= 0 && end > start {
			return s[start : end+1]
		}
		// Fall back to array.
		start = strings.Index(s, "[")
		end = strings.LastIndex(s, "]")
		if start >= 0 && end > start {
			return s[start : end+1]
		}
		return ""
	}

	jsonStr := extractJSON(raw)
	if jsonStr != "" {
		var parsed resp
		if err := json.Unmarshal([]byte(jsonStr), &parsed); err == nil {
			scores = make(map[int]float64, n)
			seen := make(map[int]struct{}, n)

			if len(parsed.Ranked) > 0 {
				for _, it := range parsed.Ranked {
					if it.Index < 0 || it.Index >= n {
						continue
					}
					if _, ok := seen[it.Index]; ok {
						continue
					}
					seen[it.Index] = struct{}{}
					order = append(order, it.Index)
					scores[it.Index] = it.Score
				}
				return order, scores
			}

			if len(parsed.Order) > 0 {
				for i, idx := range parsed.Order {
					if idx < 0 || idx >= n {
						continue
					}
					if _, ok := seen[idx]; ok {
						continue
					}
					seen[idx] = struct{}{}
					order = append(order, idx)
					if i < len(parsed.Scores) {
						scores[idx] = parsed.Scores[i]
					}
				}
				return order, scores
			}
		}
	}

	// Fuzzy fallback: extract integers in order of appearance.
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return !(r >= '0' && r <= '9')
	})
	seen := make(map[int]struct{}, n)
	for _, f := range fields {
		if f == "" {
			continue
		}
		i, err := strconv.Atoi(f)
		if err != nil {
			continue
		}
		if i < 0 || i >= n {
			continue
		}
		if _, ok := seen[i]; ok {
			continue
		}
		seen[i] = struct{}{}
		order = append(order, i)
	}
	if len(order) == 0 {
		return nil, nil
	}
	return order, nil
}
