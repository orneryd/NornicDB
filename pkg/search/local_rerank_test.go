package search

import (
	"context"
	"testing"
)

// mockScorer returns fixed scores for testing.
type mockScorer map[string]float32

func (m mockScorer) Score(ctx context.Context, query, document string) (float32, error) {
	key := query + "|" + document
	if s, ok := m[key]; ok {
		return s, nil
	}
	return 0.5, nil
}

func TestLocalReranker_Enabled(t *testing.T) {
	scorer := mockScorer{}
	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = false
	r := NewLocalReranker(scorer, cfg)
	if r.Enabled() {
		t.Error("expected disabled when config.Enabled=false")
	}
	cfg.Enabled = true
	r = NewLocalReranker(scorer, cfg)
	if !r.Enabled() {
		t.Error("expected enabled when config.Enabled=true and scorer non-nil")
	}
	r = NewLocalReranker(nil, cfg)
	if r.Enabled() {
		t.Error("expected disabled when scorer is nil")
	}
}

func TestLocalReranker_Name(t *testing.T) {
	r := NewLocalReranker(mockScorer{}, nil)
	if got := r.Name(); got != "local_gguf" {
		t.Errorf("Name() = %q, want local_gguf", got)
	}
}

func TestLocalReranker_IsAvailable(t *testing.T) {
	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = true

	okReranker := NewLocalReranker(mockScorer{
		"health|check": 0.9,
	}, cfg)
	if !okReranker.IsAvailable(context.Background()) {
		t.Fatal("expected IsAvailable true when health check scoring succeeds")
	}

	badReranker := NewLocalReranker(failingScorer{}, cfg)
	if badReranker.IsAvailable(context.Background()) {
		t.Fatal("expected IsAvailable false when health check scoring fails")
	}

	var nilReranker *LocalReranker
	if nilReranker.IsAvailable(context.Background()) {
		t.Fatal("expected IsAvailable false for nil reranker")
	}
}

func TestLocalReranker_RerankDisabledPassThrough(t *testing.T) {
	candidates := []RerankCandidate{{ID: "a", Content: "alpha", Score: 0.7}}

	var nilReranker *LocalReranker
	results, err := nilReranker.Rerank(context.Background(), "query", candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "a" || results[0].FinalScore != 0.7 {
		t.Fatalf("nil reranker pass-through = %#v", results)
	}

	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = false
	disabled := NewLocalReranker(mockScorer{}, cfg)
	results, err = disabled.Rerank(context.Background(), "query", candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "a" || results[0].FinalScore != 0.7 {
		t.Fatalf("disabled reranker pass-through = %#v", results)
	}
}

func TestLocalReranker_Rerank_EmptyCandidates(t *testing.T) {
	r := NewLocalReranker(mockScorer{}, DefaultLocalRerankerConfig())
	got, err := r.Rerank(context.Background(), "q", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("Rerank(nil) = %v, want nil", got)
	}
	got, _ = r.Rerank(context.Background(), "q", []RerankCandidate{})
	if len(got) != 0 {
		t.Errorf("Rerank(empty) = %d results, want 0", len(got))
	}
}

func TestLocalReranker_Rerank_ReordersByScore(t *testing.T) {
	scorer := mockScorer{
		"q|docA": 0.2,
		"q|docB": 0.9,
		"q|docC": 0.5,
	}
	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = true
	r := NewLocalReranker(scorer, cfg)

	candidates := []RerankCandidate{
		{ID: "a", Content: "docA", Score: 0.1},
		{ID: "b", Content: "docB", Score: 0.2},
		{ID: "c", Content: "docC", Score: 0.3},
	}
	results, err := r.Rerank(context.Background(), "q", candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	// Order should be B (0.9), C (0.5), A (0.2)
	eps := 1e-5
	if results[0].ID != "b" || results[0].FinalScore < 0.9-eps || results[0].FinalScore > 0.9+eps {
		t.Errorf("first result: id=%s score=%.4f, want id=b score=0.90", results[0].ID, results[0].FinalScore)
	}
	if results[1].ID != "c" || results[1].FinalScore < 0.5-eps || results[1].FinalScore > 0.5+eps {
		t.Errorf("second result: id=%s score=%.4f, want id=c score=0.50", results[1].ID, results[1].FinalScore)
	}
	if results[2].ID != "a" || results[2].FinalScore < 0.2-eps || results[2].FinalScore > 0.2+eps {
		t.Errorf("third result: id=%s score=%.4f, want id=a score=0.20", results[2].ID, results[2].FinalScore)
	}
}

// A scorer error is reported, so the search can fall back and say so.
func TestLocalReranker_Rerank_ScorerErrorIsReported(t *testing.T) {
	scorer := &failingScorer{}
	cfg := DefaultLocalRerankerConfig()
	cfg.Enabled = true
	r := NewLocalReranker(scorer, cfg)
	candidates := []RerankCandidate{
		{ID: "a", Content: "docA", Score: 0.5},
	}
	results, err := r.Rerank(context.Background(), "q", candidates)
	if err == nil {
		t.Fatalf("expected the scorer error, got results %v", results)
	}
	if results != nil {
		t.Errorf("expected no results on scorer error: got %v", results)
	}
}

type failingScorer struct{}

func (failingScorer) Score(context.Context, string, string) (float32, error) {
	return 0, context.DeadlineExceeded
}

type captureScorer struct {
	queries []string
	docs    []string
	score   float32
}

func (c *captureScorer) Score(ctx context.Context, query, document string) (float32, error) {
	c.queries = append(c.queries, query)
	c.docs = append(c.docs, document)
	return c.score, nil
}

func TestLocalReranker_RerankNilContextCapsAndTruncates(t *testing.T) {
	scorer := &captureScorer{score: 0.75}
	cfg := &LocalRerankerConfig{
		Enabled:       true,
		Timeout:       0,
		MaxCandidates: 1,
		MaxDocChars:   3,
	}
	r := NewLocalReranker(scorer, cfg)

	var nilCtx context.Context
	results, err := r.Rerank(nilCtx, " query ", []RerankCandidate{
		{ID: "a", Content: " abcdef ", Score: 0.1},
		{ID: "b", Content: "second", Score: 0.2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "a" {
		t.Fatalf("got results %#v, want only candidate a", results)
	}
	if len(scorer.queries) != 1 || scorer.queries[0] != "query" {
		t.Fatalf("queries = %#v, want trimmed query", scorer.queries)
	}
	if len(scorer.docs) != 1 || scorer.docs[0] != "abc" {
		t.Fatalf("docs = %#v, want truncated document", scorer.docs)
	}
}

func TestLocalReranker_RerankAllFilteredFallsBack(t *testing.T) {
	cfg := &LocalRerankerConfig{
		Enabled:       true,
		Timeout:       0,
		MaxCandidates: 0,
		MaxDocChars:   0,
		MinScore:      0.5,
	}
	r := NewLocalReranker(&captureScorer{score: 0.1}, cfg)
	candidates := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.9},
		{ID: "b", Content: "beta", Score: 0.8},
	}

	results, err := r.Rerank(context.Background(), "query", candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ID != "a" || results[1].ID != "b" {
		t.Fatalf("expected pass-through results, got %#v", results)
	}
	if results[0].FinalScore != candidates[0].Score || results[1].FinalScore != candidates[1].Score {
		t.Fatalf("expected original scores preserved, got %#v", results)
	}
}
