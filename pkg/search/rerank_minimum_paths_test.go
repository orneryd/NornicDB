package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// minimumPathReranker gives every candidate the same score, fails, or
// returns nothing, so each rerank outcome path can be reached through the
// public APIs.
type minimumPathReranker struct {
	disabled bool
	score    float64
	err      error
	empty    bool
}

func (minimumPathReranker) Name() string                     { return "minimum-path" }
func (r minimumPathReranker) Enabled() bool                  { return !r.disabled }
func (minimumPathReranker) IsAvailable(context.Context) bool { return true }
func (r minimumPathReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.empty {
		return nil, nil
	}
	results := make([]RerankResult, 0, len(candidates))
	for index, candidate := range candidates {
		results = append(results, RerankResult{ID: candidate.ID, Content: candidate.Content, OriginalRank: index + 1, CrossScore: r.score, FinalScore: r.score})
	}
	return results, nil
}

// For explicit candidates RerankMinScore is a floor on every returned row's
// final score on every outcome path. When a fallback keeps the candidates' own
// order, a row's final score is its candidate score, and that is what the
// minimum compares.
func TestRerankCandidatesMinimumGovernsEveryOutcomePath(t *testing.T) {
	candidates := func(scores ...float64) []RerankCandidate {
		out := make([]RerankCandidate, len(scores))
		for index, score := range scores {
			id := string(rune('a' + index))
			out[index] = RerankCandidate{ID: id, Content: "text " + id, Score: score}
		}
		return out
	}
	withoutContent := func(in []RerankCandidate) []RerankCandidate {
		for index := range in {
			in[index].Content = ""
		}
		return in
	}
	for _, testCase := range []struct {
		name       string
		reranker   minimumPathReranker
		candidates []RerankCandidate
		minScore   float64
		wantIDs    []string
		wantScores []float64
	}{
		{name: "flat provider scores below the minimum", reranker: minimumPathReranker{score: 0.2}, candidates: candidates(0.1, 0.3), minScore: 0.5, wantIDs: []string{}},
		{name: "flat fallback keeps candidates at or above the minimum", reranker: minimumPathReranker{score: 0.2}, candidates: candidates(0.9, 0.3, 0.5), minScore: 0.5, wantIDs: []string{"a", "c"}, wantScores: []float64{0.9, 0.5}},
		{name: "flat fallback without a minimum keeps every candidate", reranker: minimumPathReranker{score: 0.2}, candidates: candidates(0.9, 0.3), wantIDs: []string{"a", "b"}, wantScores: []float64{0.9, 0.3}},
		{name: "no candidate text", reranker: minimumPathReranker{score: 0.2}, candidates: withoutContent(candidates(0.9, 0.3)), minScore: 0.5, wantIDs: []string{"a"}, wantScores: []float64{0.9}},
		{name: "reranker disabled", reranker: minimumPathReranker{disabled: true}, candidates: candidates(0.9, 0.3), minScore: 0.5, wantIDs: []string{"a"}, wantScores: []float64{0.9}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			service := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
			t.Cleanup(func() { _ = service.Close() })
			service.SetReranker(testCase.reranker)
			options := DefaultSearchOptions()
			options.RerankMinScore = testCase.minScore
			results, err := service.RerankCandidates(context.Background(), "query", testCase.candidates, options)
			require.NoError(t, err)
			ids := make([]string, 0, len(results))
			for index, result := range results {
				ids = append(ids, result.ID)
				require.GreaterOrEqual(t, result.FinalScore, testCase.minScore)
				require.Equal(t, index+1, result.NewRank)
				if testCase.wantScores != nil {
					require.Equal(t, testCase.wantScores[index], result.FinalScore)
				}
			}
			require.Equal(t, testCase.wantIDs, ids)
		})
	}

	// An explicit rerank whose provider fails or answers nothing is an error,
	// whatever the minimum.
	for name, reranker := range map[string]minimumPathReranker{
		"provider error": {err: errors.New("provider down")},
		"provider empty": {empty: true},
	} {
		t.Run(name, func(t *testing.T) {
			service := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
			t.Cleanup(func() { _ = service.Close() })
			service.SetReranker(reranker)
			options := DefaultSearchOptions()
			options.RerankMinScore = 0.5
			results, err := service.RerankCandidates(context.Background(), "query", candidates(0.9, 0.3), options)
			require.Error(t, err)
			require.Nil(t, results)
		})
	}
}

// A node search fallback without rerank output keeps the fused order and RRF
// scores, unfiltered by the rerank minimum, and reports why it fell back.
func TestNodeSearchFallbackWithoutRerankOutputIgnoresMinimum(t *testing.T) {
	newService := func(t *testing.T) *Service {
		t.Helper()
		engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
		service := NewServiceWithDimensions(engine, 2)
		t.Cleanup(func() { _ = service.Close() })
		for index, fixture := range []struct {
			content   string
			embedding []float32
		}{
			{content: "query query query alpha", embedding: []float32{1, 0}},
			{content: "query query beta", embedding: []float32{0.9, 0.1}},
			{content: "query gamma", embedding: []float32{0.8, 0.2}},
		} {
			node := &storage.Node{
				ID: storage.NodeID(string(rune('a' + index))), Labels: []string{"Doc"},
				Properties: map[string]any{"content": fixture.content}, ChunkEmbeddings: [][]float32{fixture.embedding},
			}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, service.IndexNode(node))
		}
		return service
	}
	for _, testCase := range []struct {
		name     string
		reranker minimumPathReranker
		reason   SearchFallbackReason
	}{
		{name: "flat provider scores", reranker: minimumPathReranker{score: 0.2}, reason: SearchFallbackRerankScoresFlat},
		{name: "provider error", reranker: minimumPathReranker{err: errors.New("provider down")}, reason: SearchFallbackRerankFailed},
		{name: "provider empty", reranker: minimumPathReranker{empty: true}, reason: SearchFallbackRerankFailed},
		{name: "reranker disabled", reranker: minimumPathReranker{disabled: true}, reason: SearchFallbackNone},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			service := newService(t)
			options := DefaultSearchOptions()
			options.Limit = 3
			options.RerankTopK = 3
			options.MinRRFScore = 0
			query := "query"
			baseline, err := service.Search(context.Background(), query, []float32{1, 0}, options)
			require.NoError(t, err)
			require.Len(t, baseline.Results, 3)
			require.Greater(t, baseline.Results[0].Score, baseline.Results[1].Score)

			service.SetReranker(testCase.reranker)
			options.RerankEnabled = true
			// Between the first and second fused scores, and above every one.
			for _, minimum := range []float64{(baseline.Results[0].Score + baseline.Results[1].Score) / 2, baseline.Results[0].Score + 1} {
				options.RerankMinScore = minimum
				response, err := service.Search(context.Background(), query, []float32{1, 0}, options)
				require.NoError(t, err)
				require.Equal(t, testCase.reason, response.FallbackReason)
				require.Equal(t, testCase.reason != SearchFallbackNone, response.FallbackTriggered)
				require.False(t, strings.HasSuffix(response.SearchMethod, "+rerank"), response.SearchMethod)
				require.Equal(t, reviewerSearchResultIDs(baseline.Results), reviewerSearchResultIDs(response.Results))
				for index, result := range response.Results {
					require.Equal(t, baseline.Results[index].Score, result.Score)
				}
			}
		})
	}
}
