package search

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// keepOneReranker returns only the candidate whose text holds keep, as a
// reranker with a minimum score or top K filters the others out.
type keepOneReranker struct{ keep string }

func (keepOneReranker) Name() string                     { return "keep-one" }
func (keepOneReranker) Enabled() bool                    { return true }
func (keepOneReranker) IsAvailable(context.Context) bool { return true }
func (r keepOneReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	for index, candidate := range candidates {
		if strings.Contains(candidate.Content, r.keep) {
			return []RerankResult{{ID: candidate.ID, Content: candidate.Content, OriginalRank: index + 1, FinalScore: 0.8}}, nil
		}
	}
	return nil, nil
}

// One score left of several candidates is the reranker's filtering, kept;
// one candidate's one score is flat; no score is a failed provider batch. A
// candidate's header longer than the budget is cut to it.
func TestRerankSingleSurvivorAndHeaderBudget(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	svc := startRelationshipService(t, engine, nil, nil)
	svc.SetReranker(keepOneReranker{keep: "bridge"})
	opts := DefaultSearchOptions()
	pool := func() []*graphCandidate {
		return []*graphCandidate{
			{result: SearchResult{ID: "a", NodeID: "a"}},
			{result: SearchResult{ID: "r1", Relationship: &storage.Edge{ID: "r1", Type: "CITES"}}},
		}
	}
	ordered, outcome := svc.rerankGraphPool(ctx, "bridge", pool(), opts)
	require.Equal(t, rerankMethodApplied, outcome.Method)
	require.Equal(t, SearchFallbackNone, outcome.FallbackReason)
	require.Len(t, ordered, 2)
	require.Equal(t, "r1", ordered[0].result.ID)
	require.Equal(t, "a", ordered[1].result.ID)

	ordered, outcome = svc.rerankGraphPool(ctx, "bridge", pool()[1:], opts)
	require.Equal(t, rerankMethodOriginal, outcome.Method)
	require.Equal(t, SearchFallbackRerankScoresFlat, outcome.FallbackReason)
	require.Len(t, ordered, 1)

	svc.SetReranker(keepOneReranker{keep: "nothing at all"})
	ordered, outcome = svc.rerankGraphPool(ctx, "bridge", pool(), opts)
	require.Equal(t, rerankMethodOriginal, outcome.Method)
	require.Equal(t, SearchFallbackRerankFailed, outcome.FallbackReason)
	require.Len(t, ordered, 2)

	require.True(t, rerankScoresFlat(0, 3, nil))
	require.False(t, rerankScoresFlat(1, 3, nil))
	require.True(t, rerankScoresFlat(1, 1, func() float64 { return 0 }))
	require.False(t, rerankScoresFlat(2, 2, func() float64 { return 0.5 }))

	titled := &storage.Node{ID: "t", Properties: map[string]any{"title": strings.Repeat("т", 100), "content": "body text"}}
	content := svc.rerankCandidateContent(titled, rrfResult{ID: "t"}, "body", 5)
	require.LessOrEqual(t, utf8.RuneCountInString(content), 5)
}
