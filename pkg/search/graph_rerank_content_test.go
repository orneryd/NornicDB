package search

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// recordingGraphReranker records what it was given and scores a candidate
// by whether its text holds "bridge".
type recordingGraphReranker struct{ contents *[]string }

func (recordingGraphReranker) Name() string                     { return "recording" }
func (recordingGraphReranker) Enabled() bool                    { return true }
func (recordingGraphReranker) IsAvailable(context.Context) bool { return true }
func (r recordingGraphReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	ranked := make([]RerankResult, 0, len(candidates))
	for index, candidate := range candidates {
		*r.contents = append(*r.contents, candidate.Content)
		score := 0.1
		if strings.Contains(candidate.Content, "bridge") {
			score = 0.9
		}
		ranked = append(ranked, RerankResult{ID: candidate.ID, Content: candidate.Content, OriginalRank: index + 1, FinalScore: score})
	}
	return ranked, nil
}

// The graph rerank reads a candidate's text from storage, as the node rerank
// does, so a hit without its properties (a query chunk's vector hit, or a
// projection) is still reranked; one reranked candidate is rerank_scores_flat,
// as in the node rerank.
func TestGraphRerankReadsCandidateTextFromStorage(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	svc := startRelationshipService(t, engine, nil, nil)
	var contents []string
	svc.SetReranker(recordingGraphReranker{contents: &contents})
	opts := DefaultSearchOptions()

	bare := func() []*graphCandidate {
		return []*graphCandidate{
			{result: SearchResult{ID: "a", NodeID: "a"}},
			{result: SearchResult{ID: "r1", Relationship: &storage.Edge{ID: "r1", Type: "CITES", StartNode: "a", EndNode: "b"}}},
		}
	}
	ordered, reason, reranked := svc.rerankGraphPool(ctx, "zephyrine", bare(), opts)
	require.True(t, reranked)
	require.Equal(t, SearchFallbackNone, reason)
	require.Len(t, contents, 2)
	require.Contains(t, contents[0], "doc a")
	require.Contains(t, contents[1], "zephyrine bridge")
	require.Equal(t, "r1", ordered[0].result.ID)

	// An empty pool: nothing reranked, no reason.
	_, reason, reranked = svc.rerankGraphPool(ctx, "zephyrine", nil, opts)
	require.False(t, reranked)
	require.Equal(t, SearchFallbackNone, reason)

	// One candidate: flat scores, the fused order kept.
	ordered, reason, reranked = svc.rerankGraphPool(ctx, "zephyrine", bare()[:1], opts)
	require.False(t, reranked)
	require.Equal(t, SearchFallbackRerankScoresFlat, reason)
	require.Len(t, ordered, 1)

	// An entity no longer readable falls back to the hit's own text, or has none.
	contents = nil
	_, reason, _ = svc.rerankGraphPool(ctx, "zephyrine", []*graphCandidate{
		{result: SearchResult{ID: "gone", Properties: map[string]any{"name": "gone node"}}},
		{result: SearchResult{ID: "gone-rel", Relationship: &storage.Edge{ID: "gone-rel", Type: "CITES", Properties: map[string]any{"reason": "old bridge"}}}},
	}, opts)
	require.Equal(t, SearchFallbackNone, reason)
	require.Len(t, contents, 2)
	require.Contains(t, contents[0], "gone node")
	require.Contains(t, contents[1], "old bridge")
	_, reason, _ = svc.rerankGraphPool(ctx, "zephyrine", []*graphCandidate{{result: SearchResult{ID: "nothing"}}}, opts)
	require.Equal(t, SearchFallbackRerankNoCandidates, reason)
}
