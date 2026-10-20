package search

import (
	"context"
	"sort"
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
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].FinalScore > ranked[j].FinalScore })
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
	ordered, outcome := svc.rerankGraphPool(ctx, "zephyrine", bare(), opts)
	require.Equal(t, rerankMethodApplied, outcome.Method)
	require.Equal(t, SearchFallbackNone, outcome.FallbackReason)
	require.Len(t, contents, 2)
	require.Contains(t, contents[0], "doc a")
	require.Contains(t, contents[1], "zephyrine bridge")
	require.Equal(t, "r1", ordered[0].result.ID)

	// TopK is both the provider-work bound and the final rerank output cap.
	contents = nil
	bounded := append(bare(), &graphCandidate{result: SearchResult{ID: "b", NodeID: "b"}})
	boundedOpts := *opts
	boundedOpts.RerankTopK = 2
	ordered, outcome = svc.rerankGraphPool(ctx, "zephyrine", bounded, &boundedOpts)
	require.Equal(t, rerankMethodApplied, outcome.Method)
	require.Len(t, contents, 2)
	require.Len(t, ordered, 2)
	require.Equal(t, []string{"r1", "a"}, []string{ordered[0].result.ID, ordered[1].result.ID})

	// An empty pool: nothing reranked, no reason.
	_, outcome = svc.rerankGraphPool(ctx, "zephyrine", nil, opts)
	require.Equal(t, rerankMethodOriginal, outcome.Method)
	require.Equal(t, SearchFallbackNone, outcome.FallbackReason)

	// One candidate: flat scores, the fused order kept.
	ordered, outcome = svc.rerankGraphPool(ctx, "zephyrine", bare()[:1], opts)
	require.Equal(t, rerankMethodOriginal, outcome.Method)
	require.Equal(t, SearchFallbackRerankScoresFlat, outcome.FallbackReason)
	require.Len(t, ordered, 1)

	// An entity no longer readable falls back to the hit's own text, or has none.
	contents = nil
	_, outcome = svc.rerankGraphPool(ctx, "zephyrine", []*graphCandidate{
		{result: SearchResult{ID: "gone", Properties: map[string]any{"name": "gone node"}}},
		{result: SearchResult{ID: "gone-rel", Relationship: &storage.Edge{ID: "gone-rel", Type: "CITES", Properties: map[string]any{"reason": "old bridge"}}}},
	}, opts)
	require.Equal(t, SearchFallbackNone, outcome.FallbackReason)
	require.Len(t, contents, 2)
	require.Contains(t, contents[0], "gone node")
	require.Contains(t, contents[1], "old bridge")
	_, outcome = svc.rerankGraphPool(ctx, "zephyrine", []*graphCandidate{{result: SearchResult{ID: "nothing"}}}, opts)
	require.Equal(t, SearchFallbackRerankNoCandidates, outcome.FallbackReason)

	// A relationship's text is cut at the rerank's character limit.
	contents = nil
	opts.RerankMaxChars = 5
	_, _ = svc.rerankGraphPool(ctx, "zephyrine", bare()[1:], opts)
	require.Equal(t, []string{"CITES"}, contents)
}

// A relationship's rerank text reads relationship search's property list:
// no property outside it reaches the reranker, for a stored relationship or
// one read from the hit.
func TestGraphRerankRelationshipTextReadsItsPropertyList(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "opted", StartNode: "a", EndNode: "c", Type: "LINK", Properties: map[string]any{"search_text": "why they are linked", "text": "unopted metadata zephyrcensus"}}))
	svc := startRelationshipService(t, engine, nil, func(s *Service) {
		s.SetFulltextProperties([]string{"text", "search_text"})
		s.SetRelationshipFulltextProperties([]string{"search_text"})
	})
	var contents []string
	svc.SetReranker(recordingGraphReranker{contents: &contents})
	_, _ = svc.rerankGraphPool(ctx, "linked", []*graphCandidate{
		{result: SearchResult{ID: "opted", Relationship: &storage.Edge{ID: "opted", Type: "LINK"}}},
		{result: SearchResult{ID: "gone", Relationship: &storage.Edge{ID: "gone", Type: "LINK", Properties: map[string]any{"search_text": "old why", "text": "unopted old"}}}},
	}, DefaultSearchOptions())
	require.Len(t, contents, 2)
	for _, content := range contents {
		require.NotContains(t, content, "unopted")
	}
	require.Contains(t, contents[0], "why they are linked")
	require.Contains(t, contents[1], "old why")
}
