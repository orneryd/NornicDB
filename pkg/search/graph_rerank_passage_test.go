package search

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The graph rerank is given each candidate's matched passage, not its
// beginning: a node's best chunk (from its kept chunk texts, or from the
// chunk's span), a relationship's best chunk span in a listed property; and
// its budget counts characters, so a Cyrillic relationship text isn't cut
// short (I67, I69).
func TestGraphRerankGetsTheMatchedPassage(t *testing.T) {
	engine := relationshipTestEngine(t)
	filler := strings.Repeat("unrelated opening material ", 200)
	late := "the decisive closing paragraph"
	long := filler + late
	lateStart := utf8.RuneCountInString(filler)
	lateEnd := lateStart + utf8.RuneCountInString(late)
	_, err := engine.CreateNode(&storage.Node{ID: "kept", Labels: []string{"Doc"}, Properties: map[string]any{"text": long},
		EmbedMeta: map[string]any{"chunk_texts": []any{filler[:len(filler)/2], filler[len(filler)/2:], late}}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "spanned", Labels: []string{"Doc"}, Properties: map[string]any{"text": long}})
	require.NoError(t, err)
	russian := strings.Repeat("объяснение связи ", 95)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "linked", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"search_text": long, "text": "unopted " + long}}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "russian", StartNode: "a", EndNode: "c", Type: "LINK", Properties: map[string]any{"search_text": russian}}))
	svc := startRelationshipService(t, engine, nil, func(s *Service) {
		s.SetRelationshipFulltextProperties([]string{"search_text"})
	})
	const budget = 300
	chunkMatch := func(chunk int, property string) []SearchMatch {
		return []SearchMatch{{Kind: SearchMatchChunk, Chunk: chunk, Score: 0.9, Property: property, Start: lateStart, End: lateEnd}}
	}

	kept := svc.graphRerankContent(&SearchResult{ID: "kept", NodeID: "kept", Matches: chunkMatch(2, "text")}, "closing", budget)
	require.Contains(t, kept, late)
	spanned := svc.graphRerankContent(&SearchResult{ID: "spanned", NodeID: "spanned", Matches: chunkMatch(1, "text")}, "closing", budget)
	require.Contains(t, spanned, late)
	require.LessOrEqual(t, utf8.RuneCountInString(spanned), budget)

	edge := &storage.Edge{ID: "linked", Type: "LINK"}
	linked := svc.graphRerankContent(&SearchResult{ID: "linked", Relationship: edge, Matches: chunkMatch(1, "search_text")}, "closing", budget)
	require.Contains(t, linked, late)
	require.NotContains(t, linked, "unopted")
	// An unplaced best chunk gives way to a placed term match.
	termed := svc.graphRerankContent(&SearchResult{ID: "linked", Relationship: edge, Matches: []SearchMatch{
		{Kind: SearchMatchChunk, Chunk: 0, Score: 0.9},
		{Kind: SearchMatchTerm, Term: "decisive", Property: "search_text", Start: lateStart + 4, End: lateStart + 12},
	}}, "decisive", budget)
	require.Contains(t, termed, late)

	// A match in a property outside the list gives no passage from it.
	unlisted := svc.graphRerankContent(&SearchResult{ID: "linked", Relationship: edge, Matches: chunkMatch(1, "text")}, "closing", budget)
	require.NotContains(t, unlisted, "unopted")
	require.LessOrEqual(t, utf8.RuneCountInString(unlisted), budget)

	// Characters, not bytes: the whole Cyrillic text fits a budget of its length.
	full := svc.graphRerankContent(&SearchResult{ID: "russian", Relationship: &storage.Edge{ID: "russian", Type: "LINK"}}, "связи", 2048)
	require.Equal(t, utf8.RuneCountInString(strings.TrimSpace(russian)), utf8.RuneCountInString(strings.TrimSpace(full)))
}
