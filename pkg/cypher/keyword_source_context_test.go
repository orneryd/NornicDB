package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
)

// Author: #1663 implementation lane. A malformed input still needs its actual
// original context during framing; a cached standalone answer must not hide it.
func TestKeywordScanRetainsWindowContextAndRootCache(t *testing.T) {
	text := "RETURN {x:CASE WHEN true THEN 1 RETURN 2 END}"
	source := cyphertext.NewSource(t.Context(), text, cyphertext.AcceptedSource)
	start, end := strings.IndexByte(text, ':'), strings.LastIndexByte(text, '}')
	window := braceView{source: source.Window(start, end)}
	standalone := braceView{source: cyphertext.NewSource(t.Context(), window.text(), cyphertext.AcceptedSource)}
	rootOptions := defaultKeywordScanOpts(standalone)
	require.True(t, isDefaultKeywordScanOpts(rootOptions))
	standaloneAnswer := keywordIndexFrom(standalone.text(), "RETURN", 0, rootOptions)
	require.Equal(t, strings.Index(window.text(), "RETURN"), standaloneAnswer)
	key := keywordIndexCacheKey{s: window.text(), keyword: "RETURN", from: 0}
	cached, present := defaultKeywordIndexCache.get(key)
	require.True(t, present)
	require.Equal(t, standaloneAnswer, cached)
	require.Equal(t, standaloneAnswer, keywordIndexFrom(standalone.text(), "RETURN", 0, rootOptions))

	windowOptions := defaultKeywordScanOpts(window)
	require.False(t, isDefaultKeywordScanOpts(windowOptions))
	require.Equal(t, -1, keywordIndexFrom(window.text(), "RETURN", 0, windowOptions))
	cached, present = defaultKeywordIndexCache.get(key)
	require.True(t, present)
	require.Equal(t, standaloneAnswer, cached, "the contextual scan must not overwrite the text-only cache")
}

// Author: #1663 implementation lane. Construction uses only pre-existing
// separator identities. After publication every separator decision available
// to each actual boundary callback must agree with the final contextual roles.
func TestPolicyConstructionBoundaryFactsAgreeWithFinishedRoles(t *testing.T) {
	for _, query := range []string{
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { ON ACCESS { SET n.a = CASE WHEN true THEN {x:CASE WHEN false THEN 1 ELSE 2 END} ELSE {x:3} END SET n.b = 4 } }",
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { WHEN CASE WHEN true THEN {x:1}.x = 1 ELSE false END APPLY PROFILE 'p' ON ACCESS { SET n.a = {x:coalesce(n.x, 1)} } }",
	} {
		t.Run(query, func(t *testing.T) {
			type decision struct {
				source    *cyphertext.Source
				colon     cyphertext.Span
				separator bool
			}
			var decisions []decision
			calls := 0
			reader := func(source *cyphertext.Source, keyword string, from int, boundary cypherantlr.PolicyBoundaryKind) int {
				calls++
				for offset := range source.Text {
					if source.Text[offset] != ':' {
						continue
					}
					colon := cyphertext.Span{Start: offset, End: offset + 1}
					_, separator := source.OriginalEntryAtColon(colon)
					decisions = append(decisions, decision{source, colon, separator})
				}
				return readPolicyKeywordBoundary(source, keyword, from, boundary)
			}
			source := cyphertext.NewSource(t.Context(), query, cyphertext.AcceptedSource)
			product := cypherantlr.OriginalBraceAnalysis(source, false, reader)
			require.NoError(t, product.Admission())
			_, handled, err := product.PolicyCommand(true)
			require.True(t, handled)
			require.NoError(t, err)
			require.Positive(t, calls)
			require.NotEmpty(t, decisions)
			for _, decision := range decisions {
				require.Equal(t, decision.separator, product.MapColonAt(decision.source, decision.colon), "%q at %v", decision.source.Text, decision.colon)
			}
		})
	}
}
