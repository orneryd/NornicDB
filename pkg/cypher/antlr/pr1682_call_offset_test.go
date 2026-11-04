package antlr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPR1682LeadingCallOriginalOffsets(t *testing.T) {
	for _, stage := range []SourceStage{RawSource, AcceptedSource} {
		for _, source := range []string{
			"/* é prefix */ WITH 1 AS x CALL (x) { RETURN x AS value } RETURN value",
			"WITH 1 AS x OPTIONAL CALL (x) { RETURN x AS value } RETURN value",
			"CALL db.labels() YIELD label CALL (label) { RETURN label AS value } RETURN value",
			"WITH 'CALL { decoy }' AS x CALL /* { decoy } */ () { RETURN 1 AS value } RETURN value",
		} {
			t.Run(source+"/"+string(rune('0'+stage)), func(t *testing.T) {
				syntax, err := ParseSyntax(t.Context(), source, StatementEntry, false, stage)
				require.NoError(t, err)
				var expected SyntaxNode
				syntax.Walk(func(node SyntaxNode) func() {
					if node.Rule() == CypherParserRULE_callSubquery {
						expected = node
					}
					return nil
				})
				require.True(t, expected.Valid())
				call, ok := expected.DirectToken(CypherLexerCALL)
				require.True(t, ok)
				from, _ := call.Span()
				actual, found := syntax.LeadingCall(from)
				require.True(t, found)
				require.Equal(t, expected.ID(), actual.ID())
				start, _ := expected.Span()
				actual, found = syntax.LeadingCall(start)
				require.True(t, found, "OPTIONAL and CALL select the same frame")
				require.Equal(t, expected.ID(), actual.ID())
				for _, invalid := range []int{-1, len(source), len(source) + 1, from + 1} {
					_, found := syntax.LeadingCall(invalid)
					require.False(t, found, "offset %d must not select a later frame", invalid)
				}
				leading, found := syntax.LeadingCall(0)
				if strings.HasPrefix(source, "CALL db.") {
					require.True(t, found)
					require.NotEqual(t, CypherParserRULE_callSubquery, leading.Rule())
				} else {
					require.False(t, found, "a preceding WITH is not a CALL")
				}
			})
		}
	}
}
