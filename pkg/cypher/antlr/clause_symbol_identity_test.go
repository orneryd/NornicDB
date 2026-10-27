package antlr

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClauseInfoDecodedSymbolIdentity(t *testing.T) {
	for _, tc := range []struct{ written, identity string }{
		{"plain", "plain"},
		{"`a  b`", "a  b"},
		{"`a``b`", "a`b"},
		{"`a\\u0062`", "ab"},
	} {
		t.Run(tc.written, func(t *testing.T) {
			query := "UNWIND [1] AS " + tc.written + " RETURN " + tc.written
			parsed, err := Parse(query)
			require.NoError(t, err)
			info := ExtractClauses(query, parsed)
			require.Equal(t, "[1]", info.UnwindExpr)
			require.Equal(t, tc.identity, info.UnwindAs)
			require.Equal(t, []string{tc.identity}, info.Variables)
			require.Equal(t, tc.written, info.ReturnItems, "expression text remains source spelling")
			syntax, err := ParseSyntax(t.Context(), query, StatementEntry, false, RawSource)
			require.NoError(t, err)
			var names []string
			var sources []string
			syntax.Walk(func(node SyntaxNode) func() {
				if node.Rule() == CypherParserRULE_symbol {
					name, valid := node.Name()
					require.True(t, valid)
					names = append(names, name)
					sources = append(sources, node.Text())
				}
				return nil
			})
			require.Equal(t, []string{tc.identity, tc.identity}, names, "retained syntax callbacks use the same identity owner")
			require.Equal(t, []string{tc.written, tc.written}, sources, "callback text retains raw source extents")

		})
	}
}
