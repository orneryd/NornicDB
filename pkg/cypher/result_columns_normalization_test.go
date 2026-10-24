package cypher

import (
	"context"
	"strings"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
)

func TestStatementNormalizationKeepsAnonymousColumnIdentity(t *testing.T) {
	for _, source := range []string{
		"/* leading */ RETURN 1 /* column */ + 2 /* trailing */",
		"/* leading */ RETURN size([1, /* column */ 2]) /* trailing */",
		"/* leading */ RETURN (1 // column\n + 2) /* trailing */",
	} {
		t.Run(source, func(t *testing.T) {
			canonical, rewrite := canonicalizeStatementText(source)
			require.NotContains(t, canonical, "leading")
			require.NotContains(t, canonical, "trailing")
			require.Contains(t, canonical, "column")
			original, err := cypherantlr.ParseSyntax(context.Background(), source, cypherantlr.StatementEntry, false)
			require.NoError(t, err)
			normalized, err := cypherantlr.ParseSyntax(context.Background(), canonical, cypherantlr.StatementEntry, false)
			require.NoError(t, err)
			var originalName, normalizedName string
			for index, syntax := range []cypherantlr.StatementSyntax{original, normalized} {
				syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
					if node.Rule() == cypherantlr.CypherParserRULE_projectionItem {
						name := projectionNameFromSyntax(node.ChildRule(cypherantlr.CypherParserRULE_expression))
						if index == 0 {
							originalName = name
						} else {
							normalizedName = name
						}
					}
					return nil
				})
			}
			require.Equal(t, originalName, normalizedName)
			start := strings.Index(canonical, normalizedName)
			require.NotNil(t, rewrite)
			require.Equal(t, originalName, source[rewrite.originalOffset(start, true):rewrite.originalOffset(start+len(normalizedName), false)])
			again, cached := canonicalizeStatementText(source)
			require.Equal(t, canonical, again)
			require.Same(t, rewrite, cached)
		})
	}
}

func TestStatementNormalizationStillNormalizesExplicitNames(t *testing.T) {
	for _, source := range []string{
		"/* before */ RETURN 1 /* inside */ + 2 AS value",
		"/* before */ WITH 7 AS `all` RETURN `all` /* after */",
	} {
		canonical, _ := canonicalizeStatementText(source)
		require.NotContains(t, canonical, "/*")
	}
	canonical, rewrite := canonicalizeStatementText("RETURN 1 /* column */ + 2")
	unchanged, again := canonicalizeStatementText(canonical)
	asserted := "RETURN 1 /* column */ + 2"
	require.Equal(t, asserted, canonical)
	require.Equal(t, asserted, unchanged)
	require.Nil(t, rewrite)
	require.Nil(t, again)
}
