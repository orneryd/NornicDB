package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
)

func TestStatementNormalizationKeepsAnonymousColumnIdentity(t *testing.T) {
	t.Run("anonymous_source_identity", func(t *testing.T) {
		for _, source := range []string{
			"/* leading */ RETURN 1 /* column */ + 2 /* trailing */",
			"/* leading */ RETURN size([1, /* column */ 2]) /* trailing */",
			"/* leading */ RETURN (1 // column\n + 2) /* trailing */",
		} {
			t.Run(source, func(t *testing.T) {
				canonical, rewrite := canonicalizeStatementText(t.Context(), source)
				require.NotContains(t, canonical, "leading")
				require.NotContains(t, canonical, "trailing")
				require.Contains(t, canonical, "column")
				original, err := cypherantlr.ParseSyntax(context.Background(), source, cypherantlr.StatementEntry, false, cypherantlr.RawSource)
				require.NoError(t, err)
				normalized, err := cypherantlr.ParseSyntax(context.Background(), canonical, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
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
				span, ok := rewrite.OriginalSpan(cyphertext.Span{Start: start, End: start + len(normalizedName)}, cyphertext.SourceExtent)
				require.True(t, ok)
				require.Equal(t, originalName, source[span.Start:span.End])
				again, cached := canonicalizeStatementText(t.Context(), source)
				require.Equal(t, canonical, again)
				require.Same(t, rewrite, cached)
			})
		}
	})
	t.Run("explicit_and_unchanged_names", func(t *testing.T) {
		for _, source := range []string{
			"/* before */ RETURN 1 /* inside */ + 2 AS value",
			"/* before */ WITH 7 AS `all` RETURN `all` /* after */",
		} {
			canonical, _ := canonicalizeStatementText(t.Context(), source)
			require.NotContains(t, canonical, "/*")
		}
		canonical, rewrite := canonicalizeStatementText(t.Context(), "RETURN 1 /* column */ + 2")
		unchanged, again := canonicalizeStatementText(t.Context(), canonical)
		asserted := "RETURN 1 /* column */ + 2"
		require.Equal(t, asserted, canonical)
		require.Equal(t, asserted, unchanged)
		require.Nil(t, rewrite)
		require.Nil(t, again)
	})
}
