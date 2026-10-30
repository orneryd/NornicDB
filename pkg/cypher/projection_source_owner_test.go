// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
// Rewritten for the retained item reader: Claude R17 implementation agent, 30 October 2026.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The pre-rewrite splitter preserves its loose source-spelling contract;
// admitted projection syntax exposes the retained AS and decoded identity.
func TestProjectionSourceStagePopulation(t *testing.T) {
	for _, tc := range []struct {
		item              string
		rewrite, admitted [2]string
		as                int
	}{
		{"n AS x", [2]string{"n", "x"}, [2]string{"n", "x"}, 2},
		{"n AS `a b`", [2]string{"n", "a b"}, [2]string{"n", "a b"}, 2},
		{"n AS ``", [2]string{"n", ""}, [2]string{"n", ""}, 2},
		{"n AS as", [2]string{"n AS", ""}, [2]string{"n", "as"}, 2},
		{"n AS return", [2]string{"n", "return"}, [2]string{"n", "return"}, 2},
		{"n/*AS fake*/ AS x", [2]string{"n/*AS fake*/", "x"}, [2]string{"n", "x"}, 13},
		{"n AS /*note*/ x", [2]string{"n", "/*note*/ x"}, [2]string{"n", "x"}, 2},
		{"n /* AS bad */", [2]string{"n /*", "bad */"}, [2]string{"n", "n"}, -1},
		{"n AS x AS y", [2]string{"n AS x", "y"}, [2]string{"", ""}, -1},
		{"AS x", [2]string{"", "x"}, [2]string{"", ""}, -1},
		{"x AS", [2]string{"x", ""}, [2]string{"", ""}, -1},
		{"", [2]string{"", ""}, [2]string{"", ""}, -1},
		{"*", [2]string{"*", "*"}, [2]string{"*", "*"}, -1},
		{" n ", [2]string{"n", "n"}, [2]string{"n", "n"}, -1},
		{"CASE WHEN true THEN 'AS' END AS x", [2]string{"CASE WHEN true THEN 'AS' END", "x"}, [2]string{"CASE WHEN true THEN 'AS' END", "x"}, 29},
		{"count { MATCH (n) } AS c", [2]string{"count { MATCH (n) }", "c"}, [2]string{"count { MATCH (n) }", "c"}, 20},
	} {
		t.Run(tc.item, func(t *testing.T) {
			expression, alias := rewriteStageProjectionItem(tc.item)
			require.Equal(t, tc.rewrite, [2]string{expression, alias})
			itemSource := projectionItemForTest(t.Context(), tc.item)
			require.Equal(t, tc.admitted, [2]string{itemSource.expression, itemSource.alias})
			require.Equal(t, tc.as, itemSource.as)
		})
	}
}

func TestProjectionSourceRawAndDecodedViews(t *testing.T) {
	const item = " \tn AS /* c */ `a b` "
	// The retained occurrence starts at its expression: AS is at offset 2 of
	// the item, and the alias spelling is its symbol's.
	admitted := projectionItemForTest(t.Context(), item)
	require.Equal(t, 2, admitted.as)
	require.Equal(t, "n", admitted.rawExpression)
	require.Equal(t, "`a b`", admitted.rawAlias)
	require.Equal(t, "n", admitted.expression)
	require.Equal(t, "a b", admitted.alias)
	expression, alias := rewriteStageProjectionItem(item)
	require.Equal(t, "n", expression)
	require.Equal(t, "/* c */ `a b`", alias)
	// Several items are not one item occurrence.
	multiple := projectionItemForTest(t.Context(), "n AS x, m AS y")
	require.Equal(t, -1, multiple.as)
	require.Empty(t, multiple.expression)
	require.Empty(t, multiple.alias)
}

func TestProjectionSourceCanceledAdmittedReadRetainsStageContract(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	source := projectionItemForTest(ctx, "n AS x")
	require.Equal(t, -1, source.as)
	require.Empty(t, source.expression)
	require.Empty(t, source.alias)
	// Pre-rewrite splitting is context-free; its lexical view remains so.
	expression, alias := rewriteStageProjectionItem("n AS x")
	require.Equal(t, "n", expression)
	require.Equal(t, "x", alias)
	source = projectionItemForTest(t.Context(), "n AS x")
	require.Equal(t, 2, source.as)
	require.Equal(t, "x", source.alias)
}

func TestProjectionSourcePublicKeywordAndQuotedNames(t *testing.T) {
	restoreParserTypeAfter(t)
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			for _, tc := range []struct {
				query, column string
				value         any
			}{
				{"RETURN 1 AS `return`", "return", int64(1)},
				{"WITH 2 AS as RETURN as AS `a b`", "a b", int64(2)},
				{"RETURN 'AS inside' AS `AS`", "AS", "AS inside"},
				{"RETURN 1 AS ``", "", int64(1)},
			} {
				result, err := executor.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err, tc.query)
				require.Equal(t, []string{tc.column}, result.Columns)
				require.Equal(t, [][]any{{tc.value}}, result.Rows)
			}
		})
	}
}
