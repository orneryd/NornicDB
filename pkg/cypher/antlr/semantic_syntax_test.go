package antlr

import (
	"context"
	"testing"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"
)

func TestTransactionalSyntaxRetainsExpressionPositions(t *testing.T) {
	for _, query := range []string{
		"WITH {value:COUNT { MATCH (n:Record) }} AS k",
		"WITH 1 AS k WHERE EXISTS { MATCH (n:Record) }",
		"WITH 1 AS k ORDER BY COUNT { MATCH (n:Record) }",
		"WITH 1 AS k SKIP COUNT { MATCH (n:Record) }",
		"WITH 1 AS k LIMIT COUNT { MATCH (n:Record) }",
		"MATCH (m:Other) WHERE EXISTS { MATCH (n:Record) }",
		"CALL { WITH COUNT { MATCH (n:Record) } AS k RETURN k }",
	} {
		t.Run(query, func(t *testing.T) {
			patterns := []string{}
			err := ReadClauseSyntax(context.Background(), query, func(tree antlr4.Tree, source []rune) {
				var visit func(antlr4.Tree)
				visit = func(node antlr4.Tree) {
					if pattern, ok := node.(*NodePatternContext); ok {
						patterns = append(patterns, SyntaxText(pattern, source))
					}
					for _, child := range node.GetChildren() {
						visit(child)
					}
				}
				visit(tree)
			})
			require.NoError(t, err)
			require.Contains(t, patterns, "(n:Record)")
		})
	}
	err := ReadExpressionSyntax(context.Background(), "coalesce(head($sizes), 1)", func(tree antlr4.Tree, source []rune) {
		require.IsType(t, &ExpressionContext{}, tree)
		require.Equal(t, "coalesce(head($sizes), 1)", SyntaxText(tree.(antlr4.ParserRuleContext), source))
	})
	require.NoError(t, err)
	err = ReadClauseSyntax(context.Background(), "WITH 'é' AS k", func(tree antlr4.Tree, source []rune) {
		require.Equal(t, "WITH 'é' AS k", SyntaxText(tree.(antlr4.ParserRuleContext), source))
	})
	require.NoError(t, err)
}

func TestTransactionalSyntaxRejectsPartialReads(t *testing.T) {
	consume := func(antlr4.Tree, []rune) { t.Error("invalid syntax reached its consumer") }
	require.Error(t, ReadExpressionSyntax(context.Background(), "1 ROWS", consume))
	require.Error(t, ReadClauseSyntax(context.Background(), "WITH 1 AS k INVALID_TAIL", consume))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ReadExpressionSyntax(ctx, "1", consume), context.Canceled)
	require.ErrorIs(t, ReadClauseSyntax(ctx, "WITH 1 AS k", consume), context.Canceled)
}

func TestTransactionalSyntaxStatementAdmission(t *testing.T) {
	limits := 0
	err := ReadStatementSyntax(context.Background(), "EXPLAIN CALL { RETURN 1 AS x LIMIT 1 } IN TRANSACTIONS OF 1 ROWS RETURN x", func(tree antlr4.Tree, source []rune) {
		require.IsType(t, &ScriptContext{}, tree)
		var visit func(antlr4.Tree)
		visit = func(node antlr4.Tree) {
			if limit, ok := node.(*LimitStContext); ok {
				limits++
				require.Equal(t, "1", SyntaxText(limit.Expression(), source))
			}
			for _, child := range node.GetChildren() {
				visit(child)
			}
		}
		visit(tree)
	})
	require.NoError(t, err)
	require.Equal(t, 1, limits)
}
