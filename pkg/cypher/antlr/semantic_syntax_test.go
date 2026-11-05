package antlr

import (
	"context"
	"testing"

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
			err := ReadClauseSyntax(context.Background(), query, func(syntax StatementSyntax) {
				syntax.Walk(func(node SyntaxNode) func() {
					if node.Rule() == CypherParserRULE_nodePattern {
						patterns = append(patterns, node.Text())
					}
					return nil
				})
			})
			require.NoError(t, err)
			require.Contains(t, patterns, "(n:Record)")
		})
	}
	err := ReadExpressionSyntax(context.Background(), "coalesce(head($sizes), 1)", func(syntax StatementSyntax) {
		require.Equal(t, CypherParserRULE_expression, syntax.Root().Rule())
		require.Equal(t, "coalesce(head($sizes), 1)", syntax.Root().Text())
	})
	require.NoError(t, err)
	err = ReadClauseSyntax(context.Background(), "WITH 'é' AS k", func(syntax StatementSyntax) {
		require.Equal(t, "WITH 'é' AS k", syntax.Root().Text())
	})
	require.NoError(t, err)
}

func TestTransactionalSyntaxRejectsPartialReads(t *testing.T) {
	consume := func(StatementSyntax) { t.Error("invalid syntax reached its consumer") }
	require.Error(t, ReadExpressionSyntax(context.Background(), "1 ROWS", consume))
	require.Error(t, ReadClauseSyntax(context.Background(), "WITH 1 AS k INVALID_TAIL", consume))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ReadExpressionSyntax(ctx, "1", consume), context.Canceled)
	require.ErrorIs(t, ReadClauseSyntax(ctx, "WITH 1 AS k", consume), context.Canceled)
}

func TestTransactionalSyntaxStatementAdmission(t *testing.T) {
	limits := 0
	err := ReadStatementSyntax(context.Background(), "EXPLAIN CALL { RETURN 1 AS x LIMIT 1 } IN TRANSACTIONS OF 1 ROWS RETURN x", func(syntax StatementSyntax) {
		require.Equal(t, CypherParserRULE_script, syntax.Root().Rule())
		syntax.Walk(func(node SyntaxNode) func() {
			if node.Rule() == CypherParserRULE_limitSt {
				limits++
				require.Equal(t, "1", node.ChildRule(CypherParserRULE_expression).Text())
			}
			return nil
		})
	})
	require.NoError(t, err)
	require.Equal(t, 1, limits)
}
