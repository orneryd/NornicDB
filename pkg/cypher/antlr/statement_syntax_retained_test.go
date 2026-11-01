package antlr

import (
	"context"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestRetainedRealSyntaxWindows(t *testing.T) {
	ctx := context.Background()
	query := "WITH 7 AS seed CALL (seed) { WITH seed AS `é` RETURN `é` AS value } WITH value RETURN value AS answer"
	whole, err := ParseSyntax(ctx, query, StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	tail := query[strings.Index(query, "CALL"):]
	derived, err := ParseSyntax(ctx, tail, FragmentEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, tail, derived.Source())
	require.Equal(t, whole.Origin().arena, derived.Origin().arena)
	call, ok := derived.LeadingCall()
	require.True(t, ok)
	require.Equal(t, CypherParserRULE_callSubquery, call.Rule())
	require.Equal(t, "WITH seed AS `é` RETURN `é` AS value", DescribeCall(call).Body)
	require.Equal(t, "WITH value RETURN value AS answer", DescribeCall(call).After)
	var outputs []string
	derived.Walk(func(node SyntaxNode) func() {
		a, b := node.Span()
		require.GreaterOrEqual(t, a, 0)
		require.LessOrEqual(t, b, len(tail))
		require.Equal(t, tail[a:b], node.Text())
		if node.Rule() == CypherParserRULE_projectionItem {
			outputs = append(outputs, node.Code())
		}
		return nil
	})
	require.NotContains(t, outputs, "7 AS seed")
	require.Contains(t, outputs, "value AS answer")
	body, err := ParseSyntax(ctx, DescribeCall(call).Body, StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, whole.Origin().arena, body.Origin().arena)
	patternQuery := "MATCH (d:R17Window {id: 7}) RETURN d"
	original, err := ParseSyntax(ctx, patternQuery, StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	pattern, err := ParseSyntax(ctx, "(d:R17Window {id: 7})", PatternEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, original.Origin().arena, pattern.Origin().arena)
	item, ok := RetainedProjectionItem(ctx, "value AS answer", false)
	require.True(t, ok)
	require.Equal(t, CypherParserRULE_projectionItem, item.Rule())
	require.Equal(t, whole.Origin().arena, &item.data.nodes[0])
}

func TestRetainedRealSyntaxWindowEffects(t *testing.T) {
	ctx := context.Background()
	query := "WITH 1 AS seed CALL (seed) { CREATE (n:R17Window {id:seed}) RETURN n } RETURN n"
	_, err := ParseSyntax(ctx, query, StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	syntax, err := ParseSyntax(ctx, "CREATE (n:R17Window {id:seed})", StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	require.True(t, syntax.Effects().Writes())
	require.False(t, syntax.Effects().ReadOnly(nil))
}
