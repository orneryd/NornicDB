// Author: Codex R17 implementation agent, 26 October 2026.
package antlr

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatementSyntaxScopeSharesCallWindows(t *testing.T) {
	source := "WITH 1 AS x CALL (x) { /* é */ WITH x AS y RETURN [v IN [y] | {`名`: v}] AS items } CALL () { RETURN 2 AS z } RETURN items, z"
	ctx := WithStatementSyntax(context.Background())
	whole, err := ParseSyntax(ctx, ReadStatement(source), StatementEntry, false, RawSource)
	require.NoError(t, err)
	from := strings.Index(source, "CALL")
	suffix := source[from:]
	call, err := LeadingCallSyntax(ctx, ReadStatement(suffix), false, RawSource)
	require.NoError(t, err)
	require.True(t, call.Valid())
	require.Same(t, &whole.data.nodes[0], &call.data.nodes[0], "a window shares all grammar node storage")
	require.Equal(t, suffix, call.Source())
	start, _ := call.Span()
	require.Zero(t, start)
	_, parent := call.Parent()
	require.False(t, parent)
	description := DescribeCall(call)
	require.Equal(t, "/* é */ WITH x AS y RETURN [v IN [y] | {`名`: v}] AS items", description.Body)
	require.Equal(t, "CALL () { RETURN 2 AS z } RETURN items, z", description.After)

	standalone, err := ParseSyntax(context.Background(), ReadStatement(suffix), fragmentEntry, false, RawSource)
	require.NoError(t, err)
	parsed, ok := standalone.LeadingCall()
	require.True(t, ok)
	require.Equal(t, DescribeCall(parsed), description)
	require.Equal(t, parsed.Code(), call.Code())
	first, after := call.TokenRange()
	require.Equal(t, CypherLexerCALL, (StatementSyntax{call.data}).Token(first).Kind())
	require.Greater(t, after, first)

	clause := strings.TrimSpace(description.Body)
	body, err := ParseSyntax(ctx, ReadStatement(clause), ClauseEntry, false, RawSource)
	require.NoError(t, err)
	// Trivia outside the grammar body is a standalone adapter input; its
	// source spelling is preserved even when no exact window matches.
	require.Equal(t, clause, body.Source())
	view, err := InspectSyntax(ctx, ReadStatement(call.Text()), false, RawSource)
	require.NoError(t, err)
	require.Equal(t, call.Text(), view.Root().Text())
}

func TestStatementSyntaxScopeRetainsProductsAcrossGlobalEviction(t *testing.T) {
	ctx := WithStatementSyntax(context.Background())
	source := "RETURN 918273 AS statementLifetime"
	first, err := ParseSyntax(ctx, ReadStatement(source), StatementEntry, false, RawSource)
	require.NoError(t, err)
	statementSyntaxCache.Lock()
	clear(statementSyntaxCache.entries)
	statementSyntaxCache.Unlock()
	second, err := ParseSyntax(ctx, ReadStatement(source), StatementEntry, false, RawSource)
	require.NoError(t, err)
	require.Same(t, first.data, second.data)
	other, err := ParseSyntax(context.Background(), ReadStatement(source), StatementEntry, false, RawSource)
	require.NoError(t, err)
	require.NotSame(t, first.data, other.data)
}

func TestStatementSyntaxScopeRetainsOneStatementProduct(t *testing.T) {
	source := "WITH 1 AS x CALL (x) { RETURN x AS y } RETURN y"
	ctx := WithNewStatementSyntax(context.Background(), source)
	whole, err := ParseSyntax(ctx, ReadStatement(source), StatementEntry, false, RawSource)
	require.NoError(t, err)
	for _, unrelated := range []string{"RETURN 711 AS a", "RETURN 712 AS b", "RETURN 713 AS c"} {
		_, err = ParseSyntax(ctx, ReadStatement(unrelated), StatementEntry, false, RawSource)
		require.Error(t, err, "an internal consumer cannot create another parsing owner")
	}
	scope := statementSyntaxScopeFrom(ctx)
	require.Len(t, scope.roots, 1)
	require.Same(t, whole.data, scope.roots[0].data)
	other := WithNewStatementSyntax(ctx, "RETURN 714 AS independent")
	require.NotSame(t, scope, statementSyntaxScopeFrom(other))
	require.Empty(t, statementSyntaxScopeFrom(other).roots)
	independent, err := ParseSyntax(other, ReadStatement("RETURN 714 AS independent"), StatementEntry, false, RawSource)
	require.NoError(t, err)
	require.NotSame(t, &whole.data.nodes[0], &independent.data.nodes[0])
}

func TestStatementSyntaxScopeDoesNotInferPartialOrAmbiguousWindows(t *testing.T) {
	ctx := WithStatementSyntax(context.Background())
	source := "CALL () { RETURN 1 AS x } CALL () { RETURN 1 AS x } RETURN x"
	_, err := ParseSyntax(ctx, ReadStatement(source), StatementEntry, false, RawSource)
	require.NoError(t, err)
	_, err = LeadingCallSyntax(ctx, ReadStatement("CALL () { RETURN 1 AS x } RETURN"), false, RawSource)
	require.Error(t, err, "a valid first CALL cannot admit its malformed continuation")
	_, err = ParseSyntax(ctx, ReadStatement("1 ROWS"), ExpressionEntry, false, RawSource)
	require.Error(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = InspectSyntax(cancelled, ReadStatement(source), false, RawSource)
	require.ErrorIs(t, err, context.Canceled)
	_, err = LeadingCallSyntax(cancelled, ReadStatement(source), false, RawSource)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStatementSyntaxScopeEstablishesCanonicalRootBeforeInspection(t *testing.T) {
	original := "/* written */ WITH 1 AS x CALL (x) { RETURN ln(x) AS y } RETURN y"
	current := "WITH 1 AS x CALL (x) { RETURN log(x) AS y } RETURN y"
	ctx := WithNewStatementSyntax(context.Background(), original)
	ctx = WithStatementSyntax(ctx, original, current)
	call, err := LeadingCallSyntax(ctx, ReadStatement(current[strings.Index(current, "CALL"):]), false, AcceptedSource)
	require.NoError(t, err)
	require.True(t, call.Valid())
	scope := statementSyntaxScopeFrom(ctx)
	whole := scope.product(statementSyntaxKey{source: current, entry: StatementEntry, stage: AcceptedSource}, nil)
	require.NotNil(t, whole)
	require.Same(t, &whole.nodes[0], &call.data.nodes[0])
	require.Equal(t, "RETURN log(x) AS y", DescribeCall(call).Body)
	require.Len(t, scope.roots, 1)
	ctx = WithStatementSyntax(ctx, original, "RETURN 2 AS unrelated")
	_, err = QuerySyntax(ctx, ReadStatement("RETURN 2 AS unrelated"), false, AcceptedSource)
	require.Error(t, err, "a different statement requires a distinct ingress")
	require.Len(t, scope.roots, 1)
}
