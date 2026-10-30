// Author: Codex R17 implementation agent, 26 October 2026.
package antlr

import (
	"context"
	"testing"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"
)

func TestStatementSyntaxInspectionRetainsCanonicalStatementAndAdmissionReusesIt(t *testing.T) {
	source := "/* inspection owner */ WITH 2718 AS x CALL (x) { RETURN x AS y } RETURN y"
	ctx := WithStatementSyntax(context.Background(), source)
	inspection, err := InspectSyntax(ctx, source, false, RawSource)
	require.NoError(t, err)
	require.Equal(t, CypherParserRULE_script, inspection.Root().Rule())
	expected, err := parseStatementSyntaxPrediction(context.Background(), source, StatementEntry, nil, antlr4.PredictionModeLL)
	require.NoError(t, err)
	require.Equal(t, expected, inspection.data)
	query, err := QuerySyntax(ctx, source, false, RawSource)
	require.NoError(t, err)
	require.Same(t, inspection.data, query.data)
	scope := statementSyntaxScopeFrom(ctx)
	require.Same(t, inspection.data, scope.product(statementSyntaxKey{source: source, entry: StatementEntry, stage: RawSource}), "whole inspection must carry the canonical grammar meaning")
	require.Len(t, scope.roots, 1)
	admitted, err := ParseSyntax(ctx, source, StatementEntry, false, RawSource)
	require.NoError(t, err)
	require.Equal(t, CypherParserRULE_script, admitted.Root().Rule())
	require.Same(t, inspection.data, admitted.data, "admission must reuse the original canonical syntax")
	require.Equal(t, inspection.Origin(), admitted.Origin())
	require.Len(t, scope.roots, 1, "inspection and admission share one root lifetime")
	current, err := InspectSyntax(ctx, source, false, RawSource)
	require.NoError(t, err)
	require.Same(t, admitted.data, current.data)
}

func TestStatementSyntaxInspectionUsesOriginalDiagnosticEntry(t *testing.T) {
	for _, source := range []string{
		"CALL (1) { RETURN 1 AS value } RETURN value",
		"CALL () { RETURN 1 AS x } RETURN",
		"WITH 1 AS x CALL (x) { RETURN x AS y } @",
		"RETURN [x IN [1, 2] WHERE ] AS invalid",
	} {
		_, want := parseStatementSyntaxPrediction(context.Background(), source, StatementEntry, nil, antlr4.PredictionModeLL)
		require.Error(t, want)
		ctx := WithStatementSyntax(context.Background(), source)
		_, got := InspectSyntax(ctx, source, false, RawSource)
		require.EqualError(t, got, want.Error())
		_, got = QuerySyntax(ctx, source, false, RawSource)
		require.EqualError(t, got, want.Error())
	}
}

func TestQuerySyntaxStandaloneDiagnosticsUseStatementEntry(t *testing.T) {
	for _, source := range []string{"WHERE x", "ORDER BY", "[", "coalesce(1, 2)"} {
		_, want := parseStatementSyntaxPrediction(context.Background(), source, StatementEntry, nil, antlr4.PredictionModeLL)
		require.Error(t, want)
		_, got := QuerySyntax(context.Background(), source, false, RawSource)
		require.EqualError(t, got, want.Error())
	}
}

func TestQuerySyntaxAdoptsCachedStatementBeforeScopeReadersRun(t *testing.T) {
	source := "WITH 161803 AS cachedInspection CALL () { RETURN 1 AS value } RETURN cachedInspection, value"
	cached, err := InspectSyntax(context.Background(), source, false, RawSource)
	require.NoError(t, err)
	require.Equal(t, CypherParserRULE_script, cached.Root().Rule())
	ctx := WithStatementSyntax(context.Background(), "/* written */ "+source, source)
	// Cached semantic checks can leave a new execution with no reader visit
	// to its current root. Query compilation must adopt the existing product.
	query, err := QuerySyntax(ctx, source, false, RawSource)
	require.NoError(t, err)
	require.Same(t, cached.data, query.data)
	require.Same(t, cached.data, statementSyntaxScopeFrom(ctx).product(statementSyntaxKey{source: source, entry: StatementEntry, stage: RawSource}), "a cache hit must adopt the canonical product without another parse")
}
