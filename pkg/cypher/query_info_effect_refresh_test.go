package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A cached analysis carries the statement-effect owner's answer for the live
// procedure registry: an unresolvable procedure is a conservative write, and a
// registration that changes its mode refreshes the same analysis in place.
func TestQueryAnalyzerRefreshesStatementEffectOnProcedureRegistryChange(t *testing.T) {
	registry := NewProcedureRegistry()
	replaceProcedureRegistryForTest(t, registry)
	handler := func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		t.Fatal("effect inspection must not execute a procedure")
		return nil, nil
	}
	const query = "CALL effect_fixture.live()"
	analyzer := NewQueryAnalyzer(4)

	unknown := mustAnalyze(t, analyzer, t.Context(), query)
	require.True(t, unknown.Writes(), "an unresolvable procedure has an unknown effect, so it is a write")
	require.False(t, unknown.ReadOnly())

	require.NoError(t, registry.RegisterUser(ProcedureSpec{Name: "effect_fixture.live", Mode: ProcedureModeRead}, handler))
	read := mustAnalyze(t, analyzer, t.Context(), query)
	require.Same(t, unknown, read, "the cached analysis is the one refreshed")
	require.False(t, read.Writes(), "a registered READ procedure is no write")

	require.NoError(t, registry.RegisterUser(ProcedureSpec{Name: "effect_fixture.live", Mode: ProcedureModeWrite}, handler))
	write := mustAnalyze(t, analyzer, t.Context(), query)
	require.True(t, write.Writes(), "the same analysis follows the registry's new mode")
	require.False(t, write.ReadOnly())

	require.NoError(t, registry.RegisterUser(ProcedureSpec{Name: "effect_fixture.live", Mode: ProcedureModeRead}, handler))
	again := mustAnalyze(t, analyzer, t.Context(), query)
	require.Same(t, unknown, again)
	require.False(t, again.Writes(), "a restored READ mode makes it a read again")
}

// Text the statement-effect owner cannot read has an unknown effect, which
// keeps the read-only optimizations off; a real read statement stays a read.
func TestQueryAnalyzerStatementEffectStaysConservativeForUnreadableText(t *testing.T) {
	analyzer := NewQueryAnalyzer(4)

	read := mustAnalyze(t, analyzer, t.Context(), "MATCH (n:Doc) RETURN n")
	require.False(t, read.Writes())
	require.True(t, read.ReadOnly())

	unreadable := mustAnalyze(t, analyzer, t.Context(), "MATCH (n:Doc) RETURN n(")
	require.True(t, unreadable.Writes(), "unreadable text is a write for optimizations")
	require.False(t, unreadable.ReadOnly())
}

// A cancellation while computing a fresh effect is the caller's, and it leaves
// no analysis behind for a later statement to inherit.
func TestQueryAnalyzerReturnsCancellationWithoutCaching(t *testing.T) {
	analyzer := NewQueryAnalyzer(4)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	info, err := analyzer.Analyze(ctx, "MATCH (n:Doc) RETURN n")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, info)
	require.Zero(t, analyzer.CacheSize())

	live := mustAnalyze(t, analyzer, t.Context(), "MATCH (n:Doc) RETURN n")
	require.True(t, live.ReadOnly())
}

// A cache hit must honor the same caller cancellation contract as a cold read.
func TestQueryAnalyzerWarmCacheHonorsCancellation(t *testing.T) {
	analyzer := NewQueryAnalyzer(4)
	const query = "MATCH (n:Doc) RETURN n"
	live, err := analyzer.Analyze(t.Context(), query)
	require.NoError(t, err)
	require.True(t, live.ReadOnly())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	info, err := analyzer.Analyze(ctx, query)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, info)
	resumed, err := analyzer.Analyze(t.Context(), query)
	require.NoError(t, err)
	require.Same(t, live, resumed)
	require.True(t, resumed.ReadOnly())
}

func TestQueryAnalyzerCachedEffectAgreesAcrossStatementVersions(t *testing.T) {
	analyzer := NewQueryAnalyzer(32)
	for _, tc := range []struct {
		query  string
		writes bool
	}{
		{"MATCH (n:Doc) RETURN n", false},
		{"RETURN 'CREATE SET DELETE' AS text", false},
		{"MATCH (n:Doc) RETURN n.set", false},
		{"CREATE (:Doc)", true},
		{"FOREACH (x IN [1] | CREATE (:Doc))", true},
		{"CALL kp.suppress(null)", true},
		{"CALL db.labels()", false},
		{"CALL missing.effect()", true},
		{"CREATE INDEX effect_index FOR (n:Doc) ON (n.id)", true},
		{"DROP DATABASE effect_database IF EXISTS", true},
		{"SHOW DATABASES", false},
		{"EXPLAIN CREATE (:Doc)", false},
	} {
		for _, version := range []string{"5", "25", "5"} {
			ctx := context.WithValue(t.Context(), cypherVersionKey{}, version)
			info, err := analyzer.Analyze(ctx, tc.query)
			require.NoError(t, err, "%s: %s", version, tc.query)
			require.Equal(t, tc.writes, info.Writes(), "%s: %s", version, tc.query)
		}
	}
}
