package cypher

import (
	"context"
	"testing"
)

// mustAnalyze is a test's analysis of a statement on a live context. A test
// that analyzes under cancellation asserts the returned error instead.
func mustAnalyze(tb testing.TB, analyzer *QueryAnalyzer, ctx context.Context, query string) *QueryInfo {
	tb.Helper()
	info, err := analyzer.Analyze(ctx, query)
	if err != nil {
		tb.Fatalf("analyze %q: %v", query, err)
	}
	return info
}

// analyzedReadQueryInfo is a statement analysis whose effect was read as no
// write at the current procedure registry generation; readShape says whether
// its text returns rows.
func analyzedReadQueryInfo(readShape bool) *QueryInfo {
	return analyzedEffectQueryInfo(readShape, false)
}

// analyzedEffectQueryInfo is a statement analysis carrying the given shape and
// effect without parsing its text, for a test that supplies the statement's
// effect itself.
func analyzedEffectQueryInfo(readShape, writes bool) *QueryInfo {
	effect := statementEffectRead
	if writes {
		effect = statementEffectWrite
	}
	return analyzedStatementEffectQueryInfo(readShape, effect)
}

func analyzedStatementEffectQueryInfo(readShape bool, effect statementEffect) *QueryInfo {
	ensureBuiltInProceduresRegistered()
	info := &QueryInfo{readShape: readShape}
	info.effect.Store(&queryStatementEffect{generation: globalProcedureRegistry.generation.Load() + 1, effect: effect})
	return info
}
