package cypher

import (
	"context"
	"testing"
)

// mustAnalyze is a test's analysis of a statement on a live context.
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
	info := &QueryInfo{readShape: readShape}
	info.effect.Store((globalProcedureRegistry.generation.Load() + 1) << 1)
	return info
}
