package cypher

import (
	"context"
	"strconv"
	"testing"
)

// BenchmarkQuotedRewriteDecomposition separates the parts of a first
// literal-binding rewrite: the token walk that proves the binding, the
// complete plan (edits, internal identifier, output), and publication of the
// plan for the statement's later executions.
func BenchmarkQuotedRewriteDecomposition(b *testing.B) {
	prefix := "MATCH ()-[true]->() RETURN type(`true`) AS result_"
	queries := func(b *testing.B) []string {
		texts := make([]string, b.N)
		for i := range texts {
			texts[i] = prefix + strconv.Itoa(b.N) + "_" + strconv.Itoa(i)
		}
		return texts
	}
	b.Run("token_walk", func(b *testing.B) {
		texts := queries(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			scanQuotedTokens(context.Background(), texts[i])
		}
	})
	b.Run("rewrite", func(b *testing.B) {
		texts := queries(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canonicalizeQuotedVariablesWithBindings(context.Background(), texts[i], nil)
		}
	})
	b.Run("repeat_rewrite", func(b *testing.B) {
		texts := queries(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canonicalizeQuotedVariables(context.Background(), texts[i])
		}
	})
}
