package cypher

import (
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
		var buffer [8]quotedToken
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			scanQuotedTokensInto(texts[i], buffer[:0])
		}
	})
	b.Run("plan", func(b *testing.B) {
		texts := queries(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			compileQuotedVariablePlan(texts[i])
		}
	})
	b.Run("plan_and_publication", func(b *testing.B) {
		texts := queries(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canonicalizeQuotedVariables(texts[i])
		}
	})
}
