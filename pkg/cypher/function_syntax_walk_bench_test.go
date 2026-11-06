package cypher

import (
	"strconv"
	"strings"
	"testing"
)

// This statement exceeds the lexical retention bound. Both implementations
// must enumerate every call without relying on a cached statement result.
func BenchmarkFunctionSyntaxWholeQueryWalk(b *testing.B) {
	query := "RETURN " + strings.Repeat("coalesce(/* don't split , */ null, custom.fn(1)),", 512) + "1"
	count := 0
	if err := forEachFunctionCall(b.Context(), query, func(_, _ string) error { count++; return nil }); err != nil || count != 1024 {
		b.Fatalf("call inventory: count=%d error=%v", count, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := forEachFunctionCall(b.Context(), query, func(_, _ string) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

// Exercise a whole-query consumer beyond the retention bound, so repeatedly
// requesting a fresh lexical tree for each call cannot hide behind the cache.
func BenchmarkFunctionSyntaxGraphRewrite(b *testing.B) {
	for _, calls := range []int{2000, 4000} {
		query := "/* prefix */ RETURN [" + strings.Repeat("head([null]):A,", calls) + "null] AS v"
		b.Run(strconv.Itoa(calls), func(b *testing.B) {
			rewritten, restore, err := desugarLabelExpressions(b.Context(), query, nil, false)
			if err != nil || restore == nil || rewritten == query {
				b.Fatalf("rewrite missing: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := desugarLabelExpressions(b.Context(), query, nil, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
