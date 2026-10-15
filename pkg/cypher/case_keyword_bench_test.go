package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkCaseKeywordQuery measures uncached public query execution on valid
// NOT/end controls supported by both the parent and the ambiguity fix. It keeps
// correctness checks in the workload so allocation comparisons cannot hide a
// different result. Concurrent runs support B/op comparisons, not latency claims.
func BenchmarkCaseKeywordQuery(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"Simple", "WITH false AS end RETURN CASE WHEN true THEN NOT end END"},
		{"Nested", "WITH false AS end RETURN CASE WHEN true THEN CASE WHEN true THEN NOT end END ELSE false END"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "case_keyword_bench"))
			executor.cache = nil
			executor.planCache = nil
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := executor.Execute(ctx, tc.query, nil)
				if err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != true {
					b.Fatalf("incorrect CASE result: result=%+v err=%v", result, err)
				}
			}
		})
	}
}
