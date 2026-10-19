package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkPipelineWithScope compares the shared streaming WITH owner with
// and without a predicate. A changing parameter prevents result-cache hits.
func BenchmarkPipelineWithScope(b *testing.B) {
	for _, tc := range []struct {
		name, predicate string
		total           int64
	}{
		{"projection", "", 524800},
		{"predicate", " WHERE x % 2 = 0", 262656},
	} {
		b.Run(tc.name, func(b *testing.B) {
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
			executor := NewStorageExecutor(engine)
			query := "UNWIND range(1, 1024) AS v WITH v AS x" + tc.predicate + " RETURN sum(x) AS total"
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := executor.Execute(ctx, query, map[string]interface{}{"nonce": i})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != tc.total {
					b.Fatalf("incorrect sum: %v, want %d", result.Rows, tc.total)
				}
			}
		})
	}
}
