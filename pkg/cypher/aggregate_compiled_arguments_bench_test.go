package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkAggregateCompiledArguments is an aggregation over map literal
// in a statement with parameters: collect of
// a map per pair.
func BenchmarkAggregateCompiledArguments(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"))
	exec.cache = nil
	ctx := context.Background()
	params := map[string]interface{}{"per_group": int64(3), "limit": int64(10)}
	collect := "UNWIND range(1, 200) AS a UNWIND range(1, 40) AS b WITH a % 50 AS group, a, b " +
		"RETURN group, collect({thing: b, having: a, weight: 1.0 / (a + b)}) AS shared ORDER BY group LIMIT $limit"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range []string{collect} {
			if _, err := exec.Execute(ctx, query, params); err != nil {
				b.Fatal(err)
			}
		}
	}
}
