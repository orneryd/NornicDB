package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkExecute_ScalarParameter(b *testing.B) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "allocation-execute")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	params := map[string]interface{}{"value": int64(7)}
	const query = "RETURN $value AS value"
	warm, err := exec.Execute(ctx, query, params)
	if err != nil || len(warm.Rows) != 1 || len(warm.Rows[0]) != 1 || warm.Rows[0][0] != int64(7) {
		b.Fatalf("unexpected warm result: %v, %v", warm, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := exec.Execute(ctx, query, params)
		if err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(7) {
			b.Fatalf("unexpected result: %v, %v", result, err)
		}
	}
	b.StopTimer()
}
