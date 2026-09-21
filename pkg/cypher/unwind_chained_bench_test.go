package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func newChainedUnwindBenchExecutor(b *testing.B) *StorageExecutor {
	b.Helper()
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "bench")
	return NewStorageExecutor(store)
}

func BenchmarkChainedUnwindIndependentLists(b *testing.B) {
	exec := newChainedUnwindBenchExecutor(b)
	ctx := context.Background()
	query := "UNWIND range(1, 256) AS i UNWIND [1,2,3,4] AS j RETURN i, j"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		if len(res.Rows) != 1024 {
			b.Fatalf("unexpected row count: got %d want %d", len(res.Rows), 1024)
		}
	}
}

func BenchmarkChainedUnwindDependentRange(b *testing.B) {
	exec := newChainedUnwindBenchExecutor(b)
	ctx := context.Background()
	query := "UNWIND range(1, 256) AS i UNWIND range(1, i) AS j RETURN i, j"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		// Sum_{i=1}^{256} i = 32896
		if len(res.Rows) != 32896 {
			b.Fatalf("unexpected row count: got %d want %d", len(res.Rows), 32896)
		}
	}
}
