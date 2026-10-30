package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkForeach_CreatePerItem pins the per-item update execution of FOREACH
// (25 CREATEs per op on a fresh memory engine).
func BenchmarkForeach_CreatePerItem(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "test")
		exec := NewStorageExecutor(store)
		if _, err := exec.Execute(ctx, `FOREACH (x IN range(1, 25) | CREATE (:FB {v: x}))`, nil); err != nil {
			b.Fatal(err)
		}
	}
}
