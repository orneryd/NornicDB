package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

// BenchmarkVisibleCountMetadata measures the same correct public label count
// with and without one unrelated internal record. Both populations contain
// exactly 5,000 counted nodes; the unrelated record must not affect the result.
func BenchmarkVisibleCountMetadata(b *testing.B) {
	for _, hidden := range []bool{false, true} {
		b.Run(fmt.Sprintf("unrelated_internal=%v", hidden), func(b *testing.B) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "countbench")
			for i := 0; i < 5000; i++ {
				_, err := store.CreateNode(b.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Counted"}})
				if err != nil {
					b.Fatal(err)
				}
			}
			if hidden {
				if _, err := store.CreateNode(b.Context(), &storage.Node{ID: "metadata", Labels: []string{"_internal"}}); err != nil {
					b.Fatal(err)
				}
			}
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(b.Context(), "MATCH (n:Counted) RETURN count(n)", nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0][0] != int64(5000) {
					b.Fatalf("wrong count: %v", result.Rows)
				}
			}
		})
	}
}
