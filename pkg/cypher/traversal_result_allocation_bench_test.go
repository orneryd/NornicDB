package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkTraversalResultAllocation isolates result collection from disk and
// parser costs while exercising the public MATCH consumer on a branching graph.
// Fixture construction and warmup are outside the measured interval.
func BenchmarkTraversalResultAllocation(b *testing.B) {
	for _, width := range []int{16, 256, 4096} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			base := testfixture.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			b.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "traversal-results")
			ctx := b.Context()
			_, err := store.CreateNode(ctx, &storage.Node{ID: "root", Labels: []string{"ResultRoot"}})
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < width; i++ {
				id := storage.NodeID(fmt.Sprintf("child-%d", i))
				if _, err := store.CreateNode(ctx, &storage.Node{ID: id}); err != nil {
					b.Fatal(err)
				}
				if err := store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(id), StartNode: "root", EndNode: id, Type: "R"}); err != nil {
					b.Fatal(err)
				}
			}
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			const query = "MATCH (s:ResultRoot)-[:R]->(n) RETURN count(n) AS total ORDER BY total"
			check := func() {
				result, err := exec.Execute(ctx, query, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(width) {
					b.Fatalf("expected one count of %d, got %#v", width, result)
				}
			}
			check()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				check()
			}
		})
	}
}
