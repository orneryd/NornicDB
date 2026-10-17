package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkSeekThroughImpliedLabelIndex looks identities up by id at 7,300
// identities, where only the GraphNode label has a unique id: the label order
// in the pattern, a WHERE label test, and the per-row lookups of an UNWIND.
func BenchmarkSeekThroughImpliedLabelIndex(b *testing.B) {
	const population = 7300
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench_implied"))
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "CREATE CONSTRAINT bench_graph_id FOR (n:GraphNode) REQUIRE n.id IS UNIQUE", nil); err != nil {
		b.Fatal(err)
	}
	if _, err := exec.Execute(ctx, "UNWIND range(0, $count - 1) AS i CREATE (:GraphNode:Identity {id: 'id' + toString(i), n: i})", map[string]interface{}{"count": population}); err != nil {
		b.Fatal(err)
	}
	for _, query := range []struct{ name, text string }{
		{"pattern_order", "MATCH (n:Identity:GraphNode {id: $x}) RETURN n.n"},
		{"where_label", "MATCH (n:Identity {id: $x}) WHERE n:GraphNode RETURN n.n"},
		{"unwind_rows", "UNWIND $items AS item MATCH (n:Identity:GraphNode {id: item.id}) RETURN n.n"},
	} {
		b.Run(query.name, func(b *testing.B) {
			exec.cache = nil
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n := i % population
				params := map[string]interface{}{
					"x":     fmt.Sprintf("id%d", n),
					"items": []interface{}{map[string]interface{}{"id": fmt.Sprintf("id%d", n)}, map[string]interface{}{"id": fmt.Sprintf("id%d", (n+7)%population)}},
				}
				if _, err := exec.Execute(ctx, query.text, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
