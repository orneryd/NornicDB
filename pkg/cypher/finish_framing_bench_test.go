package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkFinishFraming times statements through the FINISH framing and the
// keyword-as-name scan (#958): one with no FINISH, one ending in the FINISH
// clause, one ending in a variable named finish, and a DELETE. The result
// cache is off.
func BenchmarkFinishFraming(b *testing.B) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"), 0, 0)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "UNWIND range(1, 100) AS i CREATE (:F {i: i})", nil); err != nil {
		b.Fatal(err)
	}
	for _, query := range []struct{ name, cypher string }{
		{"no-finish", "MATCH (n:F) WHERE n.i < 10 RETURN n.i AS v"},
		{"finish-clause", "MATCH (n:F) WHERE n.i < 10 FINISH"},
		{"finish-variable", "MATCH (finish:F) WHERE finish.i < 10 RETURN finish.i AS v"},
		{"delete-none", "MATCH (n:F) WHERE n.i < 0 DELETE n"},
	} {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, query.cypher, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
