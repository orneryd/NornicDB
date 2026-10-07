package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkMergeUniqueKey times MERGE on a uniquely constrained key (#961):
// creating a new key, matching an existing one, a 100-row UNWIND batch, and
// a MERGE on a label without a constraint. The result cache is off.
func BenchmarkMergeUniqueKey(b *testing.B) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"), 0, 0)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "CREATE CONSTRAINT u_k FOR (u:U) REQUIRE u.k IS UNIQUE", nil); err != nil {
		b.Fatal(err)
	}
	if _, err := exec.Execute(ctx, "MERGE (u:U {k: -1})", nil); err != nil {
		b.Fatal(err)
	}
	next := int64(0)
	for _, query := range []struct {
		name, cypher string
		step         int64
	}{
		{"merge-create", "MERGE (u:U {k: $k}) RETURN u.k", 1},
		{"merge-match", "MERGE (u:U {k: -1}) RETURN u.k", 0},
		{"unwind-merge-100", "UNWIND range($k, $k + 99) AS i MERGE (u:U {k: i})", 100},
		{"merge-unconstrained", "MERGE (p:P {k: $k}) RETURN p.k", 1},
	} {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				next += query.step + 1
				if _, err := exec.Execute(ctx, query.cypher, map[string]interface{}{"k": next}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
