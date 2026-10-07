package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkValueOrdering times comparisons and ORDER BY over 5,000 :V nodes
// with a number property, and ORDER BY over 2,000 maps (#907). The result
// cache is off.
func BenchmarkValueOrdering(b *testing.B) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"), 0, 0)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "UNWIND range(1, 5000) AS i CREATE (:V {v: (i * 7919) % 5000})", nil); err != nil {
		b.Fatal(err)
	}
	for _, query := range []struct{ name, cypher string }{
		{"where-number-less", "MATCH (n:V) WHERE n.v < 2500 RETURN count(n)"},
		{"order-by-number", "MATCH (n:V) RETURN n.v ORDER BY n.v LIMIT 10"},
		{"order-by-node", "MATCH (n:V) RETURN n.v ORDER BY n LIMIT 10"},
		{"order-by-map", "UNWIND range(1, 2000) AS i WITH {k: (i * 7919) % 2000, s: toString(i)} AS m RETURN m ORDER BY m LIMIT 10"},
		{"map-less", "UNWIND range(1, 2000) AS i WITH {k: i} AS m WHERE m < {k: 1000} RETURN count(m)"},
		{"map-less-incomparable", "UNWIND range(1, 2000) AS i WITH {k: i} AS m WHERE m < {k: 'x'} RETURN count(m)"},
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
