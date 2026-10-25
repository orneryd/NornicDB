package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

// BenchmarkStatementFramingAndOrdering retains separate FINISH and value-ordering
// populations with a cache-disabled executor per family.
func BenchmarkStatementFramingAndOrdering(b *testing.B) {
	for _, family := range []struct {
		name, seed string
		queries    []struct{ name, cypher string }
	}{
		{"FinishFraming", "UNWIND range(1, 100) AS i CREATE (:F {i: i})", []struct{ name, cypher string }{
			{"no-finish", "MATCH (n:F) WHERE n.i < 10 RETURN n.i AS v"},
			{"finish-clause", "MATCH (n:F) WHERE n.i < 10 FINISH"},
			{"finish-variable", "MATCH (finish:F) WHERE finish.i < 10 RETURN finish.i AS v"},
			{"delete-none", "MATCH (n:F) WHERE n.i < 0 DELETE n"},
		}},
		{"ValueOrdering", "UNWIND range(1, 5000) AS i CREATE (:V {v: (i * 7919) % 5000})", []struct{ name, cypher string }{
			{"where-number-less", "MATCH (n:V) WHERE n.v < 2500 RETURN count(n)"},
			{"order-by-number", "MATCH (n:V) RETURN n.v ORDER BY n.v LIMIT 10"},
			{"order-by-node", "MATCH (n:V) RETURN n.v ORDER BY n LIMIT 10"},
			{"order-by-map", "UNWIND range(1, 2000) AS i WITH {k: (i * 7919) % 2000, s: toString(i)} AS m RETURN m ORDER BY m LIMIT 10"},
			{"map-less", "UNWIND range(1, 2000) AS i WITH {k: i} AS m WHERE m < {k: 1000} RETURN count(m)"},
			{"map-less-incomparable", "UNWIND range(1, 2000) AS i WITH {k: i} AS m WHERE m < {k: 'x'} RETURN count(m)"},
		}},
	} {
		b.Run(family.name, func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
			ctx := context.Background()
			if _, err := exec.Execute(ctx, family.seed, nil); err != nil {
				b.Fatal(err)
			}
			for _, query := range family.queries {
				b.Run(query.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, err := exec.Execute(ctx, query.cypher, nil); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
