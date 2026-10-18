package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkStatementDeadlineHealthyPath measures each pipeline stage on a
// statement that is never cancelled, under the three contexts a statement
// runs with: one that can't be cancelled (embedded callers), one that can but
// has no deadline (a Bolt or HTTP request), and one with a deadline that is
// never reached (a configured statement timeout). The deadline's checks must
// cost the healthy path next to nothing. The unused $nonce parameter keeps
// the result cache from answering.
func BenchmarkStatementDeadlineHealthyPath(b *testing.B) {
	queries := []struct{ name, cypher string }{
		{"distinct", "MATCH (n:Person) RETURN DISTINCT n.age AS age"},
		{"sort", "MATCH (n:Person) RETURN n.name AS name ORDER BY n.age DESC, n.name LIMIT 50"},
		{"aggregation", "MATCH (n:Person) RETURN n.age % 10 AS k, count(*) AS c, collect(n.name)[0..3] AS some"},
		{"projection", "MATCH (n:Person) RETURN n.name AS name, toUpper(n.name) AS upper, n.age * 2 + 1 AS score, [x IN range(1, 3) | x * n.age] AS multiples"},
		{"unwind", "UNWIND range(1, 5000) AS x WITH x WHERE x % 3 = 0 RETURN sum(x) AS total"},
		{"traversal", "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN count(b) AS reached"},
		{"filter", "MATCH (n:Person) WHERE n.age > 30 AND n.name STARTS WITH 'p1' RETURN count(n) AS c"},
	}
	contexts := []struct {
		name string
		make func() (context.Context, context.CancelFunc)
	}{
		{"background", func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }},
		{"cancellable", func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }},
		{"deadline", func() (context.Context, context.CancelFunc) { return context.WithTimeout(context.Background(), time.Hour) }},
	}
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
	for i := 0; i < 2000; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("person-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"id": i, "name": fmt.Sprintf("p%d", i), "age": i % 60},
		})
		require.NoError(b, err)
	}
	for i := 0; i < 1999; i++ {
		require.NoError(b, store.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("knows-%d", i)),
			StartNode: storage.NodeID(fmt.Sprintf("person-%d", i)),
			EndNode:   storage.NodeID(fmt.Sprintf("person-%d", i+1)),
			Type:      "KNOWS",
		}))
	}
	exec := NewStorageExecutor(store)
	for _, q := range queries {
		for _, c := range contexts {
			b.Run(q.name+"/"+c.name, func(b *testing.B) {
				ctx, cancel := c.make()
				defer cancel()
				_, err := exec.Execute(ctx, q.cypher, map[string]interface{}{"nonce": -1})
				require.NoError(b, err)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := exec.Execute(ctx, q.cypher, map[string]interface{}{"nonce": i}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
