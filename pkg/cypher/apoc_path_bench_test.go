package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkApocPath runs apoc.path procedures and apoc.neighbors.tohop from
// one node of a 2,000-node graph (a binary tree of :R relationships with a
// :T shortcut from every tenth node), with the result cache off (#907).
// results/op is the count each call returns.
func BenchmarkApocPath(b *testing.B) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"), 0, 0)
	ctx := context.Background()
	for _, statement := range []string{
		"UNWIND range(0, 1999) AS i CREATE (:P {id: 'n' + toString(i), i: i})",
		"MATCH (child:P) WHERE child.i > 0 MATCH (parent:P {i: (child.i - 1) / 2}) CREATE (parent)-[:R]->(child)",
		"MATCH (from:P) WHERE from.i % 10 = 0 MATCH (to:P {i: (from.i * 7 + 3) % 2000}) CREATE (from)-[:T]->(to)",
	} {
		if _, err := exec.Execute(ctx, statement, nil); err != nil {
			b.Fatal(err)
		}
	}
	for _, query := range []struct{ name, cypher string }{
		{"subgraphNodes/maxLevel=3", "MATCH (s {id: 'n0'}) CALL apoc.path.subgraphNodes(s, {maxLevel: 3}) YIELD node RETURN count(node)"},
		{"subgraphNodes/all", "MATCH (s {id: 'n0'}) CALL apoc.path.subgraphNodes(s, {}) YIELD node RETURN count(node)"},
		{"expandConfig/R>/maxLevel=4", "MATCH (s {id: 'n0'}) CALL apoc.path.expandConfig(s, {relationshipFilter: 'R>', maxLevel: 4}) YIELD path RETURN count(path)"},
		{"spanningTree/all", "MATCH (s {id: 'n0'}) CALL apoc.path.spanningTree(s, {}) YIELD path RETURN count(path)"},
		{"neighbors.tohop/2", "MATCH (s {id: 'n0'}) CALL apoc.neighbors.tohop(s, 'R>', 2) YIELD node RETURN count(node)"},
	} {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			var count interface{}
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, query.cypher, nil)
				if err != nil {
					b.Fatal(err)
				}
				count = result.Rows[0][0]
			}
			if results, ok := count.(int64); ok {
				b.ReportMetric(float64(results), "results/op")
			}
		})
	}
}
