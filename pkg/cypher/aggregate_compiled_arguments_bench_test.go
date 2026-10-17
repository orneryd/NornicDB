package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkAggregateCompiledArguments is an aggregation over map literal
// and multi-argument aggregates in a statement with parameters: collect of
// a map per pair, and a GDS Cypher projection with map arguments.
func BenchmarkAggregateCompiledArguments(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"))
	exec.cache = nil
	ctx := context.Background()
	params := map[string]interface{}{"per_group": int64(3), "limit": int64(10)}
	collect := "UNWIND range(1, 200) AS a UNWIND range(1, 40) AS b WITH a % 50 AS group, a, b " +
		"RETURN group, collect({thing: b, having: a, weight: 1.0 / (a + b)}) AS shared ORDER BY group LIMIT $limit"
	project := "UNWIND range(1, 4000) AS i WITH {a: i % 400, b: (i * 7) % 400, weight: 1.0 / i} AS edge " +
		"WITH gds.graph.project('bench-agg', edge.a, edge.b, {relationshipType: 'RELATED', relationshipProperties: {weight: edge.weight}}, " +
		"{undirectedRelationshipTypes: ['*']}) AS projected CALL gds.graph.drop('bench-agg', false) YIELD graphName RETURN projected.relationshipCount AS r"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range []string{collect, project} {
			if _, err := exec.Execute(ctx, query, params); err != nil {
				b.Fatal(err)
			}
		}
	}
}
