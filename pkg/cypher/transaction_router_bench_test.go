package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The unused $nonce parameter changes on every iteration so the result cache never
// answers; the benchmark measures execution, not cache hits.
//
// BenchmarkStatementRouting compares the same read statements executed as
// auto-commit statements and inside one explicit transaction (BEGIN ... ROLLBACK).
func BenchmarkStatementRouting(b *testing.B) {
	benchmarkStatementRouting(b, []statementRoutingBenchQuery{
		{"simple_match_limit", "MATCH (n:Person) RETURN n.name LIMIT 10"},
		{"property_lookup", "MATCH (n:Person {id: 42}) RETURN n.name"},
		{"filter_count", "MATCH (n:Person) WHERE n.age > 30 RETURN count(n)"},
		{"one_hop_limit", "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 20"},
		{"return_literal", "RETURN 1"},
	})
}

type statementRoutingBenchQuery struct{ name, cypher string }

// The same fixture isolates the property-lookup control without timing the
// other routing workloads in a transactional CALL comparison.
func BenchmarkTransactionalCallPropertyLookupControl(b *testing.B) {
	benchmarkStatementRouting(b, []statementRoutingBenchQuery{
		{"property_lookup", "MATCH (n:Person {id: 42}) RETURN n.name"},
	})
}

func benchmarkStatementRouting(b *testing.B, queries []statementRoutingBenchQuery) {
	setup := func(b *testing.B) (*StorageExecutor, context.Context) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
		for i := 0; i < 2000; i++ {
			_, err := store.CreateNode(&storage.Node{
				ID:         storage.NodeID(fmt.Sprintf("person-%d", i)),
				Labels:     []string{"Person"},
				Properties: map[string]interface{}{"id": i, "name": fmt.Sprintf("p%d", i), "age": i % 60},
			})
			require.NoError(b, err)
		}
		for i := 0; i < 500; i++ {
			err := store.CreateEdge(&storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("knows-%d", i)),
				StartNode: storage.NodeID(fmt.Sprintf("person-%d", i)),
				EndNode:   storage.NodeID(fmt.Sprintf("person-%d", i+1)),
				Type:      "KNOWS",
			})
			require.NoError(b, err)
		}
		return NewStorageExecutor(store), context.Background()
	}
	for _, q := range queries {
		b.Run("autocommit/"+q.name, func(b *testing.B) {
			exec, ctx := setup(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, q.cypher, map[string]interface{}{"nonce": i}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("explicit_tx/"+q.name, func(b *testing.B) {
			exec, ctx := setup(b)
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, q.cypher, map[string]interface{}{"nonce": i}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
		})
	}
}
