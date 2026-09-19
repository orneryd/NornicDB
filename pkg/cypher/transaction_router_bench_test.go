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
	queries := []struct{ name, cypher string }{
		{"simple_match_limit", "MATCH (n:Person) RETURN n.name LIMIT 10"},
		{"property_lookup", "MATCH (n:Person {id: 42}) RETURN n.name"},
		{"filter_count", "MATCH (n:Person) WHERE n.age > 30 RETURN count(n)"},
		{"one_hop_limit", "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 20"},
		{"return_literal", "RETURN 1"},
	}
	setup := func(b *testing.B) (*StorageExecutor, context.Context) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
		exec := NewStorageExecutor(store)
		ctx := context.Background()
		_, err := exec.Execute(ctx, "UNWIND range(0, 1999) AS i CREATE (:Person {id: i, name: 'p' + toString(i), age: i % 60})", nil)
		require.NoError(b, err)
		for i := 0; i < 500; i++ {
			_, err := exec.Execute(ctx, fmt.Sprintf("MATCH (a:Person {id: %d}), (b:Person {id: %d}) CREATE (a)-[:KNOWS]->(b)", i, i+1), nil)
			require.NoError(b, err)
		}
		return exec, ctx
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
