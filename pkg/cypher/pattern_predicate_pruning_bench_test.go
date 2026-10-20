package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkPatternPredicatePruning(b *testing.B) {
	for _, predicate := range []struct{ name, text string }{
		{"equality", "a.id = 0"},
		{"not-null", "a.id IS NOT NULL"},
		{"conjunction", "a.id >= 0 AND a.id < 1"},
		{"graph-predicate", "(a)-[:T {w:1}]->()"},
	} {
		b.Run(predicate.name, func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "predicate_pruning"))
			exec.cache = nil
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (a:PredicateSource {id:0})-[:T {w:1}]->(:PredicateTarget), (a)-[:T {w:1}]->(:PredicateTarget), (a)-[:T {w:1}]->(:PredicateTarget)", nil)
			if err != nil {
				b.Fatal(err)
			}
			query := "MATCH (a:PredicateSource)-[r:T]->() WHERE " + predicate.text + " RETURN count(r)"
			for i := 0; i < 10; i++ {
				if _, err := exec.Execute(ctx, query, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, query, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0][0] != int64(3) {
					b.Fatalf("expected three matches, got %v", result.Rows)
				}
			}
		})
	}
}
