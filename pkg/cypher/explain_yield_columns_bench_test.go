package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkExplainYieldColumns(b *testing.B) {
	for _, query := range []struct{ name, text string }{
		{"with", "EXPLAIN WITH 'row' AS value CALL db.labels() YIELD label RETURN *"},
		{"two_calls", "EXPLAIN WITH 'row' AS value CALL db.labels() YIELD label CALL db.labels() YIELD label AS other RETURN *"},
		{"projected", "EXPLAIN WITH 'row' AS value CALL db.labels() YIELD label WITH value AS kept RETURN *, 1 AS extra"},
	} {
		b.Run(query.name, func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "explain-yield"))
			ctx := context.Background()
			if _, err := exec.Execute(ctx, query.text, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, query.text, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
