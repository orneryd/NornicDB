package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkQueryStatisticsCollection times one simple statement with query
// collection on (the default, as in Neo4j) and stopped.
func BenchmarkQueryStatisticsCollection(b *testing.B) {
	for _, collecting := range []bool{true, false} {
		name := "collecting"
		if !collecting {
			name = "stopped"
		}
		b.Run(name, func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "bench"))
			ctx := context.Background()
			if !collecting {
				if _, err := exec.Execute(ctx, "CALL db.stats.stop('QUERIES')", nil); err != nil {
					b.Fatal(err)
				}
			}
			params := map[string]interface{}{"value": int64(1)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, "RETURN $value AS value", params); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(name+"/parallel", func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "bench"))
			ctx := context.Background()
			if !collecting {
				if _, err := exec.Execute(ctx, "CALL db.stats.stop('QUERIES')", nil); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				params := map[string]interface{}{"value": int64(1)}
				for pb.Next() {
					if _, err := exec.Execute(ctx, "RETURN $value AS value", params); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
