package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func BenchmarkNativeStatementOwner(b *testing.B) {
	for _, work := range []struct{ name, query string }{{"read", "MATCH (n:OwnerBench) RETURN n.value"}, {"dynamic-read", "CALL apoc.cypher.run('MATCH (n:OwnerBench) RETURN n.value',{}) YIELD value RETURN value"}, {"noop-write", "MATCH (n:OwnerAbsent) SET n.value = 1"}, {"dynamic-noop-write", "CALL apoc.cypher.doIt('MATCH (n:OwnerAbsent) SET n.value = 1',{}) YIELD value RETURN value"}} {
		b.Run(work.name, func(b *testing.B) {
			root := newTestMemoryEngine(b)
			store := storage.NewNamespacedEngine(root, "bench")
			_, err := store.CreateNode(b.Context(), &storage.Node{ID: "bench", Labels: []string{"OwnerBench"}, Properties: map[string]any{"value": int64(1)}})
			if err != nil {
				b.Fatal(err)
			}
			executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			ctx := context.Background()
			if _, err := executor.Execute(ctx, work.query, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := executor.Execute(ctx, work.query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
