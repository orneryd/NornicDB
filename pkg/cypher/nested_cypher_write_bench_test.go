package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkNestedCypherRunWrite(b *testing.B) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "nested_bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	// Delete the created node within the same body to bound retained graph size.
	// Both mutation summaries still pass through the plain dynamic CALL boundary.
	params := map[string]any{"body": "CREATE (n:NestedBench) DELETE n RETURN 1 AS value"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", params); err != nil {
			b.Fatal(err)
		}
	}
}
