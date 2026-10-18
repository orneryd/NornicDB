package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Both revisions run identical public Cypher against an ONLINE relationship
// RANGE index. The write benchmark warms the read index first and isolates
// storage mutation maintenance from the query lookup being improved.
func BenchmarkRelationshipPropertyIndex(b *testing.B) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	store := storage.NewNamespacedEngine(base, "bench")
	for i := 0; i < 2048; i++ {
		if _, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprint(i))}); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 5120; i++ {
		typ := "CONTROL"
		if i < 1024 {
			typ = "KEYED"
		}
		if err := store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: storage.NodeID(fmt.Sprint(i % 2048)), EndNode: storage.NodeID(fmt.Sprint((i + 1) % 2048)), Type: typ, Properties: map[string]interface{}{"_ci_key": fmt.Sprint(i)}}); err != nil {
			b.Fatal(err)
		}
	}
	exec := NewStorageExecutor(store)
	ctx := context.WithValue(context.Background(), profileExecutionKey{}, true)
	if _, err := exec.Execute(ctx, "CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r._ci_key)", nil); err != nil {
		b.Fatal(err)
	}
	keys := make([]interface{}, 1000)
	for i := range keys {
		keys[i] = fmt.Sprint(i)
	}
	query := "UNWIND $keys AS k MATCH ()-[r:KEYED {_ci_key:k}]->() RETURN r._ci_key AS key"
	params := map[string]interface{}{"keys": keys}
	if result, err := exec.Execute(ctx, query, params); err != nil || len(result.Rows) != len(keys) {
		b.Fatalf("warmup: result=%v err=%v", result, err)
	}
	b.Run("Lookup1000", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(ctx, query, params)
			if err != nil || len(result.Rows) != len(keys) {
				b.Fatalf("lookup: result=%v err=%v", result, err)
			}
		}
	})
	b.Run("WriteMaintenance", func(b *testing.B) {
		edge, err := store.GetEdge("e0")
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			edge.Properties["_ci_key"] = fmt.Sprintf("write-%d", i&1)
			if err := store.UpdateEdge(edge); err != nil {
				b.Fatal(err)
			}
		}
	})
}
