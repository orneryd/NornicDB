package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func BenchmarkNOR19IndexedTraversalWindow(b *testing.B) {
	ctx := context.Background()
	base := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(base, "nor19bench")
	e := NewStorageExecutor(store)
	_, err := store.CreateNode(ctx, &storage.Node{ID: "target", Labels: []string{"Q"}})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 250; i++ {
		id := storage.NodeID(fmt.Sprintf("p-%03d", i))
		_, err = store.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"id": int64(i)}})
		if err != nil {
			b.Fatal(err)
		}
		err = store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e-%03d", i)), StartNode: id, EndNode: "target", Type: "R", Properties: map[string]interface{}{"value": int64(1)}})
		if err != nil {
			b.Fatal(err)
		}
	}
	if _, err = e.Execute(ctx, "CREATE INDEX p_id FOR (p:P) ON (p.id)", nil); err != nil {
		b.Fatal(err)
	}
	items := []returnItem{{expr: "p.id"}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, used, err := e.tryExecuteTraversalSeedOrderLimit(ctx, "(p:P)-[:R {value:1}]->(q:Q)", "p.id >= 0", items, "", "p.id ASC", 1, traversalStartSeed)
		if err != nil || !used || len(result.Rows) != 1 {
			b.Fatalf("unexpected result: used=%v err=%v", used, err)
		}
	}
}
