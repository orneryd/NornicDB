package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Identical on both sides: public planning plus uncached anchored execution.
func BenchmarkRelationshipAccessReaders(b *testing.B) {
	base := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "access_readers_bench")
	if _, err := store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Key"}}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		id := storage.NodeID(fmt.Sprintf("b%d", i))
		if _, err := store.CreateNode(&storage.Node{ID: id}); err != nil {
			b.Fatal(err)
		}
		value := "other"
		if i == 0 {
			value = "x"
		}
		if err := store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: "a", EndNode: id, Type: "T", Properties: map[string]interface{}{"p": value}}); err != nil {
			b.Fatal(err)
		}
	}
	e := NewStorageExecutor(store)
	e.cache = nil
	ctx := context.Background()
	if _, err := e.Execute(ctx, "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)", nil); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, query string }{
		{"AnchoredCount", "MATCH (a:Key) RETURN COUNT { MATCH (a)-[r:T]->() WHERE r.p='x' }"},
		{"AnchoredExists", "MATCH (a:Key) RETURN EXISTS { MATCH (a)-[r:T]->() WHERE r.p='x' }"},
		{"ExplainAnchored", "EXPLAIN MATCH (a:Key) RETURN COUNT { MATCH (a)-[r:T]->() WHERE r.p='x' }"},
		{"ExplainAllToken", "EXPLAIN CREATE (n:$all(head(COLLECT { MATCH ()-[r:T]->() WHERE r.p='x' RETURN r.p }))) RETURN labels(n)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := e.Execute(ctx, tc.query, nil)
				if err != nil || result == nil {
					b.Fatalf("execute: %v, %v", result, err)
				}
			}
		})
	}
}
