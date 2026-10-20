package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The baseline has the known global-sum bug. This fixture is identical on
// base/head and measures the production read chain without result-cache hits;
// correctness belongs to TestTypedCountNamespaceLifecycle's expected rows.
func BenchmarkTypedCountNamespace(b *testing.B) {
	base := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = base.Close() })
	for i := 0; i < 16; i++ {
		view := storage.NewNamespacedEngine(base, fmt.Sprintf("bench%d", i))
		for _, id := range []storage.NodeID{"start", "end"} {
			if _, err := view.CreateNode(&storage.Node{ID: id, Labels: []string{"Probe"}}); err != nil {
				b.Fatal(err)
			}
		}
		for j := 0; j < 8; j++ {
			if err := view.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprint(j)), StartNode: "start", EndNode: "end", Type: "T"}); err != nil {
				b.Fatal(err)
			}
		}
	}
	for _, shape := range []struct{ name, pattern string }{
		{"type", "()-[r:T]->()"},
		{"start", "(:Probe)-[r:T]->()"},
		{"end", "()-[r:T]->(:Probe)"},
		{"untyped", "()-[r]->()"},
	} {
		b.Run(shape.name, func(b *testing.B) {
			engine := storage.NewNamespacedEngine(storage.NewTracedEngine(storage.NewWALEngine(base, nil)), "bench0")
			exec := NewStorageExecutor(engine)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(context.Background(), fmt.Sprintf("MATCH %s RETURN count(r) AS c%d", shape.pattern, i), nil)
				if err != nil || len(result.Rows) != 1 {
					b.Fatalf("count: %v %v", result, err)
				}
			}
		})
	}
}
