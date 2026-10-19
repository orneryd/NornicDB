package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkUnscopedCallImplicitImport measures an unscoped CALL body that
// imports the outer p implicitly, run once per outer row, with and without
// a nested EXISTS whose names are not imports (#1317 S08).
func BenchmarkUnscopedCallImplicitImport(b *testing.B) {
	exec := callImportBenchExecutor(b)
	ctx := context.Background()
	for _, tc := range []struct{ name, query string }{
		{"plain", "MATCH (p:P) CALL { MATCH (p)-[r:R]->(q:Q) WHERE r.w >= p.m RETURN count(q) AS c } RETURN sum(c) AS total"},
		{"nested_exists", "MATCH (p:P) CALL { MATCH (p)-[r:R]->(q:Q) WHERE EXISTS { (q)<-[:R]-(:P) } RETURN count(q) AS c } RETURN sum(c) AS total"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			want, err := exec.Execute(ctx, tc.query, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exec.InvalidateCommittedWriteCaches()
				got, err := exec.Execute(ctx, tc.query, nil)
				if err != nil {
					b.Fatal(err)
				}
				if !reflect.DeepEqual(got.Rows, want.Rows) {
					b.Fatalf("rows changed: %v, want %v", got.Rows, want.Rows)
				}
			}
		})
	}
}

// callImportBenchExecutor: 400 :P nodes, each with a :R to 4 of 40 :Q nodes
// (w = 0..3 on the relationship, k = 0..3 on the target).
func callImportBenchExecutor(tb testing.TB) *StorageExecutor {
	tb.Helper()
	base := storage.NewMemoryEngine()
	tb.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "callplan")
	nodes := make([]*storage.Node, 0, 440)
	for i := 0; i < 40; i++ {
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("q%d", i)), Labels: []string{"Q"}, Properties: map[string]interface{}{"k": int64(i % 4), "name": fmt.Sprintf("q%d", i)}})
	}
	edges := make([]*storage.Edge, 0, 1600)
	for i := 0; i < 400; i++ {
		id := storage.NodeID(fmt.Sprintf("p%d", i))
		nodes = append(nodes, &storage.Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"i": int64(i), "m": int64(i % 3)}})
		for j := 0; j < 4; j++ {
			edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d_%d", i, j)), Type: "R", StartNode: id, EndNode: storage.NodeID(fmt.Sprintf("q%d", (i+j*7)%40)), Properties: map[string]interface{}{"w": int64(j)}})
		}
	}
	if err := store.BulkCreateNodes(nodes); err != nil {
		tb.Fatal(err)
	}
	if err := store.BulkCreateEdges(edges); err != nil {
		tb.Fatal(err)
	}
	return NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
}
