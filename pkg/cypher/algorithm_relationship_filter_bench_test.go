package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkAlgorithmRelationshipFilters(b *testing.B) {
	base := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "algorithm_bench")
	const size = 32
	for i := 0; i < size; i++ {
		if _, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprint(i)), Labels: []string{"P"}}); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < size; i++ {
		for _, kind := range []string{"K", "L"} {
			step := 1
			if kind == "L" {
				step = 7
			}
			if err := store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("%s%d", kind, i)), StartNode: storage.NodeID(fmt.Sprint(i)), EndNode: storage.NodeID(fmt.Sprint((i+step)%size)), Type: kind}); err != nil {
				b.Fatal(err)
			}
		}
	}
	for _, proc := range []string{"pageRank", "betweenness", "closeness", "louvain", "labelPropagation", "wcc"} {
		for _, filtered := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/filtered=%t", proc, filtered), func(b *testing.B) {
				exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
				args := "['P']"
				if filtered {
					args += ", ['K']"
				}
				query := "CALL apoc.algo." + proc + "(" + args + ") YIELD node RETURN count(node)"
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := exec.Execute(context.Background(), query, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
