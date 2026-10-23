package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// existsBenchExecutor: 2,000 :P nodes, every other one with a :R to one of
// 20 :Q nodes, half of which have k = 1.
func existsBenchExecutor(b *testing.B) *StorageExecutor {
	b.Helper()
	base := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "bench")
	nodes := make([]*storage.Node, 0, 2020)
	for i := 0; i < 20; i++ {
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("q%d", i)), Labels: []string{"Q"}, Properties: map[string]interface{}{"k": int64(i % 2)}})
	}
	edges := make([]*storage.Edge, 0, 1000)
	for i := 0; i < 2000; i++ {
		id := storage.NodeID(fmt.Sprintf("p%d", i))
		nodes = append(nodes, &storage.Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"i": int64(i)}})
		if i%2 == 0 {
			edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), Type: "R", StartNode: id, EndNode: storage.NodeID(fmt.Sprintf("q%d", i%20))})
		}
	}
	if err := store.BulkCreateNodes(nodes); err != nil {
		b.Fatal(err)
	}
	if err := store.BulkCreateEdges(edges); err != nil {
		b.Fatal(err)
	}
	return NewStorageExecutor(store)
}

// EXISTS over a correlated pattern, in WHERE and RETURN, the shapes that ran
// on the path matcher before EXISTS shared COUNT's evaluator.
func BenchmarkExistsSubquery(b *testing.B) {
	exec := existsBenchExecutor(b)
	ctx := context.Background()
	for _, tc := range []struct{ name, query string }{
		{"pattern", "MATCH (p:P) WHERE EXISTS { (p)-[:R]->(:Q) } RETURN count(p)"},
		{"pattern_property", "MATCH (p:P) WHERE EXISTS { (p)-[:R]->(:Q {k: 1}) } RETURN count(p)"},
		{"match_where", "MATCH (p:P) WHERE EXISTS { MATCH (p)-[:R]->(q:Q) WHERE q.k = 1 } RETURN count(p)"},
		{"not_exists", "MATCH (p:P) WHERE NOT EXISTS { (p)-[:R]->() } RETURN count(p)"},
		{"in_return", "MATCH (p:P) WHERE p.i < 200 RETURN p.i, EXISTS { (p)-[:R]->(:Q) } AS x"},
		{"two_match", "MATCH (p:P) WHERE p.i < 200 RETURN p.i, EXISTS { MATCH (p)-[:R]->(q) MATCH (q2:Q {k: q.k}) } AS x"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			if _, err := exec.Execute(ctx, tc.query, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Each run evaluates the subqueries: no cached result.
				exec.InvalidateCommittedWriteCaches()
				if _, err := exec.Execute(ctx, tc.query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
