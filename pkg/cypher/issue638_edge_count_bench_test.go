package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// issue638_edge_count_bench_test.go benchmarks the issue #638 shapes:
//
//	MATCH ()-[r:T]->() RETURN count(r)        (typed)
//	MATCH ()-[r]->() RETURN count(r)          (untyped)
//	MATCH (s:Label)-[r:T]->() RETURN count(r) (one labeled endpoint)
//
// Each iteration invalidates the edge-type cache and uses a unique statement
// text, matching the issue's probe (no result-cache help). A count must be
// O(1) in the type counter, not a function of store size.

func newIssue638BenchStore(b *testing.B) (*storage.NamespacedEngine, *storage.BadgerEngine, *StorageExecutor) {
	b.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "nornic")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if _, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("f%d", i)),
			Labels: []string{"Function"},
		}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := exec.Execute(ctx, "UNWIND range(0,19) AS i CREATE (:Tick {n:i})-[:TICK]->(:Tick {n:i})", nil); err != nil {
		b.Fatal(err)
	}

	const n = 30000
	callsEdges := make([]*storage.Edge, 0, n)
	linksEdges := make([]*storage.Edge, 0, n)
	calNodes := make([]*storage.Node, 0, n)
	xNodes := make([]*storage.Node, 0, n)
	yNodes := make([]*storage.Node, 0, n)
	for i := 0; i < n; i++ {
		callsEdges = append(callsEdges, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("c%d", i)),
			StartNode: storage.NodeID(fmt.Sprintf("f%d", i%20)),
			EndNode:   storage.NodeID(fmt.Sprintf("cal%d", i)),
			Type:      "CALLS",
		})
		linksEdges = append(linksEdges, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("l%d", i)),
			StartNode: storage.NodeID(fmt.Sprintf("x%d", i)),
			EndNode:   storage.NodeID(fmt.Sprintf("y%d", i)),
			Type:      "LINKS",
		})
		calNodes = append(calNodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("cal%d", i)), Labels: []string{"Callee"}})
		xNodes = append(xNodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("x%d", i))})
		yNodes = append(yNodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("y%d", i))})
	}
	for start := 0; start < n; start += 2000 {
		end := min(start+2000, n)
		if err := store.BulkCreateNodes(calNodes[start:end]); err != nil {
			b.Fatal(err)
		}
		if err := store.BulkCreateNodes(xNodes[start:end]); err != nil {
			b.Fatal(err)
		}
		if err := store.BulkCreateNodes(yNodes[start:end]); err != nil {
			b.Fatal(err)
		}
		if err := store.BulkCreateEdges(callsEdges[start:end]); err != nil {
			b.Fatal(err)
		}
		if err := store.BulkCreateEdges(linksEdges[start:end]); err != nil {
			b.Fatal(err)
		}
	}
	return store, base, exec
}

func issue638Run(b *testing.B, base *storage.BadgerEngine, exec *StorageExecutor, template string) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base.InvalidateEdgeTypeCache()
		q := fmt.Sprintf(template, i)
		result, err := exec.Execute(ctx, q, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 {
			b.Fatalf("unexpected rows: %v", result.Rows)
		}
	}
}

func BenchmarkIssue638_TypedRelCount_FewEdges(b *testing.B) {
	_, base, exec := newIssue638BenchStore(b)
	issue638Run(b, base, exec, "MATCH ()-[r:TICK]->() RETURN count(r) AS c%d")
}

func BenchmarkIssue638_TypedRelCount_ManyEdges(b *testing.B) {
	_, base, exec := newIssue638BenchStore(b)
	issue638Run(b, base, exec, "MATCH ()-[r:CALLS]->() RETURN count(r) AS c%d")
}

func BenchmarkIssue638_UntypedRelCount(b *testing.B) {
	_, base, exec := newIssue638BenchStore(b)
	issue638Run(b, base, exec, "MATCH ()-[r]->() RETURN count(r) AS c%d")
}

func BenchmarkIssue638_StartLabelRelCount(b *testing.B) {
	_, base, exec := newIssue638BenchStore(b)
	issue638Run(b, base, exec, "MATCH (s:Function)-[r:CALLS]->() RETURN count(r) AS c%d")
}
