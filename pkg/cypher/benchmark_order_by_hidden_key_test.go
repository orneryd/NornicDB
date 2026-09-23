package cypher

// Benchmarks for the ORDER BY hidden-column paths added by issue #500: a
// term that references a bound variable but is not itself a RETURN item now
// gets projected once per row as a hidden column instead of being silently
// dropped. These are new query shapes (no equivalent existed on main before
// the fix), so unlike BenchmarkTraversalOptionalMatch_* and
// BenchmarkIndexedOrderTiedPage there is no meaningful "before" number: the
// pre-fix build returns the wrong rows in less time, which is not a
// comparable baseline. Only the after-fix ns/op and allocs/op are reported.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkTraversalOptionalMatch_OrderByHiddenKey is
// BenchmarkTraversalOptionalMatch_FanOutProjection's shape with a hidden
// ORDER BY key (e.name, unprojected in FanOut's RETURN) plus LIMIT, exercising
// buildTraversalHiddenOrderBy's per-row hidden-column projection and sort.
func BenchmarkTraversalOptionalMatch_OrderByHiddenKey(b *testing.B) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "bench")
	exec := NewStorageExecutor(store)
	// Disable the query result cache so every iteration measures real
	// routing, join, projection, and sort work instead of a cache hit.
	exec.cache = nil
	ctx := context.Background()
	seedTraversalOptionalBenchGraph(b, exec, ctx, 50)

	query := `
		MATCH (e:BenchClass)-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:BenchFile)
		RETURN type(rel) AS type,
		       coalesce(target.id, target.uid) AS target_id,
		       target.name AS target_name,
		       tf.relative_path AS target_file
		ORDER BY e.name, coalesce(target.id, target.uid) DESC
		LIMIT 20`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) == 0 {
			b.Fatal("expected rows")
		}
	}
}

// seedRelationshipOrderByBenchGraph creates count (a)-[:LINKS]->(b) pairs, b
// carrying a name and id that are not returned by the benchmark query below,
// so ORDER BY b.name, b.id must resolve through the relationship-pattern
// hidden-column path (match.go's executeMatchWithRelationshipsWithPath site).
func seedRelationshipOrderByBenchGraph(b *testing.B, store storage.Engine, count int) {
	b.Helper()
	for i := 0; i < count; i++ {
		aID := storage.NodeID(fmt.Sprintf("a-%06d", i))
		bID := storage.NodeID(fmt.Sprintf("b-%06d", i))
		if _, err := store.CreateNode(&storage.Node{
			ID: aID, Labels: []string{"BenchLinkSource"},
			Properties: map[string]interface{}{"id": string(aID)},
		}); err != nil {
			b.Fatalf("seed source node failed: %v", err)
		}
		if _, err := store.CreateNode(&storage.Node{
			ID: bID, Labels: []string{"BenchLinkTarget"},
			// name is reverse-ordered relative to id so the sort does real work.
			Properties: map[string]interface{}{"id": string(bID), "name": fmt.Sprintf("target-%06d", count-i)},
		}); err != nil {
			b.Fatalf("seed target node failed: %v", err)
		}
		if err := store.CreateEdge(&storage.Edge{
			ID: storage.EdgeID(fmt.Sprintf("e-%06d", i)), StartNode: aID, EndNode: bID, Type: "LINKS",
		}); err != nil {
			b.Fatalf("seed edge failed: %v", err)
		}
	}
}

// BenchmarkRelationshipPattern_OrderByHiddenKey runs a relationship-pattern
// MATCH over ~10k rows, ordering by two properties of the traversed (but not
// returned) end node -- match.go's non-DISTINCT, non-aggregate relationship
// pattern site building and sorting on two hidden columns per row.
func BenchmarkRelationshipPattern_OrderByHiddenKey(b *testing.B) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "bench")
	exec := NewStorageExecutor(store)
	exec.cache = nil
	ctx := context.Background()
	seedRelationshipOrderByBenchGraph(b, store, 10000)

	query := `MATCH (a:BenchLinkSource)-[:LINKS]->(bnode:BenchLinkTarget) RETURN a.id AS id ORDER BY bnode.name, bnode.id`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 10000 {
			b.Fatalf("expected 10000 rows, got %d", len(res.Rows))
		}
	}
}
