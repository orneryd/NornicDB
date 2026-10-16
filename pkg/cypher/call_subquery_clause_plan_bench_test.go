package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// callPlanBenchExecutor: 400 :P nodes, each with a :R to 4 of 40 :Q nodes
// (w = 0..3 on the relationship, k = 0..3 on the target).
func callPlanBenchExecutor(tb testing.TB) *StorageExecutor {
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
	return NewStorageExecutor(store)
}

// callPlanBenchQueries are per-row correlated CALL subqueries: the outer
// MATCH streams 400 rows and the body runs once for each.
var callPlanBenchQueries = []struct{ name, query string }{
	// A traversal whose WHERE mixes conjuncts on the pattern's own variables
	// (pushed into the traversal) with one on the imported p.
	{"traversal_pushdown", "MATCH (p:P) CALL { WITH p MATCH (p)-[r:R]->(q:Q) WHERE q.k <> 3 AND coalesce(q.name, 'callplan-once') <> 'none' AND r.w >= p.m AND (q.k = 0 OR q.k = 1 OR q.k = 2) RETURN count(q) AS c } RETURN sum(c) AS total"},
	{"scoped_traversal_pushdown", "MATCH (p:P) CALL (p) { MATCH (p)-[r:R]->(q:Q) WHERE q.k <> 3 AND coalesce(q.name, 'callplan-once') <> 'none' AND r.w >= p.m RETURN count(q) AS c } RETURN sum(c) AS total"},
	// OPTIONAL MATCH of a quantified path with an inner WHERE.
	{"optional_quantified", "MATCH (p:P) CALL (p) { OPTIONAL MATCH (p)((a)-[r:R]->(b) WHERE r.w >= 0 AND b.k IN [0, 1, 2] AND coalesce(b.name, 'callplan-once') <> 'none')+(q:Q) RETURN count(q) AS c } RETURN sum(c) AS total"},
	// Projection, grouping and ordering inside the body.
	{"with_grouping", "MATCH (p:P) CALL { WITH p MATCH (p)-[r:R]->(q:Q) WHERE coalesce(q.name, 'callplan-once') <> 'none' WITH q.k AS k, count(*) AS n ORDER BY k RETURN collect(k) AS ks, sum(n) AS n } RETURN sum(n) AS total, count(ks) AS lists"},
}

// BenchmarkCallSubqueryClausePlan measures a correlated CALL subquery run
// once per outer row: the body's clause texts are the same for every row.
func BenchmarkCallSubqueryClausePlan(b *testing.B) {
	exec := callPlanBenchExecutor(b)
	ctx := context.Background()
	for _, tc := range callPlanBenchQueries {
		b.Run(tc.name, func(b *testing.B) {
			want, err := exec.Execute(ctx, tc.query, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Each run executes the statement: no cached result.
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
