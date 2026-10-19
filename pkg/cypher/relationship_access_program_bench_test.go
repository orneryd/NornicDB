package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Public planning path; the base deliberately has the dynamic-token display gap.
// Keep this fixture identical on both sides of the paired comparison.
func BenchmarkRelationshipAccessPlanning(b *testing.B) {
	store := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = store.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "planning_bench"))
	ctx := context.Background()
	for _, q := range []string{"CREATE ()-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			b.Fatal(err)
		}
	}
	for _, tc := range []struct{ name, query string }{
		{"Match", "EXPLAIN WITH $x AS k MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p"},
		{"Expression", "EXPLAIN WITH $x AS k RETURN COLLECT { MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p }"},
		{"DynamicToken", "EXPLAIN WITH $x AS k CREATE (n:$(head(COLLECT { MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p }))) RETURN labels(n)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			params := map[string]interface{}{"x": "x"}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, tc.query, params)
				if err != nil || result == nil || result.Metadata["plan"] == nil {
					b.Fatalf("plan: %v, %v", result, err)
				}
			}
		})
	}
}
