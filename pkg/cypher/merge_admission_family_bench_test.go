package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Measure the public explicit-transaction route, including admission on every
// transaction. Rollback keeps the create workloads at a fixed cardinality.
func BenchmarkMergeAdmissionFamily(b *testing.B) {
	for _, shape := range []struct{ name, constraint, query string }{
		{"scalar_node", "k.id IS UNIQUE", "MERGE (k:BenchAdmission {tenant:7,id:$id}) RETURN k.id"},
		{"scalar_path", "k.id IS UNIQUE", "MERGE (k:BenchAdmission {tenant:7,id:$id})-[:LINK]->(:BenchOther) RETURN k.id"},
		{"scalar_two_hops", "k.id IS UNIQUE", "MERGE (k:BenchAdmission {tenant:7,id:$id})-[:LINK]->(:BenchMid)-[:LINK]->(:BenchEnd) RETURN k.id"},
		{"tuple_node", "(k.tenant,k.id) IS UNIQUE", "MERGE (k:BenchAdmission {tenant:7,id:$id}) RETURN k.id"},
		{"tuple_path", "(k.tenant,k.id) IS UNIQUE", "MERGE (k:BenchAdmission {tenant:7,id:$id})-[:LINK]->(:BenchOther) RETURN k.id"},
		{"node_key_node", "(k.tenant,k.id) IS NODE KEY", "MERGE (k:BenchAdmission {tenant:7,id:$id}) RETURN k.id"},
		{"node_key_path", "(k.tenant,k.id) IS NODE KEY", "MERGE (k:BenchAdmission {tenant:7,id:$id})-[:LINK]->(:BenchOther) RETURN k.id"},
		{"unconstrained_path", "", "MERGE (k:BenchAdmission {tenant:7,id:$id})-[:LINK]->(:BenchOther) RETURN k.id"},
	} {
		for _, mode := range []struct {
			name string
			id   int64
		}{{"match", 42}, {"create", 43}} {
			b.Run(shape.name+"/"+mode.name, func(b *testing.B) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "test")
				exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
				ctx := context.Background()
				run := func(query string, params map[string]interface{}) {
					if _, err := exec.Execute(ctx, query, params); err != nil {
						b.Fatal(err)
					}
				}
				if shape.constraint != "" {
					run("CREATE CONSTRAINT bench_admission FOR (k:BenchAdmission) REQUIRE "+shape.constraint, nil)
				}
				run(shape.query, map[string]interface{}{"id": int64(42)})
				params := map[string]interface{}{"id": mode.id}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run("BEGIN", nil)
					run(shape.query, params)
					run("ROLLBACK", nil)
				}
			})
		}
	}
}
