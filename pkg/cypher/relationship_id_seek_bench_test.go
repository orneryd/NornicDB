package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkRelationshipIDSeek looks up one relationship by its element ID in
// a graph of 16,000 relationships. On 12 CPUs, 5 runs of 200 lookups:
// main took 1.32–1.52 ms, 1.19 MB and 14,725 allocations per lookup, scanning
// every node; seeded from the relationship, 5.7–6.2 µs, 2 KB and 26.
func BenchmarkRelationshipIDSeek(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench"))
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "UNWIND range(0, 15999) AS i CREATE (:X {id: i})-[:R {id: i}]->(:Y {id: i})", nil); err != nil {
		b.Fatal(err)
	}
	result, err := exec.Execute(ctx, "MATCH (:X {id: 7})-[r:R]->() RETURN elementId(r)", nil)
	if err != nil {
		b.Fatal(err)
	}
	params := map[string]interface{}{"r": result.Rows[0][0]}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, "MATCH ()-[r]->() WHERE elementId(r) = $r RETURN count(r)", params); err != nil {
			b.Fatal(err)
		}
	}
}
