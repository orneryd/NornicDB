package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkUnwindMergeBatch_HopUpsert(b *testing.B) {
	base := newTestMemoryEngine(b)
	store := storage.NewNamespacedEngine(base, "bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	hops := make([]map[string]interface{}, 0, 72)
	for row := 0; row < 12; row++ {
		for depth := 1; depth <= 6; depth++ {
			hops = append(hops, map[string]interface{}{
				"hopId": fmt.Sprintf("benchhop-%03d:%d", row, depth),
				"runID": "bench-deep-hop-v1",
			})
		}
	}

	query := `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, map[string]interface{}{"hops": hops})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		if len(res.Rows) != 1 {
			b.Fatalf("unexpected row count: %d", len(res.Rows))
		}
	}
}
