package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkNOR160ExplicitTxPredicateScan measures a projected label scan with a
// predicate inside an explicit transaction, where every candidate comes from
// the transaction's snapshot cache: none, half or all candidates match.
func BenchmarkNOR160ExplicitTxPredicateScan(b *testing.B) {
	const count = 1000
	for _, shape := range []struct {
		name  string
		query string
		rows  int
	}{
		{"match=none", "MATCH (n:P) WHERE n.g = 2 RETURN n.v AS v", 0},
		{"match=half", "MATCH (n:P) WHERE n.g = 0 RETURN n.v AS v", count / 2},
		{"match=all", "MATCH (n:P) WHERE n.g < 2 RETURN n.v AS v", count},
	} {
		b.Run(shape.name, func(b *testing.B) {
			ctx := context.Background()
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "scan"))
			rows := make([]interface{}, 0, count)
			for index := 0; index < count; index++ {
				rows = append(rows, map[string]interface{}{"g": int64(index % 2), "v": int64(index), "k": fmt.Sprintf("key-%04d", index)})
			}
			if _, err := exec.Execute(ctx, "UNWIND $rows AS row CREATE (n:P) SET n = row", map[string]interface{}{"rows": rows}); err != nil {
				b.Fatal(err)
			}
			if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			run := func() {
				result, err := exec.Execute(ctx, shape.query, nil)
				if err != nil || len(result.Rows) != shape.rows {
					b.Fatalf("rows %v, err %v", result, err)
				}
			}
			run() // fills the transaction's projected label cache
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				run()
			}
		})
	}
}
