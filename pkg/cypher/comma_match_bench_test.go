package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkCommaMatch runs comma-separated node MATCHes whose WHERE selects
// each node by its own conditions over 16 to 256 :P nodes (#940), with the
// equivalent WITH form for comparison. The result cache is off, so every
// query runs.
func BenchmarkCommaMatch(b *testing.B) {
	queries := []struct {
		name  string
		query string
	}{
		{"ID", "MATCH (a),(b) WHERE id(a) = $a AND id(b) = $b RETURN a.k, b.k"},
		{"ElementID", "MATCH (a),(b) WHERE elementId(a) = $ea AND elementId(b) = $eb RETURN a.k, b.k"},
		{"IDList", "MATCH (a),(b) WHERE id(a) IN [$a] AND id(b) IN [$b] RETURN a.k, b.k"},
		{"StartsWith", "MATCH (a:P),(b:P) WHERE a.s STARTS WITH 'first' AND b.s STARTS WITH 'last' RETURN a.k, b.k"},
		{"Function", "MATCH (a:P),(b:P) WHERE toLower(a.s) = 'first' AND toLower(b.s) = 'last' RETURN a.k, b.k"},
		{"WithForm", "MATCH (a) WHERE id(a) = $a WITH a MATCH (b) WHERE id(b) = $b RETURN a.k, b.k"},
	}
	for _, nodes := range []int{16, 128, 256} {
		exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"), 0, 0)
		ctx := context.Background()
		create := fmt.Sprintf("UNWIND range(1, %d) AS i CREATE (:P {k: i, s: CASE i WHEN 1 THEN 'first' WHEN %d THEN 'last' ELSE 'middle' END})", nodes, nodes)
		if _, err := exec.Execute(ctx, create, nil); err != nil {
			b.Fatal(err)
		}
		ids, err := exec.Execute(ctx, fmt.Sprintf("MATCH (a:P {k: 1}), (b:P {k: %d}) RETURN id(a), id(b), elementId(a), elementId(b)", nodes), nil)
		if err != nil {
			b.Fatal(err)
		}
		params := map[string]interface{}{"a": ids.Rows[0][0], "b": ids.Rows[0][1], "ea": ids.Rows[0][2], "eb": ids.Rows[0][3]}
		for _, q := range queries {
			b.Run(fmt.Sprintf("%s/nodes=%d", q.name, nodes), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					result, err := exec.Execute(ctx, q.query, params)
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Rows) != 1 {
						b.Fatalf("got %d rows", len(result.Rows))
					}
				}
			})
		}
	}
}
