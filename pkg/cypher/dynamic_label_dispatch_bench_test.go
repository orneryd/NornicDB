package cypher

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Both sides of the paired comparison run these successful dynamic-label
// statements with result caching disabled. PROFILE's formerly failing route is
// covered by the public regression tests rather than timed against an error.
func BenchmarkDynamicLabelDispatch(b *testing.B) {
	for _, workload := range []struct {
		name, query string
		want        int64
	}{
		{"parameter_any", "MATCH (n:$any($labels)) RETURN count(n) AS c", 1000},
		{"row_any", "WITH $labels AS ls MATCH (n:$any(ls)) RETURN count(n) AS c", 1000},
		{"row_all", "WITH $labels AS ls MATCH (n:$all(ls)) RETURN count(n) AS c", 500},
	} {
		b.Run(workload.name, func(b *testing.B) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "dynamic_bench")
			for i := range 1000 {
				labels := []string{"A"}
				if i%2 == 0 {
					labels = append(labels, "B")
				}
				if _, err := store.CreateNode(&storage.Node{ID: storage.NodeID(strconv.Itoa(i)), Labels: labels}); err != nil {
					b.Fatal(err)
				}
			}
			exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
			ctx := context.Background()
			params := map[string]interface{}{"labels": []interface{}{"A", "B"}}
			result, err := exec.Execute(ctx, workload.query, params)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != workload.want {
				b.Fatalf("got %v, want [[%d]]", result.Rows, workload.want)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := exec.Execute(ctx, workload.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
