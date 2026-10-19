package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkQueryMemorySelectivePlans(b *testing.B) {
	for _, decoys := range []int{0, 4000, 20000} {
		for _, tc := range []struct{ name, query string }{
			{"label", "MATCH (a:Function)-[r]->(b) RETURN type(r)"},
			{"types", "MATCH ()-[r:QUERIES_TABLE|ALTERNATIVE]->() RETURN type(r)"},
		} {
			b.Run(fmt.Sprintf("%s_%d_decoys", tc.name, decoys), func(b *testing.B) {
				exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"), 0, 0)
				seedSinkProbeGraph(b, exec, decoys)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := exec.Execute(context.Background(), tc.query, nil)
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Rows) != 1 || result.Rows[0][0] != "QUERIES_TABLE" {
						b.Fatal("incorrect selective result", result.Rows)
					}
				}
			})
		}
	}
}
