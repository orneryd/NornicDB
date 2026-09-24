package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkPipelineCreateBound measures the pipeline CREATE step with bound
// endpoints (WITH / UNWIND / multi-row MATCH feeding CREATE).
func BenchmarkPipelineCreateBound(b *testing.B) {
	ctx := context.Background()
	for _, bc := range []struct {
		name  string
		query string
	}{
		{name: "with_create_rel", query: "MATCH (a:BenchPC {id: 1}) WITH a CREATE (a)-[:R]->(:BenchPCX {i: $i})"},
		{name: "unwind_create_rel", query: "UNWIND [1, 2, 3] AS k MATCH (a:BenchPC {id: 1}) CREATE (a)-[:R {k: k}]->(:BenchPCX {i: $i})"},
		{name: "create_path_return", query: "MATCH (a:BenchPC {id: 1}) WITH a CREATE p = (a)-[:R]->(:BenchPCX {i: $i}) RETURN length(p) AS l"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			exec, _ := newTestExecutor(b)
			_, err := exec.Execute(ctx, "CREATE (:BenchPC {id: 1})", nil)
			require.NoError(b, err)
			params := map[string]interface{}{"i": int64(0)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				params["i"] = int64(i)
				if _, err := exec.Execute(ctx, bc.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
