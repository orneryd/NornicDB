package cypher

import (
	"context"
	"reflect"
	"testing"
)

// BenchmarkUnscopedCallImplicitImport measures an unscoped CALL body that
// imports the outer p implicitly, run once per outer row, with and without
// a nested EXISTS whose names are not imports (#1317 S08).
func BenchmarkUnscopedCallImplicitImport(b *testing.B) {
	exec := callPlanBenchExecutor(b)
	ctx := context.Background()
	for _, tc := range []struct{ name, query string }{
		{"plain", "MATCH (p:P) CALL { MATCH (p)-[r:R]->(q:Q) WHERE r.w >= p.m RETURN count(q) AS c } RETURN sum(c) AS total"},
		{"nested_exists", "MATCH (p:P) CALL { MATCH (p)-[r:R]->(q:Q) WHERE EXISTS { (q)<-[:R]-(:P) } RETURN count(q) AS c } RETURN sum(c) AS total"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			want, err := exec.Execute(ctx, tc.query, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
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
