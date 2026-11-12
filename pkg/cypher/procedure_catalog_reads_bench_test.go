package cypher

import (
	"fmt"
	"testing"
)

func BenchmarkCustomProcedureCatalogListing(b *testing.B) {
	for _, size := range []int{0, 128} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			ClearUserProcedures()
			b.Cleanup(ClearUserProcedures)
			exec, _ := newClauseSemanticsBenchmarkExecutor(b)
			for i := 0; i < size; i++ {
				query := fmt.Sprintf("CALL apoc.custom.installProcedure('Bench%d() :: (n :: INTEGER)', 'RETURN 1 AS n')", i)
				if _, err := exec.Execute(b.Context(), query, nil); err != nil {
					b.Fatal(err)
				}
			}
			query := "CALL apoc.custom.list() YIELD name RETURN count(name) AS count"
			result, err := exec.Execute(b.Context(), query, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Rows) != 1 || result.Rows[0][0] != int64(size) {
				b.Fatalf("unexpected catalog result: %#v", result.Rows)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(b.Context(), query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
