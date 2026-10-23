package cypher

import "testing"

// These inputs have the same successful classification on the unfixed and
// corrected sources. Commented bug cases are measured by correctness tests.
func BenchmarkInnerAggregateBoundary(b *testing.B) {
	for _, tc := range []struct {
		expr string
		want int
	}{
		{"COUNT { RETURN count(*) AS c }", len("COUNT { RETURN count(*) AS c }")},
		{"EXISTS { RETURN count(*) AS c }", len("EXISTS { RETURN count(*) AS c }")},
		{"COLLECT { RETURN count(*) AS c }", len("COLLECT { RETURN count(*) AS c }")},
		{"CASE WHEN n THEN count(*) ELSE 0 END", -1},
	} {
		b.Run(tc.expr, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := subqueryExpressionEnd(tc.expr, 0); got != tc.want {
					b.Fatalf("boundary changed: got%d want%d", got, tc.want)
				}
			}
		})
	}
}
