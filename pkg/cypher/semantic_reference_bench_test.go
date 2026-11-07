package cypher

import "testing"

// BenchmarkSemanticReferenceScope measures correct-on-both-version scope
// validation, including typed map keys and free-reference traversal.
func BenchmarkSemanticReferenceScope(b *testing.B) {
	for _, tc := range []struct{ name, clause string }{
		{"entity", "RETURN DISTINCT n ORDER BY n.x"},
		{"properties", "RETURN DISTINCT n.a, n.b, n.c, n.d, n.e, n.f, n.g, n.h ORDER BY n.a,n.b,n.c,n.d,n.e,n.f,n.g,n.h"},
		{"aggregate", "RETURN n.a, count(*) AS c ORDER BY n.a, count(*)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := b.Context()
			if err := validateReturnOrderBySemanticScope(ctx, tc.clause); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := validateReturnOrderBySemanticScope(ctx, tc.clause); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
