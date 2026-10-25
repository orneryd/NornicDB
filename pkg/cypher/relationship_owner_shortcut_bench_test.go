package cypher

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkRelationshipOwnerControls(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"unquoted_binding", "MATCH ()-[is]->() RETURN is"},
		{"quoted_binding", "MATCH ()-[`r r`:R]->() RETURN type(`r r`)"},
		{"literal_binding", "MATCH ()-[true]->() RETURN type(`true`)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				canonicalizeQuotedVariables(context.Background(), tc.query)
			}
		})
	}
	b.Run("lookup_plain", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			parseCreateLookupIndex("CREATE LOOKUP INDEX ix FOR ()-[r]-() ON EACH type(r)")
		}
	})
	b.Run("literal_binding_uncached", func(b *testing.B) {
		prefix := "MATCH ()-[true]->() RETURN type(\x60true\x60) AS result_" + strconv.Itoa(b.N) + "_"
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			canonicalizeQuotedVariables(context.Background(), prefix+strconv.Itoa(i))
		}
	})
	b.Run("degree_cached", func(b *testing.B) {
		parseBoundDegreeShape("(a)-[:R]->()")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			parseBoundDegreeShape("(a)-[:R]->()")
		}
	})
	b.Run("row_shape_uncached", func(b *testing.B) {
		// Different prefixes for calibration runs keep every measured lookup
		// cold; both binaries use exactly this fixture and query family.
		prefix := "COUNT { (a)-[ in :\x60A|B\x60|R]->() } + " + strconv.Itoa(b.N) + " + "
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !rowContextShapeOf(prefix + strconv.Itoa(i)).pattern {
				b.Fatal("expected a graph expression")
			}
		}
	})
}
