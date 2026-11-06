package cypher

import (
	"runtime"
	"testing"
)

// BenchmarkBoundDegreeShapeParsing separates cold degree-shape preparation
// from the cached per-row lookup used by COUNT and EXISTS.
func BenchmarkBoundDegreeShapeParsing(b *testing.B) {
	for _, tc := range []struct{ name, body string }{
		{"ordinary", "(n)-[:R]->()"},
		{"alternation", "(n)-[:R|S]->()"},
		{"quoted_pipe", "(n)-[:`R|S`]->()"},
		{"escaped_backtick", "(n)-[:`r``s`]->()"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("cold", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					shape := parseBoundDegreeShapeText(tc.body)
					runtime.KeepAlive(shape)
				}
			})
			b.Run("cached", func(b *testing.B) {
				parseBoundDegreeShape(tc.body)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					shape := parseBoundDegreeShape(tc.body)
					runtime.KeepAlive(shape)
				}
			})
		})
	}
}
