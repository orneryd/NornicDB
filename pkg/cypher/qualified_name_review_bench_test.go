package cypher

import "testing"

func BenchmarkReviewedQualifiedFunctionParse(b *testing.B) {
	for _, tc := range []struct{ query, name string }{{"range(1,3)", "range"}, {"custom.range('ok')", "custom.range"}, {"custom.path.value('ok')", "custom.path.value"}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				name, _, ok := parseFunctionCallWS(tc.query)
				if !ok || name != tc.name {
					b.Fatal(name, ok)
				}
			}
		})
	}
}
