package label_test

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/label"
)

var normalizedLabelSink string

func BenchmarkNormalizeCodepoints(b *testing.B) {
	for _, tc := range []struct{ name, input string }{
		{"ascii", "person"}, {"unchanged", "Person"}, {"unicode", "élan"},
		{"supplementary", "𐐨a"}, {"admitted-unit", "\xed\xa0\x80élan"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				normalizedLabelSink = label.Normalize(tc.input)
			}
		})
	}
}
