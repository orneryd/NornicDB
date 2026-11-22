package fn

import (
	"strings"
	"testing"
)

var sigmaBenchmarkResult string

func BenchmarkLowerCaseSigma(b *testing.B) {
	cases := []struct{ name, input string }{
		{"ascii8", "abcdefgh"},
		{"ascii128", strings.Repeat("a", 128)},
		{"ascii4096", strings.Repeat("a", 4096)},
		{"shortGreek", "ΑΒΓΔΕΣ"},
		{"boundary127", strings.Repeat("a", 127) + "Σ"},
		{"marks31", "AΣ" + strings.Repeat("\u0301", 31) + "B"},
		{"marks1024", "AΣ" + strings.Repeat("\u0301", 1024) + "B"},
		{"sigmas1024", strings.Repeat("Σ", 1024)},
		{"expansion", "İ" + strings.Repeat("a", 125) + "Σ"},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sigmaBenchmarkResult = LowerCaseText(tc.input)
			}
		})
	}
}
