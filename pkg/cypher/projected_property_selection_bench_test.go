package cypher

import (
	"slices"
	"testing"
)

// These selector workloads return the same fields on the pre-fix release. The
// quoted control uses a colon, avoiding an invalid dotted-name baseline result.
func BenchmarkProjectedPropertySelection(b *testing.B) {
	for _, tc := range []struct {
		name, expression string
		want             []string
	}{
		{"ordinary", "n.id = 'value' AND n.active = true", []string{"active", "id"}},
		{"quoted", "n.`external:id` = 'value'", []string{"external:id"}},
		{"nested", "n.profile.id", []string{"profile"}},
		{"eight_fields", "n.a + n.b + n.c + n.d + n.e + n.f + n.g + n.h", []string{"a", "b", "c", "d", "e", "f", "g", "h"}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			got := pipelineNodePredicateProperties("n", tc.expression, nil)
			if !slices.Equal(tc.want, got) {
				b.Fatalf("wrong projected fields: want %q, got %q", tc.want, got)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got = pipelineNodePredicateProperties("n", tc.expression, nil)
			}
			b.StopTimer()
			if !slices.Equal(tc.want, got) {
				b.Fatalf("wrong projected fields: want %q, got %q", tc.want, got)
			}
		})
	}
}
