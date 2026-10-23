package cypher

import (
	"fmt"
	"strings"
	"testing"
)

// This fixture uses reader APIs present on the preceding implementation too.
// Multiple comments and scalar subqueries expose repeated fact-prefix walks
// that a single leading-comment benchmark cannot detect.
func functionFactConsumerInput(count int, kind string) string {
	var text strings.Builder
	text.WriteString("RETURN [")
	for index := 0; index < count; index++ {
		if index > 0 {
			text.WriteByte(',')
		}
		switch kind {
		case "block":
			text.WriteString("coalesce(/* don't range('bad'), ( */ null,1)")
		case "line":
			text.WriteString("coalesce(// don't range('bad'), (\n null,1)")
		case "subqueries":
			text.WriteString("COUNT { RETURN sum(1) AS n }")
		}
	}
	text.WriteString("] AS v")
	return text.String()
}

func BenchmarkFunctionSyntaxFactConsumers(b *testing.B) {
	for _, count := range []int{2000, 4000} {
		for _, kind := range []string{"block", "line", "subqueries"} {
			text := functionFactConsumerInput(count, kind)
			b.Run(fmt.Sprintf("calls=%d/%s", count, kind), func(b *testing.B) {
				if kind == "subqueries" {
					if containsAggregateFunc(text) {
						b.Fatal("an inner aggregate must not escape its scalar subquery")
					}
				} else if got := namedFunctionArguments(text, "coalesce"); len(got) != count {
					b.Fatalf("arguments=%d, want %d", len(got), count)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					if kind == "subqueries" {
						if containsAggregateFunc(text) {
							b.Fatal("inner aggregate escaped")
						}
					} else if len(namedFunctionArguments(text, "coalesce")) != count {
						b.Fatal("missing argument spans")
					}
				}
			})
		}
	}
}
