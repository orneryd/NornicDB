package cypher

import (
	"fmt"
	"strings"
	"testing"
)

// Runnable unchanged on the approved head: the helper API is unchanged, so
// the A/B includes the uncached long-text adapter cost.
func BenchmarkFunctionSyntaxLongDeletedReads(b *testing.B) {
	for _, count := range []int{600, 1200} {
		text := strings.Repeat("/* don't range('bad'), ( */ 0 + ", count) + "labels(n) + n.name"
		b.Run(fmt.Sprintf("comments=%d", count), func(b *testing.B) {
			if len(deletedEntityReadsIn(text)) != 2 {
				b.Fatal("unexpected reads")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if len(deletedEntityReadsIn(text)) != 2 {
					b.Fatal("unexpected reads")
				}
			}
		})
	}
}
