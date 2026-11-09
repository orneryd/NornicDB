package fabric

import (
	"fmt"
	"strings"
	"testing"
)

// BenchmarkBatchedProjectionRead measures the complete consumed projection:
// typed items plus aliases. Earlier reader-only results omit alias work and
// are not a directly comparable baseline for this contract.
func BenchmarkBatchedProjectionRead(b *testing.B) {
	for _, size := range []int{1, 8, 64} {
		properties := make([]string, size)
		counts := make([]string, size)
		for i := range properties {
			properties[i] = fmt.Sprintf("n.p%d AS value%d", i, i)
			counts[i] = fmt.Sprintf("count(n.p%d) AS value%d", i, i)
		}
		for _, count := range []bool{false, true} {
			name, source := "property", strings.Join(properties, ", ")
			if count {
				name, source = "count", strings.Join(counts, ", ")
			}
			shape := applyLookupShapeRows
			if count {
				shape = applyLookupShapeCount
			}
			b.Run(fmt.Sprintf("%s/%d", name, size), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					facts, ok := readApplyLookupProjection(shape, source, "n")
					items := len(facts.returnItems)
					if count {
						items = len(facts.countItems)
					}
					if !ok || items != size || len(facts.aliases) != size {
						b.Fatal("complete typed projection and aliases declined")
					}
				}
			})
		}
	}
}
