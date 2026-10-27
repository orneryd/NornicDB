package fabric

import (
	"fmt"
	"strings"
	"testing"
)

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
			b.Run(fmt.Sprintf("%s/%d", name, size), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if count {
						items, ok := readCountReturnItems(source)
						if !ok || len(items) != size {
							b.Fatal("count projection declined")
						}
					} else {
						items, ok := readPropertyReturnItems(source, "n")
						if !ok || len(items) != size {
							b.Fatal("property projection declined")
						}
					}
				}
			})
		}
	}
}
