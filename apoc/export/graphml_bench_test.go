package apocexport

import (
	"fmt"
	"testing"
)

var graphMLBenchmarkOutput string

func BenchmarkGraphMLExport(b *testing.B) {
	for _, size := range []int{1, 100} {
		for _, special := range []bool{false, true} {
			b.Run(fmt.Sprintf("nodes%d/special%t", size, special), func(b *testing.B) {
				key, value, kind := "name", "ordinary value", "RELATES"
				if special {
					key = "key\"&\n漢"
					value = "a&b<>\"'\t\n\r漢😀"
					kind = "REL&<>漢"
				}
				nodes := make([]*Node, size)
				rels := make([]*Relationship, size)
				for i := range nodes {
					nodes[i] = &Node{ID: int64(i), Properties: map[string]interface{}{key: value}}
					rels[i] = &Relationship{ID: int64(i), StartNode: int64(i), EndNode: int64((i + 1) % size), Type: kind, Properties: map[string]interface{}{key: value}}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					var err error
					graphMLBenchmarkOutput, err = ToString(nodes, rels, "graphml")
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
