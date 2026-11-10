package storage

import (
	"fmt"
	"testing"
)

var valueCopyNodeSink *Node

// BenchmarkNativeEntityCallerCopy guards the already-correct native copy path
// while result holders begin sharing its value owner.
func BenchmarkNativeEntityCallerCopy(b *testing.B) {
	for _, shape := range []string{"flat", "nested"} {
		for _, count := range []int{8, 64} {
			b.Run(fmt.Sprintf("%s/%d", shape, count), func(b *testing.B) {
				node := &Node{ID: "copy:node", Labels: []string{"Doc"}, Properties: make(map[string]any, count)}
				for i := range count {
					var value any = int64(i)
					if shape == "nested" {
						value = map[string]any{"tags": []any{"a", "b"}, "rank": int64(i)}
					}
					node.Properties[fmt.Sprintf("p%d", i)] = value
				}
				b.ReportAllocs()
				for b.Loop() {
					valueCopyNodeSink = CopyNode(node)
				}
			})
		}
	}
}
