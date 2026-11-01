package storage

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkCompositeSourceProjection covers copying and already-bound inputs.
func BenchmarkCompositeSourceProjection(b *testing.B) {
	for _, size := range []int{8, 4096} {
		for _, bound := range []bool{false, true} {
			nodes := make([]*Node, size)
			edges := make([]*Edge, size)
			for i := range nodes {
				nodes[i] = &Node{ID: NodeID(fmt.Sprint(i))}
				edges[i] = &Edge{ID: EdgeID(fmt.Sprint(i))}
				if bound {
					nodes[i] = nodeFromDatabase(nodes[i], "source")
					edges[i].Database = "source"
				}
			}
			b.Run(fmt.Sprintf("nodes/%d/bound=%t", size, bound), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					result, err := entitiesFromDatabase(context.Background(), nodes, "source")
					if err != nil {
						b.Fatal(err)
					}
					if len(result) != size {
						b.Fatal(len(result))
					}
				}
			})
			b.Run(fmt.Sprintf("edges/%d/bound=%t", size, bound), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					result, err := entitiesFromDatabase(context.Background(), edges, "source")
					if err != nil {
						b.Fatal(err)
					}
					if len(result) != size {
						b.Fatal(len(result))
					}
				}
			})
		}
	}
}
