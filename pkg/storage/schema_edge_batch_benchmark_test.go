package storage

import (
	"fmt"
	"testing"
)

// BenchmarkRegisterEdgeTokens measures token registration for homogeneous and
// mixed batches after their types have been registered.
func BenchmarkRegisterEdgeTokens(b *testing.B) {
	for _, size := range []int{1, 100, 4000} {
		for _, distinct := range []int{1, 8, 100} {
			if distinct > size {
				continue
			}
			b.Run(fmt.Sprintf("n=%d/types=%d", size, distinct), func(b *testing.B) {
				edges := make([]*Edge, size)
				names := make([]string, distinct)
				for i := range names {
					names[i] = fmt.Sprintf("type-%d", i)
				}
				for i := range edges {
					edges[i] = &Edge{Type: names[i%distinct]}
				}
				schema := NewSchemaManager()
				if err := schema.RegisterTokens(nil, names); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := schema.RegisterEdgeTokens(edges); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
