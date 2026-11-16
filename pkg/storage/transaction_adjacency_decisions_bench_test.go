package storage

import (
	"fmt"
	"testing"
)

// BenchmarkEmptyAdjacencyDecisionAccumulation isolates runtime dependency
// bookkeeping for one hub and distinct far endpoints. No graph data is stored.
func BenchmarkEmptyAdjacencyDecisionAccumulation(b *testing.B) {
	for _, size := range []int{1024, 8192} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			requests := make([]AdjacentEdgeRead, size)
			for i := range requests {
				requests[i] = AdjacentEdgeRead{Node: "test:hub", Other: NodeID(fmt.Sprint("test:leaf-", i)), Types: []string{"LINK"}, ReadPolicy: CurrentEdgeMatch}
			}
			b.ReportAllocs()
			ctx := b.Context()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx := &BadgerTransaction{}
				for _, request := range requests {
					if err := tx.observeEmptyAdjacencyLocked(ctx, request); err != nil {
						b.Fatal(err)
					}
				}
				if len(tx.entityLocks.absent) != size {
					b.Fatal("lost dependency")
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/range")
		})
	}
}
