package search

import (
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkSearchMutationReservation covers the hot reservation path a live
// write pays before its index application: one identity reservation, the
// retirement check the refresh makes before reading storage, and release.
// Both identity kinds are measured, since they now share one implementation.
func BenchmarkSearchMutationReservation(b *testing.B) {
	svc := &Service{}
	for _, kind := range []string{"node", "relationship"} {
		b.Run(kind, func(b *testing.B) {
			var counter atomic.Uint64
			ids := make([]string, 64)
			for index := range ids {
				ids[index] = string(rune('a' + index%26))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				id := ids[counter.Add(1)%uint64(len(ids))]
				if kind == "node" {
					mutation := svc.ReserveNodeMutation(storage.NodeID(id), storage.CaptureVectorWorkOrigin())
					if !mutation.mutation.retire() {
						mutation.mutation.discard()
					}
					continue
				}
				mutation := svc.ReserveEdgeMutation(storage.EdgeID(id), storage.CaptureVectorWorkOrigin())
				if !mutation.mutation.retire() {
					mutation.mutation.discard()
				}
			}
		})
	}
}
