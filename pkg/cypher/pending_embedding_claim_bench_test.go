package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkPendingEmbeddingClaim uses only the public storage API so the same
// fixture can run unchanged against the product baseline. Setup retains marker
// tombstones in the memtable; startup/recovery cost is outside the timed drain.
func BenchmarkPendingEmbeddingClaim(b *testing.B) {
	for _, deleted := range []int{0, 10000} {
		for _, pending := range []int{1, 128, 1024} {
			b.Run(fmt.Sprintf("deleted=%d/pending=%d", deleted, pending), func(b *testing.B) {
				e, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngine(b.TempDir()))
				if err != nil {
					b.Fatal(err)
				}
				defer e.Close()
				e.SetEmbeddingsEnabled(true)
				for i := 0; i < deleted; i++ {
					id := storage.NodeID(fmt.Sprintf("test:deleted-%06d", i))
					e.AddToPendingEmbeddingsWithOrigin(id, storage.CaptureVectorWorkOrigin())
					e.MarkNodeEmbedded(id)
				}
				for i := 0; i < pending; i++ {
					_, err := e.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:candidate-%06d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"content": fmt.Sprintf("document %d", i)}})
					if err != nil {
						b.Fatal(err)
					}
				}
				if e.PendingEmbeddingsCount() != pending {
					b.Fatal("invalid pending fixture")
				}
				if e.FindNodeNeedingEmbedding() == nil {
					b.Fatal("missing startup work")
				}
				completed := make([]storage.NodeID, 0, pending)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					node := e.FindNodeNeedingEmbedding()
					if node == nil {
						b.Fatal("lost pending work")
					}
					e.MarkNodeEmbedded(node.ID)
					completed = append(completed, node.ID)
					if len(completed) == pending {
						for _, id := range completed {
							e.AddToPendingEmbeddingsWithOrigin(id, storage.CaptureVectorWorkOrigin())
						}
						completed = completed[:0]
					}
				}
				b.StopTimer()
			})
		}
	}
}
