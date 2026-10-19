package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Exercise one complete worker batch, including persistence on both versions.
// Cancellation from the last provider result ends the drain, then the worker
// synchronously saves its batch before returning. No count polling is timed.
func BenchmarkRelationshipEmbeddingBatch(b *testing.B) {
	for _, persist := range []bool{false, true} {
		for _, chunks := range []int{1, 4} {
			b.Run(fmt.Sprintf("persist=%t/chunks=%d", persist, chunks), func(b *testing.B) {
				engine := storage.NewMemoryEngine()
				defer engine.Close()
				svc := NewServiceWithDimensions(engine, 128)
				defer svc.Close()
				svc.SetPersistenceEnabled(persist)
				if persist {
					svc.SetFulltextIndexPath(filepath.Join(b.TempDir(), "bm25"))
				}
				result := RelationshipEmbedding{}
				for chunk := 0; chunk < chunks; chunk++ {
					vector := make([]float32, 128)
					vector[chunk] = 1
					result.Vectors = append(result.Vectors, vector)
					result.Spans = append(result.Spans, embeddingutil.ChunkSpan{Chunk: chunk, Property: "reason", Start: chunk, End: chunk + 1})
				}
				var cancel context.CancelFunc
				remaining := 0
				svc.EnableRelationshipSearch(func(context.Context, *storage.Edge) (RelationshipEmbedding, error) {
					remaining--
					if remaining == 0 {
						cancel()
					}
					return result, nil
				})
				edges := make([]*storage.Edge, 32)
				for i := range edges {
					edges[i] = &storage.Edge{ID: storage.EdgeID(fmt.Sprint(i)), Type: "CITES", Properties: map[string]any{"reason": "bridge over the river"}}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					ctx, stop := context.WithCancel(context.Background())
					cancel, remaining = stop, len(edges)
					for _, edge := range edges {
						edge.Properties["version"] = iteration
						svc.IndexRelationship(edge)
					}
					svc.embedRelationships(ctx, svc.relationshipSearch())
					stop()
				}
			})
		}
	}
}
