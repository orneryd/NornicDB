package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkSearchNodeMutationOrdering(b *testing.B) {
	engine := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer engine.Close()
	svc := NewServiceWithDimensions(engine, 2)
	defer svc.Close()
	node := &storage.Node{ID: "source", Properties: map[string]any{"content": "bravo"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := svc.IndexNode(node); err != nil {
			b.Fatal(err)
		}
		if err := svc.RemoveNode(node.ID); err != nil {
			b.Fatal(err)
		}
	}
}
