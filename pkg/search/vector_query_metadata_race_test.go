package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Vector queries read the node and relationship metadata while nodes are
// indexed: both hold indexMu, the lock index writes take, so a query never
// reads a map being written (a concurrent map read and write stopped the
// process).
func TestVectorQueriesWhileNodesAreIndexed(t *testing.T) {
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	svc := NewServiceWithDimensions(engine, 3)
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "n0", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}))

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				node := &storage.Node{
					ID:              storage.NodeID(fmt.Sprintf("n%d-%d", w, i)),
					Labels:          []string{"Doc"},
					Properties:      map[string]any{"v": []float32{0, 1, 0}},
					ChunkEmbeddings: [][]float32{{1, float32(i), 0}, {0, 1, float32(w)}},
				}
				_ = svc.IndexNode(node)
			}
		}(w)
	}
	for q := 0; q < 2; q++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, _ = svc.VectorQueryNodes(context.Background(), []float32{1, 0, 0}, VectorQuerySpec{Limit: 50})
				_, _ = svc.VectorQueryNodes(context.Background(), []float32{1, 0, 0}, VectorQuerySpec{Limit: 50, Label: "Doc", Property: "v", Similarity: "euclidean"})
				_ = svc.HasRelationshipVectorEntries("", "v")
			}
		}()
	}
	wg.Wait()
}
