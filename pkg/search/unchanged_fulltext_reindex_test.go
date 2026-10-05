package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingBM25Index counts the writes reaching the wrapped fulltext index.
type countingBM25Index struct {
	bm25Index
	indexed, removed int
}

func (c *countingBM25Index) Index(id, text string) {
	c.indexed++
	c.bm25Index.Index(id, text)
}

func (c *countingBM25Index) Remove(id string) {
	c.removed++
	c.bm25Index.Remove(id)
}

func newCountingFulltextService(t *testing.T) (*Service, *countingBM25Index) {
	t.Helper()
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	counting := &countingBM25Index{bm25Index: svc.fulltext()}
	svc.setFulltext(counting)
	return svc, counting
}

// A node's default searchable text lists its other properties in key order.
func TestExtractSearchableTextOrdersPropertiesByKey(t *testing.T) {
	svc := NewService(storage.NewMemoryEngine())
	node := &storage.Node{Labels: []string{"V"}, Properties: map[string]any{"zeta": "z", "alpha": int64(1), "mid": true, "content": "body"}}
	for i := 0; i < 20; i++ {
		require.Equal(t, "V body alpha 1 mid true zeta z", svc.extractSearchableText(node))
	}
}

// Re-indexing a node keeps its fulltext document when the text hasn't
// changed, also when its vectors change, and replaces it when the text does.
func TestIndexNodeKeepsUnchangedFulltextDocument(t *testing.T) {
	svc, counting := newCountingFulltextService(t)
	node := func(id int64, embedding []float32) *storage.Node {
		n := &storage.Node{ID: "n1", Labels: []string{"V"}, Properties: map[string]any{"id": id, "x": int64(1)}}
		if embedding != nil {
			n.ChunkEmbeddings = [][]float32{embedding}
		}
		return n
	}

	require.NoError(t, svc.IndexNode(node(1, []float32{1, 0, 0})))
	require.Equal(t, 1, counting.indexed)
	removed := counting.removed
	usage := svc.indexCapacityByNode["n1"]
	bm25Resident, bm25Metadata := svc.bm25ResidentBytes, svc.bm25MetadataBytes

	// Same text, same vector.
	require.NoError(t, svc.IndexNode(node(1, []float32{1, 0, 0})))
	// Same text, new vector.
	require.NoError(t, svc.IndexNode(node(1, []float32{0, 1, 0})))
	require.Equal(t, 1, counting.indexed)
	require.Equal(t, removed, counting.removed)
	require.Equal(t, usage.bm25Resident, svc.indexCapacityByNode["n1"].bm25Resident)
	require.Equal(t, usage.bm25Metadata, svc.indexCapacityByNode["n1"].bm25Metadata)
	require.Equal(t, bm25Resident, svc.bm25ResidentBytes)
	require.Equal(t, bm25Metadata, svc.bm25MetadataBytes)
	require.Equal(t, 1, svc.EmbeddingCount())
	text, ok := svc.fulltext().GetDocument("n1")
	require.True(t, ok)
	require.Equal(t, "V id 1 x 1", text)

	// New text, same vector; then new text and new vector.
	require.NoError(t, svc.IndexNode(node(2, []float32{0, 1, 0})))
	require.NoError(t, svc.IndexNode(node(3, []float32{0, 0, 1})))
	require.Equal(t, 3, counting.indexed)
	require.Equal(t, removed+2, counting.removed)
	text, _ = svc.fulltext().GetDocument("n1")
	require.Equal(t, "V id 3 x 1", text)

	// A document whose size wasn't recorded (as after loading a saved index)
	// is sized from its text.
	usage = svc.indexCapacityByNode["n1"]
	delete(svc.indexCapacityByNode, "n1")
	require.NoError(t, svc.IndexNode(node(3, []float32{0, 0, 1})))
	require.Equal(t, 3, counting.indexed)
	require.Equal(t, usage, svc.indexCapacityByNode["n1"])
}
