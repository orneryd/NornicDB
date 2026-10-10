package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A database with an embedder keeps its vector index at the configured
// dimension: a node whose only vector-shaped value is an ordinary numeric
// list (page offsets) indexed before any embedding can't make the index
// 2-dimensional and reject every embedding after it.
func TestFixedVectorDimensionsIgnoreNumericListProperties(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.FixVectorDimensions()
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "source", Labels: []string{"Source"},
		Properties: map[string]interface{}{"page_starts": []interface{}{int64(0), int64(30)}, "page_ends": []interface{}{int64(30), int64(200)}}}))
	require.Equal(t, 4, svc.vectorIndex.GetDimensions())
	require.Zero(t, svc.vectorIndex.Count(), "a 2-element list is not a 4-dimension vector")

	require.NoError(t, svc.IndexNode(&storage.Node{ID: "doc", Labels: []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 0, 0, 0}}}))
	require.Equal(t, 4, svc.vectorIndex.GetDimensions())
	require.True(t, svc.vectorIndex.HasVector("doc"), "the embedding is indexed")

	// A 4-element numeric list property is a vector of the index.
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "prop", Labels: []string{"P"},
		Properties: map[string]interface{}{"v": []interface{}{1.0, 0.0, 0.0, 1.0}}}))
	require.True(t, svc.vectorIndex.HasVector("prop-prop-v"))
}

// Without an embedder the first vector-shaped value still sets an empty
// index's dimension (a database storing its own vectors).
func TestUnfixedVectorDimensionsFollowFirstVector(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "own", Labels: []string{"V"},
		Properties: map[string]interface{}{"embedding": []interface{}{1.0, 0.5}}}))
	require.Equal(t, 2, svc.vectorIndex.GetDimensions())
}
