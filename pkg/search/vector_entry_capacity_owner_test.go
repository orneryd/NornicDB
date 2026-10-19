package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorEntryMarkerCapacityOwnership(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), vectorOwnerDims)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	alias := nodeNamedVectorKey("doc", "v")
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "doc", NamedEmbeddings: map[string][]float32{"v": vectorOwnerAxis(0)}}))
	require.NoError(t, svc.IndexNode(vectorOwnerChunkNode(alias, 1)))
	require.Equal(t, int64(2*vectorOwnerDims*4), svc.vectorResidentBytes, "charge each vector to its own node once")
	require.NoError(t, svc.RemoveNode(storage.NodeID(alias)))
	require.Equal(t, int64(vectorOwnerDims*4), svc.vectorResidentBytes)
	require.Equal(t, 1, svc.EmbeddingCount())
	require.NoError(t, svc.RemoveNode("doc"))
	require.Zero(t, svc.vectorResidentBytes)
}
