package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorEntryCapacityDoesNotInventNodeOwner(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), vectorOwnerDims)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	alias := undecodableVectorKey
	require.NoError(t, svc.IndexNode(vectorOwnerChunkNode(alias, 0)))
	// A retained, unowned cache entry can have exactly the bytes of a real
	// node ID. Its payload is charged, but it is not that node's vector.
	require.NoError(t, svc.vectorIndex.Add(alias, vectorOwnerAxis(1)))
	svc.indexMu.Lock()
	svc.reconcileVectorSourceCapacityLocked(svc.vectorIndex)
	svc.indexMu.Unlock()
	require.Equal(t, int64(vectorOwnerDims*4), svc.indexCapacityByNode[alias].vectorResident)
	require.Equal(t, int64(2*vectorOwnerDims*4), svc.vectorResidentBytes)
	require.NoError(t, svc.RemoveNode(storage.NodeID(alias)))
	require.Equal(t, int64(vectorOwnerDims*4), svc.vectorResidentBytes, "removing a node cannot release an unowned entry's charge")
	require.Equal(t, 1, svc.EmbeddingCount())
}
