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
	// The source can be reconciled repeatedly without double-charging its
	// unowned entries or treating them as relationship payload.
	svc.indexMu.Lock()
	svc.reconcileVectorSourceCapacityLocked(svc.vectorIndex)
	svc.indexMu.Unlock()
	require.Equal(t, int64(2*vectorOwnerDims*4), svc.vectorResidentBytes)
	require.Equal(t, int64(vectorOwnerDims*4), svc.indexCapacityByNode[alias].vectorResident)
	require.NoError(t, svc.RemoveNode(storage.NodeID(alias)))
	require.Equal(t, int64(vectorOwnerDims*4), svc.vectorResidentBytes, "removing a node cannot release an unowned entry's charge")
	require.Equal(t, 1, svc.EmbeddingCount())
	// Replacing the complete source releases its unknown payload while retaining
	// independent resident usage, and restoring it charges the payload once.
	require.NoError(t, svc.IndexEdge(&storage.Edge{ID: "independent", Type: "RELATED", Properties: map[string]any{"embedding": vectorOwnerAxis(2)}}))
	svc.indexMu.Lock()
	svc.reconcileVectorSourceCapacityLocked(nil)
	clearedBytes, clearedUnowned := svc.vectorResidentBytes, svc.vectorUnownedBytes
	svc.reconcileVectorSourceCapacityLocked(svc.vectorIndex)
	svc.reconcileVectorSourceCapacityLocked(svc.vectorIndex)
	svc.indexMu.Unlock()
	require.Equal(t, int64(vectorOwnerDims*4), clearedBytes)
	require.Zero(t, clearedUnowned)
	require.Equal(t, int64(2*vectorOwnerDims*4), svc.vectorResidentBytes)
	svc.ClearVectorIndex()
	require.Zero(t, svc.vectorResidentBytes)
	require.Zero(t, svc.vectorUnownedBytes)
	require.Zero(t, svc.EmbeddingCount())
}
