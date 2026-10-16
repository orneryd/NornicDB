package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorSourceEmptyFileDimensionReplacement(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	sourceRetirementNode(t, svc, "old", []float32{1, 0, 0})
	require.NoError(t, svc.persistBaseIndexes())
	old := svc.vectorFileStore
	require.NoError(t, svc.RemoveNode("old"))
	require.Zero(t, old.Count())
	sourceRetirementNode(t, svc, "replacement", []float32{1, 0, 0, 0, 0})
	require.Equal(t, 5, svc.vectorIndex.GetDimensions())
	require.NotSame(t, old, svc.vectorFileStore)
	require.NotNil(t, svc.vectorFileStore)
	require.Equal(t, 5, svc.vectorFileStore.GetDimensions())
	require.True(t, svc.vectorFileStore.Has("replacement"))
	require.False(t, svc.vectorFileStore.Has("old"))
	results, err := svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0, 0, 0}, &SearchOptions{Limit: 1})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "replacement", results[0].ID)
}

func TestVectorSourceTransferCapacityLedger(t *testing.T) {
	for _, mode := range []string{"auto", "disk"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
			svc := NewServiceWithDimensionsAndBM25EngineAndOptions(newNamespacedEngine(t), 3, "v2", &ServiceOptions{VectorStorageMode: mode, VectorMemoryMaxBytes: 48})
			t.Cleanup(func() { _ = svc.Close() })
			svc.FixVectorDimensions()
			svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
			svc.SetPersistenceEnabled(true)
			for i := 0; i < 4; i++ {
				sourceRetirementNode(t, svc, fmt.Sprintf("n%d", i), []float32{1, 0, 0})
			}
			svc.SetPersistenceEnabled(false)
			require.Nil(t, svc.vectorFileStore)
			// The transferred normalized payload fits exactly. Rejection must
			// precede membership mutation, including in disk-configured mode.
			err := svc.IndexNode(&storage.Node{ID: "overflow", ChunkEmbeddings: [][]float32{{1, 0, 0}}})
			require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
			_, present := svc.vectorIndex.GetVector("overflow")
			require.False(t, present)
			require.Equal(t, int64(48), svc.vectorResidentBytes)
			for i := 0; i < 4; i++ {
				require.Equal(t, int64(12), svc.indexCapacityByNode[fmt.Sprintf("n%d", i)].vectorResident)
			}
			// Replacing a unit vector needs no extra payload; a non-unit
			// replacement needs a raw copy and must fail without removing it.
			require.NoError(t, svc.IndexNode(&storage.Node{ID: "n0", ChunkEmbeddings: [][]float32{{0, 1, 0}}}))
			err = svc.IndexNode(&storage.Node{ID: "n0", ChunkEmbeddings: [][]float32{{2, 0, 0}}})
			require.ErrorIs(t, err, ErrIndexMemoryBudgetExceeded)
			value, ok := svc.vectorIndex.GetVector("n0")
			require.True(t, ok)
			require.Equal(t, []float32{0, 1, 0}, value)
			require.NoError(t, svc.RemoveNode("n1"))
			require.Equal(t, int64(36), svc.vectorResidentBytes)
			require.NoError(t, svc.IndexNode(&storage.Node{ID: "n0", ChunkEmbeddings: [][]float32{{2, 0, 0}}}))
			require.Equal(t, int64(48), svc.vectorResidentBytes)
			svc.SetPersistenceEnabled(true)
			sourceRetirementNode(t, svc, "new", []float32{1, 0, 0})
			require.NotNil(t, svc.vectorFileStore)
			require.Zero(t, svc.vectorResidentBytes)
			require.False(t, svc.vectorFileStore.Has("n1"))
			svc.SetPersistenceEnabled(false)
			require.Nil(t, svc.vectorFileStore)
			require.Equal(t, int64(48), svc.vectorResidentBytes)
			sourceRetirementIDs(t, svc, "n0", "n2", "n3", "new")
		})
	}
}

func TestVectorSourceTransferReservesRelationshipCapacity(t *testing.T) {
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(newNamespacedEngine(t), 3, "v2", &ServiceOptions{VectorMemoryMaxBytes: 48})
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	for i := 0; i < 4; i++ {
		sourceRetirementNode(t, svc, fmt.Sprintf("n%d", i), []float32{1, 0, 0})
	}
	require.NoError(t, svc.IndexEdge(&storage.Edge{ID: "edge", Type: "RELATED", Properties: map[string]any{"embedding": []float32{1, 0, 0}}}))
	old := svc.vectorFileStore
	svc.SetPersistenceEnabled(false)
	require.Same(t, old, svc.vectorFileStore, "node transfer must reserve the resident relationship payload")
	require.True(t, svc.persistEnabled.Load())
	require.Equal(t, int64(12), svc.indexCapacityByEdge["edge"].vectorResident)
	require.NoError(t, svc.RemoveEdge("edge"))
	svc.SetPersistenceEnabled(false)
	require.Nil(t, svc.vectorFileStore)
	require.Equal(t, int64(48), svc.vectorResidentBytes)
}

func TestVectorSourceTransferredEntryOwnership(t *testing.T) {
	for _, reload := range []bool{false, true} {
		t.Run(fmt.Sprint(reload), func(t *testing.T) {
			engine := newNamespacedEngine(t)
			options := &ServiceOptions{VectorMemoryMaxBytes: 48}
			svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", options)
			t.Cleanup(func() { _ = svc.Close() })
			path := filepath.Join(t.TempDir(), "vectors")
			svc.SetVectorIndexPath(path)
			svc.SetPersistenceEnabled(true)
			node := &storage.Node{ID: "owner", NamedEmbeddings: map[string][]float32{"name": {1, 0, 0}}, ChunkEmbeddings: [][]float32{{1, 0, 0}, {0, 1, 0}}, Properties: map[string]any{"embedding": []float32{0, 0, 1}}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(node))
			require.Equal(t, 4, svc.vectorFileStore.Count())
			if reload {
				require.NoError(t, svc.persistBaseIndexes())
				require.NoError(t, svc.Close())
				svc = NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", options)
				svc.SetVectorIndexPath(path)
				svc.SetPersistenceEnabled(true)
				require.NoError(t, svc.BuildIndexes(context.Background()))
			}
			svc.SetPersistenceEnabled(false)
			require.Nil(t, svc.vectorFileStore)
			require.Equal(t, int64(48), svc.vectorResidentBytes)
			require.NoError(t, svc.RemoveNode("owner"))
			require.Zero(t, svc.vectorResidentBytes)
			require.Zero(t, svc.vectorIndex.Count())
			// Clear retires both node and relationship charges as it retires
			// their stores; later idempotent deletion cannot make them negative.
			sourceRetirementNode(t, svc, "after", []float32{1, 0, 0})
			require.NoError(t, svc.IndexEdge(&storage.Edge{ID: "edge", Type: "RELATED", Properties: map[string]any{"embedding": []float32{1, 0, 0}}}))
			svc.ClearVectorIndex()
			require.Zero(t, svc.vectorResidentBytes)
			require.NoError(t, svc.RemoveNode("after"))
			require.NoError(t, svc.RemoveEdge("edge"))
			require.Zero(t, svc.vectorResidentBytes)
		})
	}
}
