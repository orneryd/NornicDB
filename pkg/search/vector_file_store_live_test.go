package search

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLiveVectorIndexingCreatesConfiguredFileStore(t *testing.T) {
	vectorPath := filepath.Join(t.TempDir(), "vectors")
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.SetVectorIndexPath(vectorPath)
	svc.SetPersistenceEnabled(true)

	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              "first-live-node",
		ChunkEmbeddings: [][]float32{{1, 0}},
	}))

	require.NotNil(t, svc.vectorFileStore)
	require.Equal(t, 1, svc.vectorFileStore.Count())
	_, ok := svc.vectorFileStore.GetVector("first-live-node")
	require.True(t, ok)
	require.FileExists(t, vectorPath+".vec")
	require.NoError(t, svc.persistBaseIndexes())
	require.FileExists(t, vectorPath+".meta")

	svc.vectorIndex.mu.RLock()
	require.Empty(t, svc.vectorIndex.vectors)
	require.Empty(t, svc.vectorIndex.rawVectors)
	svc.vectorIndex.mu.RUnlock()
}

func TestEnablingVectorPersistenceMigratesExistingLiveVectors(t *testing.T) {
	vectorPath := filepath.Join(t.TempDir(), "vectors")
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })

	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              "before-persistence",
		ChunkEmbeddings: [][]float32{{0, 1}},
	}))
	require.Equal(t, 1, svc.vectorIndex.Count())

	svc.SetVectorIndexPath(vectorPath)
	svc.SetPersistenceEnabled(true)
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              "after-persistence",
		ChunkEmbeddings: [][]float32{{1, 0}},
	}))

	require.NotNil(t, svc.vectorFileStore)
	require.Equal(t, 2, svc.vectorFileStore.Count())
	for _, id := range []string{"before-persistence", "after-persistence"} {
		_, ok := svc.vectorFileStore.GetVector(id)
		require.True(t, ok, "missing migrated vector %q", id)
	}
	require.Zero(t, svc.vectorIndex.Count())
}

func TestLiveVectorIndexSurvivesRestartWithoutStorageRebuild(t *testing.T) {
	engine := newNamespacedEngine(t)
	node := &storage.Node{
		ID:              "live-node",
		Labels:          []string{"Document"},
		ChunkEmbeddings: [][]float32{{1, 0}},
		Properties: map[string]any{
			"text":      "live persisted document",
			"embedding": []float32{1, 0},
		},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	indexDir := t.TempDir()
	fulltextPath := filepath.Join(indexDir, "bm25")
	vectorPath := filepath.Join(indexDir, "vectors")
	first := NewServiceWithDimensions(engine, 2)
	first.SetFulltextIndexPath(fulltextPath)
	first.SetVectorIndexPath(vectorPath)
	first.SetPersistenceEnabled(true)
	require.NoError(t, first.IndexNode(node))
	first.PersistIndexesToDisk()
	require.NoError(t, first.Close())

	restarted := NewServiceWithDimensions(&iteratorEngine{
		Engine:     engine,
		iterateErr: errors.New("persisted live index must not rebuild from storage"),
	}, 2)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	restarted.SetFulltextIndexPath(fulltextPath)
	restarted.SetVectorIndexPath(vectorPath)
	restarted.SetPersistenceEnabled(true)

	require.NoError(t, restarted.BuildIndexes(context.Background()))
	require.Equal(t, 2, restarted.EmbeddingCount())
	results, err := restarted.VectorQueryNodes(context.Background(), []float32{1, 0}, VectorQuerySpec{
		Label:    "Document",
		Property: "embedding",
		Limit:    1,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "live-node", results[0].ID)
}
