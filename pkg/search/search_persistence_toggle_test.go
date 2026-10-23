package search

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBuildIndexes_DoesNotPersistWhenDisabled(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	_, err := engine.CreateNode(&storage.Node{
		ID:     "doc-1",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"title":   "hello",
			"content": "search index persistence toggle test",
		},
	})
	require.NoError(t, err)

	tmp := t.TempDir()
	bm25Path := filepath.Join(tmp, "bm25")
	vectorPath := filepath.Join(tmp, "vectors")
	hnswPath := filepath.Join(tmp, "hnsw")

	svc := NewService(engine)
	svc.SetFulltextIndexPath(bm25Path)
	svc.SetVectorIndexPath(vectorPath)
	svc.SetHNSWIndexPath(hnswPath)
	svc.SetPersistenceEnabled(false)

	require.NoError(t, svc.BuildIndexes(context.Background()))
	svc.PersistIndexesToDisk() // should remain a no-op when disabled

	_, bm25Err := os.Stat(bm25Path)
	require.True(t, os.IsNotExist(bm25Err), "bm25 file should not be written when persistence disabled")

	_, vecMetaErr := os.Stat(vectorPath + ".meta")
	require.True(t, os.IsNotExist(vecMetaErr), "vector meta should not be written when persistence disabled")

	_, vecDataErr := os.Stat(vectorPath + ".vec")
	require.True(t, os.IsNotExist(vecDataErr), "vector data should not be written when persistence disabled")
}

func TestSetPersistenceEnabledFalse_DisablesExistingVectorFileStoreWrites(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	_, err := engine.CreateNode(&storage.Node{
		ID:     "doc-1",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"title": "hello",
		},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	})
	require.NoError(t, err)

	tmp := t.TempDir()
	vectorPath := filepath.Join(tmp, "vectors")

	svc := NewServiceWithDimensions(engine, 3)
	svc.SetVectorIndexPath(vectorPath)
	svc.SetPersistenceEnabled(true)
	require.NoError(t, svc.BuildIndexes(context.Background()))

	_, beforeErr := os.Stat(vectorPath + ".vec")
	require.NoError(t, beforeErr, "sanity check: vector file should exist when persistence enabled")

	svc.SetPersistenceEnabled(false)
	fiBefore, statErr := os.Stat(vectorPath + ".vec")
	require.NoError(t, statErr)
	sizeBefore := fiBefore.Size()

	_, err = engine.CreateNode(&storage.Node{
		ID:     "doc-2",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"title": "world",
		},
		ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}},
	})
	require.NoError(t, err)

	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:     "doc-2",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"title": "world",
		},
		ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}},
	}))

	svc.PersistIndexesToDisk()

	fiAfter, statErr := os.Stat(vectorPath + ".vec")
	require.NoError(t, statErr)
	require.Equal(t, sizeBefore, fiAfter.Size(), "vector append-only file should not change after persistence disabled")
}
