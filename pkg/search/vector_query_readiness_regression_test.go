package search

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorQueryNodesLazyFirstReadBuilds(t *testing.T) {
	for _, similarity := range []string{"cosine", "dot", "euclidean"} {
		t.Run(similarity, func(t *testing.T) {
			engine := newNamespacedEngine(t)
			svc := NewServiceWithDimensions(engine, 4)
			_, err := engine.CreateNode(&storage.Node{ID: "point", ChunkEmbeddings: [][]float32{{1, 0, 0, 0}}})
			require.NoError(t, err)
			svc.SetLazyWarming(true, func() { require.NoError(t, svc.BuildIndexes(context.Background())) })
			hits, err := svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, 0, 0, 0}, VectorQuerySpec{Similarity: similarity, Limit: 1}, nil)
			require.NoError(t, err)
			require.Len(t, hits, 1)
			require.Equal(t, "point", hits[0].ID)
		})
	}
}

func TestVectorQueryNodesRejectsIncompleteBuild(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 4)
	indexTestNode(t, svc, "partial", []float32{1, 0, 0, 0})
	svc.buildAttempted.Store(true)
	_, err := svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, 0, 0, 0}, VectorQuerySpec{Limit: 1}, nil)
	require.ErrorIs(t, err, ErrSearchIndexBuilding)
}

func TestPersistedVectorQueriesPreserveMagnitude(t *testing.T) {
	engine := newNamespacedEngine(t)
	node := &storage.Node{ID: "point", NamedEmbeddings: map[string][]float32{"default": {3, 4, 0, 0}}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "vectors")
	makeService := func() *Service {
		svc := NewServiceWithDimensions(engine, 4)
		svc.SetVectorIndexPath(path)
		svc.SetPersistenceEnabled(true)
		return svc
	}
	svc := makeService()
	require.NoError(t, svc.BuildIndexes(context.Background()))
	check := func(svc *Service) {
		hits, err := svc.VectorQueryNodes(context.Background(), []float32{3, 4, 0, 0}, VectorQuerySpec{Property: "default", Similarity: "dot", Limit: 1, ScoreScale: VectorScoreSimilarity})
		require.NoError(t, err)
		require.Len(t, hits, 1)
		require.InDelta(t, 25, hits[0].Score, 1e-6)
		hits, err = svc.VectorQueryNodes(context.Background(), []float32{3, 4, 0, 0}, VectorQuerySpec{Property: "default", Similarity: "euclidean", Limit: 1, ScoreScale: VectorScoreSimilarity})
		require.NoError(t, err)
		require.Len(t, hits, 1)
		require.InDelta(t, 1, hits[0].Score, 1e-6)
	}
	check(svc)
	require.NoError(t, svc.Close())
	svc = makeService()
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	check(svc)
}
