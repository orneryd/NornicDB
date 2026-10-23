package search

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestModelSpaceExclusionPrecedesDimensionValidation(t *testing.T) {
	engine := newNamespacedEngine(t)
	old := &storage.Node{ID: "old", Properties: map[string]any{"content": "obsoleteword"}, ChunkEmbeddings: [][]float32{{1, 0}}, EmbedMeta: map[string]any{"embedding_space": "old:2"}, NamedEmbeddings: map[string][]float32{"default": {0, 1, 0, 0}}}
	_, err := engine.CreateNode(old)
	require.NoError(t, err)
	svc := NewServiceWithDimensions(engine, 4)
	defer svc.Close()
	svc.FixVectorDimensions()
	svc.SetEmbeddingSpace("new:4")
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.NoError(t, svc.IndexNode(old))
	require.Equal(t, 1, svc.EmbeddingCount())
	_, ok := svc.fulltext().GetDocument("old")
	require.True(t, ok)
}

func TestPersistedExactQueriesRespectAdmittedModelSpace(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[persist], func(t *testing.T) {
			engine := newNamespacedEngine(t)
			for _, node := range []*storage.Node{
				{ID: "current", ChunkEmbeddings: [][]float32{{1, 0, 0, 0}}, EmbedMeta: map[string]any{"embedding_space": "new:4"}},
				{ID: "excluded", ChunkEmbeddings: [][]float32{{0, 1, 0, 0}}, EmbedMeta: map[string]any{"embedding_space": "old:4"}},
			} {
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
			}
			svc := NewServiceWithDimensions(engine, 4)
			defer svc.Close()
			svc.SetEmbeddingSpace("new:4")
			if persist {
				svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
				svc.SetPersistenceEnabled(true)
			}
			require.NoError(t, svc.BuildIndexes(context.Background()))
			for _, sim := range []string{"dot", "euclidean", "cosine"} {
				hits, err := svc.VectorQueryNodesWithOptions(context.Background(), []float32{0, 1, 0, 0}, VectorQuerySpec{Similarity: sim, Limit: 10, ScoreScale: VectorScoreSimilarity, Exact: true}, nil)
				require.NoError(t, err)
				require.Len(t, hits, 1)
				require.Equal(t, "current", hits[0].ID)
			}
		})
	}
}
