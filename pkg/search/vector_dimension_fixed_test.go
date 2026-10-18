package search

import (
	"context"
	"fmt"
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

// The dimension readers give 0 for a missing entity or one without a vector,
// and an empty service without a vector index gets one of the first
// embedding's dimension (when the dimension isn't fixed).
func TestVectorDimensionReadersAndFirstIndex(t *testing.T) {
	require.Equal(t, 0, firstVectorDimensions(nil))
	require.Equal(t, 0, firstEdgeVectorDimensions(nil))
	require.Equal(t, 0, firstEdgeVectorDimensions(&storage.Edge{Properties: map[string]interface{}{"name": "x"}}))
	require.Equal(t, 3, firstEdgeVectorDimensions(&storage.Edge{Properties: map[string]interface{}{"v": []float32{1, 2, 3}}}))

	service := NewService(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"))
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.mu.Lock()
	service.vectorIndex = nil
	service.mu.Unlock()
	service.maybeAutoSetVectorDimensions(4)
	service.mu.RLock()
	defer service.mu.RUnlock()
	require.NotNil(t, service.vectorIndex)
	require.Equal(t, 4, service.vectorIndex.GetDimensions())
}

// Rebuilding from storage follows the same rule: without an embedder, the
// first stored vector sets an empty index's dimension, even when the first
// node stored has none (a Qdrant collection's metadata node, a document). A
// rebuild after a restart then indexes the database's vectors.
func TestBuildIndexesUnfixedDimensionsFollowFirstStoredVector(t *testing.T) {
	badger, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	engine := storage.NewNamespacedEngine(badger, "points")
	// Badger iterates in key order: the vector-less node comes first.
	_, err = engine.CreateNode(&storage.Node{ID: "a-meta", Labels: []string{"Meta"}, Properties: map[string]interface{}{"dimensions": int64(2)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "z-point", Labels: []string{"Point"}, NamedEmbeddings: map[string][]float32{"default": {1, 0}}})
	require.NoError(t, err)

	svc := NewServiceWithDimensions(engine, 4)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Equal(t, 2, svc.VectorIndexDimensions())
	require.Equal(t, 1, svc.EmbeddingCount(), "the stored 2-dimension vector is indexed")
}

func TestFixedDimensionsRejectExplicitEmbeddingsBeforeMutation(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 4)
	svc.FixVectorDimensions()
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "point", NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}}))
	for _, node := range []*storage.Node{
		{ID: "point", NamedEmbeddings: map[string][]float32{"default": {1, 0}}},
		{ID: "point", ChunkEmbeddings: [][]float32{{1, 0}}},
	} {
		require.ErrorIs(t, svc.IndexNode(node), ErrDimensionMismatch)
		require.Equal(t, 1, svc.EmbeddingCount())
	}
}

func TestAdoptingCollectionDimensionsUpdatesClusterBackend(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "1")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 1024)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(10)
	for i := 0; i < 20; i++ {
		indexTestNode(t, svc, fmt.Sprintf("point-%d", i), []float32{float32(i + 1), 1, 0, 0})
	}
	require.Equal(t, 4, svc.VectorIndexDimensions())
	require.Equal(t, 4, svc.clusterIndex.Dimensions())
	require.Equal(t, 20, svc.clusterIndex.Count())
	require.NoError(t, svc.TriggerClustering(context.Background()))
	require.NotZero(t, svc.ClusterStats().NumClusters)
	pipeline, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.IsType(t, &IVFHNSWCandidateGen{}, pipeline.candidateGen)
}

// AdmitsVectorDimensions answers what IndexNode accepts: a fixed index only
// its own dimension, an unfixed empty one any dimension (the first vector sets
// it), an unfixed one holding vectors only theirs, and a service with vector
// search off every dimension.
func TestAdmitsVectorDimensionsMatchesIndexNode(t *testing.T) {
	fixed := NewServiceWithDimensions(storage.NewMemoryEngine(), 8)
	fixed.FixVectorDimensions()
	require.True(t, fixed.AdmitsVectorDimensions(8))
	require.False(t, fixed.AdmitsVectorDimensions(4))
	require.ErrorIs(t, fixed.IndexNode(&storage.Node{ID: "four", NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}}), ErrDimensionMismatch)
	require.True(t, fixed.AdmitsVectorDimensions(0), "a write without vectors has nothing to reject")

	unfixed := NewServiceWithDimensions(storage.NewMemoryEngine(), 8)
	require.True(t, unfixed.AdmitsVectorDimensions(4), "the first vector sets an empty index's dimension")
	require.NoError(t, unfixed.IndexNode(&storage.Node{ID: "four", NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}}))
	require.True(t, unfixed.AdmitsVectorDimensions(4))
	require.False(t, unfixed.AdmitsVectorDimensions(8))
	require.ErrorIs(t, unfixed.IndexNode(&storage.Node{ID: "eight", NamedEmbeddings: map[string][]float32{"default": make([]float32, 8)}}), ErrDimensionMismatch)

	fixed.SetIndexFlags(true, false)
	require.True(t, fixed.AdmitsVectorDimensions(4), "vector search off indexes no vectors")

	var none *Service
	require.True(t, none.AdmitsVectorDimensions(4))
	fixed.mu.Lock()
	fixed.vectorIndex = nil
	fixed.mu.Unlock()
	fixed.SetIndexFlags(true, true)
	require.False(t, fixed.AdmitsVectorDimensions(8), "a fixed service without an index indexes nothing")

	// A file-backed store holding vectors decides the dimension, as it does
	// for IndexNode, even when the in-memory index is empty and unfixed.
	stored := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	vfs, err := NewVectorFileStore(t.TempDir()+"/vectors", 2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vfs.Close() })
	require.NoError(t, vfs.Add("two", []float32{1, 0}))
	stored.indexMu.Lock()
	stored.vectorFileStore = vfs
	stored.indexMu.Unlock()
	require.True(t, stored.AdmitsVectorDimensions(2))
	require.False(t, stored.AdmitsVectorDimensions(4))
	require.ErrorIs(t, stored.IndexNode(&storage.Node{ID: "four", NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}}), ErrDimensionMismatch)
}
