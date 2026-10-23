package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A database with an embedder keeps its vector index at the configured
// dimension: a node whose only vector-shaped value is an ordinary numeric
// list (page offsets) indexed before any embedding can't make the index
// 2-dimensional and reject every embedding after it.
func TestFixedVectorDimensionsIgnoreNumericListProperties(t *testing.T) {
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 4)
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
	require.True(t, svc.vectorIndex.HasVector(nodePropertyVectorKey("prop", "v")))
}

// Without an embedder the first vector-shaped value still sets an empty
// index's dimension (a database storing its own vectors).
func TestUnfixedVectorDimensionsFollowFirstVector(t *testing.T) {
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 4)
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

	service := NewService(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
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
	fixed := NewServiceWithDimensions(newTestMemoryEngine(t), 8)
	fixed.FixVectorDimensions()
	require.True(t, fixed.AdmitsVectorDimensions(8))
	require.False(t, fixed.AdmitsVectorDimensions(4))
	require.ErrorIs(t, fixed.IndexNode(&storage.Node{ID: "four", NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}}), ErrDimensionMismatch)
	require.True(t, fixed.AdmitsVectorDimensions(0), "a write without vectors has nothing to reject")

	unfixed := NewServiceWithDimensions(newTestMemoryEngine(t), 8)
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
	stored := NewServiceWithDimensions(newTestMemoryEngine(t), 4)
	vfs, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
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

// A file-backed store decides the dimension even when it is empty: vectors
// are written to it, and its records keep the size it was created with. An
// unfixed service whose last vector was removed therefore neither adopts a
// new dimension nor reports success for a vector it cannot store.
func TestEmptyFileBackedStoreKeepsItsDimension(t *testing.T) {
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 8)
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	t.Cleanup(func() { _ = svc.Close() })
	eight := make([]float32, 8)
	eight[0] = 1
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "previous", NamedEmbeddings: map[string][]float32{"default": eight}}))
	require.NoError(t, svc.RemoveNode("previous"))
	require.Zero(t, svc.EmbeddingCount())
	svc.mu.RLock()
	require.NotNil(t, svc.vectorFileStore, "the vector went to the file-backed store")
	svc.mu.RUnlock()

	require.False(t, svc.AdmitsVectorDimensions(4))
	require.ErrorIs(t, svc.IndexNode(&storage.Node{ID: "four", NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}}), ErrDimensionMismatch)
	require.Equal(t, 8, svc.VectorIndexDimensions(), "an empty file-backed store is not re-dimensioned")
	require.Zero(t, svc.EmbeddingCount())

	require.True(t, svc.AdmitsVectorDimensions(8))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "eight", NamedEmbeddings: map[string][]float32{"default": eight}}))
	require.Equal(t, 1, svc.EmbeddingCount())
}

// Moving in-memory vectors into a new file-backed store takes vectorIndex.mu
// inside s.mu, as dimension admission does: AdmitsVectorDimensions holds s.mu
// while it counts the index's vectors for a mismatched dimension, so a
// migration that held vectorIndex.mu while waiting for s.mu deadlocked a
// Qdrant write of another dimension.
func TestFileStoreMigrationDoesNotDeadlockDimensionAdmission(t *testing.T) {
	_ = t.TempDir()
	svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.vectorIndex.Add("live", []float32{1, 0}))
	// Persistence enabled after live ingestion began: the next index
	// mutation migrates the in-memory vector to disk. Nothing else here takes
	// s.mu for writing, so a pending writer below is the migration.
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)

	// Admission of a 3-dimensional write between its two steps: s.mu held,
	// the index count (read only on a dimension mismatch) next.
	svc.mu.RLock()
	migrated := make(chan struct{})
	go func() {
		svc.indexMu.Lock()
		svc.ensureVectorFileStore()
		svc.indexMu.Unlock()
		close(migrated)
	}()
	// The migration reaches publication: its s.mu.Lock is pending, which
	// makes new read locks fail.
	require.Eventually(t, func() bool {
		if svc.mu.TryRLock() {
			svc.mu.RUnlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond)
	admitted := make(chan bool, 1)
	go func() { admitted <- svc.activeVectorDimensions() == 3 || svc.vectorDimensionsAdoptable() }()
	var admits, deadlocked bool
	select {
	case admits = <-admitted:
	case <-time.After(2 * time.Second):
		deadlocked = true
	}
	// Release admission's lock either way so no goroutine is stranded.
	svc.mu.RUnlock()
	select {
	case <-migrated:
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not finish after admission released s.mu")
	}
	if deadlocked {
		<-admitted
		t.Fatal("admission's index read waited on the migration, which waited on admission's s.mu")
	}
	require.False(t, admits, "a 2-dimensional index holding a vector admits no 3-dimensional write")

	svc.mu.RLock()
	vfs := svc.vectorFileStore
	svc.mu.RUnlock()
	require.NotNil(t, vfs, "the migration published the file-backed store")
	require.True(t, vfs.Has("live"), "the in-memory vector moved to the file-backed store")
	require.Equal(t, 1, svc.EmbeddingCount())
	require.True(t, svc.AdmitsVectorDimensions(2))
	require.False(t, svc.AdmitsVectorDimensions(3))
}

// Retiring the file-backed store (persistence turned off, the service closed)
// changes s.vectorFileStore under s.mu, as publishing it does, so readers
// holding s.mu (VectorIndexDimensions, search) are ordered against it. The
// reader and the retirement share no other lock, so under -race a retirement
// that skips s.mu is reported whichever runs first.
func TestRetiringFileStoreHoldsServiceLock(t *testing.T) {
	for _, retire := range []struct {
		name string
		run  func(*Service)
	}{
		{"SetPersistenceEnabled(false)", func(s *Service) { s.SetPersistenceEnabled(false) }},
		{"Close", func(s *Service) { _ = s.Close() }},
	} {
		t.Run(retire.name, func(t *testing.T) {
			svc := NewServiceWithDimensions(newTestMemoryEngine(t), 2)
			t.Cleanup(func() { _ = svc.Close() })
			svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
			svc.SetPersistenceEnabled(true)
			svc.indexMu.Lock()
			svc.ensureVectorFileStore()
			svc.indexMu.Unlock()
			require.Equal(t, 2, svc.VectorIndexDimensions())

			read := make(chan int, 1)
			go func() { read <- svc.VectorIndexDimensions() }()
			retire.run(svc)
			require.Equal(t, 2, <-read, "the store or the in-memory index, both 2-dimensional")
			svc.mu.RLock()
			require.Nil(t, svc.vectorFileStore)
			svc.mu.RUnlock()
		})
	}
}
