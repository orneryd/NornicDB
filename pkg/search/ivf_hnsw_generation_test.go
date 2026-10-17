package search

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Growth makes automatic k-means choose more clusters (22 for 1,000 vectors,
// 24 for 1,200). While the replacement generation's graphs are built, public
// queries keep the previous complete generation; once it is published they use
// the new routing and graphs together, including the new clusters.
func TestIVFHNSWPublicQueriesSpanGrowthDrivenReclustering(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	t.Setenv("NORNICDB_KMEANS_NPROBE", "64") // every query routes to every cluster
	const dimensions, initial, grown = 8, 1000, 200
	ctx := context.Background()
	rng := rand.New(rand.NewSource(1372))
	vectors := make(map[string][]float32, initial+grown)
	newNode := func(id string) *storage.Node {
		vec := make([]float32, dimensions)
		for d := range vec {
			vec[d] = float32(rng.NormFloat64())
		}
		vectors[id] = vec
		return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, NamedEmbeddings: map[string][]float32{"default": vec}}
	}
	engine := newNamespacedEngine(t)
	for i := 0; i < initial; i++ {
		_, err := engine.CreateNode(newNode(fmt.Sprintf("initial-%04d", i)))
		require.NoError(t, err)
	}
	svc := NewServiceWithDimensions(engine, dimensions)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.EnableClustering(nil, 0)
	svc.SetMinEmbeddingsForClustering(1)
	require.NoError(t, svc.BuildIndexes(ctx))
	require.Equal(t, 22, svc.clusterIndex.NumClusters())
	pipeline, err := svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	require.IsType(t, &IVFHNSWCandidateGen{}, pipeline.candidateGen)

	for i := 0; i < grown; i++ {
		node := newNode(fmt.Sprintf("grown-%04d", i))
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}

	// A wide rescoring pool keeps the check about graph membership in every
	// compression mode (1-bit codes of 8-D vectors tie heavily).
	membership := &SearchOptions{QuantizationOversampling: 64}
	findsItself := func(id string) {
		t.Helper()
		hits, err := svc.VectorQueryNodesWithOptions(ctx, vectors[id], VectorQuerySpec{Limit: 3}, membership)
		require.NoError(t, err, "query for %s", id)
		ids := make([]string, len(hits))
		for i, hit := range hits {
			ids[i] = hit.ID
		}
		require.Contains(t, ids, id)
	}

	building, release := make(chan struct{}), make(chan struct{})
	ivfHNSWBuildHook = func() {
		close(building)
		<-release
	}
	t.Cleanup(func() { ivfHNSWBuildHook = nil })
	done := make(chan error, 1)
	go func() { done <- svc.TriggerClustering(ctx) }()
	select {
	case <-building:
	case err := <-done:
		t.Fatalf("reclustering finished without building graphs: %v", err)
	case <-time.After(2 * time.Minute):
		t.Fatal("reclustering did not start building graphs")
	}
	// The 24-cluster generation exists but its graphs are not built yet.
	for i := 0; i < 40; i++ {
		findsItself(fmt.Sprintf("initial-%04d", i*25))
		candidates, err := svc.VectorSearchCandidates(ctx, vectors[fmt.Sprintf("grown-%04d", i)], DefaultSearchOptions())
		require.NoError(t, err)
		require.NotEmpty(t, candidates)
	}
	require.Equal(t, 22, svc.clusterIndex.NumClusters())
	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Minute):
		t.Fatal("reclustering did not finish")
	}

	require.Equal(t, 24, svc.clusterIndex.NumClusters())
	svc.clusterHNSWMu.RLock()
	require.Len(t, svc.clusterHNSW, 24)
	svc.clusterHNSWMu.RUnlock()
	pipeline, err = svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	require.IsType(t, &IVFHNSWCandidateGen{}, pipeline.candidateGen)
	// The new generation's graphs hold the vectors added before reclustering.
	for i := 40; i < grown; i += 4 {
		findsItself(fmt.Sprintf("grown-%04d", i))
	}
}

// A dimension change (first vector of a new dimension while the in-memory
// index is empty, as with file-backed vectors) can replace the cluster index
// while a clustering generation is built. That generation describes the
// retired population and is discarded, never published.
func TestIVFHNSWGenerationForReplacedClusterIndexIsDiscarded(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	const dimensions = 4
	ctx := context.Background()
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), dimensions)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 40; i++ {
		vec := make([]float32, dimensions)
		for d := range vec {
			vec[d] = float32(rng.NormFloat64())
		}
		require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n-%02d", i)), ChunkEmbeddings: [][]float32{vec}}))
	}
	original := svc.clusterIndex
	require.Zero(t, svc.vectorIndex.Count(), "file-backed vectors leave the in-memory index empty")

	ivfHNSWBuildHook = func() { svc.maybeAutoSetVectorDimensions(dimensions + 1) }
	t.Cleanup(func() { ivfHNSWBuildHook = nil })
	require.NoError(t, svc.TriggerClustering(ctx))

	require.NotSame(t, original, svc.clusterIndex, "the dimension change replaced the cluster index")
	require.False(t, svc.clusterIndex.IsClustered())
	svc.clusterHNSWMu.RLock()
	require.Nil(t, svc.clusterHNSW, "the superseded generation's graphs are not published")
	svc.clusterHNSWMu.RUnlock()
	svc.clusterLexicalMu.RLock()
	require.Empty(t, svc.clusterLexicalProfiles)
	svc.clusterLexicalMu.RUnlock()
}
