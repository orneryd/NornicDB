package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BenchmarkKMeansCandidateGen benchmarks k-means candidate generation for very large N.
func BenchmarkKMeansCandidateGen(b *testing.B) {
	dimensions := 128
	sizes := []int{10000, 50000, 100000, 500000}

	for _, numVectors := range sizes {
		b.Run(fmt.Sprintf("N=%d", numVectors), func(b *testing.B) {
			// Create GPU manager (nil = CPU-only mode)
			manager, _ := gpu.NewManager(nil)

			// Create cluster index
			embConfig := gpu.DefaultEmbeddingIndexConfig(dimensions)
			kmeansConfig := gpu.DefaultKMeansConfig()
			kmeansConfig.NumClusters = optimalKForBenchmark(numVectors)
			clusterIndex := gpu.NewClusterIndex(manager, embConfig, kmeansConfig)

			// Create vector index for fallback
			vectorIndex := NewVectorIndex(dimensions)

			// Generate random vectors
			rng := rand.New(rand.NewSource(42))
			for i := 0; i < numVectors; i++ {
				vec := make([]float32, dimensions)
				for j := 0; j < dimensions; j++ {
					vec[j] = rng.Float32()*2 - 1
				}
				vec = vector.Normalize(vec)

				id := fmt.Sprintf("node-%d", i)
				clusterIndex.Add(id, vec)
				vectorIndex.Add(id, vec)
			}

			// Run clustering
			if err := clusterIndex.Cluster(); err != nil {
				b.Fatalf("Clustering failed: %v", err)
			}

			// Create candidate generator
			candidateGen := NewKMeansCandidateGen(clusterIndex, vectorIndex, 3)

			// Generate random query
			query := make([]float32, dimensions)
			for j := 0; j < dimensions; j++ {
				query[j] = rng.Float32()*2 - 1
			}
			query = vector.Normalize(query)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := candidateGen.SearchCandidates(context.Background(), query, 10, 0.0)
				if err != nil {
					b.Fatalf("SearchCandidates failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkKMeansVsHNSW compares k-means routing vs HNSW for very large datasets.
func BenchmarkKMeansVsHNSW(b *testing.B) {
	dimensions := 128
	numVectors := 100000

	// Setup: Create both indexes
	manager, _ := gpu.NewManager(nil)
	embConfig := gpu.DefaultEmbeddingIndexConfig(dimensions)
	kmeansConfig := gpu.DefaultKMeansConfig()
	kmeansConfig.NumClusters = optimalKForBenchmark(numVectors)
	clusterIndex := gpu.NewClusterIndex(manager, embConfig, kmeansConfig)

	vectorIndex := NewVectorIndex(dimensions)
	hnswConfig := DefaultHNSWConfig()
	hnswIndex := NewHNSWIndex(dimensions, hnswConfig)

	// Generate random vectors
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			vec[j] = rng.Float32()*2 - 1
		}
		vec = vector.Normalize(vec)

		id := fmt.Sprintf("node-%d", i)
		clusterIndex.Add(id, vec)
		vectorIndex.Add(id, vec)
		hnswIndex.Add(id, vec)
	}

	// Run clustering
	if err := clusterIndex.Cluster(); err != nil {
		b.Fatalf("Clustering failed: %v", err)
	}

	// Create candidate generators
	kmeansGen := NewKMeansCandidateGen(clusterIndex, vectorIndex, 3)
	hnswGen := NewHNSWCandidateGen(hnswIndex)

	// Generate random query
	query := make([]float32, dimensions)
	for j := 0; j < dimensions; j++ {
		query[j] = rng.Float32()*2 - 1
	}
	query = vector.Normalize(query)

	b.Run("k-means", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := kmeansGen.SearchCandidates(context.Background(), query, 10, 0.0)
			if err != nil {
				b.Fatalf("K-means SearchCandidates failed: %v", err)
			}
		}
	})

	b.Run("HNSW", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := hnswGen.SearchCandidates(context.Background(), query, 10, 0.0)
			if err != nil {
				b.Fatalf("HNSW SearchCandidates failed: %v", err)
			}
		}
	})
}

// BenchmarkKMeansPipeline benchmarks the full pipeline with k-means candidate generation.
func BenchmarkKMeansPipeline(b *testing.B) {
	dimensions := 128
	numVectors := 100000

	// Setup
	manager, _ := gpu.NewManager(nil)
	embConfig := gpu.DefaultEmbeddingIndexConfig(dimensions)
	kmeansConfig := gpu.DefaultKMeansConfig()
	kmeansConfig.NumClusters = optimalKForBenchmark(numVectors)
	clusterIndex := gpu.NewClusterIndex(manager, embConfig, kmeansConfig)

	vectorIndex := NewVectorIndex(dimensions)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			vec[j] = rng.Float32()*2 - 1
		}
		vec = vector.Normalize(vec)

		id := fmt.Sprintf("node-%d", i)
		clusterIndex.Add(id, vec)
		vectorIndex.Add(id, vec)
	}

	if err := clusterIndex.Cluster(); err != nil {
		b.Fatalf("Clustering failed: %v", err)
	}

	// Create pipeline
	candidateGen := NewKMeansCandidateGen(clusterIndex, vectorIndex, 3)
	exactScorer := NewCPUExactScorer(vectorIndex)
	pipeline := NewVectorSearchPipeline(candidateGen, exactScorer)

	query := make([]float32, dimensions)
	for j := 0; j < dimensions; j++ {
		query[j] = rng.Float32()*2 - 1
	}
	query = vector.Normalize(query)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pipeline.Search(context.Background(), query, 10, 0.0)
		if err != nil {
			b.Fatalf("Pipeline Search failed: %v", err)
		}
	}
}

// optimalKForBenchmark calculates optimal K for benchmark datasets.
func optimalKForBenchmark(n int) int {
	// Use sqrt(n/2) heuristic, but cap at reasonable values
	k := int(float64(n) / 200.0) // ~0.5% of data
	if k < 10 {
		k = 10
	}
	if k > 500 {
		k = 500
	}
	return k
}

// TestKMeansCandidateGen_Integration tests k-means candidate generation in a real scenario.
func TestKMeansCandidateGen_Integration(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)

	// Enable clustering
	manager, _ := gpu.NewManager(nil)
	svc.EnableClustering(manager, 10) // 10 clusters for small test

	// Add test nodes
	for i := 0; i < 100; i++ {
		node := &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("node-%d", i)),
			ChunkEmbeddings: [][]float32{{float32(i % 4), float32((i + 1) % 4), 0, 0}},
		}
		require.NoError(t, svc.IndexNode(node))
	}

	// Trigger clustering
	require.NoError(t, svc.TriggerClustering())

	// Perform search - should use k-means routing
	query := []float32{1, 0, 0, 0}
	opts := DefaultSearchOptions()
	opts.Limit = 10

	candidates, err := svc.VectorSearchCandidates(context.Background(), query, opts)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(candidates), 1)
}

