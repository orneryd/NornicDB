package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHNSWRecall_ANNOnly tests recall@k for ANN-only search (before exact rerank).
//
// This test verifies that HNSW approximate search finds a reasonable fraction
// of the true top-k results. Recall@k measures the intersection between ANN
// results and exact brute-force results.
func TestHNSWRecall_ANNOnly(t *testing.T) {
	dimensions := 128
	numVectors := 1000
	k := 10

	// Create HNSW index
	hnswConfig := DefaultHNSWConfig()
	hnswIndex := NewHNSWIndex(dimensions, hnswConfig)

	// Create brute-force index for ground truth
	bruteIndex := NewVectorIndex(dimensions)

	// Generate random vectors
	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			vec[j] = rng.Float32()*2 - 1 // Random in [-1, 1]
		}
		vec = vector.Normalize(vec) // Normalize for cosine similarity
		vectors[i] = vec

		id := string(rune('a' + i%26)) + string(rune(i))
		require.NoError(t, hnswIndex.Add(id, vec))
		require.NoError(t, bruteIndex.Add(id, vec))
	}

	// Generate random query
	query := make([]float32, dimensions)
	for j := 0; j < dimensions; j++ {
		query[j] = rng.Float32()*2 - 1
	}
	query = vector.Normalize(query)

	// Get ground truth (exact brute-force top-k)
	exactResults, err := bruteIndex.Search(context.Background(), query, k, 0.0)
	require.NoError(t, err)
	require.Equal(t, k, len(exactResults))

	// Get ANN results (HNSW approximate)
	annResults, err := hnswIndex.Search(context.Background(), query, k, 0.0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(annResults), 1)

	// Calculate recall@k: intersection / k
	exactIDs := make(map[string]bool, k)
	for _, r := range exactResults {
		exactIDs[r.ID] = true
	}

	intersection := 0
	for _, r := range annResults {
		if exactIDs[r.ID] {
			intersection++
		}
	}

	recall := float64(intersection) / float64(k)
	t.Logf("Recall@%d (ANN-only): %.2f%% (%d/%d)", k, recall*100, intersection, k)

	// With default config (balanced), we expect reasonable recall
	// Typical: 70-95% recall@10 for 1000 vectors with balanced config
	assert.Greater(t, recall, 0.5, "ANN-only recall should be >50% with balanced config")
}

// TestHNSWRecall_ANNWithRerank tests recall@k for ANN + exact rerank.
//
// This test verifies that exact reranking improves recall by re-scoring
// ANN candidates with exact similarity. The pipeline should achieve
// higher recall than ANN-only.
func TestHNSWRecall_ANNWithRerank(t *testing.T) {
	dimensions := 128
	numVectors := 1000
	k := 10

	// Create indexes
	hnswConfig := DefaultHNSWConfig()
	hnswIndex := NewHNSWIndex(dimensions, hnswConfig)
	bruteIndex := NewVectorIndex(dimensions)

	// Generate random vectors
	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			vec[j] = rng.Float32()*2 - 1
		}
		vec = vector.Normalize(vec)
		vectors[i] = vec

		id := string(rune('a' + i%26)) + string(rune(i))
		require.NoError(t, hnswIndex.Add(id, vec))
		require.NoError(t, bruteIndex.Add(id, vec))
	}

	// Generate random query
	query := make([]float32, dimensions)
	for j := 0; j < dimensions; j++ {
		query[j] = rng.Float32()*2 - 1
	}
	query = vector.Normalize(query)

	// Get ground truth (exact brute-force top-k)
	exactResults, err := bruteIndex.Search(context.Background(), query, k, 0.0)
	require.NoError(t, err)
	require.Equal(t, k, len(exactResults))

	// Create pipeline: HNSW candidate gen + exact CPU scorer
	candidateGen := NewHNSWCandidateGen(hnswIndex)
	exactScorer := NewCPUExactScorer(bruteIndex)
	pipeline := NewVectorSearchPipeline(candidateGen, exactScorer)

	// Get ANN + exact rerank results
	rerankResults, err := pipeline.Search(context.Background(), query, k, 0.0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rerankResults), 1)

	// Calculate recall@k
	exactIDs := make(map[string]bool, k)
	for _, r := range exactResults {
		exactIDs[r.ID] = true
	}

	intersection := 0
	for _, r := range rerankResults {
		if exactIDs[r.ID] {
			intersection++
		}
	}

	recall := float64(intersection) / float64(k)
	t.Logf("Recall@%d (ANN + exact rerank): %.2f%% (%d/%d)", k, recall*100, intersection, k)

	// With exact rerank, recall should be higher than ANN-only
	// Typical: 85-99% recall@10 for 1000 vectors with balanced config + rerank
	assert.Greater(t, recall, 0.7, "ANN + rerank recall should be >70%")
}

// BenchmarkHNSWRecall_ANNOnly benchmarks ANN-only recall for different dataset sizes.
func BenchmarkHNSWRecall_ANNOnly(b *testing.B) {
	dimensions := 128
	k := 10

	sizes := []int{100, 1000, 10000}
	for _, numVectors := range sizes {
		b.Run(fmt.Sprintf("N=%d", numVectors), func(b *testing.B) {
			hnswConfig := DefaultHNSWConfig()
			hnswIndex := NewHNSWIndex(dimensions, hnswConfig)
			bruteIndex := NewVectorIndex(dimensions)

			rng := rand.New(rand.NewSource(42))
			for i := 0; i < numVectors; i++ {
				vec := make([]float32, dimensions)
				for j := 0; j < dimensions; j++ {
					vec[j] = rng.Float32()*2 - 1
				}
				vec = vector.Normalize(vec)

				id := string(rune('a' + i%26)) + string(rune(i))
				hnswIndex.Add(id, vec)
				bruteIndex.Add(id, vec)
			}

			query := make([]float32, dimensions)
			for j := 0; j < dimensions; j++ {
				query[j] = rng.Float32()*2 - 1
			}
			query = vector.Normalize(query)

			// Get ground truth once
			exactResults, _ := bruteIndex.Search(context.Background(), query, k, 0.0)
			exactIDs := make(map[string]bool, k)
			for _, r := range exactResults {
				exactIDs[r.ID] = true
			}

			b.ResetTimer()
			totalRecall := 0.0
			for i := 0; i < b.N; i++ {
				annResults, _ := hnswIndex.Search(context.Background(), query, k, 0.0)
				intersection := 0
				for _, r := range annResults {
					if exactIDs[r.ID] {
						intersection++
					}
				}
				totalRecall += float64(intersection) / float64(k)
			}
			avgRecall := totalRecall / float64(b.N)
			b.ReportMetric(avgRecall, "recall@k")
		})
	}
}

// BenchmarkHNSWRecall_ANNWithRerank benchmarks ANN + exact rerank recall.
func BenchmarkHNSWRecall_ANNWithRerank(b *testing.B) {
	dimensions := 128
	numVectors := 1000
	k := 10

	hnswConfig := DefaultHNSWConfig()
	hnswIndex := NewHNSWIndex(dimensions, hnswConfig)
	bruteIndex := NewVectorIndex(dimensions)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			vec[j] = rng.Float32()*2 - 1
		}
		vec = vector.Normalize(vec)

		id := string(rune('a' + i%26)) + string(rune(i))
		hnswIndex.Add(id, vec)
		bruteIndex.Add(id, vec)
	}

	query := make([]float32, dimensions)
	for j := 0; j < dimensions; j++ {
		query[j] = rng.Float32()*2 - 1
	}
	query = vector.Normalize(query)

	// Get ground truth once
	exactResults, _ := bruteIndex.Search(context.Background(), query, k, 0.0)
	exactIDs := make(map[string]bool, k)
	for _, r := range exactResults {
		exactIDs[r.ID] = true
	}

	candidateGen := NewHNSWCandidateGen(hnswIndex)
	exactScorer := NewCPUExactScorer(bruteIndex)
	pipeline := NewVectorSearchPipeline(candidateGen, exactScorer)

	b.ResetTimer()
	totalRecall := 0.0
	for i := 0; i < b.N; i++ {
		rerankResults, _ := pipeline.Search(context.Background(), query, k, 0.0)
		intersection := 0
		for _, r := range rerankResults {
			if exactIDs[r.ID] {
				intersection++
			}
		}
		totalRecall += float64(intersection) / float64(k)
	}
	avgRecall := totalRecall / float64(b.N)
	b.ReportMetric(avgRecall, "recall@k")
}

