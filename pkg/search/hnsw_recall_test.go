package search

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
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

		id := string(rune('a'+i%26)) + string(rune(i))
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

		id := string(rune('a'+i%26)) + string(rune(i))
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

func TestHNSWCandidateGenerationUsesWideBeamBeforeSelectingResults(t *testing.T) {
	const (
		dimensions = 32
		vectors    = 2_000
		limit      = 200
		beamFactor = 4
	)
	config := DefaultHNSWConfig()
	config.EfSearch = 50
	index := NewHNSWIndex(dimensions, config)
	rng := rand.New(rand.NewSource(429))
	for i := 0; i < vectors; i++ {
		value := make([]float32, dimensions)
		for dimension := range value {
			value[dimension] = rng.Float32()*2 - 1
		}
		require.NoError(t, index.Add(fmt.Sprintf("vector-%04d", i), vector.Normalize(value)))
	}
	query := make([]float32, dimensions)
	for dimension := range query {
		query[dimension] = rng.Float32()*2 - 1
	}

	actual, err := NewHNSWCandidateGen(index).SearchCandidates(context.Background(), query, limit, -1)
	require.NoError(t, err)
	wider, err := index.SearchWithEf(context.Background(), query, limit*beamFactor, -1, limit*beamFactor)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(wider), limit)

	expectedIDs := make([]string, limit)
	actualIDs := make([]string, len(actual))
	for i := range expectedIDs {
		expectedIDs[i] = wider[i].ID
	}
	for i := range actualIDs {
		actualIDs[i] = actual[i].ID
	}
	require.Equal(t, expectedIDs, actualIDs)
}

func TestHNSWCandidateGenerationSelectsDistinctNodesFromChunkedResults(t *testing.T) {
	config := DefaultHNSWConfig()
	index := NewHNSWIndex(2, config)
	for i := 0; i < 8; i++ {
		require.NoError(t, index.Add(nodeChunkVectorKey("doc-a", i), []float32{1, float32(i) / 100}))
	}
	require.NoError(t, index.Add(nodeChunkVectorKey("doc-b", 0), []float32{0.95, 0.05}))
	require.NoError(t, index.Add(nodeChunkVectorKey("doc-c", 0), []float32{0.9, 0.1}))
	require.NoError(t, index.Add(nodeChunkVectorKey("doc-d", 0), []float32{0.85, 0.15}))

	// The limit counts nodes; every chunk the beam surfaced for a selected
	// node stays, for the exact stage to score.
	candidates, err := NewHNSWCandidateGen(index).SearchCandidates(context.Background(), []float32{1, 0}, 3, -1)
	require.NoError(t, err)
	var nodes []string
	for _, candidate := range candidates {
		node := mustVectorEntryOwner(t, candidate.ID)
		if !slices.Contains(nodes, node) {
			nodes = append(nodes, node)
		}
	}
	require.Equal(t, []string{"doc-a", "doc-b", "doc-c"}, nodes)
	require.Len(t, candidates, 10, "doc-a's eight chunks, doc-b and doc-c")
}

func TestHNSWSearchBeamScalesIndependentlyOfResultLimit(t *testing.T) {
	require.Equal(t, 800, hnswSearchBeam(200, 50, 4))
	require.Equal(t, 1_000, hnswSearchBeam(200, 1_000, 4))
	require.Equal(t, 800, hnswSearchBeam(200, 50, 0))
}

func BenchmarkHNSWCandidateGenerationBeamFactor(b *testing.B) {
	const (
		dimensions = 128
		vectors    = 10_000
		limit      = 200
	)
	config := DefaultHNSWConfig()
	index := NewHNSWIndex(dimensions, config)
	rng := rand.New(rand.NewSource(429))
	for i := 0; i < vectors; i++ {
		value := make([]float32, dimensions)
		for dimension := range value {
			value[dimension] = rng.Float32()*2 - 1
		}
		require.NoError(b, index.Add(fmt.Sprintf("vector-%05d", i), vector.Normalize(value)))
	}
	query := make([]float32, dimensions)
	for dimension := range query {
		query[dimension] = rng.Float32()*2 - 1
	}
	generator := NewHNSWCandidateGen(index)

	for _, factor := range []int{1, defaultHNSWSearchBeamFactor} {
		b.Run(fmt.Sprintf("factor=%d", factor), func(b *testing.B) {
			index.setSearchBeamFactor(factor)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				candidates, err := generator.SearchCandidates(context.Background(), query, limit, -1)
				if err != nil {
					b.Fatal(err)
				}
				if len(candidates) != limit {
					b.Fatalf("got %d candidates, want %d", len(candidates), limit)
				}
			}
		})
	}
}

func BenchmarkHNSWSearchLexicalEntryPoints(b *testing.B) {
	const (
		dimensions = 128
		vectors    = 10_000
		limit      = 200
	)
	config := DefaultHNSWConfig()
	index := NewHNSWIndex(dimensions, config)
	rng := rand.New(rand.NewSource(433))
	for i := 0; i < vectors; i++ {
		value := make([]float32, dimensions)
		for dimension := range value {
			value[dimension] = rng.Float32()*2 - 1
		}
		require.NoError(b, index.Add(fmt.Sprintf("vector-%05d", i), vector.Normalize(value)))
	}
	query := make([]float32, dimensions)
	for dimension := range query {
		query[dimension] = rng.Float32()*2 - 1
	}
	entryIDs := make([]string, 32)
	for i := range entryIDs {
		entryIDs[i] = fmt.Sprintf("vector-%05d", i*97)
	}

	for _, benchmark := range []struct {
		name    string
		entries []string
	}{
		{name: "vector_entry", entries: nil},
		{name: "bm25_entries", entries: entryIDs},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				results, err := index.SearchWithEfFromEntries(context.Background(), query, limit, -1, limit, benchmark.entries)
				if err != nil {
					b.Fatal(err)
				}
				if len(results) != limit {
					b.Fatalf("got %d results, want %d", len(results), limit)
				}
			}
		})
	}
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

				id := string(rune('a'+i%26)) + string(rune(i))
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

		id := string(rune('a'+i%26)) + string(rune(i))
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

// A beam that covers the whole index but is cut to the limit isn't
// exhausted: the vectors past the cut weren't returned. The full set, or a
// short one, is.
func TestHNSWCandidateGenerationCutToTheLimitIsNotExhausted(t *testing.T) {
	index := NewHNSWIndex(4, DefaultHNSWConfig())
	for i := 0; i < 60; i++ {
		require.NoError(t, index.Add(fmt.Sprintf("v%02d", i), vector.Normalize([]float32{1, float32(i) * 0.01, 0, 0})))
	}
	generator := NewHNSWCandidateGen(index)
	query := []float32{1, 0, 0, 0}
	for limit, wantExhausted := range map[int]bool{10: false, 59: false, 60: true, 100: true} {
		candidates, exhausted, err := generator.searchCandidatesWithExhaustion(context.Background(), query, limit, -1)
		require.NoError(t, err)
		require.Len(t, candidates, min(limit, 60))
		require.Equal(t, wantExhausted, exhausted, "limit %d", limit)
	}
}
