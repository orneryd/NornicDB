//go:build darwin && cgo && !nometal

package search

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

// BenchmarkHNSWIndex_Search_Metal_BatchSizes benchmarks HNSW search with varying
// batch sizes to demonstrate Metal GPU performance improvements for large batches.
func BenchmarkHNSWIndex_Search_Metal_BatchSizes(b *testing.B) {
	index := NewHNSWIndex(128, DefaultHNSWConfig())

	// Pre-populate with 50000 vectors for realistic large-scale testing
	numVectors := 50000
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		index.Add(string(rune(i)), vec)
	}

	query := make([]float32, 128)
	for i := range query {
		query[i] = rand.Float32()
	}
	ctx := context.Background()

	// Test with different EfSearch values to vary candidate batch sizes
	efSearchValues := []int{50, 100, 200, 500, 1000}

	for _, efSearch := range efSearchValues {
		// Temporarily override EfSearch for this benchmark
		originalEfSearch := index.config.EfSearch
		index.config.EfSearch = efSearch

		b.Run(fmt.Sprintf("EfSearch=%d", efSearch), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Use SearchWithEf to control candidate batch size
				index.SearchWithEf(ctx, query, 10, 0.0, efSearch)
			}
		})

		// Restore original value
		index.config.EfSearch = originalEfSearch
	}
}

// BenchmarkHNSWIndex_Search_Metal_vs_CPU compares Metal GPU batch operations
// against CPU SIMD for different batch sizes.
func BenchmarkHNSWIndex_Search_Metal_vs_CPU(b *testing.B) {
	index := NewHNSWIndex(128, DefaultHNSWConfig())

	// Pre-populate with 50000 vectors
	numVectors := 50000
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		index.Add(string(rune(i)), vec)
	}

	query := make([]float32, 128)
	for i := range query {
		query[i] = rand.Float32()
	}

	// Generate candidate sets of varying sizes
	candidateSizes := []int{25, 50, 100, 200, 500, 1000}

	// Normalize query for batch operations
	normalizedQuery := make([]float32, 128)
	copy(normalizedQuery, query)
	// Simple normalization (in real code this uses vector.Normalize)
	sum := float32(0)
	for _, v := range normalizedQuery {
		sum += v * v
	}
	if sum > 0 {
		norm := float32(1.0 / math.Sqrt(float64(sum)))
		for i := range normalizedQuery {
			normalizedQuery[i] *= norm
		}
	}

	for _, numCandidates := range candidateSizes {
		// Generate candidate IDs (use sequential IDs for consistent testing)
		index.mu.RLock()
		candidates := make([]uint32, 0, numCandidates)
		maxID := uint32(len(index.nodeLevel))
		for i := 0; i < numCandidates && i < int(maxID); i++ {
			candidates = append(candidates, uint32(i))
		}
		index.mu.RUnlock()

		b.Run(fmt.Sprintf("BatchSize=%d", numCandidates), func(b *testing.B) {
			minSim32 := float32(0.0)

			b.Run("Metal", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = index.batchScoreCandidatesMetal(normalizedQuery, candidates, minSim32)
				}
			})

			b.Run("CPU", func(b *testing.B) {
				// Use direct CPU SIMD path (same as main search code)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					results := make([]ANNResult, 0, len(candidates))
					for _, candidateID := range candidates {
						if int(candidateID) >= len(index.nodeLevel) || index.deleted[candidateID] {
							continue
						}
						index.mu.RLock()
						similarity := vector.DotProductSIMD(normalizedQuery, index.vectorAtLocked(candidateID))
						index.mu.RUnlock()
						if similarity >= minSim32 {
							results = append(results, ANNResult{
								ID:    index.internalToID[candidateID],
								Score: similarity,
							})
						}
					}
					_ = results
				}
			})
		})
	}
}

// BenchmarkHNSWIndex_Search_LargeScale benchmarks HNSW search on very large
// datasets to show Metal GPU benefits in production-like scenarios.
func BenchmarkHNSWIndex_Search_LargeScale(b *testing.B) {
	sizes := []struct {
		name       string
		numVectors int
		efSearch   int
		k          int
	}{
		{"Small_10K_ef100", 10000, 100, 10},
		{"Medium_50K_ef200", 50000, 200, 10},
		{"Large_100K_ef500", 100000, 500, 10},
		{"XLarge_500K_ef1000", 500000, 1000, 10},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			index := NewHNSWIndex(128, DefaultHNSWConfig())
			index.config.EfSearch = size.efSearch

			// Pre-populate index
			for i := 0; i < size.numVectors; i++ {
				vec := make([]float32, 128)
				for j := range vec {
					vec[j] = rand.Float32()
				}
				index.Add(string(rune(i)), vec)
			}

			query := make([]float32, 128)
			for i := range query {
				query[i] = rand.Float32()
			}
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				index.Search(ctx, query, size.k, 0.0)
			}
		})
	}
}
