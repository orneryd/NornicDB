package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// Benchmark vector sizes typical for embeddings
var benchmarkSizes = []int{128, 256, 384, 512, 768, 1024, 1536, 3072}

// generateTestVectors creates random float32 vectors for benchmarking
func generateTestVectors(size int) ([]float32, []float32) {
	a := make([]float32, size)
	b := make([]float32, size)
	for i := 0; i < size; i++ {
		a[i] = rand.Float32()*2 - 1 // [-1, 1]
		b[i] = rand.Float32()*2 - 1
	}
	return a, b
}

// Reference implementations for comparison (pure Go, no optimization)
func dotProductReference(a, b []float32) float32 {
	sum := float32(0)
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

func cosineSimilarityReference(a, b []float32) float32 {
	dot := float32(0)
	normA := float32(0)
	normB := float32(0)
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(normA*normB)))
}

func euclideanDistanceReference(a, b []float32) float32 {
	sum := float32(0)
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

func normReference(v []float32) float32 {
	sum := float32(0)
	for i := range v {
		sum += v[i] * v[i]
	}
	return float32(math.Sqrt(float64(sum)))
}

// BenchmarkDotProduct benchmarks dot product at various vector sizes
func BenchmarkDotProduct(b *testing.B) {
	for _, size := range benchmarkSizes {
		a, bv := generateTestVectors(size)
		name := fmt.Sprintf("%d", size)

		b.Run("SIMD-"+name, func(b *testing.B) {
			b.SetBytes(int64(size * 4 * 2)) // 2 vectors * 4 bytes per float32
			for i := 0; i < b.N; i++ {
				_ = DotProduct(a, bv)
			}
		})

		b.Run("Reference-"+name, func(b *testing.B) {
			b.SetBytes(int64(size * 4 * 2))
			for i := 0; i < b.N; i++ {
				_ = dotProductReference(a, bv)
			}
		})
	}
}

// BenchmarkCosineSimilarity benchmarks cosine similarity at various vector sizes
func BenchmarkCosineSimilarity(b *testing.B) {
	for _, size := range benchmarkSizes {
		a, bv := generateTestVectors(size)
		name := fmt.Sprintf("%d", size)

		b.Run("SIMD-"+name, func(b *testing.B) {
			b.SetBytes(int64(size * 4 * 2))
			for i := 0; i < b.N; i++ {
				_ = CosineSimilarity(a, bv)
			}
		})

		b.Run("Reference-"+name, func(b *testing.B) {
			b.SetBytes(int64(size * 4 * 2))
			for i := 0; i < b.N; i++ {
				_ = cosineSimilarityReference(a, bv)
			}
		})
	}
}

// BenchmarkEuclideanDistance benchmarks Euclidean distance at various vector sizes
func BenchmarkEuclideanDistance(b *testing.B) {
	for _, size := range benchmarkSizes {
		a, bv := generateTestVectors(size)
		name := fmt.Sprintf("%d", size)

		b.Run("SIMD-"+name, func(b *testing.B) {
			b.SetBytes(int64(size * 4 * 2))
			for i := 0; i < b.N; i++ {
				_ = EuclideanDistance(a, bv)
			}
		})

		b.Run("Reference-"+name, func(b *testing.B) {
			b.SetBytes(int64(size * 4 * 2))
			for i := 0; i < b.N; i++ {
				_ = euclideanDistanceReference(a, bv)
			}
		})
	}
}

// BenchmarkNorm benchmarks vector norm at various vector sizes
func BenchmarkNorm(b *testing.B) {
	for _, size := range benchmarkSizes {
		v, _ := generateTestVectors(size)
		name := fmt.Sprintf("%d", size)

		b.Run("SIMD-"+name, func(b *testing.B) {
			b.SetBytes(int64(size * 4))
			for i := 0; i < b.N; i++ {
				_ = Norm(v)
			}
		})

		b.Run("Reference-"+name, func(b *testing.B) {
			b.SetBytes(int64(size * 4))
			for i := 0; i < b.N; i++ {
				_ = normReference(v)
			}
		})
	}
}

// BenchmarkTypicalEmbedding benchmarks typical embedding operations
func BenchmarkTypicalEmbedding(b *testing.B) {
	// Common embedding sizes: 384 (MiniLM), 768 (BERT), 1536 (OpenAI)
	sizes := map[string]int{
		"MiniLM-384":  384,
		"BERT-768":    768,
		"OpenAI-1536": 1536,
	}

	for name, size := range sizes {
		query, _ := generateTestVectors(size)

		// Generate a "database" of vectors to search
		const numVectors = 1000
		vectors := make([][]float32, numVectors)
		for i := 0; i < numVectors; i++ {
			vectors[i], _ = generateTestVectors(size)
		}

		b.Run("Search-"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var maxSim float32 = -1
				var maxIdx int
				for j, v := range vectors {
					sim := CosineSimilarity(query, v)
					if sim > maxSim {
						maxSim = sim
						maxIdx = j
					}
				}
				_ = maxIdx
			}
		})
	}
}

// BenchmarkMemoryBandwidth measures effective memory bandwidth
func BenchmarkMemoryBandwidth(b *testing.B) {
	// Large vectors to measure memory bandwidth
	sizes := []int{8192, 16384, 32768, 65536}

	for _, size := range sizes {
		a, bv := generateTestVectors(size)
		bytes := int64(size * 4 * 2)
		name := fmt.Sprintf("%dK", size/1024)

		b.Run("DotProduct-"+name, func(b *testing.B) {
			b.SetBytes(bytes)
			for i := 0; i < b.N; i++ {
				_ = DotProduct(a, bv)
			}
		})
	}
}
