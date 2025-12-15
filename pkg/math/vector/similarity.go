// Package vector provides vector math operations for NornicDB.
//
// This package consolidates all vector similarity and distance calculations
// used throughout the codebase. Use these functions instead of implementing
// your own to ensure consistency and correctness.
//
// Main Functions:
//   - CosineSimilarity: Standard similarity for float32 vectors (most common)
//   - CosineSimilarityFloat64: High-precision similarity for float64 vectors
//   - CosineSimilaritySIMD: SIMD-accelerated similarity for high-throughput
//   - DotProduct: Dot product for float32 vectors
//   - EuclideanSimilarity: Distance-based similarity
//   - Normalize: Returns normalized copy of vector
//   - NormalizeInPlace: Normalizes vector in-place (modifies input)
package vector

import (
	"math"

	"github.com/orneryd/nornicdb/pkg/simd"
)

// CosineSimilarity calculates cosine similarity between two float32 vectors.
// Returns value in range [-1, 1] where 1 = identical, 0 = orthogonal, -1 = opposite.
//
// This is the STANDARD implementation for all non-GPU code.
// Uses float64 accumulation for high precision, even with float32 inputs.
//
// Example:
//
//	a := []float32{1.0, 2.0, 3.0}
//	b := []float32{4.0, 5.0, 6.0}
//	sim := CosineSimilarity(a, b)  // Returns 0.9746318461970762
func CosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dotProd, normA, normB float64
	for i := range a {
		dotProd += float64(a[i] * b[i])
		normA += float64(a[i] * a[i])
		normB += float64(b[i] * b[i])
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProd / (math.Sqrt(normA) * math.Sqrt(normB))
}

// CosineSimilarityFloat64 calculates cosine similarity between two float64 vectors.
// Returns value in range [-1, 1] where 1 = identical, 0 = orthogonal, -1 = opposite.
//
// Use this when working with float64 vectors directly.
//
// Example:
//
//	a := []float64{1.0, 2.0, 3.0}
//	b := []float64{4.0, 5.0, 6.0}
//	sim := CosineSimilarityFloat64(a, b)  // Returns 0.9746318461970762
func CosineSimilarityFloat64(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

// CosineSimilarityGPU calculates cosine similarity optimized for GPU operations.
// Returns float32 for GPU compatibility.
//
// Uses SIMD-accelerated implementation for maximum throughput.
// Slightly less accurate than CosineSimilarity() due to float32 accumulation.
//
// Use this for high-throughput batch operations.
func CosineSimilarityGPU(a, b []float32) float32 {
	return simd.CosineSimilarity(a, b)
}

// CosineSimilaritySIMD calculates cosine similarity using SIMD acceleration.
// Returns float32 for compatibility with embedding operations.
//
// This is the fastest implementation for high-throughput similarity searches.
// Uses platform-specific SIMD instructions (AVX2 on x86, NEON on ARM).
//
// On Apple Silicon: ~4-8x faster than scalar loop
// On x86 with AVX2: ~10x faster than scalar loop
//
// Example:
//
//	query := []float32{0.1, 0.2, 0.3, ...}  // 1536-dim embedding
//	doc := []float32{0.4, 0.5, 0.6, ...}
//	sim := CosineSimilaritySIMD(query, doc)  // Fast similarity calculation
func CosineSimilaritySIMD(a, b []float32) float32 {
	return simd.CosineSimilarity(a, b)
}

// DotProduct calculates the dot product of two float32 vectors.
// Returns float64 for API compatibility.
//
// Uses SIMD acceleration internally for maximum throughput.
// For normalized vectors, dot product equals cosine similarity.
//
// Example:
//
//	a := []float32{1.0, 2.0, 3.0}
//	b := []float32{4.0, 5.0, 6.0}
//	dot := DotProduct(a, b)  // Returns 32.0
func DotProduct(a, b []float32) float64 {
	return float64(simd.DotProduct(a, b))
}

// DotProductSIMD calculates the dot product using SIMD acceleration.
// Returns float32 for maximum performance in hot paths.
//
// This is the fastest implementation for normalized vector comparisons.
// Use this in tight loops where float32 precision is acceptable.
//
// Example:
//
//	query := []float32{0.1, 0.2, 0.3, ...}
//	doc := []float32{0.4, 0.5, 0.6, ...}
//	sim := DotProductSIMD(query, doc)
func DotProductSIMD(a, b []float32) float32 {
	return simd.DotProduct(a, b)
}

// EuclideanSimilarity calculates similarity based on Euclidean distance.
// Returns value in range [0, 1] where 1 = identical, 0 = very different.
//
// Uses SIMD acceleration internally for the distance calculation.
// Formula: 1 / (1 + distance)
//
// Example:
//
//	a := []float32{1.0, 2.0, 3.0}
//	b := []float32{4.0, 5.0, 6.0}
//	sim := EuclideanSimilarity(a, b)  // Returns ~0.161
func EuclideanSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	dist := simd.EuclideanDistance(a, b)
	return 1.0 / (1.0 + float64(dist))
}

// EuclideanDistanceSIMD calculates Euclidean distance using SIMD acceleration.
// Returns float32 for maximum performance in hot paths.
//
// Example:
//
//	a := []float32{0, 0}
//	b := []float32{3, 4}
//	dist := EuclideanDistanceSIMD(a, b)  // Returns 5.0
func EuclideanDistanceSIMD(a, b []float32) float32 {
	return simd.EuclideanDistance(a, b)
}

// EuclideanSimilarityFloat64 calculates similarity for float64 vectors.
func EuclideanSimilarityFloat64(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var sum float64
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}

	return 1.0 / (1.0 + math.Sqrt(sum))
}

// Normalize returns a normalized copy of the vector.
// The input vector is not modified (immutable operation).
//
// Uses SIMD acceleration for the norm calculation.
//
// Example:
//
//	original := []float32{3.0, 4.0}
//	normalized := Normalize(original)  // Returns [0.6, 0.8]
//	// original is unchanged
func Normalize(vec []float32) []float32 {
	n := simd.Norm(vec)
	if n == 0 {
		result := make([]float32, len(vec))
		return result
	}

	invNorm := 1.0 / n
	normalized := make([]float32, len(vec))
	for i, v := range vec {
		normalized[i] = v * invNorm
	}
	return normalized
}

// NormalizeInPlace normalizes a vector in-place (modifies the input).
// After normalization, the vector has unit length (magnitude = 1).
//
// Uses SIMD acceleration for the norm calculation.
// WARNING: Modifies the input slice. Use Normalize() to preserve original.
//
// Example:
//
//	v := []float32{3.0, 4.0}
//	NormalizeInPlace(v)  // v is now [0.6, 0.8]
func NormalizeInPlace(v []float32) {
	simd.NormalizeInPlace(v)
}
