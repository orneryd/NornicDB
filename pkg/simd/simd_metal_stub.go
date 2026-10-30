//go:build !darwin || !cgo || nometal
// +build !darwin !cgo nometal

package simd

import "errors"

// Metal backend errors
var (
	ErrMetalNotAvailable = errors.New("simd/metal: Metal GPU not available (build without metal tag)")
)

// MetalAvailable returns false when Metal is not compiled in
func MetalAvailable() bool {
	return false
}

// MetalDotProduct falls back to CPU implementation
func MetalDotProduct(a, b []float32) float32 {
	return DotProduct(a, b)
}

// MetalCosineSimilarity falls back to CPU implementation
func MetalCosineSimilarity(a, b []float32) float32 {
	return CosineSimilarity(a, b)
}

// MetalEuclideanDistance falls back to CPU implementation
func MetalEuclideanDistance(a, b []float32) float32 {
	return EuclideanDistance(a, b)
}

// MetalNorm falls back to CPU implementation
func MetalNorm(v []float32) float32 {
	return Norm(v)
}

// BatchCosineSimilarityMetal returns error when Metal is not available
func BatchCosineSimilarityMetal(embeddings []float32, query []float32, scores []float32) error {
	return ErrMetalNotAvailable
}

// BatchDotProductMetal returns error when Metal is not available
func BatchDotProductMetal(embeddings []float32, query []float32, results []float32) error {
	return ErrMetalNotAvailable
}

// BatchEuclideanDistanceMetal returns error when Metal is not available
func BatchEuclideanDistanceMetal(embeddings []float32, query []float32, distances []float32) error {
	return ErrMetalNotAvailable
}

// BatchNormalizeMetal returns error when Metal is not available
func BatchNormalizeMetal(vectors []float32, numVectors, dimensions int) error {
	return ErrMetalNotAvailable
}
