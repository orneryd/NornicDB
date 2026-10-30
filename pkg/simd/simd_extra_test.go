package simd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// Exported API – zero-vector / zero-norm branches
// ============================================================================

func TestCosineSimilarity_ZeroVector_ReturnsZero(t *testing.T) {
	// Both zero → NaN from vek32, should be clamped to 0
	a := []float32{0, 0, 0}
	b := []float32{0, 0, 0}
	result := CosineSimilarity(a, b)
	assert.Equal(t, float32(0), result)
}

func TestCosineSimilarity_OneZero_ReturnsZero(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{1, 0, 0}
	result := CosineSimilarity(a, b)
	assert.Equal(t, float32(0), result)
}

func TestNormalizeInPlace_ZeroVector_NoChange(t *testing.T) {
	v := []float32{0, 0, 0}
	// Should not panic, should remain zeros
	NormalizeInPlace(v)
	assert.Equal(t, []float32{0, 0, 0}, v)
}

func TestNormalizeInPlace_Empty(t *testing.T) {
	var v []float32
	NormalizeInPlace(v) // no panic
}

func TestDotProduct_Empty(t *testing.T) {
	result := DotProduct([]float32{}, []float32{})
	assert.Equal(t, float32(0), result)
}

func TestEuclideanDistance_Empty(t *testing.T) {
	result := EuclideanDistance([]float32{}, []float32{})
	assert.Equal(t, float32(0), result)
}

func TestCosineSimilarity_Empty(t *testing.T) {
	result := CosineSimilarity([]float32{}, []float32{})
	assert.Equal(t, float32(0), result)
}

// ============================================================================
// BatchNormalize – CPU fallback (Metal not available in test)
// ============================================================================

func TestBatchNormalize_ZeroNormVector(t *testing.T) {
	// All-zero vector should remain zero after normalize
	vecs := []float32{0, 0, 0, 1, 0, 0}
	BatchNormalize(vecs, 2, 3)
	// first vector all-zero stays zero
	assert.Equal(t, float32(0), vecs[0])
	assert.Equal(t, float32(0), vecs[1])
	assert.Equal(t, float32(0), vecs[2])
}

func TestBatchCosineSimilarity_ZeroQueryVector(t *testing.T) {
	query := []float32{0, 0, 0}
	embeddings := []float32{1, 0, 0, 0, 1, 0}
	scores := make([]float32, 2)
	BatchCosineSimilarity(embeddings, query, scores)
	assert.Equal(t, float32(0), scores[0])
	assert.Equal(t, float32(0), scores[1])
}

func TestBatchDotProduct_ZeroQuery(t *testing.T) {
	query := []float32{0, 0, 0}
	embeddings := []float32{1, 2, 3, 4, 5, 6}
	results := make([]float32, 2)
	BatchDotProduct(embeddings, query, results)
	assert.Equal(t, float32(0), results[0])
	assert.Equal(t, float32(0), results[1])
}

func TestBatchEuclideanDistance_ZeroQuery(t *testing.T) {
	query := []float32{0, 0, 0}
	embeddings := []float32{1, 0, 0, 0, 1, 0}
	distances := make([]float32, 2)
	BatchEuclideanDistance(embeddings, query, distances)
	assert.InDelta(t, 1.0, distances[0], 0.001)
	assert.InDelta(t, 1.0, distances[1], 0.001)
}
