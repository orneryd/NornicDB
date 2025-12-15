//go:build darwin && cgo && !nometal
// +build darwin,cgo,!nometal

package simd

import (
	"math"
	"math/rand"
	"testing"
)

func TestMetalAvailable(t *testing.T) {
	available := MetalAvailable()
	t.Logf("Metal available: %v", available)
	// Don't fail if not available - just skip GPU tests
}

func TestMetalDotProduct(t *testing.T) {
	if !MetalAvailable() {
		t.Skip("Metal not available")
	}

	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
	}{
		{
			name:     "large vector",
			a:        make([]float32, 8192),
			b:        make([]float32, 8192),
			expected: 8192, // 1*1 * 8192
		},
	}

	// Initialize large vector test
	for i := range tests[0].a {
		tests[0].a[i] = 1
		tests[0].b[i] = 1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MetalDotProduct(tt.a, tt.b)
			if math.Abs(float64(result-tt.expected)) > 0.1 {
				t.Errorf("MetalDotProduct() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMetalCosineSimilarity(t *testing.T) {
	if !MetalAvailable() {
		t.Skip("Metal not available")
	}

	// Test with large identical vectors
	size := 8192
	a := make([]float32, size)
	b := make([]float32, size)
	for i := 0; i < size; i++ {
		a[i] = float32(i%100) / 100.0
		b[i] = float32(i%100) / 100.0
	}

	result := MetalCosineSimilarity(a, b)
	if math.Abs(float64(result-1.0)) > 0.001 {
		t.Errorf("MetalCosineSimilarity() = %v, want ~1.0", result)
	}
}

func TestMetalEuclideanDistance(t *testing.T) {
	if !MetalAvailable() {
		t.Skip("Metal not available")
	}

	// Test with large vectors
	size := 8192
	a := make([]float32, size)
	b := make([]float32, size)
	// All zeros vs all zeros should be 0
	result := MetalEuclideanDistance(a, b)
	if math.Abs(float64(result)) > 0.001 {
		t.Errorf("MetalEuclideanDistance() = %v, want 0", result)
	}
}

func TestMetalNorm(t *testing.T) {
	if !MetalAvailable() {
		t.Skip("Metal not available")
	}

	// Test with large unit vector
	size := 8192
	v := make([]float32, size)
	for i := 0; i < size; i++ {
		v[i] = 1.0
	}

	result := MetalNorm(v)
	expected := float32(math.Sqrt(float64(size)))
	if math.Abs(float64(result-expected)) > 0.1 {
		t.Errorf("MetalNorm() = %v, want %v", result, expected)
	}
}

func TestBatchCosineSimilarityMetal(t *testing.T) {
	if !MetalAvailable() {
		t.Skip("Metal not available")
	}

	numVectors := 10000
	dimensions := 768

	// Generate random embeddings
	embeddings := make([]float32, numVectors*dimensions)
	for i := range embeddings {
		embeddings[i] = rand.Float32()*2 - 1
	}

	// Generate random query
	query := make([]float32, dimensions)
	for i := range query {
		query[i] = rand.Float32()*2 - 1
	}

	// Allocate scores
	scores := make([]float32, numVectors)

	// Run batch operation
	err := BatchCosineSimilarityMetal(embeddings, query, scores)
	if err != nil {
		t.Fatalf("BatchCosineSimilarityMetal failed: %v", err)
	}

	// Verify scores are in valid range
	for i, score := range scores {
		if score < -1.0 || score > 1.0 {
			t.Errorf("Score[%d] = %v, out of range [-1, 1]", i, score)
		}
	}

	// Verify against CPU implementation for first few
	for i := 0; i < 10; i++ {
		embStart := i * dimensions
		embEnd := embStart + dimensions
		cpuScore := CosineSimilarity(embeddings[embStart:embEnd], query)
		if math.Abs(float64(scores[i]-cpuScore)) > 0.001 {
			t.Errorf("Score[%d]: GPU=%v, CPU=%v", i, scores[i], cpuScore)
		}
	}
}

func BenchmarkMetalBatchCosineSimilarity(b *testing.B) {
	if !MetalAvailable() {
		b.Skip("Metal not available")
	}

	numVectors := 100000
	dimensions := 768

	embeddings := make([]float32, numVectors*dimensions)
	for i := range embeddings {
		embeddings[i] = rand.Float32()*2 - 1
	}

	query := make([]float32, dimensions)
	for i := range query {
		query[i] = rand.Float32()*2 - 1
	}

	scores := make([]float32, numVectors)

	b.ResetTimer()
	b.SetBytes(int64(numVectors * dimensions * 4))

	for i := 0; i < b.N; i++ {
		err := BatchCosineSimilarityMetal(embeddings, query, scores)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMetalVsCPUCosineSimilarity(b *testing.B) {
	if !MetalAvailable() {
		b.Skip("Metal not available")
	}

	sizes := []struct {
		name       string
		numVectors int
		dimensions int
	}{
		{"1K-768", 1000, 768},
		{"10K-768", 10000, 768},
		{"100K-768", 100000, 768},
		{"1K-1536", 1000, 1536},
		{"10K-1536", 10000, 1536},
	}

	for _, size := range sizes {
		embeddings := make([]float32, size.numVectors*size.dimensions)
		for i := range embeddings {
			embeddings[i] = rand.Float32()*2 - 1
		}

		query := make([]float32, size.dimensions)
		for i := range query {
			query[i] = rand.Float32()*2 - 1
		}

		scores := make([]float32, size.numVectors)

		b.Run("Metal-"+size.name, func(b *testing.B) {
			b.SetBytes(int64(size.numVectors * size.dimensions * 4))
			for i := 0; i < b.N; i++ {
				_ = BatchCosineSimilarityMetal(embeddings, query, scores)
			}
		})

		b.Run("CPU-"+size.name, func(b *testing.B) {
			b.SetBytes(int64(size.numVectors * size.dimensions * 4))
			for i := 0; i < b.N; i++ {
				for j := 0; j < size.numVectors; j++ {
					start := j * size.dimensions
					end := start + size.dimensions
					scores[j] = CosineSimilarity(embeddings[start:end], query)
				}
			}
		})
	}
}
