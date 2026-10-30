//go:build darwin && cgo && !nometal
// +build darwin,cgo,!nometal

package simd

import (
	"math"
	"math/rand"
	"testing"
)

func withMetalAvailability(t *testing.T, available bool) {
	t.Helper()
	initMetal()
	prev := metalAvailable
	metalAvailable = available
	t.Cleanup(func() {
		metalAvailable = prev
	})
}

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

func TestMetalFunctions_SmallVectorFallbackAndValidation(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}

	requireDot := DotProduct(a, b)
	requireCos := CosineSimilarity(a, b)
	requireDist := EuclideanDistance(a, b)
	requireNorm := Norm(a)

	if got := MetalDotProduct(a, b); math.Abs(float64(got-requireDot)) > 1e-5 {
		t.Fatalf("MetalDotProduct small-vector fallback = %v, want %v", got, requireDot)
	}
	if got := MetalCosineSimilarity(a, b); math.Abs(float64(got-requireCos)) > 1e-5 {
		t.Fatalf("MetalCosineSimilarity small-vector fallback = %v, want %v", got, requireCos)
	}
	if got := MetalEuclideanDistance(a, b); math.Abs(float64(got-requireDist)) > 1e-5 {
		t.Fatalf("MetalEuclideanDistance small-vector fallback = %v, want %v", got, requireDist)
	}
	if got := MetalNorm(a); math.Abs(float64(got-requireNorm)) > 1e-5 {
		t.Fatalf("MetalNorm small-vector fallback = %v, want %v", got, requireNorm)
	}

	if got := MetalDotProduct(nil, b); got != 0 {
		t.Fatalf("MetalDotProduct nil input = %v, want 0", got)
	}
	if got := MetalDotProduct(a, b[:2]); got != 0 {
		t.Fatalf("MetalDotProduct mismatched input = %v, want 0", got)
	}
	if got := MetalCosineSimilarity(nil, b); got != 0 {
		t.Fatalf("MetalCosineSimilarity nil input = %v, want 0", got)
	}
	if got := MetalCosineSimilarity(a, b[:2]); got != 0 {
		t.Fatalf("MetalCosineSimilarity mismatched input = %v, want 0", got)
	}
	if got := MetalEuclideanDistance(nil, b); got != 0 {
		t.Fatalf("MetalEuclideanDistance nil input = %v, want 0", got)
	}
	if got := MetalEuclideanDistance(a, b[:2]); got != 0 {
		t.Fatalf("MetalEuclideanDistance mismatched input = %v, want 0", got)
	}
	if got := MetalNorm(nil); got != 0 {
		t.Fatalf("MetalNorm nil input = %v, want 0", got)
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

func TestBatchMetalFunctions_NotAvailableAndNoOp(t *testing.T) {
	withMetalAvailability(t, false)

	embeddings := []float32{1, 0, 0, 1}
	query := []float32{1, 0}
	scores := make([]float32, 2)

	if err := BatchCosineSimilarityMetal(embeddings, query, scores); err != ErrMetalNotAvailable {
		t.Fatalf("BatchCosineSimilarityMetal err = %v, want %v", err, ErrMetalNotAvailable)
	}
	if err := BatchDotProductMetal(embeddings, query, scores); err != ErrMetalNotAvailable {
		t.Fatalf("BatchDotProductMetal err = %v, want %v", err, ErrMetalNotAvailable)
	}
	if err := BatchEuclideanDistanceMetal(embeddings, query, scores); err != ErrMetalNotAvailable {
		t.Fatalf("BatchEuclideanDistanceMetal err = %v, want %v", err, ErrMetalNotAvailable)
	}
	if err := BatchNormalizeMetal(embeddings, 2, 2); err != ErrMetalNotAvailable {
		t.Fatalf("BatchNormalizeMetal err = %v, want %v", err, ErrMetalNotAvailable)
	}
}

func TestBatchMetalFunctions_NoOpValidation(t *testing.T) {
	if !MetalAvailable() {
		t.Skip("Metal not available")
	}

	if err := BatchCosineSimilarityMetal([]float32{1, 2}, nil, []float32{9}); err != nil {
		t.Fatalf("BatchCosineSimilarityMetal empty query err = %v, want nil", err)
	}
	if err := BatchCosineSimilarityMetal([]float32{}, []float32{1, 2}, []float32{}); err != nil {
		t.Fatalf("BatchCosineSimilarityMetal empty embeddings err = %v, want nil", err)
	}
	if err := BatchCosineSimilarityMetal([]float32{1, 0, 0, 1}, []float32{1, 0}, []float32{0}); err != nil {
		t.Fatalf("BatchCosineSimilarityMetal short scores err = %v, want nil", err)
	}

	if err := BatchDotProductMetal([]float32{1, 2}, nil, []float32{9}); err != nil {
		t.Fatalf("BatchDotProductMetal empty query err = %v, want nil", err)
	}
	if err := BatchDotProductMetal([]float32{}, []float32{1, 2}, []float32{}); err != nil {
		t.Fatalf("BatchDotProductMetal empty embeddings err = %v, want nil", err)
	}
	if err := BatchDotProductMetal([]float32{1, 0, 0, 1}, []float32{1, 0}, []float32{0}); err != nil {
		t.Fatalf("BatchDotProductMetal short results err = %v, want nil", err)
	}

	if err := BatchEuclideanDistanceMetal([]float32{1, 2}, nil, []float32{9}); err != nil {
		t.Fatalf("BatchEuclideanDistanceMetal empty query err = %v, want nil", err)
	}
	if err := BatchEuclideanDistanceMetal([]float32{}, []float32{1, 2}, []float32{}); err != nil {
		t.Fatalf("BatchEuclideanDistanceMetal empty embeddings err = %v, want nil", err)
	}
	if err := BatchEuclideanDistanceMetal([]float32{1, 0, 0, 1}, []float32{1, 0}, []float32{0}); err != nil {
		t.Fatalf("BatchEuclideanDistanceMetal short distances err = %v, want nil", err)
	}

	if err := BatchNormalizeMetal([]float32{1, 2}, 0, 2); err != nil {
		t.Fatalf("BatchNormalizeMetal zero vectors err = %v, want nil", err)
	}
	if err := BatchNormalizeMetal([]float32{1, 2}, 1, 0); err != nil {
		t.Fatalf("BatchNormalizeMetal zero dimensions err = %v, want nil", err)
	}
	if err := BatchNormalizeMetal([]float32{1, 2}, 2, 2); err != nil {
		t.Fatalf("BatchNormalizeMetal short buffer err = %v, want nil", err)
	}
}

func TestBatchWrappers_CPUFallbackWhenMetalDisabled(t *testing.T) {
	withMetalAvailability(t, false)

	embeddings := []float32{
		1, 0, 0,
		0, 1, 0,
	}
	query := []float32{1, 0, 0}

	scores := make([]float32, 2)
	BatchCosineSimilarity(embeddings, query, scores)
	if !approxEqual(scores[0], 1.0, 1e-5) || !approxEqual(scores[1], 0.0, 1e-5) {
		t.Fatalf("BatchCosineSimilarity fallback scores = %v", scores)
	}

	results := make([]float32, 2)
	BatchDotProduct(embeddings, query, results)
	if !approxEqual(results[0], 1.0, 1e-5) || !approxEqual(results[1], 0.0, 1e-5) {
		t.Fatalf("BatchDotProduct fallback results = %v", results)
	}

	distances := make([]float32, 2)
	BatchEuclideanDistance(embeddings, query, distances)
	if !approxEqual(distances[0], 0.0, 1e-5) || !approxEqual(distances[1], float32(math.Sqrt2), 1e-5) {
		t.Fatalf("BatchEuclideanDistance fallback distances = %v", distances)
	}

	vectors := []float32{3, 4, 1, 0}
	BatchNormalize(vectors, 2, 2)
	if !approxEqual(vectors[0], 0.6, 1e-5) || !approxEqual(vectors[1], 0.8, 1e-5) {
		t.Fatalf("BatchNormalize fallback first vector = %v", vectors[:2])
	}
	if !approxEqual(vectors[2], 1.0, 1e-5) || !approxEqual(vectors[3], 0.0, 1e-5) {
		t.Fatalf("BatchNormalize fallback second vector = %v", vectors[2:])
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
