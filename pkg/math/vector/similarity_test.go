package vector

import (
	"math"
	"testing"
)

func TestCosineSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float64
		epsilon  float64
	}{
		{
			name:     "identical vectors",
			a:        []float32{1.0, 0.0, 0.0},
			b:        []float32{1.0, 0.0, 0.0},
			expected: 1.0,
			epsilon:  0.001,
		},
		{
			name:     "orthogonal vectors",
			a:        []float32{1.0, 0.0, 0.0},
			b:        []float32{0.0, 1.0, 0.0},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "opposite vectors",
			a:        []float32{1.0, 0.0, 0.0},
			b:        []float32{-1.0, 0.0, 0.0},
			expected: -1.0,
			epsilon:  0.001,
		},
		{
			name:     "similar vectors",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{4.0, 5.0, 6.0},
			expected: 0.9746318461970762,
			epsilon:  0.001,
		},
		{
			name:     "empty vectors",
			a:        []float32{},
			b:        []float32{},
			expected: 0,
			epsilon:  0.001,
		},
		{
			name:     "mismatched dimensions",
			a:        []float32{1.0, 2.0},
			b:        []float32{1.0, 2.0, 3.0},
			expected: 0,
			epsilon:  0.001,
		},
		{
			name:     "zero vector",
			a:        []float32{0.0, 0.0, 0.0},
			b:        []float32{1.0, 2.0, 3.0},
			expected: 0,
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CosineSimilarity(tt.a, tt.b)
			if math.Abs(result-tt.expected) > tt.epsilon {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestCosineSimilarityFloat64(t *testing.T) {
	tests := []struct {
		name     string
		a        []float64
		b        []float64
		expected float64
		epsilon  float64
	}{
		{
			name:     "identical vectors",
			a:        []float64{1.0, 0.0, 0.0},
			b:        []float64{1.0, 0.0, 0.0},
			expected: 1.0,
			epsilon:  0.001,
		},
		{
			name:     "orthogonal vectors",
			a:        []float64{1.0, 0.0, 0.0},
			b:        []float64{0.0, 1.0, 0.0},
			expected: 0.0,
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CosineSimilarityFloat64(tt.a, tt.b)
			if math.Abs(result-tt.expected) > tt.epsilon {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestCosineSimilarityGPU(t *testing.T) {
	a := []float32{1.0, 2.0, 3.0}
	b := []float32{4.0, 5.0, 6.0}
	
	result := CosineSimilarityGPU(a, b)
	expected := float32(0.9746318)
	
	if math.Abs(float64(result-expected)) > 0.001 {
		t.Errorf("expected %f, got %f", expected, result)
	}
}

func TestDotProduct(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float64
	}{
		{
			name:     "simple dot product",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{4.0, 5.0, 6.0},
			expected: 32.0,
		},
		{
			name:     "orthogonal vectors",
			a:        []float32{1.0, 0.0},
			b:        []float32{0.0, 1.0},
			expected: 0.0,
		},
		{
			name:     "mismatched dimensions",
			a:        []float32{1.0, 2.0},
			b:        []float32{1.0},
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DotProduct(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestEuclideanSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float64
		epsilon  float64
	}{
		{
			name:     "identical vectors",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{1.0, 2.0, 3.0},
			expected: 1.0,
			epsilon:  0.001,
		},
		{
			name:     "distant vectors",
			a:        []float32{0.0, 0.0},
			b:        []float32{3.0, 4.0},
			expected: 1.0 / 6.0, // 1 / (1 + 5)
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EuclideanSimilarity(tt.a, tt.b)
			if math.Abs(result-tt.expected) > tt.epsilon {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	t.Run("normalizes vector to unit length", func(t *testing.T) {
		vec := []float32{3.0, 4.0}
		result := Normalize(vec)
		
		// Expected: [0.6, 0.8]
		if math.Abs(float64(result[0]-0.6)) > 0.001 {
			t.Errorf("expected [0] = 0.6, got %f", result[0])
		}
		if math.Abs(float64(result[1]-0.8)) > 0.001 {
			t.Errorf("expected [1] = 0.8, got %f", result[1])
		}
		
		// Original should be unchanged
		if vec[0] != 3.0 || vec[1] != 4.0 {
			t.Error("original vector was modified")
		}
	})

	t.Run("zero vector returns zero vector", func(t *testing.T) {
		vec := []float32{0.0, 0.0, 0.0}
		result := Normalize(vec)
		
		for i, v := range result {
			if v != 0.0 {
				t.Errorf("expected [%d] = 0, got %f", i, v)
			}
		}
	})
}

func TestNormalizeInPlace(t *testing.T) {
	t.Run("normalizes vector in place", func(t *testing.T) {
		vec := []float32{3.0, 4.0}
		NormalizeInPlace(vec)
		
		// Expected: [0.6, 0.8]
		if math.Abs(float64(vec[0]-0.6)) > 0.001 {
			t.Errorf("expected [0] = 0.6, got %f", vec[0])
		}
		if math.Abs(float64(vec[1]-0.8)) > 0.001 {
			t.Errorf("expected [1] = 0.8, got %f", vec[1])
		}
	})

	t.Run("zero vector unchanged", func(t *testing.T) {
		vec := []float32{0.0, 0.0}
		NormalizeInPlace(vec)
		
		if vec[0] != 0.0 || vec[1] != 0.0 {
			t.Error("zero vector should remain unchanged")
		}
	})
}

func BenchmarkCosineSimilarity(b *testing.B) {
	a := make([]float32, 1024)
	vecB := make([]float32, 1024)
	for i := range a {
		a[i] = float32(i) / 1024
		vecB[i] = float32(1024-i) / 1024
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineSimilarity(a, vecB)
	}
}

func BenchmarkCosineSimilarityGPU(b *testing.B) {
	a := make([]float32, 1024)
	vecB := make([]float32, 1024)
	for i := range a {
		a[i] = float32(i) / 1024
		vecB[i] = float32(1024-i) / 1024
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineSimilarityGPU(a, vecB)
	}
}

func BenchmarkNormalize(b *testing.B) {
	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Normalize(vec)
	}
}

func BenchmarkNormalizeInPlace(b *testing.B) {
	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset for each iteration since we modify in place
		for j := range vec {
			vec[j] = float32(j)
		}
		NormalizeInPlace(vec)
	}
}
