package vector

import (
	"math"
	"testing"
)

func TestCosineSimilaritySIMD(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
		epsilon  float32
	}{
		{
			name:     "identical",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 2, 3},
			expected: 1.0,
			epsilon:  0.001,
		},
		{
			name:     "orthogonal",
			a:        []float32{1, 0, 0},
			b:        []float32{0, 1, 0},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "opposite",
			a:        []float32{1, 0, 0},
			b:        []float32{-1, 0, 0},
			expected: -1.0,
			epsilon:  0.001,
		},
		{
			name:     "similar",
			a:        []float32{1, 2, 3},
			b:        []float32{4, 5, 6},
			expected: 0.9746318,
			epsilon:  0.001,
		},
		{
			name:     "empty",
			a:        []float32{},
			b:        []float32{},
			expected: 0.0,
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CosineSimilaritySIMD(tt.a, tt.b)
			if math.Abs(float64(result-tt.expected)) > float64(tt.epsilon) {
				t.Errorf("CosineSimilaritySIMD() = %v, want %v (±%v)", result, tt.expected, tt.epsilon)
			}
		})
	}
}

func TestDotProductSIMD(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
	}{
		{
			name:     "simple",
			a:        []float32{1, 2, 3},
			b:        []float32{4, 5, 6},
			expected: 32.0, // 4+10+18
		},
		{
			name:     "orthogonal",
			a:        []float32{1, 0},
			b:        []float32{0, 1},
			expected: 0.0,
		},
		{
			name:     "empty",
			a:        []float32{},
			b:        []float32{},
			expected: 0.0,
		},
		{
			name:     "mismatched length",
			a:        []float32{1, 2},
			b:        []float32{1},
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DotProductSIMD(tt.a, tt.b)
			if math.Abs(float64(result-tt.expected)) > 0.001 {
				t.Errorf("DotProductSIMD() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEuclideanDistanceSIMD(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
		epsilon  float32
	}{
		{
			name:     "3-4-5 triangle",
			a:        []float32{0, 0},
			b:        []float32{3, 4},
			expected: 5.0,
			epsilon:  0.001,
		},
		{
			name:     "identical",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 2, 3},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "empty",
			a:        []float32{},
			b:        []float32{},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "mismatched",
			a:        []float32{1, 2},
			b:        []float32{1},
			expected: 0.0,
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EuclideanDistanceSIMD(tt.a, tt.b)
			if math.Abs(float64(result-tt.expected)) > float64(tt.epsilon) {
				t.Errorf("EuclideanDistanceSIMD() = %v, want %v (±%v)", result, tt.expected, tt.epsilon)
			}
		})
	}
}

func TestEuclideanSimilarityFloat64(t *testing.T) {
	tests := []struct {
		name     string
		a        []float64
		b        []float64
		expected float64
		epsilon  float64
	}{
		{
			name:     "identical",
			a:        []float64{1, 2, 3},
			b:        []float64{1, 2, 3},
			expected: 1.0,
			epsilon:  0.001,
		},
		{
			name:     "3-4-5 distance",
			a:        []float64{0, 0},
			b:        []float64{3, 4},
			expected: 1.0 / 6.0, // 1 / (1+5)
			epsilon:  0.001,
		},
		{
			name:     "empty",
			a:        []float64{},
			b:        []float64{},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "mismatched",
			a:        []float64{1, 2},
			b:        []float64{1},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "unit distance",
			a:        []float64{0, 0, 0},
			b:        []float64{1, 0, 0},
			expected: 0.5, // 1 / (1+1)
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EuclideanSimilarityFloat64(tt.a, tt.b)
			if math.Abs(result-tt.expected) > tt.epsilon {
				t.Errorf("EuclideanSimilarityFloat64() = %v, want %v (±%v)", result, tt.expected, tt.epsilon)
			}
		})
	}
}

func TestCosineSimilarityFloat64_Extended(t *testing.T) {
	tests := []struct {
		name     string
		a        []float64
		b        []float64
		expected float64
		epsilon  float64
	}{
		{
			name:     "similar",
			a:        []float64{1, 2, 3},
			b:        []float64{4, 5, 6},
			expected: 0.9746318,
			epsilon:  0.001,
		},
		{
			name:     "mismatched dimensions",
			a:        []float64{1, 2},
			b:        []float64{1, 2, 3},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "zero vector a",
			a:        []float64{0, 0, 0},
			b:        []float64{1, 2, 3},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "zero vector b",
			a:        []float64{1, 2, 3},
			b:        []float64{0, 0, 0},
			expected: 0.0,
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CosineSimilarityFloat64(tt.a, tt.b)
			if math.Abs(result-tt.expected) > tt.epsilon {
				t.Errorf("CosineSimilarityFloat64() = %v, want %v (±%v)", result, tt.expected, tt.epsilon)
			}
		})
	}
}

func TestEuclideanSimilarity_Extended(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float64
		epsilon  float64
	}{
		{
			name:     "empty",
			a:        []float32{},
			b:        []float32{},
			expected: 0.0,
			epsilon:  0.001,
		},
		{
			name:     "mismatched",
			a:        []float32{1, 2},
			b:        []float32{1},
			expected: 0.0,
			epsilon:  0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EuclideanSimilarity(tt.a, tt.b)
			if math.Abs(result-tt.expected) > tt.epsilon {
				t.Errorf("EuclideanSimilarity() = %v, want %v (±%v)", result, tt.expected, tt.epsilon)
			}
		})
	}
}
