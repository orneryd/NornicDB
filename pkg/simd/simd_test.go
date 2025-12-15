package simd

import (
	"math"
	"testing"
)

const epsilon = 1e-5

func approxEqual(a, b, eps float32) bool {
	return math.Abs(float64(a-b)) < float64(eps)
}

func TestDotProduct(t *testing.T) {
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
			expected: 32, // 1*4 + 2*5 + 3*6
		},
		{
			name:     "zeros",
			a:        []float32{0, 0, 0},
			b:        []float32{0, 0, 0},
			expected: 0,
		},
		{
			name:     "empty",
			a:        []float32{},
			b:        []float32{},
			expected: 0,
		},
		{
			name:     "unit vectors",
			a:        []float32{1, 0, 0},
			b:        []float32{0, 1, 0},
			expected: 0, // perpendicular
		},
		{
			name:     "same vector",
			a:        []float32{3, 4},
			b:        []float32{3, 4},
			expected: 25, // 9 + 16
		},
		{
			name:     "negative",
			a:        []float32{-1, -2, -3},
			b:        []float32{4, 5, 6},
			expected: -32,
		},
		{
			name:     "large vector (for SIMD)",
			a:        make([]float32, 256),
			b:        make([]float32, 256),
			expected: 256, // 1*1 * 256
		},
	}

	// Initialize large vector test
	for i := range tests[len(tests)-1].a {
		tests[len(tests)-1].a[i] = 1
		tests[len(tests)-1].b[i] = 1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DotProduct(tt.a, tt.b)
			if !approxEqual(result, tt.expected, epsilon) {
				t.Errorf("DotProduct() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCosineSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
	}{
		{
			name:     "identical vectors",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 2, 3},
			expected: 1.0,
		},
		{
			name:     "opposite vectors",
			a:        []float32{1, 0, 0},
			b:        []float32{-1, 0, 0},
			expected: -1.0,
		},
		{
			name:     "perpendicular vectors",
			a:        []float32{1, 0, 0},
			b:        []float32{0, 1, 0},
			expected: 0.0,
		},
		{
			name:     "zero vector a",
			a:        []float32{0, 0, 0},
			b:        []float32{1, 2, 3},
			expected: 0.0,
		},
		{
			name:     "zero vector b",
			a:        []float32{1, 2, 3},
			b:        []float32{0, 0, 0},
			expected: 0.0,
		},
		{
			name:     "empty",
			a:        []float32{},
			b:        []float32{},
			expected: 0.0,
		},
		{
			name:     "scaled vectors (same direction)",
			a:        []float32{1, 2, 3},
			b:        []float32{2, 4, 6},
			expected: 1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CosineSimilarity(tt.a, tt.b)
			if !approxEqual(result, tt.expected, epsilon) {
				t.Errorf("CosineSimilarity() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEuclideanDistance(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
	}{
		{
			name:     "3-4-5 triangle",
			a:        []float32{0, 0},
			b:        []float32{3, 4},
			expected: 5.0,
		},
		{
			name:     "same point",
			a:        []float32{1, 2, 3},
			b:        []float32{1, 2, 3},
			expected: 0.0,
		},
		{
			name:     "empty",
			a:        []float32{},
			b:        []float32{},
			expected: 0.0,
		},
		{
			name:     "unit distance",
			a:        []float32{0, 0, 0},
			b:        []float32{1, 0, 0},
			expected: 1.0,
		},
		{
			name:     "negative coords",
			a:        []float32{-3, -4},
			b:        []float32{0, 0},
			expected: 5.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EuclideanDistance(tt.a, tt.b)
			if !approxEqual(result, tt.expected, epsilon) {
				t.Errorf("EuclideanDistance() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNorm(t *testing.T) {
	tests := []struct {
		name     string
		v        []float32
		expected float32
	}{
		{
			name:     "3-4-5 triangle",
			v:        []float32{3, 4},
			expected: 5.0,
		},
		{
			name:     "unit vector",
			v:        []float32{1, 0, 0},
			expected: 1.0,
		},
		{
			name:     "zero vector",
			v:        []float32{0, 0, 0},
			expected: 0.0,
		},
		{
			name:     "empty",
			v:        []float32{},
			expected: 0.0,
		},
		{
			name:     "negative",
			v:        []float32{-3, -4},
			expected: 5.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Norm(tt.v)
			if !approxEqual(result, tt.expected, epsilon) {
				t.Errorf("Norm() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNormalizeInPlace(t *testing.T) {
	tests := []struct {
		name     string
		v        []float32
		expected []float32
	}{
		{
			name:     "3-4 vector",
			v:        []float32{3, 4},
			expected: []float32{0.6, 0.8},
		},
		{
			name:     "unit vector (unchanged)",
			v:        []float32{1, 0, 0},
			expected: []float32{1, 0, 0},
		},
		{
			name:     "zero vector (unchanged)",
			v:        []float32{0, 0, 0},
			expected: []float32{0, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := make([]float32, len(tt.v))
			copy(v, tt.v)
			NormalizeInPlace(v)

			for i := range v {
				if !approxEqual(v[i], tt.expected[i], epsilon) {
					t.Errorf("NormalizeInPlace()[%d] = %v, want %v", i, v[i], tt.expected[i])
				}
			}

			// Verify norm is 1 (unless zero vector)
			if Norm(tt.v) > 0 {
				norm := Norm(v)
				if !approxEqual(norm, 1.0, epsilon) {
					t.Errorf("Normalized vector norm = %v, want 1.0", norm)
				}
			}
		})
	}
}

func TestInfo(t *testing.T) {
	info := Info()

	// Basic sanity checks
	if info.Implementation == "" {
		t.Error("Implementation should not be empty")
	}

	// On any platform, we should have some implementation
	validImpls := map[Implementation]bool{
		ImplGeneric: true,
		ImplAVX2:    true,
		ImplNEON:    true,
	}

	if !validImpls[info.Implementation] {
		t.Errorf("Unknown implementation: %s", info.Implementation)
	}

	t.Logf("SIMD Info: %s (accelerated=%v, features=%v)",
		info.Implementation, info.Accelerated, info.Features)
}

// TestLargeVectors tests SIMD paths with large vectors
func TestLargeVectors(t *testing.T) {
	sizes := []int{16, 32, 64, 128, 256, 512, 768, 1024, 1536}

	for _, size := range sizes {
		t.Run(string(rune(size)), func(t *testing.T) {
			a := make([]float32, size)
			b := make([]float32, size)

			// Fill with known pattern
			for i := 0; i < size; i++ {
				a[i] = float32(i%10) / 10.0
				b[i] = float32((i+5)%10) / 10.0
			}

			// Just verify no panics and reasonable values
			dot := DotProduct(a, b)
			if math.IsNaN(float64(dot)) || math.IsInf(float64(dot), 0) {
				t.Errorf("DotProduct returned invalid value: %v", dot)
			}

			cos := CosineSimilarity(a, b)
			if math.IsNaN(float64(cos)) || cos < -1.0 || cos > 1.0 {
				t.Errorf("CosineSimilarity returned invalid value: %v", cos)
			}

			dist := EuclideanDistance(a, b)
			if math.IsNaN(float64(dist)) || dist < 0 {
				t.Errorf("EuclideanDistance returned invalid value: %v", dist)
			}

			norm := Norm(a)
			if math.IsNaN(float64(norm)) || norm < 0 {
				t.Errorf("Norm returned invalid value: %v", norm)
			}
		})
	}
}

// TestEdgeCases tests boundary conditions
func TestEdgeCases(t *testing.T) {
	t.Run("misaligned sizes (not multiple of 4 or 8)", func(t *testing.T) {
		for size := 1; size <= 17; size++ {
			a := make([]float32, size)
			b := make([]float32, size)
			for i := 0; i < size; i++ {
				a[i] = 1.0
				b[i] = 1.0
			}

			dot := DotProduct(a, b)
			expected := float32(size)
			if !approxEqual(dot, expected, epsilon) {
				t.Errorf("size=%d: DotProduct() = %v, want %v", size, dot, expected)
			}
		}
	})

	t.Run("very small values", func(t *testing.T) {
		a := []float32{1e-20, 1e-20, 1e-20}
		b := []float32{1e-20, 1e-20, 1e-20}

		cos := CosineSimilarity(a, b)
		if math.IsNaN(float64(cos)) {
			t.Error("CosineSimilarity returned NaN for small values")
		}
	})

	t.Run("large values", func(t *testing.T) {
		// Use moderate large values that won't overflow float32
		a := []float32{1e5, 1e5, 1e5}
		b := []float32{1e5, 1e5, 1e5}

		cos := CosineSimilarity(a, b)
		if !approxEqual(cos, 1.0, 1e-3) {
			t.Errorf("CosineSimilarity() = %v, want ~1.0", cos)
		}
	})
}
