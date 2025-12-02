package convert

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
		ok       bool
	}{
		// Direct numeric types
		{"float64", 3.14, 3.14, true},
		{"float32", float32(2.5), 2.5, true},
		{"int", 42, 42.0, true},
		{"int64", int64(99), 99.0, true},
		{"int32", int32(50), 50.0, true},
		{"uint", uint(10), 10.0, true},
		{"uint64", uint64(100), 100.0, true},
		{"uint32", uint32(25), 25.0, true},

		// String parsing
		{"string decimal", "3.14", 3.14, true},
		{"string negative", "-2.5", -2.5, true},
		{"string scientific", "1.5e-3", 0.0015, true},
		{"string integer", "42", 42.0, true},

		// Error cases
		{"string invalid", "hello", 0, false},
		{"string empty", "", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
		{"slice", []int{1, 2}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ToFloat64(tt.input)
			assert.Equal(t, tt.ok, ok, "ok mismatch")
			if ok {
				assert.InDelta(t, tt.expected, got, 0.0001, "value mismatch")
			}
		})
	}

	// Special case: NaN
	t.Run("string NaN", func(t *testing.T) {
		got, ok := ToFloat64("NaN")
		assert.True(t, ok)
		assert.True(t, math.IsNaN(got))
	})

	// Special case: Inf
	t.Run("string Inf", func(t *testing.T) {
		got, ok := ToFloat64("Inf")
		assert.True(t, ok)
		assert.True(t, math.IsInf(got, 1))
	})
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
		ok       bool
	}{
		// Direct integer types
		{"int64", int64(99), 99, true},
		{"int", 42, 42, true},
		{"int32", int32(50), 50, true},
		{"uint", uint(10), 10, true},
		{"uint32", uint32(25), 25, true},
		{"uint64", uint64(100), 100, true},

		// Float conversion (truncation)
		{"float64", 3.7, 3, true},
		{"float64 negative", -3.7, -3, true},
		{"float32", float32(2.9), 2, true},

		// String parsing
		{"string integer", "42", 42, true},
		{"string negative", "-10", -10, true},
		{"string float", "3.7", 3, true},

		// Error cases
		{"string invalid", "hello", 0, false},
		{"string empty", "", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ToInt64(tt.input)
			assert.Equal(t, tt.ok, ok, "ok mismatch")
			if ok {
				assert.Equal(t, tt.expected, got, "value mismatch")
			}
		})
	}
}

func TestToFloat64Slice(t *testing.T) {
	t.Run("[]float64", func(t *testing.T) {
		input := []float64{1.0, 2.0, 3.0}
		got, ok := ToFloat64Slice(input)
		assert.True(t, ok)
		assert.Equal(t, input, got)
	})

	t.Run("[]float32", func(t *testing.T) {
		input := []float32{1.0, 2.0, 3.0}
		got, ok := ToFloat64Slice(input)
		assert.True(t, ok)
		assert.Equal(t, []float64{1.0, 2.0, 3.0}, got)
	})

	t.Run("[]interface{} numeric", func(t *testing.T) {
		input := []interface{}{1, 2.5, int64(3)}
		got, ok := ToFloat64Slice(input)
		assert.True(t, ok)
		assert.Equal(t, []float64{1.0, 2.5, 3.0}, got)
	})

	t.Run("[]interface{} with string", func(t *testing.T) {
		input := []interface{}{1, "2.5", 3}
		got, ok := ToFloat64Slice(input)
		assert.True(t, ok)
		assert.Equal(t, []float64{1.0, 2.5, 3.0}, got)
	})

	t.Run("[]interface{} invalid", func(t *testing.T) {
		input := []interface{}{1, "invalid", 3}
		got, ok := ToFloat64Slice(input)
		assert.False(t, ok)
		assert.Nil(t, got)
	})

	t.Run("invalid type", func(t *testing.T) {
		got, ok := ToFloat64Slice("not a slice")
		assert.False(t, ok)
		assert.Nil(t, got)
	})
}

func TestToFloat32Slice(t *testing.T) {
	t.Run("[]float32", func(t *testing.T) {
		input := []float32{1.0, 2.0, 3.0}
		got := ToFloat32Slice(input)
		assert.Equal(t, input, got)
	})

	t.Run("[]float64", func(t *testing.T) {
		input := []float64{1.0, 2.0, 3.0}
		got := ToFloat32Slice(input)
		assert.Equal(t, []float32{1.0, 2.0, 3.0}, got)
	})

	t.Run("[]interface{}", func(t *testing.T) {
		input := []interface{}{1, 2.5, int64(3)}
		got := ToFloat32Slice(input)
		assert.Equal(t, []float32{1.0, 2.5, 3.0}, got)
	})

	t.Run("invalid type", func(t *testing.T) {
		got := ToFloat32Slice("not a slice")
		assert.Nil(t, got)
	})
}

func TestToStringSlice(t *testing.T) {
	t.Run("[]string", func(t *testing.T) {
		input := []string{"a", "b", "c"}
		got := ToStringSlice(input)
		assert.Equal(t, input, got)
	})

	t.Run("[]interface{} strings", func(t *testing.T) {
		input := []interface{}{"a", "b", "c"}
		got := ToStringSlice(input)
		assert.Equal(t, []string{"a", "b", "c"}, got)
	})

	t.Run("[]interface{} mixed", func(t *testing.T) {
		input := []interface{}{"a", 1, "c"}
		got := ToStringSlice(input)
		assert.Nil(t, got)
	})

	t.Run("invalid type", func(t *testing.T) {
		got := ToStringSlice(123)
		assert.Nil(t, got)
	})
}

// Benchmarks
func BenchmarkToFloat64_Int(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToFloat64(42)
	}
}

func BenchmarkToFloat64_String(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToFloat64("3.14159")
	}
}

func BenchmarkToFloat32Slice(b *testing.B) {
	input := []interface{}{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	for i := 0; i < b.N; i++ {
		ToFloat32Slice(input)
	}
}
