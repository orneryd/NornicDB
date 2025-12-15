//go:build arm64 && !nosimd

package simd

import (
	"math"

	"github.com/viterin/vek/vek32"
)

// ARM64 NEON-optimized implementations using viterin/vek SIMD library.
// vek32 provides NEON SIMD assembly for float32 vectors on ARM64.
//
// Performance (from vek benchmarks on ARM64):
//   - vek32.Sum: ~6-10x faster than pure Go
//   - vek32.Dot: ~4-8x faster than pure Go
//   - Float32 operations generally 4-10x faster with SIMD

func dotProduct(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return vek32.Dot(a, b)
}

func cosineSimilarity(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	// vek32.CosineSimilarity returns NaN for zero vectors, we want 0
	result := vek32.CosineSimilarity(a, b)
	if math.IsNaN(float64(result)) {
		return 0
	}
	return result
}

func euclideanDistance(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return vek32.Distance(a, b)
}

func norm(v []float32) float32 {
	if len(v) == 0 {
		return 0
	}
	return vek32.Norm(v)
}

func normalizeInPlace(v []float32) {
	if len(v) == 0 {
		return
	}
	n := vek32.Norm(v)
	if n == 0 {
		return
	}
	vek32.DivNumber_Inplace(v, n)
}

func runtimeInfo() RuntimeInfo {
	info := vek32.Info()
	if info.Acceleration {
		return RuntimeInfo{
			Implementation: ImplNEON,
			Features:       info.CPUFeatures,
			Accelerated:    true,
		}
	}
	return RuntimeInfo{
		Implementation: ImplGeneric,
		Features:       info.CPUFeatures,
		Accelerated:    false,
	}
}
