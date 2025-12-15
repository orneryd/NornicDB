//go:build !amd64 && !arm64

package simd

import (
	"math"

	"github.com/viterin/vek/vek32"
)

// Generic fallback implementations using viterin/vek library.
// On platforms without AVX2/NEON, vek32 uses optimized pure Go implementations
// that are still faster than naive loops due to better memory access patterns.

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
	return RuntimeInfo{
		Implementation: ImplGeneric,
		Features:       info.CPUFeatures,
		Accelerated:    info.Acceleration,
	}
}
