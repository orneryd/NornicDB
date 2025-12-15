//go:build !amd64 && !arm64

package simd

import "math"

// Pure Go fallback implementations for platforms without SIMD support.
// These implementations are based on github.com/viterin/vek's pure Go code.

func dotProduct(a, b []float32) float32 {
	sum := float32(0)
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

func cosineSimilarity(a, b []float32) float32 {
	dot := float32(0)
	normA := float32(0)
	normB := float32(0)
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(normA*normB)))
}

func euclideanDistance(a, b []float32) float32 {
	sum := float32(0)
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

func norm(v []float32) float32 {
	sum := float32(0)
	for i := range v {
		sum += v[i] * v[i]
	}
	return float32(math.Sqrt(float64(sum)))
}

func normalizeInPlace(v []float32) {
	n := norm(v)
	if n == 0 {
		return
	}
	invNorm := 1.0 / n
	for i := range v {
		v[i] *= invNorm
	}
}

func runtimeInfo() RuntimeInfo {
	return RuntimeInfo{
		Implementation: ImplGeneric,
		Features:       []string{},
		Accelerated:    false,
	}
}
