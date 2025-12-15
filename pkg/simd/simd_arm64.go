//go:build arm64 && !nosimd

package simd

import "math"

// ARM64 NEON-optimized implementations.
// Currently uses pure Go with optimizations that the Go compiler can vectorize.
// TODO: Add true NEON assembly for maximum performance.

// ARM64 NEON can process 4 float32s at a time (128-bit registers)
// The Go compiler often auto-vectorizes these simple loops on arm64.

func dotProduct(a, b []float32) float32 {
	n := len(a)
	if n == 0 {
		return 0
	}

	// Use 4-way unrolling to help auto-vectorization
	sum0 := float32(0)
	sum1 := float32(0)
	sum2 := float32(0)
	sum3 := float32(0)

	// Process 4 elements at a time
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += a[i] * b[i]
		sum1 += a[i+1] * b[i+1]
		sum2 += a[i+2] * b[i+2]
		sum3 += a[i+3] * b[i+3]
	}

	// Handle remaining elements
	for ; i < n; i++ {
		sum0 += a[i] * b[i]
	}

	return sum0 + sum1 + sum2 + sum3
}

func cosineSimilarity(a, b []float32) float32 {
	n := len(a)
	if n == 0 {
		return 0
	}

	// Use 4-way unrolling for better vectorization
	dot0, dot1, dot2, dot3 := float32(0), float32(0), float32(0), float32(0)
	normA0, normA1, normA2, normA3 := float32(0), float32(0), float32(0), float32(0)
	normB0, normB1, normB2, normB3 := float32(0), float32(0), float32(0), float32(0)

	i := 0
	for ; i <= n-4; i += 4 {
		a0, a1, a2, a3 := a[i], a[i+1], a[i+2], a[i+3]
		b0, b1, b2, b3 := b[i], b[i+1], b[i+2], b[i+3]

		dot0 += a0 * b0
		dot1 += a1 * b1
		dot2 += a2 * b2
		dot3 += a3 * b3

		normA0 += a0 * a0
		normA1 += a1 * a1
		normA2 += a2 * a2
		normA3 += a3 * a3

		normB0 += b0 * b0
		normB1 += b1 * b1
		normB2 += b2 * b2
		normB3 += b3 * b3
	}

	// Handle remaining elements
	for ; i < n; i++ {
		dot0 += a[i] * b[i]
		normA0 += a[i] * a[i]
		normB0 += b[i] * b[i]
	}

	dot := dot0 + dot1 + dot2 + dot3
	normA := normA0 + normA1 + normA2 + normA3
	normB := normB0 + normB1 + normB2 + normB3

	if normA == 0 || normB == 0 {
		return 0
	}

	return dot / float32(math.Sqrt(float64(normA*normB)))
}

func euclideanDistance(a, b []float32) float32 {
	n := len(a)
	if n == 0 {
		return 0
	}

	sum0 := float32(0)
	sum1 := float32(0)
	sum2 := float32(0)
	sum3 := float32(0)

	i := 0
	for ; i <= n-4; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]

		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}

	for ; i < n; i++ {
		d := a[i] - b[i]
		sum0 += d * d
	}

	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3)))
}

func norm(v []float32) float32 {
	n := len(v)
	if n == 0 {
		return 0
	}

	sum0 := float32(0)
	sum1 := float32(0)
	sum2 := float32(0)
	sum3 := float32(0)

	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += v[i] * v[i]
		sum1 += v[i+1] * v[i+1]
		sum2 += v[i+2] * v[i+2]
		sum3 += v[i+3] * v[i+3]
	}

	for ; i < n; i++ {
		sum0 += v[i] * v[i]
	}

	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3)))
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
		Implementation: ImplNEON,
		Features:       []string{"neon", "auto-vectorized"},
		Accelerated:    true,
	}
}
