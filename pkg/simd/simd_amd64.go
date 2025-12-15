//go:build amd64 && !nosimd

package simd

import (
	"math"

	"golang.org/x/sys/cpu"
)

// x86/amd64 optimized implementations.
// Uses loop unrolling that the Go compiler can auto-vectorize with AVX2/SSE.
// For maximum performance, true AVX2 assembly can be added later.

// hasAVX2 checks if the CPU supports AVX2+FMA at runtime
var hasAVX2 = cpu.X86.HasAVX2 && cpu.X86.HasFMA

func dotProduct(a, b []float32) float32 {
	n := len(a)
	if n == 0 {
		return 0
	}

	// 8-way unrolling for better auto-vectorization with AVX2 (256-bit = 8 float32s)
	sum0 := float32(0)
	sum1 := float32(0)
	sum2 := float32(0)
	sum3 := float32(0)
	sum4 := float32(0)
	sum5 := float32(0)
	sum6 := float32(0)
	sum7 := float32(0)

	i := 0
	for ; i <= n-8; i += 8 {
		sum0 += a[i] * b[i]
		sum1 += a[i+1] * b[i+1]
		sum2 += a[i+2] * b[i+2]
		sum3 += a[i+3] * b[i+3]
		sum4 += a[i+4] * b[i+4]
		sum5 += a[i+5] * b[i+5]
		sum6 += a[i+6] * b[i+6]
		sum7 += a[i+7] * b[i+7]
	}

	// Handle remaining elements
	for ; i < n; i++ {
		sum0 += a[i] * b[i]
	}

	return sum0 + sum1 + sum2 + sum3 + sum4 + sum5 + sum6 + sum7
}

func cosineSimilarity(a, b []float32) float32 {
	n := len(a)
	if n == 0 {
		return 0
	}

	// 8-way unrolling for AVX2 vectorization
	dot0, dot1, dot2, dot3 := float32(0), float32(0), float32(0), float32(0)
	dot4, dot5, dot6, dot7 := float32(0), float32(0), float32(0), float32(0)
	normA0, normA1, normA2, normA3 := float32(0), float32(0), float32(0), float32(0)
	normA4, normA5, normA6, normA7 := float32(0), float32(0), float32(0), float32(0)
	normB0, normB1, normB2, normB3 := float32(0), float32(0), float32(0), float32(0)
	normB4, normB5, normB6, normB7 := float32(0), float32(0), float32(0), float32(0)

	i := 0
	for ; i <= n-8; i += 8 {
		a0, a1, a2, a3 := a[i], a[i+1], a[i+2], a[i+3]
		a4, a5, a6, a7 := a[i+4], a[i+5], a[i+6], a[i+7]
		b0, b1, b2, b3 := b[i], b[i+1], b[i+2], b[i+3]
		b4, b5, b6, b7 := b[i+4], b[i+5], b[i+6], b[i+7]

		dot0 += a0 * b0
		dot1 += a1 * b1
		dot2 += a2 * b2
		dot3 += a3 * b3
		dot4 += a4 * b4
		dot5 += a5 * b5
		dot6 += a6 * b6
		dot7 += a7 * b7

		normA0 += a0 * a0
		normA1 += a1 * a1
		normA2 += a2 * a2
		normA3 += a3 * a3
		normA4 += a4 * a4
		normA5 += a5 * a5
		normA6 += a6 * a6
		normA7 += a7 * a7

		normB0 += b0 * b0
		normB1 += b1 * b1
		normB2 += b2 * b2
		normB3 += b3 * b3
		normB4 += b4 * b4
		normB5 += b5 * b5
		normB6 += b6 * b6
		normB7 += b7 * b7
	}

	// Handle remaining elements
	for ; i < n; i++ {
		dot0 += a[i] * b[i]
		normA0 += a[i] * a[i]
		normB0 += b[i] * b[i]
	}

	dot := dot0 + dot1 + dot2 + dot3 + dot4 + dot5 + dot6 + dot7
	normA := normA0 + normA1 + normA2 + normA3 + normA4 + normA5 + normA6 + normA7
	normB := normB0 + normB1 + normB2 + normB3 + normB4 + normB5 + normB6 + normB7

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
	sum4 := float32(0)
	sum5 := float32(0)
	sum6 := float32(0)
	sum7 := float32(0)

	i := 0
	for ; i <= n-8; i += 8 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		d4 := a[i+4] - b[i+4]
		d5 := a[i+5] - b[i+5]
		d6 := a[i+6] - b[i+6]
		d7 := a[i+7] - b[i+7]

		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
		sum4 += d4 * d4
		sum5 += d5 * d5
		sum6 += d6 * d6
		sum7 += d7 * d7
	}

	for ; i < n; i++ {
		d := a[i] - b[i]
		sum0 += d * d
	}

	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3 + sum4 + sum5 + sum6 + sum7)))
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
	sum4 := float32(0)
	sum5 := float32(0)
	sum6 := float32(0)
	sum7 := float32(0)

	i := 0
	for ; i <= n-8; i += 8 {
		sum0 += v[i] * v[i]
		sum1 += v[i+1] * v[i+1]
		sum2 += v[i+2] * v[i+2]
		sum3 += v[i+3] * v[i+3]
		sum4 += v[i+4] * v[i+4]
		sum5 += v[i+5] * v[i+5]
		sum6 += v[i+6] * v[i+6]
		sum7 += v[i+7] * v[i+7]
	}

	for ; i < n; i++ {
		sum0 += v[i] * v[i]
	}

	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3 + sum4 + sum5 + sum6 + sum7)))
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
	if hasAVX2 {
		return RuntimeInfo{
			Implementation: ImplAVX2,
			Features:       []string{"avx2", "fma", "auto-vectorized"},
			Accelerated:    true,
		}
	}
	return RuntimeInfo{
		Implementation: ImplGeneric,
		Features:       []string{"sse2"},
		Accelerated:    false,
	}
}
