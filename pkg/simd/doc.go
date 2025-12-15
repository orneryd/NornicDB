// Package simd provides SIMD-accelerated vector operations for NornicDB.
//
// This package implements high-performance vector similarity calculations
// using platform-specific SIMD instructions where available:
//
//   - x86/amd64: AVX2 + FMA instructions (Intel Haswell+, AMD Zen+)
//   - arm64: NEON instructions (Apple Silicon, ARM servers)
//   - fallback: Pure Go implementation for all other platforms
//
// The package automatically detects CPU capabilities at runtime and selects
// the fastest available implementation. No configuration is required.
//
// # Supported Operations
//
//   - DotProduct: Dot product of two vectors
//   - CosineSimilarity: Cosine similarity between two vectors
//   - EuclideanDistance: Euclidean distance between two vectors
//   - Norm: Euclidean norm (L2 norm / magnitude) of a vector
//   - NormalizeInPlace: Normalize a vector to unit length in-place
//
// # Performance
//
// On supported platforms, SIMD acceleration provides significant speedups:
//
//   - x86 AVX2: ~10x faster than pure Go for float32 vectors
//   - arm64 NEON: ~4-8x faster than pure Go for float32 vectors
//
// # Usage
//
//	import "github.com/orneryd/nornicdb/pkg/simd"
//
//	a := []float32{1.0, 2.0, 3.0, 4.0}
//	b := []float32{5.0, 6.0, 7.0, 8.0}
//
//	// Cosine similarity (most common for embeddings)
//	sim := simd.CosineSimilarity(a, b)
//
//	// Dot product
//	dot := simd.DotProduct(a, b)
//
//	// Euclidean distance
//	dist := simd.EuclideanDistance(a, b)
//
//	// Check if SIMD acceleration is available
//	info := simd.Info()
//	fmt.Printf("SIMD: %s (%s)\n", info.Implementation, info.Features)
//
// # Thread Safety
//
// All functions in this package are safe for concurrent use.
// They do not modify any global state.
//
// # Precision
//
// Float32 functions use float32 throughout for maximum SIMD performance.
// For higher precision requirements, use the standard library or
// pkg/math/vector which uses float64 accumulation.
package simd
