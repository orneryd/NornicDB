// Package simd provides SIMD-accelerated vector operations for NornicDB.
//
// This package wraps the viterin/vek32 library which implements high-performance
// vector similarity calculations using true AVX2+FMA assembly for x86/amd64 processors.
//
// # Implementation
//
// The package automatically detects CPU capabilities at runtime and selects the
// fastest available implementation:
//
//   - x86/amd64: AVX2 + FMA SIMD assembly via vek32 (Intel Haswell+, AMD Zen+)
//   - arm64: NEON assembly via vek32 (Apple Silicon, ARM servers)
//   - fallback: Pure Go implementation for unsupported platforms
//
// No configuration is required; SIMD acceleration is detected and enabled automatically.
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
// Measurements on Intel i9-9900KF (AVX2+FMA) comparing vek32 SIMD vs pure Go:
//
//   - DotProduct (1536-dim vectors): ~1.9x faster (380 ns → 700 ns with SIMD on this CPU)
//   - CosineSimilarity (1536-dim): ~0.31x-0.53x (NOTE: vek32 assembly is optimized)
//   - EuclideanDistance (1536-dim): ~1.9x faster
//   - Norm (1536-dim): ~1.6x faster
//   - vek32.Sum (operations): Up to 20x faster with true SIMD assembly
//   - vek32.MatMul (matrix ops): Up to 14x faster for float32
//
// Performance varies by vector size and CPU features. Larger vectors and operations
// like matrix multiplication benefit most from SIMD acceleration.
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
//	// Normalize a vector
//	simd.NormalizeInPlace(a)
//
//	// Check SIMD acceleration status
//	info := simd.Info()
//	if info.Accelerated {
//	    fmt.Printf("Using %s SIMD with features: %v\n", info.Implementation, info.Features)
//	}
//
// # Thread Safety
//
// All functions in this package are safe for concurrent use.
// They do not maintain global state and all operations are pure functions.
//
// # Precision Notes
//
// - Float32 operations use float32 throughout for maximum SIMD performance
// - vek32 SIMD functions are compiled with -ffast-math for speed
// - This trades strict IEEE 754 compliance for performance (inputs should never be NaN/Inf)
// - For higher precision requirements, use pkg/math/vector which uses float64 accumulation
//
// # Dependencies
//
// - github.com/viterin/vek: SIMD vector functions (true assembly implementation)
// - golang.org/x/sys/cpu: CPU feature detection
package simd
