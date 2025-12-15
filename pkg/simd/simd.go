package simd

// Implementation represents the active SIMD implementation
type Implementation string

const (
	// ImplGeneric indicates pure Go fallback (no SIMD)
	ImplGeneric Implementation = "generic"
	// ImplAVX2 indicates x86 AVX2+FMA SIMD
	ImplAVX2 Implementation = "avx2"
	// ImplNEON indicates ARM NEON SIMD
	ImplNEON Implementation = "neon"
)

// RuntimeInfo contains information about the active SIMD implementation
type RuntimeInfo struct {
	// Implementation is the active SIMD backend
	Implementation Implementation
	// Features lists specific CPU features being used
	Features []string
	// Accelerated indicates whether SIMD acceleration is active
	Accelerated bool
}

// DotProduct computes the dot product of two float32 vectors.
//
// The dot product is defined as: sum(a[i] * b[i]) for all i.
//
// Requirements:
//   - Both vectors must have the same length
//   - Returns 0 if vectors are empty or have different lengths
//
// Example:
//
//	a := []float32{1, 2, 3}
//	b := []float32{4, 5, 6}
//	result := simd.DotProduct(a, b) // 1*4 + 2*5 + 3*6 = 32
func DotProduct(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	return dotProduct(a, b)
}

// CosineSimilarity computes the cosine similarity between two float32 vectors.
//
// Cosine similarity measures the angle between two vectors, returning a value
// between -1 (opposite directions) and 1 (same direction). A value of 0
// indicates orthogonal (perpendicular) vectors.
//
// The formula is: dot(a, b) / (norm(a) * norm(b))
//
// Requirements:
//   - Both vectors must have the same length
//   - Returns 0 if vectors are empty, have different lengths, or either is zero-length
//
// Example:
//
//	a := []float32{1, 0, 0}
//	b := []float32{0, 1, 0}
//	result := simd.CosineSimilarity(a, b) // 0 (perpendicular)
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	return cosineSimilarity(a, b)
}

// EuclideanDistance computes the Euclidean distance between two float32 vectors.
//
// The Euclidean distance is the straight-line distance in N-dimensional space:
// sqrt(sum((a[i] - b[i])^2))
//
// Requirements:
//   - Both vectors must have the same length
//   - Returns 0 if vectors are empty or have different lengths
//
// Example:
//
//	a := []float32{0, 0}
//	b := []float32{3, 4}
//	result := simd.EuclideanDistance(a, b) // 5.0
func EuclideanDistance(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	return euclideanDistance(a, b)
}

// Norm computes the Euclidean norm (L2 norm / magnitude) of a float32 vector.
//
// The norm is defined as: sqrt(sum(v[i]^2))
//
// Example:
//
//	v := []float32{3, 4}
//	result := simd.Norm(v) // 5.0
func Norm(v []float32) float32 {
	return norm(v)
}

// NormalizeInPlace normalizes a vector to unit length, modifying it in place.
//
// After normalization, Norm(v) will equal 1.0 (within floating-point precision).
//
// If the vector has zero length, it will remain unchanged.
//
// Example:
//
//	v := []float32{3, 4}
//	simd.NormalizeInPlace(v)
//	// v is now {0.6, 0.8}
func NormalizeInPlace(v []float32) {
	normalizeInPlace(v)
}

// Info returns information about the active SIMD implementation.
//
// This can be used to check whether SIMD acceleration is being used
// and which specific features are enabled.
//
// Example:
//
//	info := simd.Info()
//	if info.Accelerated {
//	    fmt.Printf("Using %s SIMD\n", info.Implementation)
//	}
func Info() RuntimeInfo {
	return runtimeInfo()
}
