package vector

import (
	gomath "math"

	math "github.com/orneryd/nornicdb/pkg/math/libm"
)

// Neo4j's vector similarity scores (vector.similarity.cosine and
// vector.similarity.euclidean, and the scores of COSINE and EUCLIDEAN vector
// indexes), as Neo4j 5.26 and 2026.09 compute them, bit for bit:
//
//   - EUCLIDEAN: 1 / (1 + d²), d² summed in float32 over the coordinates as
//     float32.
//   - COSINE: each vector L2-normalized in float64 and its coordinates then
//     taken as float32; the float32 dot product p of the two; max((1 + p) / 2,
//     0).
//
// Both are in [0, 1]. A vector is invalid for a function (ok false) when it
// is empty or has a coordinate that isn't finite as a float32 (EUCLIDEAN),
// or when it is empty, has a non-finite coordinate or has no positive,
// finite L2 norm (COSINE). Neo4j raises an ArgumentError for an invalid
// vector; callers decide what an invalid vector means for them.

// Neo4jEuclideanSimilarity is Neo4j's EUCLIDEAN similarity of a and b, which
// must have the same length; ok is false when either is invalid.
func Neo4jEuclideanSimilarity[T float32 | float64](a, b []T) (float64, bool) {
	if len(a) == 0 || len(a) != len(b) {
		return 0, false
	}
	var sum float32
	for i := range a {
		x, y := float32(a[i]), float32(b[i])
		if !finite32(x) || !finite32(y) {
			return 0, false
		}
		d := x - y
		// The explicit conversions round every step to float32, as Neo4j's
		// float arithmetic does, and keep the compiler from fusing them.
		sum = float32(sum + float32(d*d))
	}
	return float64(float32(1 / float32(1+sum))), true
}

// Neo4jCosineSimilarity is Neo4j's COSINE similarity of a and b, which must
// have the same length; ok is false when either is invalid.
func Neo4jCosineSimilarity[T float32 | float64](a, b []T) (float64, bool) {
	if len(a) == 0 || len(a) != len(b) {
		return 0, false
	}
	scaleA, ok := neo4jCosineScale(a)
	if !ok {
		return 0, false
	}
	scaleB, ok := neo4jCosineScale(b)
	if !ok {
		return 0, false
	}
	var dot float32
	for i := range a {
		// A normalized coordinate is at most 1: always finite.
		x, y := float32(float64(a[i])*scaleA), float32(float64(b[i])*scaleB)
		dot = float32(dot + float32(x*y))
	}
	score := float32(float32(1+dot) / 2)
	if score < 0 {
		score = 0
	}
	return float64(score), true
}

// neo4jCosineScale is 1 / ‖v‖, the L2 norm summed in float64; ok is false
// for a non-finite coordinate or a norm that isn't positive and finite.
func neo4jCosineScale[T float32 | float64](v []T) (float64, bool) {
	var square float64
	for _, x := range v {
		value := float64(x)
		if gomath.IsNaN(value) || gomath.IsInf(value, 0) {
			return 0, false
		}
		square += value * value
	}
	if !(square > 0) || gomath.IsInf(square, 0) {
		return 0, false
	}
	// The smallest positive square still has a finite inverse root.
	return 1 / math.Sqrt(square), true
}

func finite32(x float32) bool {
	return !gomath.IsNaN(float64(x)) && !gomath.IsInf(float64(x), 0)
}
