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
	if len(a) != len(b) {
		return 0, false
	}
	query, ok := NewNeo4jCosineQuery(a)
	if !ok {
		return 0, false
	}
	return neo4jCosineScore(query.normalized, b)
}

// Neo4jCosineQuery is a query vector prepared for Neo4j's COSINE similarity
// against many candidates: L2-normalized once (in float64, then as float32
// coordinates), so each candidate costs only its own normalization and the
// dot product. Scores are Neo4jCosineSimilarity's, bit for bit.
type Neo4jCosineQuery struct {
	normalized []float32
}

// NewNeo4jCosineQuery prepares q; ok is false when q isn't a valid COSINE
// vector.
func NewNeo4jCosineQuery[T float32 | float64](q []T) (Neo4jCosineQuery, bool) {
	scale, ok := neo4jCosineScale(q)
	if !ok || len(q) == 0 {
		return Neo4jCosineQuery{}, false
	}
	normalized := make([]float32, len(q))
	for i, x := range q {
		// A normalized coordinate is at most 1: always finite.
		normalized[i] = float32(float64(x) * scale)
	}
	return Neo4jCosineQuery{normalized: normalized}, true
}

// Similarity is the COSINE similarity of the query and candidate; ok is
// false when candidate has another length or isn't a valid COSINE vector.
func (q Neo4jCosineQuery) Similarity(candidate []float32) (float64, bool) {
	if len(candidate) != len(q.normalized) {
		return 0, false
	}
	return neo4jCosineScore(q.normalized, candidate)
}

// neo4jCosineScore is max((1 + p) / 2, 0) for the float32 dot product p of
// the normalized query and b, normalized here; ok is false for an invalid b.
func neo4jCosineScore[T float32 | float64](normalized []float32, b []T) (float64, bool) {
	scale, ok := neo4jCosineScale(b)
	if !ok {
		return 0, false
	}
	var dot float32
	for i, x := range normalized {
		y := float32(float64(b[i]) * scale)
		// The explicit conversions round every step to float32, as Neo4j's
		// float arithmetic does, and keep the compiler from fusing them.
		dot = float32(dot + float32(x*y))
	}
	score := float32(float32(1+dot) / 2)
	if score < 0 {
		score = 0
	}
	return float64(score), true
}

// Neo4jEuclideanVectorValid reports whether v is a valid vector for
// Neo4j's EUCLIDEAN similarity: not empty, every coordinate finite as a
// float32.
func Neo4jEuclideanVectorValid[T float32 | float64](v []T) bool {
	for _, x := range v {
		if !finite32(float32(x)) {
			return false
		}
	}
	return len(v) > 0
}

// Neo4jCosineVectorValid reports whether v is a valid vector for Neo4j's
// COSINE similarity: not empty, finite coordinates, a positive and finite L2
// norm.
func Neo4jCosineVectorValid[T float32 | float64](v []T) bool {
	_, ok := neo4jCosineScale(v)
	return ok && len(v) > 0
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
