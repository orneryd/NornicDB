package vector

import (
	gomath "math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every expected score is Neo4j's (5.26.30 and 2026.09 agree), bit for bit.
func TestNeo4jSimilarityScores(t *testing.T) {
	type pair struct {
		a, b []float64
		want float64
	}
	q := []float64{0.9, 0.2, 0.1}
	for _, c := range []pair{
		{[]float64{1, 2, 3}, []float64{4, 5, 7}, 0.9930066466331482},
		{[]float64{0.3, 0.7}, []float64{0.9, 0.1}, 0.7465062737464905},
		{[]float64{1.1, 2.2, 3.3}, []float64{3.3, 2.2, 1.1}, 0.8571428656578064},
		{[]float64{1, 0, 0}, q, 0.9852474927902222},
		{[]float64{0.3, 0.7, 0.2}, q, 0.7944376468658447},
		{[]float64{-1, 0.1, 0}, q, 0.027890443801879883},
		{[]float64{1, 0}, []float64{-1, 0}, 0},
		{[]float64{1e40, 1}, []float64{1, 2}, 0.7236068248748779},
		// The float32 dot product rounds below -1: max((1 + p) / 2, 0) is 0.
		{[]float64{0.6, -1, -1, -0.7, 0.4}, []float64{-0.6, 1, 1, 0.7, -0.4}, 0},
	} {
		got, ok := Neo4jCosineSimilarity(c.a, c.b)
		require.True(t, ok, "%v %v", c.a, c.b)
		require.Equal(t, c.want, got, "%v %v", c.a, c.b)
	}
	for _, c := range []pair{
		{[]float64{1, 0, 0}, q, 0.9433961510658264},
		{[]float64{0.3, 0.7, 0.2}, q, 0.6172839999198914},
		{[]float64{-1, 0.1, 0}, q, 0.21598272025585175},
		{[]float64{1, 2, 3}, []float64{4, 5, 7}, 0.02857142873108387},
		{[]float64{0.3, 0.7}, []float64{0.9, 0.1}, 0.5813953876495361},
		{[]float64{1.1, 2.2, 3.3}, []float64{3.3, 2.2, 1.1}, 0.09363297373056412},
		{[]float64{0, 0}, []float64{0, 0}, 1},
	} {
		got, ok := Neo4jEuclideanSimilarity(c.a, c.b)
		require.True(t, ok, "%v %v", c.a, c.b)
		require.Equal(t, c.want, got, "%v %v", c.a, c.b)
	}
	// The same scores from float32 coordinates (stored embeddings).
	got, ok := Neo4jCosineSimilarity([]float32{1, 0, 0}, []float32{0.9, 0.2, 0.1})
	require.True(t, ok)
	require.Equal(t, 0.9852474927902222, got)
	got, ok = Neo4jEuclideanSimilarity([]float32{1, 0, 0}, []float32{0.9, 0.2, 0.1})
	require.True(t, ok)
	require.Equal(t, 0.9433961510658264, got)

	nan, inf := gomath.NaN(), gomath.Inf(1)
	for _, c := range [][2][]float64{
		{{}, {}}, {{1}, {1, 2}}, {{0, 0}, {1, 0}}, {{1, 0}, {0, 0}}, {{nan, 1}, {1, 1}}, {{1, 1}, {inf, 1}},
		{{1e300, 1e300}, {1, 1}}, {{1e-320, 0}, {1, 1}},
	} {
		_, ok := Neo4jCosineSimilarity(c[0], c[1])
		require.False(t, ok, "cosine %v", c)
	}
	for _, c := range [][2][]float64{{{}, {}}, {{1}, {1, 2}}, {{1e40}, {1}}, {{1}, {nan}}} {
		_, ok := Neo4jEuclideanSimilarity(c[0], c[1])
		require.False(t, ok, "euclidean %v", c)
	}
}
