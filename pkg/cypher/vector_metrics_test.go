package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// vector_distance and vector_norm are computed in float32, as Neo4j 2026.09
// computes them whatever the coordinate type (#907): every expected value is
// Neo4j's answer, bit for bit.
func TestVectorMetricsMatchNeo4jFloat32(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_metrics"))
	ctx := context.Background()
	for expression, want := range map[string]float64{
		"vector_distance(vector([1, 2, 3], 3, FLOAT), vector([4, 5, 7], 3, FLOAT), EUCLIDEAN)":                         5.830951690673828,
		"vector_distance(vector([1, 2, 3], 3, FLOAT), vector([4, 5, 7], 3, FLOAT), EUCLIDEAN_SQUARED)":                 34.0,
		"vector_distance(vector([1, 2, 3], 3, FLOAT), vector([4, 5, 7], 3, FLOAT), MANHATTAN)":                         10.0,
		"vector_distance(vector([1, 2, 3], 3, FLOAT), vector([4, 5, 7], 3, FLOAT), COSINE)":                            0.013986706733703613,
		"vector_distance(vector([1, 2, 3], 3, FLOAT), vector([4, 5, 7], 3, FLOAT), DOT)":                               -35.0,
		"vector_norm(vector([1, 2, 3], 3, FLOAT), EUCLIDEAN)":                                                          3.7416574954986572,
		"vector_norm(vector([1, 2, 3], 3, FLOAT), MANHATTAN)":                                                          6.0,
		"vector_distance(vector([1, 2, 3], 3, FLOAT32), vector([4, 5, 7], 3, FLOAT32), EUCLIDEAN)":                     5.830951690673828,
		"vector_distance(vector([1, 2, 3], 3, FLOAT32), vector([4, 5, 7], 3, FLOAT32), EUCLIDEAN_SQUARED)":             34.0,
		"vector_distance(vector([1, 2, 3], 3, FLOAT32), vector([4, 5, 7], 3, FLOAT32), MANHATTAN)":                     10.0,
		"vector_distance(vector([1, 2, 3], 3, FLOAT32), vector([4, 5, 7], 3, FLOAT32), COSINE)":                        0.013986706733703613,
		"vector_distance(vector([1, 2, 3], 3, FLOAT32), vector([4, 5, 7], 3, FLOAT32), DOT)":                           -35.0,
		"vector_norm(vector([1, 2, 3], 3, FLOAT32), EUCLIDEAN)":                                                        3.7416574954986572,
		"vector_norm(vector([1, 2, 3], 3, FLOAT32), MANHATTAN)":                                                        6.0,
		"vector_distance(vector([1, 2, 3], 3, FLOAT32), vector([4, 5, 7], 3, FLOAT), COSINE)":                          0.013986706733703613,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT), vector([0.9, 0.1], 2, FLOAT), EUCLIDEAN)":                       0.8485280871391296,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT), vector([0.9, 0.1], 2, FLOAT), EUCLIDEAN_SQUARED)":               0.7199999094009399,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT), vector([0.9, 0.1], 2, FLOAT), MANHATTAN)":                       1.1999999284744263,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT), vector([0.9, 0.1], 2, FLOAT), COSINE)":                          0.5069873929023743,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT), vector([0.9, 0.1], 2, FLOAT), DOT)":                             -0.3400000035762787,
		"vector_norm(vector([0.3, 0.7], 2, FLOAT), EUCLIDEAN)":                                                         0.761577308177948,
		"vector_norm(vector([0.3, 0.7], 2, FLOAT), MANHATTAN)":                                                         1.0,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT32), vector([0.9, 0.1], 2, FLOAT32), EUCLIDEAN)":                   0.8485280871391296,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT32), vector([0.9, 0.1], 2, FLOAT32), EUCLIDEAN_SQUARED)":           0.7199999094009399,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT32), vector([0.9, 0.1], 2, FLOAT32), MANHATTAN)":                   1.1999999284744263,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT32), vector([0.9, 0.1], 2, FLOAT32), COSINE)":                      0.5069873929023743,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT32), vector([0.9, 0.1], 2, FLOAT32), DOT)":                         -0.3400000035762787,
		"vector_norm(vector([0.3, 0.7], 2, FLOAT32), EUCLIDEAN)":                                                       0.761577308177948,
		"vector_norm(vector([0.3, 0.7], 2, FLOAT32), MANHATTAN)":                                                       1.0,
		"vector_distance(vector([0.3, 0.7], 2, FLOAT32), vector([0.9, 0.1], 2, FLOAT), COSINE)":                        0.5069873929023743,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT), vector([3.3, 2.2, 1.1], 3, FLOAT), EUCLIDEAN)":             3.111269474029541,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT), vector([3.3, 2.2, 1.1], 3, FLOAT), EUCLIDEAN_SQUARED)":     9.679998397827148,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT), vector([3.3, 2.2, 1.1], 3, FLOAT), MANHATTAN)":             4.399999618530273,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT), vector([3.3, 2.2, 1.1], 3, FLOAT), COSINE)":                0.28571420907974243,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT), vector([3.3, 2.2, 1.1], 3, FLOAT), DOT)":                   -12.100000381469727,
		"vector_norm(vector([1.1, 2.2, 3.3], 3, FLOAT), EUCLIDEAN)":                                                    4.115822792053223,
		"vector_norm(vector([1.1, 2.2, 3.3], 3, FLOAT), MANHATTAN)":                                                    6.600000381469727,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT32), vector([3.3, 2.2, 1.1], 3, FLOAT32), EUCLIDEAN)":         3.111269474029541,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT32), vector([3.3, 2.2, 1.1], 3, FLOAT32), EUCLIDEAN_SQUARED)": 9.679998397827148,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT32), vector([3.3, 2.2, 1.1], 3, FLOAT32), MANHATTAN)":         4.399999618530273,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT32), vector([3.3, 2.2, 1.1], 3, FLOAT32), COSINE)":            0.28571420907974243,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT32), vector([3.3, 2.2, 1.1], 3, FLOAT32), DOT)":               -12.100000381469727,
		"vector_norm(vector([1.1, 2.2, 3.3], 3, FLOAT32), EUCLIDEAN)":                                                  4.115822792053223,
		"vector_norm(vector([1.1, 2.2, 3.3], 3, FLOAT32), MANHATTAN)":                                                  6.600000381469727,
		"vector_distance(vector([1.1, 2.2, 3.3], 3, FLOAT32), vector([3.3, 2.2, 1.1], 3, FLOAT), COSINE)":              0.28571420907974243,
		"vector_distance(vector([1, 2], 2, INTEGER), vector([2, 4], 2, FLOAT), COSINE)":                                0.0,
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 RETURN "+expression+" AS d", nil)
		require.NoError(t, err, expression)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, expression)
	}
}
