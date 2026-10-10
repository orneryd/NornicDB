package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every VECTOR / UUID statement the differential sweep has Neo4j 2026.09
// reject (sweep/cases_types.jsonl, ids 900000+) fails here with Neo4j's
// status code (#907).
func TestVectorAndUUIDErrorsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_errors"))
	ctx := context.Background()
	for query, code := range map[string]string{
		"CYPHER 25 RETURN toIntegerList(vector([300], 1, INT8)) AS l":                                          "Neo.ClientError.Statement.ArithmeticError",
		"CYPHER 25 RETURN vector([1, 2, 3], 4, INTEGER) AS v":                                                  "Neo.ClientError.Statement.TypeError",
		"CYPHER 25 RETURN vector([1, 2, 3], 0, INTEGER) AS v":                                                  "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector([1, 2, 3], -1, INTEGER) AS v":                                                 "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector([], 0, INTEGER) AS v":                                                         "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector([1, null, 3], 3, INTEGER) AS v":                                               "Neo.ClientError.Statement.TypeError",
		"CYPHER 25 RETURN vector(['a'], 1, INTEGER) AS v":                                                      "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector('nope', 2, FLOAT) AS v":                                                       "Neo.ClientError.Statement.TypeError",
		"CYPHER 25 RETURN vector([1, 2], 2, BOOLEAN) AS v":                                                     "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector([1, 2], 2, 'INTEGER') AS v":                                                   "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector_dimension_count([1, 2, 3]) AS d":                                              "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector_distance(vector([1, 2], 2, FLOAT), vector([2, 4, 5], 3, FLOAT), COSINE) AS d": "Neo.ClientError.Statement.ArgumentError",
		"CYPHER 25 RETURN vector_distance([1.0, 2.0], [2.0, 4.0], COSINE) AS d":                                "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector([1, 2], 2, INTEGER)[0] AS s":                                                  "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN [x IN vector([1, 2], 2, INTEGER) | x] AS l":                                          "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector([300], 1, INT8) IS NULL AS n":                                                 "Neo.ClientError.Statement.ArithmeticError",
		"CYPHER 25 RETURN toIntegerList(vector([1e20], 1, INTEGER)) AS l":                                      "Neo.ClientError.Statement.ArithmeticError",
		"CYPHER 25 RETURN vector([1e40], 1, FLOAT32) IS NULL AS n":                                             "Neo.ClientError.Statement.ArgumentError",
		"CYPHER 25 RETURN vector($p, 2, FLOAT) IS NULL AS n":                                                   "Neo.ClientError.Statement.ParameterMissing",
		"CYPHER 25 RETURN vector([1, 2], $d, FLOAT) IS NULL AS n":                                              "Neo.ClientError.Statement.ParameterMissing",
		"CYPHER 25 RETURN vector([1, 2], 2.0, FLOAT) IS NULL AS n":                                             "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector([1, 2], 4097, FLOAT) IS NULL AS n":                                            "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector_distance(vector([1, 0], 2, FLOAT), vector([0, 1], 2, FLOAT), 'COSINE') AS d":  "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector_norm(vector([3, 4], 2, INTEGER), COSINE) AS n":                                "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector_norm(vector([3, 4], 2, INTEGER), EUCLIDEAN_SQUARED) AS n":                     "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector_norm(vector([3, 4], 2, INTEGER), DOT) AS n":                                   "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector_norm(vector([3, 4], 2, INTEGER), HAMMING) AS n":                               "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector_norm([3, 4], EUCLIDEAN) AS n":                                                 "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN toBoolean(vector([1], 1, INTEGER)) AS b":                                             "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN vector([1], 1, INTEGER) + 1 AS b":                                                    "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN uuid('550e8400e29b41d4a716446655440000') AS u":                                       "Neo.ClientError.Statement.ArgumentError",
		"CYPHER 25 RETURN uuid('{550e8400-e29b-41d4-a716-446655440000}') AS u":                                 "Neo.ClientError.Statement.ArgumentError",
		"CYPHER 25 RETURN uuid(1) AS u":                                                                        "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN uuid('a', 'b') AS u":                                                                 "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN uuid(1.5, 2) AS u":                                                                   "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, query)
	}
}
