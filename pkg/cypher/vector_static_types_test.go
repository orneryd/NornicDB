package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// VECTOR and UUID at compile time (#907): Neo4j 2026.09 rejects an operator
// or function that can't take them with a SyntaxError "Type mismatch",
// whatever the data, and accepts the rest. Every case is Neo4j's answer.
func TestVectorAndUUIDStaticTypes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_static"))
	ctx := context.Background()
	const v, u = "vector([1, 2], 2, INTEGER)", "uuid(1, 2)"
	for query, want := range map[string]string{
		"RETURN " + v + "[0]":                            "Type mismatch: expected List<T> but was Vector",
		"RETURN " + v + "['a']":                          "Type mismatch: expected Map, Node or Relationship but was Vector",
		"RETURN " + v + "[0..1]":                         "Type mismatch: expected List<T> but was Vector",
		"RETURN " + v + ".x":                             "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Vector",
		"RETURN 1 IN " + v:                               "Type mismatch: expected List<T> but was Vector",
		"RETURN NOT " + v:                                "Type mismatch: expected Boolean but was Vector",
		"RETURN " + v + " AND true":                      "Type mismatch: expected Boolean but was Vector",
		"RETURN -" + v:                                   "Type mismatch: expected Float or Integer but was Vector",
		"RETURN " + v + " - 1":                           "Type mismatch: expected Float, Integer, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Vector",
		"RETURN " + v + " * 2":                           "Type mismatch: expected Float, Integer or Duration but was Vector",
		"RETURN " + v + " + 1":                           "Type mismatch: expected String or List<T> but was Integer",
		"RETURN " + v + " + " + v:                        "Type mismatch: expected String or List<T> but was Vector",
		"RETURN 1 + " + v:                                "Type mismatch: expected Float, Integer, String or List<T> but was Vector",
		"RETURN " + v + " =~ 'x'":                        "Type mismatch: expected String but was Vector",
		"RETURN toBoolean(" + v + ")":                    "Type mismatch: expected Boolean, Integer or String but was Vector",
		"RETURN toInteger(" + v + ")":                    "Type mismatch: expected Boolean, Float, Integer or String but was Vector",
		"RETURN toFloat(" + v + ")":                      "Type mismatch: expected Float, Integer or String but was Vector",
		"RETURN toUpper(" + v + ")":                      "Type mismatch: expected String but was Vector",
		"RETURN head(" + v + ")":                         "Type mismatch: expected List<T> but was Vector",
		"RETURN point(" + v + ")":                        "Type mismatch: expected Map, Node or Relationship but was Vector",
		"RETURN vector_dimension_count('x')":             "Type mismatch: expected Vector but was String",
		"RETURN vector_norm('x', EUCLIDEAN)":             "Type mismatch: expected Vector but was String",
		"RETURN vector_distance([1], " + v + ", COSINE)": "Type mismatch: expected Vector but was List<Integer>",
		"WITH " + v + " AS v RETURN v + 1":               "Type mismatch: expected String or List<T> but was Integer",
		"MATCH (n) WHERE " + v + " RETURN n":             "Type mismatch: expected Boolean but was Vector",
		"RETURN " + u + "[0]":                            "Type mismatch: expected List<T> but was UUID",
		"RETURN " + u + " + 1":                           "Type mismatch: expected List<T> but was Integer",
		"RETURN " + u + " + 'x'":                         "Type mismatch: expected List<T> but was String",
		"RETURN uuid() + 1":                              "Type mismatch: expected List<T> but was Integer",
		"RETURN 'x' + " + u:                              "Type mismatch: expected Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime, DateTime, Vector or List<T> but was UUID",
		"RETURN " + u + ".x":                             "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was UUID",
		"RETURN 1 IN " + u:                               "Type mismatch: expected List<T> but was UUID",
		"RETURN " + u + " =~ 'x'":                        "Type mismatch: expected String but was UUID",
		"RETURN -" + u:                                   "Type mismatch: expected Float or Integer but was UUID",
		"RETURN " + u + " * 2":                           "Type mismatch: expected Float, Integer or Duration but was UUID",
		"RETURN toBoolean(" + u + ")":                    "Type mismatch: expected Boolean, Integer or String but was UUID",
		"RETURN toUpper(uuid())":                         "Type mismatch: expected String but was UUID",
		"RETURN uuid.mostSignificantBits('x')":           "Type mismatch: expected UUID but was String",
		"RETURN [x IN " + v + " | x]":                    "Type mismatch: expected List<T> but was Vector",
		"RETURN any(x IN " + v + " WHERE x > 1)":         "Type mismatch: expected List<T> but was Vector",
		"RETURN [x IN " + u + " | x]":                    "Type mismatch: expected List<T> but was UUID",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.ErrorContains(t, err, want, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	for query, want := range map[string]interface{}{
		"RETURN " + v + " + 'x'":                                   "vector([1, 2], 2, INTEGER64)x",
		"RETURN 'x' + " + v:                                        "xvector([1, 2], 2, INTEGER64)",
		"RETURN size(" + v + ")":                                   int64(2),
		"RETURN toString(" + v + ")":                               "vector([1, 2], 2, INTEGER64)",
		"RETURN toString(" + u + ")":                               "00000000-0000-0001-0000-000000000002",
		"RETURN toStringOrNull(" + v + ")":                         "vector([1, 2], 2, INTEGER64)",
		"RETURN " + v + " IN [1]":                                  false,
		"RETURN " + v + " + null":                                  nil,
		"RETURN " + u + " + null":                                  nil,
		"RETURN vector_dimension_count(" + v + ") + 'x'":           "2x",
		"RETURN uuid.mostSignificantBits(" + u + ") + 'x'":         "1x",
		"RETURN size(" + v + ") + 'x'":                             "2x",
		"RETURN vector_distance(" + v + ", " + v + ", COSINE) + 1": 1.0,
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	result, err := exec.Execute(ctx, "CYPHER 25 RETURN "+u+" + [1] AS a, [1] + "+u+" AS b, "+v+" + [1] AS c", nil)
	require.NoError(t, err)
	id, vector := uuidFromHalves(1, 2), CypherVector{Type: VectorInteger64, Ints: []int64{1, 2}}
	require.Equal(t, []interface{}{id, int64(1)}, result.Rows[0][0])
	require.Equal(t, []interface{}{int64(1), id}, result.Rows[0][1])
	require.Equal(t, []interface{}{vector, int64(1)}, result.Rows[0][2])
	require.Error(t, runtimeArithmeticTypeError('+', vector, int64(1)))
	require.NoError(t, runtimeArithmeticTypeError('+', &vector, "x"))
	require.True(t, isRuntimeVector(&vector))
	text, ok := concatOperandText(&vector)
	require.True(t, ok)
	require.Equal(t, "vector([1, 2], 2, INTEGER64)", text)
}
