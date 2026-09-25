package cypher

import (
	"context"
	stderrors "errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requireStatus runs query and requires Neo4j's status code and message for
// its failure (#657), as Bolt and HTTP report it.
func requireStatus(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}, wantCode, wantMessage string) {
	t.Helper()
	_, err := exec.Execute(context.Background(), query, params)
	require.Error(t, err, query)
	code, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, wantCode, code, query)
	require.Equal(t, wantMessage, message, query)
}

func newClassificationExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(context.Background(), "CREATE (:A {v: 1, f: 1.5, s: 'x', b: true, l: [1], ls: ['a']})-[:R]->(:B)", nil)
	require.NoError(t, err)
	return exec
}

// TestCompileTimeTypeErrorsMatchNeo4j covers the #657 section on type errors:
// an argument or operand whose type is known when the statement is compiled
// (a literal, a node / relationship / path variable, a WITH / UNWIND literal
// alias, a function result, a parameter) is a SyntaxError "Type mismatch",
// wherever it is. Messages are Neo4j 5.26's.
func TestCompileTimeTypeErrorsMatchNeo4j(t *testing.T) {
	exec := newClassificationExecutor(t)
	const syntax = "Neo.ClientError.Statement.SyntaxError"
	for query, message := range map[string]string{
		"MATCH (n:A) RETURN toInteger(n) AS y":                "Type mismatch: expected Boolean, Float, Integer or String but was Node",
		"MATCH (n:A) WITH n AS m RETURN toInteger(m) AS y":    "Type mismatch: expected Boolean, Float, Integer or String but was Node",
		"MATCH (n:A) WHERE toInteger(n) = 1 RETURN n":         "Type mismatch: expected Boolean, Float, Integer or String but was Node",
		"MATCH (n:A) WITH n ORDER BY toInteger(n) RETURN n":   "Type mismatch: expected Boolean, Float, Integer or String but was Node",
		"MATCH (n:A) SET n.x = toUpper(n)":                    "Type mismatch: expected String but was Node",
		"MATCH ()-[r:R]->() RETURN abs(r) AS y":               "Type mismatch: expected Float or Integer but was Relationship",
		"MATCH p = (:A)-->() RETURN size(p) AS y":             "Type mismatch: expected String or List<T> but was Path",
		"MATCH p = (:A)-->() WITH p AS q RETURN size(q) AS y": "Type mismatch: expected String or List<T> but was Path",
		"MATCH ()-[r*]->() RETURN type(r) AS y":               "Type mismatch: expected Relationship but was List<Relationship>",
		"MATCH (n) RETURN left('a', n) AS y":                  "Type mismatch: expected Integer but was Node",
		"RETURN toUpper(1) AS y":                              "Type mismatch: expected String but was Integer",
		"RETURN toBoolean([1]) AS y":                          "Type mismatch: expected Boolean, Integer or String but was List<Integer>",
		"UNWIND [1] AS x RETURN toUpper(x) AS y":              "Type mismatch: expected String but was Integer",
		"WITH 1.5 AS s RETURN left('ab', s) AS y":             "Type mismatch: expected Integer but was Float",
		"RETURN 'a' + true AS y":                              "Type mismatch: expected Float, Integer, String or List<T> but was Boolean",
		"RETURN 1 + {a: 1} AS y":                              "Type mismatch: expected Float, Integer, String or List<T> but was Map",
		"MATCH (n:A) RETURN n.v - 'x' AS y":                   "Type mismatch: expected Float, Integer or Duration but was String",
		"MATCH (n:A) RETURN 'a' - n.v AS y":                   "Type mismatch: expected Float, Integer, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was String",
		"MATCH (n:A) RETURN {a: 1} * n.v AS y":                "Type mismatch: expected Float, Integer or Duration but was Map",
		"MATCH (n:A) RETURN n.v / [1] AS y":                   "Type mismatch: expected Float or Integer but was List<Integer>",
		"MATCH (n:A) RETURN n.v % 'a' AS y":                   "Type mismatch: expected Float or Integer but was String",
		"MATCH (n:A) RETURN n.v ^ 'a' AS y":                   "Type mismatch: expected Float but was String",
		"MATCH (n:A) RETURN n + 1 AS y":                       "Type mismatch: expected List<T> but was Integer",
		"RETURN -[1] AS y":                                    "Type mismatch: expected Float or Integer but was List<Integer>",
		"MATCH (n:A) RETURN -n AS y":                          "Type mismatch: expected Float or Integer but was Node",
		"MATCH (n:A) RETURN (n.v + 1) * 'a' AS y":             "Type mismatch: expected Float, Integer or Duration but was String",
		"MATCH (n:A) RETURN size(n.s) - 'a' AS y":             "Type mismatch: expected Float or Integer but was String",
		"MATCH (n:A) WHERE n.v - 'x' > 0 RETURN n":            "Type mismatch: expected Float, Integer or Duration but was String",
		"MATCH (n:A) SET n.z = n.v - 'x'":                     "Type mismatch: expected Float, Integer or Duration but was String",
		"RETURN date() - 1 AS y":                              "Type mismatch: expected Duration but was Integer",
		"RETURN date() + 1 AS y":                              "Type mismatch: expected Duration or List<T> but was Integer",
		"RETURN duration('P1D') * 'a' AS y":                   "Type mismatch: expected Float, Integer or Duration but was String",
		"MATCH (n:A) WITH {a: n.v} AS m RETURN m * 2 AS y":    "Type mismatch: expected Float, Integer or Duration but was Map",
	} {
		requireStatus(t, exec, query, nil, syntax, message)
	}
	requireStatus(t, exec, "MATCH (n:A) RETURN n.v - $s AS y", map[string]interface{}{"s": "x"}, syntax,
		"Type mismatch for parameter 's': expected Float, Integer or Duration but was String")
	requireStatus(t, exec, "RETURN $a + $b AS y", map[string]interface{}{"a": int64(1), "b": true}, syntax,
		"Type mismatch for parameter 'b': expected Float, Integer, String or List<T> but was Boolean")

	// Accepted at compile time: unknown operand types, coercions Neo4j
	// allows, shadowing variables, valid temporal and list arithmetic.
	for _, query := range []string{
		"MATCH (n:A) RETURN toInteger(n.v) AS y",
		"RETURN sqrt(1) AS y",
		"MATCH (n:A) RETURN [n IN [1] | toInteger(n)] AS y",
		"MATCH (n:A) RETURN reduce(n = 0, x IN [1] | n + x) AS y",
		"RETURN [1] + {a: 1} AS y",
		"RETURN true + [1] AS y",
		"RETURN date('2025-01-01') + duration('P1D') AS y",
		"RETURN duration('P1D') * 2 AS y",
		"MATCH (n:A) RETURN n.v + n.f AS y",
		"MATCH (n:A) RETURN n {.*, z: 1} AS y",
		"MATCH (a:A)-[r:R]->(b) RETURN a.v - 1 AS y",
		"RETURN $a + $b AS y",
	} {
		_, err := exec.Execute(context.Background(), query, map[string]interface{}{"a": "x", "b": int64(1)})
		require.NoError(t, err, query)
	}
}

// TestRuntimeTypeErrorsMatchNeo4j covers the runtime half: an operand whose
// type is only known from the data is a TypeError worded by the operator
// (before, most of these returned null or the expression text).
func TestRuntimeTypeErrorsMatchNeo4j(t *testing.T) {
	exec := newClassificationExecutor(t)
	const typeError = "Neo.ClientError.Statement.TypeError"
	for query, message := range map[string]string{
		"MATCH (n:A) RETURN n.v + {a: 1} AS y":    "Cannot add `Long` and `Map`",
		"MATCH (n:A) RETURN n.s * 2 AS y":         "Cannot multiply `String` and `Long`",
		"MATCH (n:A) RETURN n.l / 2 AS y":         "Cannot divide `LongArray` by `Long`",
		"MATCH (n:A) RETURN n.ls * 2 AS y":        "Cannot multiply `StringArray` and `Long`",
		"MATCH (n:A) RETURN n.s - 2 AS y":         "Cannot subtract `Long` from `String`",
		"MATCH (n:A) RETURN n.s % 2 AS y":         "Cannot calculate modulus of `String` and `Long`",
		"MATCH (n:A) RETURN n.s ^ 2 AS y":         "Cannot raise `String` to the power of `Long`",
		"MATCH (n:A) RETURN n.b + 1 AS y":         "Cannot add `Boolean` and `Long`",
		"MATCH (n:A) RETURN n.v + n AS y":         "Cannot add `Long` and `NodeIdReference`",
		"MATCH (n:A) RETURN n.v * n.s AS y":       "Cannot multiply `Long` and `String`",
		"MATCH (n:A) RETURN -n.s AS y":            "Cannot subtract `String` from `Long`",
		"MATCH (n:A) RETURN (n.s * 2) + 1 AS y":   "Cannot multiply `String` and `Long`",
		"MATCH (n:A) RETURN [n.s * 2] AS y":       "Cannot multiply `String` and `Long`",
		"MATCH (n:A) RETURN n.v.x AS y":           "Type mismatch: expected a map but was Long(1)",
		"MATCH (n:A) RETURN n.s.x AS y":           "Type mismatch: expected a map but was String(\"x\")",
		"MATCH (n:A) RETURN n.f.x AS y":           "Type mismatch: expected a map but was Double(1.500000e+00)",
		"MATCH (n:A) RETURN n.s[0] AS y":          "`String(\"x\")` is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key (found: String(\"x\")[Long(0)])",
		"MATCH (n:A) RETURN toFloat(n.l) AS y":    "Invalid input for function 'toFloat()': Expected a String, Float or Integer, got: LongArray[1]",
		"MATCH (n:A) RETURN toBoolean(n.ls) AS y": "Invalid input for function 'toBoolean()': Expected a Boolean, Integer or String, got: StringArray[a]",
	} {
		requireStatus(t, exec, query, nil, typeError, message)
	}
	requireStatus(t, exec, "RETURN 1 / 0 AS x", nil, "Neo.ClientError.Statement.ArithmeticError", "/ by zero")
	// Arithmetic that is valid, or null, stays a value.
	for query, want := range map[string]interface{}{
		"MATCH (n:A) RETURN n.v + n.f AS y":  2.5,
		"MATCH (n:A) RETURN n.s + n.v AS y":  "x1",
		"MATCH (n:A) RETURN n.l + 2 AS y":    []interface{}{int64(1), int64(2)},
		"MATCH (n:A) RETURN n.nope * 2 AS y": nil,
		"MATCH (n:A) RETURN n.v % 2 AS y":    int64(1),
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows[0][0], query)
	}
}

// TestConstraintViolationStatusAndRolledBackCommit covers the #657 section
// on uniqueness: every path reports Schema.ConstraintValidationFailed, and a
// COMMIT that fails its constraint check is marked as rolled back (nothing
// written), which lets Bolt keep the connection.
func TestConstraintViolationStatusAndRolledBackCommit(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT u_k FOR (u:U) REQUIRE u.k IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:U {k: 5})", nil)
	require.NoError(t, err)

	for _, query := range []string{"CREATE (:U {k: 1}), (:U {k: 1})", "CREATE (:T)-[:R]->(:T), (:U {k: 5})", "MERGE (u:U {k: 7}) SET u.k = 5"} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, nornicerrors.ConstraintValidationFailed, code, query)
		require.False(t, nornicerrors.IsMergeCommitTimeUniqueConflict(err), query)
	}

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:U {k: 5})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.Error(t, err)
	require.True(t, nornicerrors.IsCommitRolledBack(err))
	code, message := nornicerrors.Neo4jCommitStatus(err)
	require.Equal(t, nornicerrors.ConstraintValidationFailed, code)
	require.Contains(t, message, "commit failed: constraint violation")

	result, err := exec.Execute(ctx, "MATCH (u:U) RETURN count(u) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
}

// TestMergeUniqueConflictIsRetrySafe covers #657: a UNIQUE violation of MERGE
// work is a retry-safe race only when every SET write of the violated property
// repeats the MERGE key; anything undecidable is not retry-safe.
func TestMergeUniqueConflictIsRetrySafe(t *testing.T) {
	violation := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "U", Properties: []string{"k"}}
	params := map[string]interface{}{"k": int64(7), "same": map[string]interface{}{"k": int64(7)}, "other": map[string]interface{}{"k": int64(5)}, "names": map[string]interface{}{"name": "x"}}
	for statement, want := range map[string]bool{
		"MERGE (u:U {k: 7})":                                    true,
		"MERGE (u:U {k: $k}) SET u.name = 'x'":                  true,
		"MERGE (u:U {k: $k}) SET u += $names":                   true,
		"MERGE (u:U {k: $k}) SET u += $same":                    true,
		"MERGE (u:U {k: $k}) SET u.k = 7":                       true,
		"MERGE (u:U {k: $k}) SET u.name = 'k = 1'":              true,
		"MERGE (u:U {k: $k}) SET u:Tagged":                      true,
		"MERGE (u:U {k: 7}) SET u.k = 5":                        false,
		"MERGE (u:U {k: $k}) SET u += $other":                   false,
		"MERGE (u:U {k: $k}) SET u = $missing":                  false,
		"MERGE (u:U {k: $k}) SET u.k = $k + 1":                  false,
		"merge (u:U {n: 1}) on create set u.k = 5":              false,
		"MERGE (u:U {n: 1}) ON MATCH SET u.name = 'x', u.k = 5": false,
		"MERGE (u:U {n: 1}) SET u = {k: 5, n: 1}":               false,
		"MERGE (u:U {n: 1}) SET u.`k` = 5":                      false,
	} {
		require.Equal(t, want, MergeUniqueConflictIsRetrySafe([]CommitStatement{{Query: statement, Params: params}}, violation), statement)
	}
	require.False(t, MergeUniqueConflictIsRetrySafe([]CommitStatement{{Query: "MERGE (u:U {k: 7})"}}, stderrors.New("other")))
}
