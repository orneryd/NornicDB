package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// At run time a string + value joins the string with the value's text when
// the value is a number, a boolean, a point, a temporal value, a duration or
// a VECTOR; a map is a TypeError and a list makes it list concatenation.
// Expected values are Neo4j 5.26's and 2026.09's (the same).
func TestStringJoinsValuesAtRunTime(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "string_join_runtime"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"UNWIND [{b: true}] AS m RETURN 'a' + m.b AS v":                                 "atrue",
		"UNWIND [{b: false}] AS m RETURN m.b + 'a' AS v":                                "falsea",
		"UNWIND [{b: 1.0}] AS m RETURN 'a' + m.b AS v":                                  "a1.0",
		"UNWIND [{b: point({x: 1, y: 2})}] AS m RETURN 'a' + m.b AS v":                  "apoint({x: 1.0, y: 2.0, crs: 'cartesian'})",
		"UNWIND [{b: date('2020-01-02')}] AS m RETURN 'a' + m.b AS v":                   "a2020-01-02",
		"UNWIND [{b: localtime('12:00')}] AS m RETURN 'a' + m.b AS v":                   "a12:00:00",
		"UNWIND [{b: datetime('2020-01-02T03:04:05Z')}] AS m RETURN 'a' + m.b AS v":     "a2020-01-02T03:04:05Z",
		"UNWIND [{b: duration('P1D')}] AS m RETURN m.b + 'a' AS v":                      "P1Da",
		"CYPHER 25 UNWIND [{b: vector([1, 2], 2, INTEGER)}] AS m RETURN 'a' + m.b AS v": "avector([1, 2], 2, INTEGER64)",
		"UNWIND [{b: [1]}] AS m RETURN 'a' + m.b AS v":                                  []interface{}{"a", int64(1)},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	_, err := exec.Execute(ctx, "UNWIND [{b: {x: 1}}] AS m RETURN 'a' + m.b AS v", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")
	// A type known when the statement compiles is still Neo4j 5's SyntaxError.
	_, err = exec.Execute(ctx, "UNWIND [true] AS b RETURN 'a' + b AS v", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	// An unclosed VECTOR<…> in a type predicate is a SyntaxError.
	_, err = exec.Execute(ctx, "CYPHER 25 RETURN 1 IS :: VECTOR<INTEGER AS v", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}
