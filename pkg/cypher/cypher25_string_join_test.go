package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A string joins any value but a map or a graph entity. In a Cypher 25
// statement that holds when the types are known as it compiles (Neo4j
// 2026.09); a Cypher 5 statement keeps Neo4j 5.26's SyntaxError there. At
// run time both join (Neo4j 5.26 and 2026.09).
func TestCypher25StringJoinsValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_string_join"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"CYPHER 25 RETURN 'ab' + true AS v":                            "abtrue",
		"CYPHER 25 RETURN true + 'ab' AS v":                            "trueab",
		"CYPHER 25 WITH true AS t RETURN 'ab' + t AS v":                "abtrue",
		"CYPHER 25 RETURN 's' + 1.5 AS v":                              "s1.5",
		"CYPHER 25 RETURN 'x' + date('2020-01-02') AS v":               "x2020-01-02",
		"CYPHER 25 RETURN duration('P1D') + 's' AS v":                  "P1Ds",
		"CYPHER 25 RETURN 's' + point({x: 1, y: 2}) AS v":              "spoint({x: 1.0, y: 2.0, crs: 'cartesian'})",
		"CYPHER 25 RETURN localtime('12:00') + 's' AS v":               "12:00:00s",
		"UNWIND [{b: true}] AS m RETURN 'a' + m.b AS v":                "atrue",
		"UNWIND [{b: date('2020-01-02')}] AS m RETURN m.b + 'a' AS v":  "2020-01-02a",
		"UNWIND [{b: duration('P1D')}] AS m RETURN 'a' + m.b AS v":     "aP1D",
		"UNWIND [{b: point({x: 1, y: 2})}] AS m RETURN 'a' + m.b AS v": "apoint({x: 1.0, y: 2.0, crs: 'cartesian'})",
		"UNWIND [{b: [1]}] AS m RETURN 'a' + m.b AS v":                 []interface{}{"a", int64(1)},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	for query, code := range map[string]string{
		"RETURN 'ab' + true AS v":                         "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 5 RETURN 'x' + date('2020-01-02') AS v":   "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN 's' + {a: 1} AS v":              "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN true + 1 AS v":                  "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 MATCH (n) RETURN n + 's' AS v":         "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [{b: {x: 1}}] AS m RETURN 'a' + m.b AS v": "Neo.ClientError.Statement.TypeError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, code)
	}
	// The validation caches keep the two versions apart.
	_, err := exec.Execute(ctx, "CYPHER 25 RETURN 'q' + true AS v", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CYPHER 5 RETURN 'q' + true AS v", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}
