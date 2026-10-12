package cypher

import (
	"context"
	"strings"
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

// Neo4j 2026.09's other compile-time rules for a Cypher 25 statement: unary
// + and a subscript take no temporal value or duration, and reverse()'s
// argument is checked at run time. A Cypher 5 statement keeps Neo4j 5.26's.
func TestCypher25CompileTimeTypes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_compile_types"))
	ctx := context.Background()
	for query, code := range map[string]string{
		"CYPHER 25 RETURN +date('2021-03-04') AS v":                         "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN +duration('PT1H') AS v":                           "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN date('2020-01-02')['a'] AS v":                     "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 WITH duration('PT1H') AS d RETURN d['a'] AS v":           "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 WITH date('2020-01-02') AS d, 'a' AS k RETURN d[k] AS v": "Neo.ClientError.Statement.SyntaxError",
		"CYPHER 25 RETURN reverse(1) AS v":                                  "Neo.ClientError.Statement.TypeError",
		"CYPHER 25 WITH true AS b RETURN reverse(b) AS v":                   "Neo.ClientError.Statement.TypeError",
		"RETURN reverse(1) AS v":                                            "Neo.ClientError.Statement.SyntaxError",
		"WITH true AS b RETURN reverse(b) AS v":                             "Neo.ClientError.Statement.SyntaxError",
		"RETURN date('2020-01-02')['a'] AS v":                               "Neo.ClientError.Statement.TypeError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, code)
	}
	result, err := exec.Execute(ctx, "RETURN +date('2021-03-04') AS v", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	result, err = exec.Execute(ctx, "CYPHER 25 RETURN reverse('ab') AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"ba"}}, result.Rows)
}

// head(), last() and reverse() of a value of the wrong kind are TypeErrors at
// run time, in both Neo4j versions; null gives null.
func TestListAccessFunctionsAtRunTime(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_access_runtime"))
	ctx := context.Background()
	for _, query := range []string{
		"UNWIND [{v: 1}] AS m RETURN head(m.v) AS h",
		"UNWIND [{v: 1}] AS m RETURN last(m.v) AS h",
		"UNWIND [{v: 'ab'}] AS m RETURN head(m.v) AS h",
		"UNWIND [{v: true}] AS m RETURN reverse(m.v) AS h",
		"UNWIND [{v: {a: 1}}] AS m RETURN reverse(m.v) AS h",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")
	}
	for query, want := range map[string]interface{}{
		"UNWIND [{v: 'ab'}] AS m RETURN reverse(m.v) AS h":   "ba",
		"UNWIND [{v: [1, 2]}] AS m RETURN reverse(m.v) AS h": []interface{}{int64(2), int64(1)},
		"UNWIND [{v: [1, 2]}] AS m RETURN head(m.v) AS h":    int64(1),
		"UNWIND [{v: [1, 2]}] AS m RETURN last(m.v) AS h":    int64(2),
		"UNWIND [{v: []}] AS m RETURN head(m.v) AS h":        nil,
		"UNWIND [{v: null}] AS m RETURN reverse(m.v) AS h":   nil,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}

// A negative start or length of substring(), left() or right() is Neo4j
// 2026.09's ArgumentError in a Cypher 25 statement and Neo4j 5.26's
// ExecutionFailed in a Cypher 5 one, in either evaluator.
func TestCypher25StringOperationOutOfRange(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_string_range"))
	ctx := context.Background()
	for _, expression := range []string{"left('Ab c', -3)", "right('Ab c', -3)", "substring('Ab c', -3)", "substring('Ab c', 1, -1)"} {
		for _, query := range []string{
			"RETURN " + expression + " AS v",
			"WITH 'Ab c' AS s RETURN " + expression + " AS v",
			"MATCH (n) WITH count(n) AS c RETURN " + expression + " AS v",
		} {
			_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
			require.Error(t, err, query)
			requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
			_, err = exec.Execute(ctx, query, nil)
			require.Error(t, err, query)
			requireStatusCode(t, err, "Neo.DatabaseError.Statement.ExecutionFailed")
		}
	}
	result, err := exec.Execute(ctx, "CYPHER 25 UNWIND [1] AS i WITH i RETURN left('Ab c', i) AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"A"}}, result.Rows)
}

// trim's specification argument (trim(specification, original) and
// trim(specification, character, original)) is matched in any case in a
// Cypher 25 statement, where a null one is null and other text Neo4j
// 2026.09's ArgumentError; a Cypher 5 statement keeps Neo4j 5.26's exact
// match, both ends for other text, and a TypeError for null.
func TestCypher25TrimSpecification(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_trim"))
	ctx := context.Background()
	for _, tc := range []struct {
		expression string
		cypher25   interface{}
		cypher5    interface{}
	}{
		{"trim('leading', ' x ')", "x ", "x"},
		{"trim('Trailing', ' x ')", " x", "x"},
		{"trim('BOTH', ' x ')", "x", "x"},
		{"trim('LEADING', 'a', 'aba')", "ba", "ba"},
		{"trim('zz', 'ab')", "Neo.ClientError.Statement.ArgumentError", "ab"},
		{"trim(' both ', ' x ')", "Neo.ClientError.Statement.ArgumentError", "x"},
		{"trim(s, s)", "Neo.ClientError.Statement.ArgumentError", "ab"},
		{"trim(null, s)", nil, "Neo.ClientError.Statement.TypeError"},
		{"trim(null, 'a', 'aba')", nil, "Neo.ClientError.Statement.TypeError"},
		{"trim(null, 'ab', 'aba')", nil, "Neo.ClientError.Statement.TypeError"},
		{"trim('zz', 'ab', 'aba')", "Neo.ClientError.Statement.ArgumentError", "Neo.ClientError.Statement.ArgumentError"},
		{"trim('zz', null)", nil, nil},
	} {
		for version, want := range map[string]interface{}{"CYPHER 25 ": tc.cypher25, "": tc.cypher5} {
			for _, query := range []string{
				version + "WITH 'ab' AS s RETURN " + tc.expression + " AS v",
				version + "MATCH (n) WITH count(n) AS c WITH c, 'ab' AS s RETURN " + tc.expression + " AS v",
			} {
				result, err := exec.Execute(ctx, query, nil)
				if code, isCode := want.(string); isCode && strings.HasPrefix(code, "Neo.") {
					require.Error(t, err, query)
					requireStatusCode(t, err, code)
					continue
				}
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
			}
		}
	}
}
