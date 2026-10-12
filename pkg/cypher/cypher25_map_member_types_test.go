package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// In a Cypher 25 statement the members of a map literal have static types
// (Neo4j 2026.09): through a WITH alias, WITH *, nesting and a string
// literal key, a member of the wrong type is a compile-time SyntaxError. A
// Cypher 5 statement keeps Neo4j 5.26's run-time TypeError, and a member read
// with a key from a variable, or of a map from UNWIND, stays a run-time
// error in both versions. Recorded on Neo4j 2026.09 and 5.26.
func TestCypher25MapMemberTypes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_map_members"))
	ctx := context.Background()
	const syntaxError, typeError = "Neo.ClientError.Statement.SyntaxError", "Neo.ClientError.Statement.TypeError"
	for _, tc := range []struct {
		query             string
		cypher25, cypher5 string
	}{
		{"WITH {a: 2, b: 'x'} AS m WHERE m.a RETURN 1 AS v", syntaxError, typeError},
		{"WITH {a: 2, b: 'x'} AS m WHERE m.b RETURN 1 AS v", syntaxError, typeError},
		{"WITH {a: 2} AS m RETURN m.a + true AS v", syntaxError, typeError},
		{"WITH {a: 2} AS m RETURN toUpper(m.a) AS v", syntaxError, ""},
		{"WITH {a: {b: 1}} AS m WHERE m.a.b RETURN 1 AS v", syntaxError, typeError},
		{"WITH {a: 2} AS m WITH m AS k WHERE k.a RETURN 1 AS v", syntaxError, typeError},
		{"WITH {a: 2} AS m WITH * WHERE m.a RETURN 1 AS v", syntaxError, typeError},
		{"WITH 1 AS x WHERE {a: 2}['a'] RETURN 1 AS v", syntaxError, typeError},
		{"WITH {a: 2} AS m, 'a' AS k WHERE m[k] RETURN 1 AS v", typeError, typeError},
		{"UNWIND [{a: 2}] AS m WITH m WHERE m.a RETURN 1 AS v", typeError, typeError},
	} {
		for prefix, want := range map[string]string{"CYPHER 25 ": tc.cypher25, "CYPHER 5 ": tc.cypher5} {
			if want == "" {
				continue
			}
			_, err := exec.Execute(ctx, prefix+tc.query, nil)
			require.Error(t, err, prefix+tc.query)
			requireStatusCode(t, err, want)
		}
	}
	for query, want := range map[string]interface{}{
		"WITH {a: 2, b: 'x'} AS m WHERE m.c RETURN 1 AS v":                        nil,
		"WITH {a: true} AS m WHERE m.a RETURN 1 AS v":                             int64(1),
		"WITH {a: 2} AS m UNWIND [{a: true}] AS m WITH m WHERE m.a RETURN 1 AS v": int64(1),
		"WITH {a: 2} AS m RETURN [m IN [{a: 'x'}] | toUpper(m.a)][0] AS v":        "X",
		"WITH {a: 2} AS m RETURN m['a'] + 1 AS v":                                 int64(3),
		"WITH {a: 2} AS m WITH m WHERE m.a > 1 RETURN 1 AS v":                     int64(1),
		"WITH {`a b`: 'x'} AS m RETURN toUpper(m.`a b`) + toUpper(m['a b']) AS v": "XX",
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		if want == nil {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	_, err := exec.Execute(ctx, "CYPHER 25 WITH 1 AS i RETURN toUpper(i.a) AS v", nil)
	requireStatusCode(t, err, syntaxError)
	scope := staticTypeScope{cypher25: true}
	require.Nil(t, projectStaticValueMembers(scope, "WITH {a: 1}"))
	require.Equal(t, map[string]map[string]staticOperand{"m": {"a": knownOperand("Integer")}}, projectStaticValueMembers(scope, "WITH {a: 1} AS m"))
	require.Nil(t, projectStaticValueMembers(staticTypeScope{}, "WITH {a: 1} AS m"))
	require.Equal(t, "", staticTypeScope{values: map[string]string{"i": "Integer"}, cypher25: true}.staticExpressionType("i.a"))
	for expression, member := range map[string]bool{"m.a": true, "m['a'].b": true, "m.`a b`": true, "m[k]": false, "m": false, "m.a + 1": false, "m.": false, "m[": false} {
		_, ok := staticMemberAccessBase(expression)
		require.Equal(t, member, ok, expression)
	}
}
