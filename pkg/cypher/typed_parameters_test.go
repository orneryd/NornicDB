package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestTypedGoParametersAreCypherValues pins #712: a parameter whose Go type is
// a typed map is a Cypher map, a slice of maps or lists is a Cypher list, and
// property access on a list is Neo4j's type error. Expected results are
// Neo4j 5.26.30's for the equivalent untyped parameter.
func TestTypedGoParametersAreCypherValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "typedparams"))
	ctx := context.Background()
	for _, tc := range []struct {
		query  string
		params map[string]interface{}
		want   [][]interface{}
	}{
		{"WITH $m AS m RETURN m.a AS a", map[string]interface{}{"m": map[string]string{"a": "x"}}, [][]interface{}{{"x"}}},
		{"UNWIND [1] AS i WITH $m AS m, i RETURN m.a AS a", map[string]interface{}{"m": map[string]string{"a": "x"}}, [][]interface{}{{"x"}}},
		{"RETURN $m.a AS a", map[string]interface{}{"m": map[string]string{"a": "x"}}, [][]interface{}{{"x"}}},
		{"WITH $m AS m RETURN m.a AS a", map[string]interface{}{"m": map[string]int64{"a": 1}}, [][]interface{}{{int64(1)}}},
		{"RETURN $m AS m", map[string]interface{}{"m": map[string][]string{"k": {"v"}}}, [][]interface{}{{map[string]interface{}{"k": []interface{}{"v"}}}}},
		{"UNWIND $m AS x RETURN x.k AS k", map[string]interface{}{"m": []map[string]string{{"k": "a"}, {"k": "b"}}}, [][]interface{}{{"a"}, {"b"}}},
		{"UNWIND $m AS x RETURN x", map[string]interface{}{"m": []string{"p", "q"}}, [][]interface{}{{"p"}, {"q"}}},
		{"RETURN size($m) AS n, $m[0] AS f", map[string]interface{}{"m": []string{"p", "q"}}, [][]interface{}{{int64(2), "p"}}},
	} {
		result, err := exec.Execute(ctx, tc.query, tc.params)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
	}

	// A map isn't a property value, typed or not.
	_, err := exec.Execute(ctx, "CREATE (n:PM {v: $m}) RETURN n.v AS v", map[string]interface{}{"m": map[string]string{"a": "x"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "TypeError")

	// Property access on a list, integer or string is a type error (Neo4j:
	// "Type mismatch: expected Map, Node, Relationship, … but was List").
	for _, value := range []interface{}{[]string{"p", "q"}, []interface{}{"p", "q"}, int64(5), "str"} {
		for _, query := range []string{"WITH $m AS m RETURN m.a AS a", "UNWIND [1] AS i WITH $m AS m, i RETURN m.a AS a"} {
			_, err := exec.Execute(ctx, query, map[string]interface{}{"m": value})
			require.Error(t, err, "%s %T", query, value)
			require.Contains(t, err.Error(), "Type mismatch: expected Map", "%s %T", query, value)
		}
	}
	// A null base is a null access.
	result, err := exec.Execute(ctx, "WITH $m AS m RETURN m.a AS a", map[string]interface{}{"m": nil})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil}}, result.Rows)
}

// TestParameterReferencesOutsideStringLiterals pins #701: a $name inside a
// string literal, or a backtick-quoted name, is text, not a parameter.
func TestParameterReferencesOutsideStringLiterals(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "paramtext"))
	ctx := context.Background()
	for _, tc := range []struct {
		query  string
		params map[string]interface{}
		want   interface{}
	}{
		{"RETURN 'price: $p USD' AS s", map[string]interface{}{"p": 12}, "price: $p USD"},
		{"RETURN \"$p\" AS s", map[string]interface{}{"p": "x"}, "$p"},
		{"RETURN 'it''s $p' AS s", map[string]interface{}{"p": "x"}, "it's $p"},
		{`RETURN 'a\'$p' AS s`, map[string]interface{}{"p": "x"}, "a'$p"},
		{"RETURN $p + ' and $p' AS s", map[string]interface{}{"p": "x"}, "x and $p"},
	} {
		result, err := exec.Execute(ctx, tc.query, tc.params)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows[0][0], tc.query)
	}
	_, err := exec.Execute(ctx, "CREATE (n:StrParam {note: 'costs $amount'})", map[string]interface{}{"amount": 5})
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n:StrParam) WHERE n.note = 'costs $amount' RETURN count(n) AS c", map[string]interface{}{"amount": 5})
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
	require.Equal(t, "WHERE n.name = 'N' AND n.v = '$name'", ReplaceParameters("WHERE n.name = $name AND n.v = '$name'", func(string) string { return "'N'" }))
}

// TestPropertyAccessOnMapProjection: a property read straight off a map
// projection is the projected value, as in Neo4j (#712 section).
func TestPropertyAccessOnMapProjection(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "mapprojaccess"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:MP {k: 'a'})", nil)
	require.NoError(t, err)
	for query, want := range map[string]interface{}{
		"MATCH (n:MP) RETURN n {.k}.k AS x":        "a",
		"MATCH (n:MP) RETURN n {.k, z: 1}.z AS x":  int64(1),
		"MATCH (n:MP) RETURN n {.*}.k AS x":        "a",
		"WITH {a: 1} AS m RETURN m {.a}.a AS x":    int64(1),
		"MATCH (n:MP) RETURN (n {.k}).k AS x":      "a",
		"MATCH (n:MP) WITH n {.k} AS m RETURN m.k": "a",
		"RETURN {a: {b: 1}}.a.b AS x":              int64(1),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	// In a pattern, a property map still needs key-value entries.
	_, err = exec.Execute(ctx, "MATCH (n {.k}) RETURN n", nil)
	require.Error(t, err)
}

// TestBacktickQuotedNames: a backtick-quoted name may hold any character.
// `$p` is a variable, not the parameter p; property keys with spaces, dots
// or $ are written by CREATE and SET as by MERGE (#712 section, Neo4j 5.26).
func TestBacktickQuotedNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "backticknames"))
	ctx := context.Background()
	params := map[string]interface{}{"p": int64(12)}

	_, err := exec.Execute(ctx, "RETURN `$p` AS v", params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not defined")
	result, err := exec.Execute(ctx, "WITH 1 AS `$p` RETURN `$p` AS v", params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	result, err = exec.Execute(ctx, "CREATE (n:BT {`a b`: 2, `x.y`: 3, `$p`: 1}) RETURN n.`a b` AS a, n.`x.y` AS x, n.`$p` AS p", params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(3), int64(1)}}, result.Rows)
	_, err = exec.Execute(ctx, "UNWIND [1] AS i CREATE (n:BT2 {`g h`: i})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:BT3)-[:R {`r s`: 1}]->(:BT3)", nil)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MATCH (n:BT) SET n.`a b` = 5, n.`c$d` = 6, n.`$q` = 7 RETURN n.`a b` AS a, n.`c$d` AS c, n.`$q` AS q", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5), int64(6), int64(7)}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH (n:BT2) RETURN n.`g h` AS g", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN r.`r s` AS s", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	// Unquoted map keys are still checked.
	_, err = exec.Execute(ctx, "CREATE (n:BT {a b: 1})", nil)
	require.Error(t, err)
}
