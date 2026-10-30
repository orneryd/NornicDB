package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// '$' goes on a name after its first character, as in Neo4j 5.26.30, whose
// answers these are (#907): a$b, L$M and k$1 are names, $a$b is the
// parameter a$b, and a $ that starts a name is a parameter.
func TestDollarInNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dollar_names"))
	ctx := context.Background()
	run := func(query string, params map[string]interface{}) [][]interface{} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	one := [][]interface{}{{int64(1)}}
	require.Equal(t, one, run("WITH 1 AS a$b RETURN a$b AS c", nil))
	require.Equal(t, one, run("WITH 1 AS a$ RETURN a$ AS c", nil))
	require.Equal(t, one, run("WITH {x$y: 1} AS m RETURN m.x$y AS c", nil))
	require.Equal(t, one, run("RETURN 1 AS c$d", nil))
	require.Equal(t, [][]interface{}{{[]interface{}{"L$M"}, []interface{}{"k$1"}}}, run("CREATE (n:L$M {k$1: 1}) RETURN labels(n), keys(n)", nil))
	require.Equal(t, one, run("MATCH (n:L$M) RETURN n.k$1", nil))
	require.Equal(t, [][]interface{}{{int64(3)}}, run("RETURN $a$b AS v", map[string]interface{}{"a$b": int64(3)}))
	require.Equal(t, [][]interface{}{{int64(3)}}, run("RETURN $a + $b AS v", map[string]interface{}{"a": int64(1), "b": int64(2)}))

	_, err := exec.Execute(ctx, "WITH 1 AS a RETURN a$b AS c", map[string]interface{}{"b": int64(1)})
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	_, err = exec.Execute(ctx, "RETURN $a$b AS v", map[string]interface{}{"a": int64(1), "b": int64(2)})
	requireStatusCode(t, err, "Neo.ClientError.Statement.ParameterMissing")
	require.Contains(t, err.Error(), "a$b")
}

// More statements Neo4j 5.26.30 runs with '$' inside names: variables,
// labels, relationship types and keys in patterns, beside parameters, and a
// dynamic label read from such a variable (#907).
func TestDollarInNamesInPatterns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dollar_patterns"))
	ctx := context.Background()
	params := map[string]interface{}{"p": int64(1), "ids": []interface{}{int64(1), int64(2)}}
	run := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	require.Equal(t, [][]interface{}{{"R$T", int64(2)}}, run("CREATE (a$:P$ {id$: 1})-[r$:R$T {w$: 2}]->(b$:P$ {id$: 2}) RETURN type(r$) AS t, r$.w$ AS w"))
	require.Equal(t, [][]interface{}{{int64(2)}}, run("MATCH (a$:P$)-[r$:R$T]->(b$) WHERE a$.id$ = $p RETURN b$.id$ AS v"))
	require.Equal(t, [][]interface{}{{int64(2)}}, run("MATCH (a$:P$) WHERE a$.id$ IN $ids RETURN count(a$) AS c"))
	require.Equal(t, [][]interface{}{{[]interface{}{int64(1), int64(2)}}}, run("UNWIND $ids AS i$ MATCH (a$:P$ {id$: i$}) RETURN collect(a$.id$) AS v"))
	require.Equal(t, [][]interface{}{{int64(2)}}, run("WITH 'P$' AS l$ MATCH (n:$(l$)) RETURN count(n) AS c"))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MERGE (m$:M$ {k$: $p}) RETURN m$.k$ AS v"))
	require.Equal(t, [][]interface{}{{int64(2)}}, run("MATCH (n:P$) RETURN n.id$ AS id$ ORDER BY id$ DESC LIMIT 1"))
	require.Equal(t, [][]interface{}{{[]interface{}{int64(4), int64(6)}}}, run("RETURN [x$ IN [1,2,3] WHERE x$ > $p | x$ * 2] AS v"))
	run("MATCH (n$:P$) DETACH DELETE n$")
	require.Equal(t, [][]interface{}{{int64(0)}}, run("MATCH (n:P$) RETURN count(n) AS c"))
}

// In Cypher 25 a '$' always starts a parameter, as in Neo4j 2026.09: L$props
// is the label L and the parameter map $props, x IN$list and LIMIT$n read
// parameters, and a$b where a name must be is a SyntaxError (a followed by
// $b), whatever Cypher 5 statement with the same text ran first; a '$' in
// a string or a quoted name is text (#907).
func TestDollarAfterNameCypher25(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dollar_25"))
	ctx := context.Background()
	params := map[string]interface{}{"props": map[string]interface{}{"k": int64(1)}, "list": []interface{}{int64(2)}, "lim": int64(1), "a": int64(1), "a$b": int64(2)}
	run := func(query string) [][]interface{} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	require.Equal(t, [][]interface{}{{[]interface{}{"L"}, int64(1)}}, run("CYPHER 25 CREATE (n:L$props) RETURN labels(n) AS l, n.k AS k"))
	require.Equal(t, [][]interface{}{{int64(1)}}, run("CYPHER 25 CREATE (a:L)-[r:T$props]->(b:L) RETURN r.k AS k"))
	require.Equal(t, [][]interface{}{{[]interface{}{int64(2)}}}, run("CYPHER 25 UNWIND [1, 2] AS x WITH x WHERE x IN$list RETURN collect(x) AS v"))
	require.Len(t, run("CYPHER 25 MATCH (n:L) RETURN n.k AS k LIMIT$lim"), 1)
	result, err := exec.Execute(ctx, "CYPHER 25 UNWIND [2] AS x RETURN x IN$list", params)
	require.NoError(t, err)
	require.Equal(t, []string{"x IN$list"}, result.Columns, "the column keeps the client's text")

	_, err = exec.Execute(ctx, "RETURN 1 AS a$b", nil)
	require.NoError(t, err)
	for _, query := range []string{"CYPHER 25 RETURN 1 AS a$b", "CYPHER 25 WITH 1 AS a RETURN a$ AS v", "CYPHER 25 RETURN $a$b AS v"} {
		_, err = exec.Execute(ctx, query, params)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	require.Equal(t, [][]interface{}{{int64(1), "x$y", int64(1)}}, run("CYPHER 25 RETURN $a AS v, 'x$y' AS s, 1 AS `a$b`"))
	_, none := separateCypher25Parameters("RETURN $a, 'b$c'")
	require.Nil(t, none)
}
