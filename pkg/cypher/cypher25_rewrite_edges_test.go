package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The NEXT / WHEN / braced-part rewrite on its edges: comments, NEXT as a
// property key, errors from nested parts, CASE inside a WHEN condition, and
// texts that aren't a conditional (left for the parser to reject).
func TestQueryStructureRewriteEdges(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "structure_edges"))
	columns := exec.StatementColumns

	rewritten, rewrite, err := desugarQueryStructure("RETURN 1 AS x /* NEXT */ NEXT RETURN x + 1 AS y", columns)
	require.NoError(t, err)
	require.NotNil(t, rewrite)
	require.Contains(t, rewritten, "/* NEXT */")
	require.Equal(t, 1, countSubstring(rewritten, "CALL (*)"), "the commented NEXT isn't one")

	_, rewrite, err = desugarQueryStructure("WITH {next: 1} AS n RETURN n. next AS v", columns)
	require.NoError(t, err)
	require.Nil(t, rewrite, "a property key named next isn't NEXT")

	for _, query := range []string{
		"WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS y NEXT RETURN 1 AS z",
		"{ WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS y }",
		"RETURN COUNT { WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS y } AS c",
		"WHEN true THEN { WHEN true THEN RETURN 1 AS x ELSE RETURN 1 AS y } ELSE RETURN 1 AS x",
		"WHEN true THEN RETURN COUNT { WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS y } AS c ELSE RETURN 1 AS c",
	} {
		_, _, err := desugarQueryStructure(query, columns)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}

	for _, query := range []string{
		"RETURN COUNT { MATCH (n)",
		"RETURN 1 AS x NEXT RETURN COUNT { MATCH (n)",
		"WHEN true WHEN false THEN RETURN 1 AS x",
		"WHEN true THEN RETURN 1 AS x THEN RETURN 2 AS x",
		"WHEN true",
		"WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x WHEN false THEN RETURN 3 AS x",
	} {
		rewritten, _, err := desugarQueryStructure(query, columns)
		require.NoError(t, err, query)
		require.NotContains(t, rewritten, whenBranchVariable, query)
	}

	result, err := exec.Execute(context.Background(),
		"CYPHER 25 WHEN CASE WHEN true THEN true ELSE false END THEN RETURN 1 AS x ELSE RETURN 2 AS x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

func countSubstring(text, part string) int {
	count := 0
	for i := 0; i+len(part) <= len(text); i++ {
		if text[i:i+len(part)] == part {
			count++
		}
	}
	return count
}

// The expression rewrites on their edges: ALL as a variable, comments,
// escapes and malformed interpolations, and texts that aren't a map
// comprehension.
func TestCypher25ExpressionRewriteEdges(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "expression_edges"))
	ctx := context.Background()
	for query, want := range map[string][][]interface{}{
		"WITH 1 AS all RETURN all, 2 AS b":        {{int64(1), int64(2)}},
		"WITH 1 AS all RETURN all AS x":           {{int64(1)}},
		"WITH 1 AS all RETURN all , 2 AS b":       {{int64(1), int64(2)}},
		`RETURN s"a\tb{1}" AS v`:                  {{"a\tb1"}},
		"RETURN [x IN ['a'] | x] /* s'{' */ AS v": {{[]interface{}{"a"}}},
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}

	for _, query := range []string{
		`RETURN s"a{b" AS v`,
		`RETURN s"a{ s'x{' }" AS v`,
		`RETURN s"abc`,
	} {
		_, _, err := desugarCypher25Expressions(query)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}

	for _, query := range []string{
		"RETURN {x: v IN m | ",
		"RETURN {k: v IN [1 | 2]} AS m",
		"RETURN {k: v IN {a: 1} | 1} AS m",
		"RETURN {k: v IN {a: 1} | : 1} AS m",
		"RETURN 1 /* s'x */ AS v",
	} {
		rewritten, _, err := desugarCypher25Expressions(query)
		require.NoError(t, err, query)
		require.NotContains(t, rewritten, "apoc.map.fromPairs", query)
	}

	var words []string
	forEachQueryWord("RETURN /* all */ all", func(start, end int) {
		words = append(words, "RETURN /* all */ all"[start:end])
	})
	require.Equal(t, []string{"RETURN", "all"}, words, "a comment's words aren't the query's")
}

// GROUP BY on its edges, with Neo4j 2026.09's answers.
func TestGroupByRewriteEdges(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "group_by_edges"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:E25 {name: 'Ann', age: 40}), (:E25 {name: 'Ann', age: 30}), (:E25 {name: 'Bob', age: 20})", nil)
	require.NoError(t, err)
	l := func(values ...interface{}) []interface{} { return values }
	for query, want := range map[string][][]interface{}{
		"MATCH (n:E25) RETURN DISTINCT n.name AS name, count(*) AS c GROUP BY n.name, n.age":        {l("Ann", int64(1)), l("Ann", int64(1)), l("Bob", int64(1))},
		"MATCH (n:E25) RETURN toUpper(n.name) AS u, count(*) AS c GROUP BY n.name":                  {l("ANN", int64(2)), l("BOB", int64(1))},
		"MATCH (n:E25) RETURN n.name IS NULL AS e, count(*) AS c GROUP BY n.name":                   {l(false, int64(2)), l(false, int64(1))},
		"CALL () { MATCH (n:E25) RETURN n.name AS y, count(*) AS c GROUP BY n.name, '}' } RETURN y": {l("Ann"), l("Bob")},
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		require.ElementsMatch(t, want, result.Rows, query)
	}
	edits, err := groupByEdits("MATCH (n) GROUP BY n")
	require.NoError(t, err)
	require.Empty(t, edits, "a GROUP BY with no RETURN or WITH before it is left to the parser")
}

// The rest of batch 2's edges: IS LABELED with a malformed label, list
// comprehensions over string lists and relationship items, SHOW
// CONSTITUENTS, and Cypher 5 import grouping under WITH DISTINCT.
func TestCypher25BatchTwoEdges(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "batch_two_edges"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:LcA:LcB)-[:LcR]->(:LcA)", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CYPHER 25 MATCH (n:LcA) RETURN n IS LABELED :LcA AS v", nil)
	require.Error(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:LcB) RETURN [x IN labels(n)] AS l", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"LcA", "LcB"}, result.Rows[0][0])

	result, err = exec.Execute(ctx, "MATCH ()-[r:LcR]->() RETURN [x IN [r] | id(x)] AS ids, [x IN [r] | type(x)] AS types", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows[0][0], 1)
	require.Equal(t, []interface{}{"LcR"}, result.Rows[0][1])

	_, err = exec.Execute(ctx, "MATCH (n:LcB) RETURN [x IN [{a: 1}] | type(x)] AS t", nil)
	require.Error(t, err)

	// The same comprehensions where the full evaluator runs them (WHERE).
	result, err = exec.Execute(ctx, "MATCH (n:LcB) WHERE size([x IN labels(n)]) = 2 RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH ()-[r:LcR]->() WHERE size([x IN [r] | id(x)]) = 1 AND [x IN [r] | type(x)] = ['LcR'] RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	// A string list a Go caller stored ([]string) is read as a list.
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "string_lists")
	stringLists := NewStorageExecutor(engine)
	_, err = engine.CreateNode(&storage.Node{ID: "s1", Labels: []string{"Tagged"}, Properties: map[string]interface{}{"tags": []string{"a", "b"}}})
	require.NoError(t, err)
	result, err = stringLists.Execute(ctx, "MATCH (n:Tagged) SET n.copy = [x IN n.tags] RETURN n.copy AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{"a", "b"}}}, result.Rows)

	_, _ = exec.Execute(ctx, "SHOW CONSTITUENTS", nil)

	// A brace in quoted text doesn't close a CALL body.
	result, err = exec.Execute(ctx, "CALL () { RETURN '}' AS y } RETURN y", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"}"}}, result.Rows)

	result, err = exec.Execute(ctx, "WITH 1 AS g RETURN COLLECT { UNWIND [] AS x WITH DISTINCT count(*) AS c RETURN c + g } AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{}}}, result.Rows)
}
