package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSubqueryExpressionsAsValues covers NornicDB issue #652: EXISTS { … },
// COUNT { … } and COLLECT { … } are values anywhere an expression is allowed —
// list and map literals, list-comprehension filters and projections,
// arithmetic, CASE, UNWIND lists, WHERE comparisons, ORDER BY, and SET /
// SET += / CREATE property values. The expected rows are Neo4j 5.26's for the
// same statements.
func TestSubqueryExpressionsAsValues(t *testing.T) {
	setup := []string{
		"CREATE (:W {id: 'a'}), (:W {id: 'b'}), (:W {id: 'c'})",
		"MATCH (a:W {id: 'a'}), (b:W {id: 'b'}) CREATE (a)-[:USES]->(b)",
	}
	tests := []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (i:W) RETURN i.id AS id, [EXISTS { MATCH (i)-->() }, 1] AS e ORDER BY id",
			[][]interface{}{{"a", []interface{}{true, int64(1)}}, {"b", []interface{}{false, int64(1)}}, {"c", []interface{}{false, int64(1)}}}},
		{"MATCH (i:W) WITH i, {k: COUNT { MATCH (i)-->() }} AS e RETURN i.id AS id, e ORDER BY id",
			[][]interface{}{{"a", map[string]interface{}{"k": int64(1)}}, {"b", map[string]interface{}{"k": int64(0)}}, {"c", map[string]interface{}{"k": int64(0)}}}},
		{"MATCH (i:W) RETURN i.id AS id, size([x IN [1] WHERE EXISTS { MATCH (i)-->() }]) AS e ORDER BY id",
			[][]interface{}{{"a", int64(1)}, {"b", int64(0)}, {"c", int64(0)}}},
		{"MATCH (i:W) RETURN i.id AS id, [x IN [0, 1] | COUNT { MATCH (i)-->() } + x] AS e ORDER BY id",
			[][]interface{}{{"a", []interface{}{int64(1), int64(2)}}, {"b", []interface{}{int64(0), int64(1)}}, {"c", []interface{}{int64(0), int64(1)}}}},
		{"MATCH (i:W) RETURN i.id AS id, [x IN ['b', 'z'] WHERE EXISTS { MATCH (i)-->(o) WHERE o.id = x }] AS e ORDER BY id",
			[][]interface{}{{"a", []interface{}{"b"}}, {"b", []interface{}{}}, {"c", []interface{}{}}}},
		{"MATCH (i:W) RETURN i.id AS id, {c: COLLECT { MATCH (i)-->(o) RETURN o.id }} AS e ORDER BY id",
			[][]interface{}{{"a", map[string]interface{}{"c": []interface{}{"b"}}}, {"b", map[string]interface{}{"c": []interface{}{}}}, {"c", map[string]interface{}{"c": []interface{}{}}}}},
		{"MATCH (i:W) WITH i UNWIND [COUNT { MATCH (i)-->() }, 5] AS v RETURN i.id AS id, v ORDER BY id, v",
			[][]interface{}{{"a", int64(1)}, {"a", int64(5)}, {"b", int64(0)}, {"b", int64(5)}, {"c", int64(0)}, {"c", int64(5)}}},
		{"MATCH (i:W) WHERE [EXISTS { MATCH (i)-->() }] = [true] RETURN i.id AS id",
			[][]interface{}{{"a"}}},
		{"MATCH (i:W) SET i.k = COUNT { MATCH (i)-->() } RETURN i.id AS id, i.k AS k ORDER BY id",
			[][]interface{}{{"a", int64(1)}, {"b", int64(0)}, {"c", int64(0)}}},
		{"MATCH (i:W) SET i += {owner: EXISTS { MATCH (i)-->() }} RETURN i.id AS id, i.owner AS o ORDER BY id",
			[][]interface{}{{"a", true}, {"b", false}, {"c", false}}},
		{"MATCH (i:W) SET i.c = COLLECT { MATCH (i)-->(o) RETURN o.id } RETURN i.id AS id, i.c AS c ORDER BY id",
			[][]interface{}{{"a", []interface{}{"b"}}, {"b", []interface{}{}}, {"c", []interface{}{}}}},
		{"MATCH (i:W) SET i.k = CASE WHEN EXISTS { MATCH (i)-->() } THEN 1 ELSE 0 END RETURN i.id AS id, i.k AS k ORDER BY id",
			[][]interface{}{{"a", int64(1)}, {"b", int64(0)}, {"c", int64(0)}}},
		{"MATCH (i:W) CREATE (f:F {id: i.id, k: COUNT { MATCH (i)-->() }}) RETURN f.id AS id, f.k AS k ORDER BY id",
			[][]interface{}{{"a", int64(1)}, {"b", int64(0)}, {"c", int64(0)}}},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornicdb652"))
			ctx := context.Background()
			for _, statement := range setup {
				_, err := executor.Execute(ctx, statement, nil)
				require.NoError(t, err)
			}
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, result.Rows)
		})
	}
}

// TestSubqueryExpressionsRejectedInMerge covers the MERGE side of #652: Neo4j
// rejects a subquery expression anywhere in a MERGE clause.
func TestSubqueryExpressionsRejectedInMerge(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornicdb652merge"))
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE (:W {id: 'a'})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (i:W) MERGE (f:G {id: i.id, k: COUNT { MATCH (i)-->() }}) RETURN f",
		"MATCH (i:W) MERGE (f:G {k: [EXISTS { MATCH (i)-->() }]}) RETURN f",
		"MATCH (i:W) MERGE (i)-[r:R {k: COLLECT { MATCH (i)-->(o) RETURN o.id }}]->(i) RETURN r",
		"MATCH (i:W) MERGE (f:G {id: 1}) ON CREATE SET f.k = COUNT { MATCH (i)-->() } RETURN f",
	} {
		_, err := executor.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError", query)
		require.Contains(t, err.Error(), "Subquery expressions are not allowed in a MERGE clause.", query)
	}
	result, err := executor.Execute(ctx, "MATCH (f:G) RETURN count(f)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Rows[0][0])
}

// TestWholeSubqueryItemsWithOrderBy covers the #652 section on ORDER BY /
// LIMIT inside a COLLECT or COUNT subquery that is a whole RETURN or WITH item
// (rejected, or null, before). Expected rows are Neo4j 5.26's.
func TestWholeSubqueryItemsWithOrderBy(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornicdb652order"))
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE (a:W {id: 'a'})-[:USES]->(b:W {id: 'b'}), (c:W {id: 'c'}), (a)-[:USES]->(c)", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (i:W) RETURN i.id AS id, COLLECT { MATCH (i)-->(o) RETURN o.id ORDER BY o.id } AS l ORDER BY id":              {{"a", []interface{}{"b", "c"}}, {"b", []interface{}{}}, {"c", []interface{}{}}},
		"MATCH (i:W) RETURN i.id AS id, COLLECT { MATCH (i)-->(o) RETURN o.id ORDER BY o.id DESC LIMIT 1 } AS l ORDER BY id": {{"a", []interface{}{"c"}}, {"b", []interface{}{}}, {"c", []interface{}{}}},
		"MATCH (i:W) WITH i, COLLECT { MATCH (i)-->(o) RETURN o.id ORDER BY o.id } AS l RETURN i.id AS id, l ORDER BY id":    {{"a", []interface{}{"b", "c"}}, {"b", []interface{}{}}, {"c", []interface{}{}}},
		"MATCH (i:W) RETURN i.id AS id, COUNT { MATCH (i)-->(o) RETURN o ORDER BY o.id } AS l ORDER BY id":                   {{"a", int64(2)}, {"b", int64(0)}, {"c", int64(0)}},
		"MATCH (i:W) RETURN i.id AS id, size(COLLECT { MATCH (i)-->(o) RETURN o.id ORDER BY o.id }) AS l ORDER BY id":        {{"a", int64(2)}, {"b", int64(0)}, {"c", int64(0)}},
	} {
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}

// TestProjectionAliasIndexIsTopLevel: the alias AS of a projection item is the
// one outside strings and nested expressions (#652, #547).
func TestProjectionAliasIndexIsTopLevel(t *testing.T) {
	for item, want := range map[string][2]string{
		"n.id AS id":                            {"n.id", "id"},
		"'a AS b' AS s":                         {"'a AS b'", "s"},
		"COUNT { UNWIND [1, 2] AS y RETURN y }": {"COUNT { UNWIND [1, 2] AS y RETURN y }", "COUNT { UNWIND [1, 2] AS y RETURN y }"},
		"COLLECT { UNWIND l AS y RETURN y ORDER BY y } AS l": {"COLLECT { UNWIND l AS y RETURN y ORDER BY y }", "l"},
		"[x IN [1] | x] AS `my list`":                        {"[x IN [1] | x]", "my list"},
		"n.alias":                                            {"n.alias", "n.alias"},
	} {
		expr, alias := parseProjectionExprAlias(item)
		require.Equal(t, want, [2]string{expr, alias}, item)
	}
}

// TestSubqueryItemsWithNestedKeywords: keywords inside a whole subquery item
// (AS, ORDER BY, LIMIT) belong to the subquery on every projection route, and
// a COUNT body reading an outer scalar isn't counted without it.
func TestSubqueryItemsWithNestedKeywords(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE (a:K {id: 'a'})-[:T]->(:K {id: 'bb'}), (a)-[:T]->(:K {id: 'cccccc'})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"UNWIND [1, 2, 3] AS x RETURN x, COLLECT { UNWIND [3, 1, 2] AS y RETURN y ORDER BY y } AS l ORDER BY x DESC LIMIT 2": {{int64(3), []interface{}{int64(1), int64(2), int64(3)}}, {int64(2), []interface{}{int64(1), int64(2), int64(3)}}},
		"UNWIND [1] AS x RETURN COUNT { UNWIND [1, 2] AS y RETURN y } AS c, x":                                               {{int64(2), int64(1)}},
		"RETURN 'a AS b' AS s, [x IN [1] | x] AS l":                                                                          {{"a AS b", []interface{}{int64(1)}}},
		"WITH 5 AS k MATCH (i:K {id: 'a'}) RETURN COUNT { (i)-->(o) WHERE size(o.id) < k } AS c":                             {{int64(1)}},
		"MATCH (i:K {id: 'a'}) RETURN COUNT { (i)-->(o) WHERE size(o.id) < 5 } AS c":                                         {{int64(1)}},
	} {
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}
