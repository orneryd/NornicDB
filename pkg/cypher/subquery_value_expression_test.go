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
