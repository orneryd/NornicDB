package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCallSubqueryImportsAnyOuterVariable: a CALL subquery after MATCH sees
// every variable the MATCH binds, not only its first node, as in Neo4j
// 5.26 (#648).
func TestCallSubqueryImportsAnyOuterVariable(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"CALL { RETURN 1 AS x } RETURN *", [][]interface{}{{int64(1)}}},
		{"CALL { RETURN 1 AS x } WITH x AS y RETURN y", [][]interface{}{{int64(1)}}},
		{"CALL { RETURN 1 AS x } WITH x AS y WHERE y = 1 RETURN y", [][]interface{}{{int64(1)}}},
		{"CALL { RETURN 1 AS x } UNWIND [x, x + 1] AS y RETURN x, y ORDER BY y", [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}}},
		{"CALL { RETURN 1 AS x } MATCH (n:C648) RETURN x, n.id AS id ORDER BY id", [][]interface{}{{int64(1), "a"}, {int64(1), "b"}, {int64(1), "c"}}},
		{"CALL { CREATE (n:C648 {id:'write'}) RETURN n } RETURN n.id AS id", [][]interface{}{{"write"}}},
		{"MATCH (o:C648 {id:'b'}) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { WITH o RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL { WITH o RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (o) { RETURN o AS z } RETURN z.id AS z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) CALL (i) { RETURN i.id AS z } RETURN z, o.id AS o ORDER BY o", [][]interface{}{{"a", "b"}, {"a", "c"}}},
		{"MATCH (i:C648 {id:'a'})-->(o) WITH o CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z", [][]interface{}{{"b"}, {"c"}}},
		{"MATCH (i:C648 {id:'a'}), (o:C648 {id:'c'}) CALL (o) { RETURN o.id AS z } RETURN i.id AS i, z", [][]interface{}{{"a", "c"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n.id AS i CALL (i) { RETURN i AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n.id AS i CALL { WITH i RETURN i AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) OPTIONAL MATCH (n)-[:NONE]->(t) WITH n, t CALL { WITH n, t WITH n, t WHERE t IS NULL RETURN n.id AS j } RETURN j", [][]interface{}{{"a"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, {k: 'v'} AS m CALL (m) { RETURN m.k AS j } RETURN j", [][]interface{}{{"v"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, 'b' AS s CALL (s) { RETURN s AS j } RETURN j", [][]interface{}{{"b"}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, elementId(n) AS s CALL (s) { RETURN s AS j } RETURN j = elementId(n) AS j", [][]interface{}{{true}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, {id: elementId(n)} AS m CALL (m) { RETURN m AS j } RETURN j.id = elementId(n) AS j", [][]interface{}{{true}}},
		{"MATCH (n:C648 {id:'a'}) WITH n, [elementId(n)] AS l CALL (l) { RETURN size(l) AS j } RETURN j", [][]interface{}{{int64(1)}}},
		{"MATCH (i:C648 {id:'a'}) RETURN COLLECT { MATCH (i)-->(o) CALL (o) { RETURN o.id AS z } RETURN z ORDER BY z } AS l", [][]interface{}{{[]interface{}{"b", "c"}}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call648"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (a:C648 {id:'a'})-[:USES]->(b:C648 {id:'b'}), (a)-[:USES]->(c:C648 {id:'c'})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
		if tc.query == "CALL { RETURN 1 AS x } RETURN *" {
			require.Equal(t, []string{"x"}, result.Columns)
		}
	}
}

func TestCallSubqueryTailPreservesEmptyWildcardSchemaAndWriteStats(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call648_tail"))
	ctx := context.Background()

	result, err := exec.Execute(ctx, "CALL { MATCH (n:C648 {id:'missing'}) RETURN n AS x } RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, result.Columns)
	require.Empty(t, result.Rows)

	result, err = exec.Execute(ctx, "CALL { CREATE (n:C648 {id:'write'}) RETURN n } WITH n AS created RETURN created.id AS id", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, result.Columns)
	require.Equal(t, [][]interface{}{{"write"}}, result.Rows)
	require.EqualValues(t, 1, result.Stats.NodesCreated)
}
