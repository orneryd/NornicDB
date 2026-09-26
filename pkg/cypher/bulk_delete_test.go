package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestIDInParamSeekIsTheWholeWhere pins the id IN $list seek the pipeline
// uses without evaluating the WHERE again per node (#703): missing ids,
// duplicates, non-string items, element ids and the pattern's label and
// properties select exactly what the WHERE would.
func TestIDInParamSeekIsTheWholeWhere(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "idinseek"))
	ctx := context.Background()
	result, err := exec.Execute(ctx, "CREATE (a:S {k: 1}), (b:S {k: 2}), (c:T {k: 1}) RETURN id(a) AS a, id(b) AS b, id(c) AS c, elementId(b) AS eb", nil)
	require.NoError(t, err)
	a, b, c, eb := result.Rows[0][0], result.Rows[0][1], result.Rows[0][2], result.Rows[0][3]
	for _, tc := range []struct {
		query string
		ids   []interface{}
		want  int64
	}{
		{"MATCH (n) WHERE id(n) IN $ids RETURN count(n) AS c", []interface{}{a, b, c, "missing", a, int64(7), nil}, 3},
		{"MATCH (n:S) WHERE id(n) IN $ids RETURN count(n) AS c", []interface{}{a, b, c}, 2},
		{"MATCH (n:S {k: 1}) WHERE id(n) IN $ids RETURN count(n) AS c", []interface{}{a, b, c}, 1},
		{"MATCH (n) WHERE elementId(n) IN $ids RETURN count(n) AS c", []interface{}{eb}, 1},
		{"MATCH (n) WHERE id(n) IN $ids RETURN count(n) AS c", []interface{}{}, 0},
	} {
		result, err := exec.Execute(ctx, tc.query, map[string]interface{}{"ids": tc.ids})
		require.NoError(t, err, tc.query)
		require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.query)
	}
	result, err = exec.Execute(ctx, "MATCH (n) WHERE id(n) IN $ids RETURN count(n) AS c", map[string]interface{}{"ids": nil})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

// TestLargeDetachDeleteInOneStatementAndInTransactions deletes a connected
// graph in one statement and in batches: every node and relationship goes,
// and the counters count each once (#703).
func TestLargeDetachDeleteInOneStatementAndInTransactions(t *testing.T) {
	for _, statement := range []string{
		"MATCH (n:BD) DETACH DELETE n",
		"MATCH (n:BD) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 70 ROWS",
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "bulkdelete"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "UNWIND range(1, 500) AS i CREATE (:BD {i: i})", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "MATCH (a:BD), (b:BD) WHERE b.i = a.i + 1 CREATE (a)-[:NEXT]->(b)", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CREATE (:Keep)", nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, 500, result.Stats.NodesDeleted, statement)
		require.Equal(t, 499, result.Stats.RelationshipsDeleted, statement)
		result, err = exec.Execute(ctx, "MATCH (n) RETURN labels(n) AS l, count(*) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{"Keep"}, int64(1)}}, result.Rows, fmt.Sprint(statement))
		result, err = exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, statement)
	}
}
