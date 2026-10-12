package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A void procedure called first leaves its row as it is, as in Neo4j: CALL
// tx.setMetaData(…) before MATCH … SET runs the update and returns its row;
// any other void procedure (db.awaitIndexes) lets the clauses after it run
// too. A procedure with results still needs YIELD for its columns.
func TestVoidProcedureBeforeAClauseKeepsTheRow(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "voidproc"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Context {id: 'ctx', name: 'a'})", nil)
	require.NoError(t, err)
	// In order: the read after the update sees it.
	for _, step := range []struct {
		query string
		want  [][]interface{}
	}{
		{"CALL tx.setMetaData({writer: 'w'}) MATCH (c:Context {id: 'ctx'}) SET c.name = 'c' RETURN c.name", [][]interface{}{{"c"}}},
		{"CALL db.awaitIndexes() MATCH (c:Context {id: 'ctx'}) RETURN c.name AS name", [][]interface{}{{"c"}}},
		{"CALL db.awaitIndexes() RETURN 1 AS one", [][]interface{}{{int64(1)}}},
		{"CALL db.awaitIndexes() UNWIND [1, 2] AS x RETURN count(x) AS n", [][]interface{}{{int64(2)}}},
	} {
		result, err := exec.Execute(ctx, step.query, nil)
		require.NoError(t, err, step.query)
		require.Equal(t, step.want, result.Rows, step.query)
	}
	result, err := exec.Execute(ctx, "MATCH (c:Context {id: 'ctx'}) RETURN c.name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"c"}}, result.Rows, "the update ran")
}
