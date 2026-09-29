package cypher

// gh648_call_subquery_writes_test.go — regression tests for #648: with the
// variable-scope CALL form, a write subquery must apply per outer row, label
// SET must parse, the returned row must reflect the write, and IN
// TRANSACTIONS must not be accepted inside an explicit transaction.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newGh648Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "gh648")
	return NewStorageExecutor(store)
}

func TestGh648_SetLabelInCallBody(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:T {id: 1, x: 5})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:T {id: 1}) CALL (n) { SET n:B } RETURN n.id AS id", nil)
	require.NoError(t, err, "SET n:B must parse inside the CALL body")
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	labels, err := exec.Execute(ctx, "MATCH (n:T {id: 1}) RETURN labels(n) AS l", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"T", "B"}, labels.Rows[0][0])
}

func TestGh648_SetPropertyReflectsInReturnedRow(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:T {id: 1, x: 5})", nil)
	require.NoError(t, err)

	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		run := func(query string) *ExecuteResult {
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}()
			}
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			return result
		}

		t.Run(mode, func(t *testing.T) {
			result := run("MATCH (n:T {id: 1}) CALL (n) { SET n.y = 2 } RETURN n.y AS y")
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, "the returned row must show the write")
		})
	}

	stored, err := exec.Execute(ctx, "MATCH (n:T {id: 1}) RETURN n.y AS y", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, stored.Rows)
}

func TestGh648_WriteSubqueryWithoutImportsRunsPerRow(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:T {id: 1}), (:T {id: 2}), (:T {id: 3})", nil)
	require.NoError(t, err)

	// The body references the outer variable without a (n) import list:
	// Neo4j still runs it once per outer row.
	result, err := exec.Execute(ctx, "MATCH (n:T) CALL { CREATE (:X {from: n.id}) } RETURN n.id AS id ORDER BY id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)

	count, err := exec.Execute(ctx, "MATCH (x:X) RETURN count(x) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), count.Rows[0][0], "one X per outer row")
}

func TestGh648_InTransactionsRejectedInExplicitTransaction(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:T {id: 1})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	defer func() {
		_, err := exec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
	}()

	_, err = exec.Execute(ctx, "MATCH (n:T) CALL { CREATE (:X {from: n.id}) } IN TRANSACTIONS RETURN count(*) AS c", nil)
	require.Error(t, err, "IN TRANSACTIONS must not run inside an explicit transaction")
}
