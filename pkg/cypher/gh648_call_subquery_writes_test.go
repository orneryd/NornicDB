package cypher

// gh648_call_subquery_writes_test.go — regression tests for #648: with the
// variable-scope CALL form, a write subquery must apply per outer row, label
// SET must parse, the returned row must reflect the write, and IN
// TRANSACTIONS must not be accepted inside an explicit transaction.

import (
	"context"
	"strings"
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

func TestGh648_UncorrelatedUnitSubqueryRunsPerRow(t *testing.T) {
	// A CALL body that references no outer variable is still a unit subquery:
	// Neo4j runs it once per incoming row, not once per statement.

	t.Run("call_empty_import_list", func(t *testing.T) {
		exec := newGh648Executor(t)
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:T {id: 1}), (:T {id: 2})", nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, "MATCH (t:T) CALL () { CREATE (:X) } RETURN count(*) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, int64(2), result.Rows[0][0], "one X per outer row")

		count, err := exec.Execute(ctx, "MATCH (x:X) RETURN count(x) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, int64(2), count.Rows[0][0])
	})

	t.Run("call_no_import_list_then_count", func(t *testing.T) {
		exec := newGh648Executor(t)
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:T {id: 1}), (:T {id: 2})", nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx,
			"MATCH (t:T) CALL { CREATE (:X) } WITH count(*) AS c MATCH (x:X) RETURN c, count(x) AS total", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(2), result.Rows[0][0])
		require.Equal(t, int64(2), result.Rows[0][1], "one X per outer row")
	})
}

func TestGh648_SharedReturnBoundary(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	inner := &ExecuteResult{Columns: []string{"value", "missing"}, Rows: [][]interface{}{{int64(1)}}, Stats: &QueryStats{}}
	unchanged, err := exec.processCallSubqueryReturn(ctx, inner, "")
	require.NoError(t, err)
	require.Same(t, inner, unchanged)
	projected, err := exec.processCallSubqueryReturn(ctx, inner, "RETURN value, missing")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), nil}}, projected.Rows)
	require.Same(t, inner.Stats, projected.Stats)
	_, err = exec.processCallSubqueryReturn(ctx, inner, "RETURN range(1, 2, 0)")
	require.Error(t, err)
}

func TestGh648_OuterScopeAndColumnNames(t *testing.T) {
	for _, testCase := range []struct {
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{"WITH 1 AS i CALL (i) { RETURN i * 2 AS d } RETURN *", []string{"d", "i"}, [][]interface{}{{int64(2), int64(1)}}},
		{"WITH 1 AS v CALL { RETURN 2 AS c } RETURN v, c", []string{"v", "c"}, [][]interface{}{{int64(1), int64(2)}}},
		{"WITH 'x' AS v CALL { RETURN 2 AS c } RETURN v, c", []string{"v", "c"}, [][]interface{}{{"x", int64(2)}}},
		{"WITH 1 AS v, 3 AS w CALL { RETURN 2 AS c } RETURN v, w, c", []string{"v", "w", "c"}, [][]interface{}{{int64(1), int64(3), int64(2)}}},
		{"MATCH (t:T) WITH t, 5 AS z CALL (t) { RETURN t.id * 2 AS d } RETURN z, d", []string{"z", "d"}, [][]interface{}{{int64(5), int64(2)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			exec := newGh648Executor(t)
			_, err := exec.Execute(context.Background(), "CREATE (:T {id: 1})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(context.Background(), testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.columns, result.Columns)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
}

func TestGh648_AllTransactionalCallsRejectExplicitTransaction(t *testing.T) {
	for _, query := range []string{
		"UNWIND [1, 2, 3] AS i CALL (i) { CREATE (:X {i: i}) } IN TRANSACTIONS OF 2 ROWS",
		"UNWIND [1, 2, 3] AS i CALL (i) { MERGE (x:X {i: i}) SET x.y = 1 } IN TRANSACTIONS OF 2 ROWS",
		"CALL { CREATE (:X {i: 1}) } IN TRANSACTIONS",
		"CALL () { CREATE (:X {i: 1}) } IN TRANSACTIONS",
		"CALL { CREATE (:X {i: 1}) } IN TRANSACTIONS OF 2 ROWS",
	} {
		t.Run(query, func(t *testing.T) {
			exec := newGh648Executor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, query, nil)
			require.ErrorContains(t, err, "TransactionStartFailed")
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			stored, err := exec.Execute(ctx, "MATCH (x:X) RETURN count(x)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
		})
	}
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

func TestMonster648TransactionalOuterRows(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:T {id: 1})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (t:T) CALL (t) { CREATE (:X {i: t.id}) } IN TRANSACTIONS RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	stored, err := exec.Execute(ctx, "MATCH (x:X) RETURN x.i", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
}

func TestMonster648TransactionalZeroInputs(t *testing.T) {
	for _, query := range []string{
		"MATCH (t:T) CALL (t) { CREATE (:X {i: t.id}) } IN TRANSACTIONS",
		"MATCH (t:T) CALL (t) { CREATE (:X {i: t.id}) } IN TRANSACTIONS RETURN count(*) AS c",
		"UNWIND [] AS i CALL (i) { CREATE (:X {i: i}) } IN TRANSACTIONS",
	} {
		t.Run(query, func(t *testing.T) {
			exec := newGh648Executor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			if len(result.Columns) == 0 {
				require.Empty(t, result.Rows)
			} else {
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			}
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		})
	}
}

func TestMonster648TransactionalBatchBindings(t *testing.T) {
	for _, body := range []string{
		"CREATE (:X {i: t.id})",
		"CREATE (:X {i: t.id}) RETURN t.id * 2 AS doubled",
	} {
		t.Run(body, func(t *testing.T) {
			exec := newGh648Executor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:T {id: 1}), (:T {id: 2}), (:T {id: 3})", nil)
			require.NoError(t, err)
			projection := "t.id AS id"
			want := [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}
			if strings.Contains(body, "RETURN") {
				projection += ", doubled"
				want = [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(4)}, {int64(3), int64(6)}}
			}
			result, err := exec.Execute(ctx, "MATCH (t:T) CALL (t) { "+body+" } IN TRANSACTIONS OF 2 ROWS RETURN "+projection+" ORDER BY id", nil)
			require.NoError(t, err)
			require.Equal(t, want, result.Rows)
			require.Equal(t, 3, result.Stats.NodesCreated)
		})
	}
}
