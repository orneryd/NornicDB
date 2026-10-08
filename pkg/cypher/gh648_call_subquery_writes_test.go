package cypher

// gh648_call_subquery_writes_test.go — regression tests for #648: with the
// variable-scope CALL form, a write subquery must apply per outer row, label
// SET must parse, the returned row must reflect the write, and IN
// TRANSACTIONS must not be accepted inside an explicit transaction.

import (
	"context"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
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
	// NornicDB imports it implicitly and runs the body once per outer row
	// (an extension: Neo4j 5.26.30 rejects it, TestGh648_ImplicitImportExtension).
	result, err := exec.Execute(ctx, "MATCH (n:T) CALL { CREATE (:X {from: n.id}) } RETURN n.id AS id ORDER BY id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)

	count, err := exec.Execute(ctx, "MATCH (x:X) RETURN count(x) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), count.Rows[0][0], "one X per outer row")
}

// An unscoped CALL { … } body reads outer variables it doesn't import with a
// leading WITH or a (vars) scope clause. This is an intentional NornicDB
// extension, kept at the project owner's direction (#907): Neo4j 5.26.30
// rejects every statement below with "Variable `x` not defined"
// (SyntaxError). It covers the body, each UNION branch, and reads after a
// local projection or a MATCH in the body.
func TestGh648_ImplicitImportExtension(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:CQ {id: 1})", nil)
	require.NoError(t, err)
	for _, testCase := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"UNWIND [1] AS i CALL { RETURN i AS j } RETURN j", [][]interface{}{{int64(1)}}},
		{"WITH 1 AS i CALL { WITH i RETURN i AS j UNION RETURN i AS j } RETURN j", [][]interface{}{{int64(1)}}},
		{"WITH 1 AS v CALL { RETURN 2 AS x UNION RETURN v AS x } RETURN x ORDER BY x", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"WITH 1 AS i CALL { MATCH (n:CQ) WHERE n.id = i RETURN n.id AS v } RETURN v", [][]interface{}{{int64(1)}}},
		{"WITH 1 AS i, 2 AS z CALL { WITH i RETURN i + z AS j } RETURN j", [][]interface{}{{int64(3)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
	// A leading WITH that reads no outer variable can't define one.
	_, err = exec.Execute(ctx, "MATCH (:CQ) CALL { WITH seed RETURN seed } RETURN 1 AS v", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
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

func TestPR771UnitCallPreservesFilteredOuterRows(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		query   string
		columns []string
		rows    [][]interface{}
		stored  [][]interface{}
	}{
		{
			name:    "legacy filtered delete",
			query:   "MATCH (n:T) CALL { WITH n WITH n WHERE n.id = 2 DETACH DELETE n } RETURN count(*) AS c",
			columns: []string{"c"},
			rows:    [][]interface{}{{int64(2)}},
			stored:  [][]interface{}{{int64(1), nil}},
		},
		{
			name:    "legacy filtered set",
			query:   "MATCH (n:T) CALL { WITH n WITH n WHERE n.id = 2 SET n.k = 1 } RETURN count(*) AS c",
			columns: []string{"c"},
			rows:    [][]interface{}{{int64(2)}},
			stored:  [][]interface{}{{int64(1), nil}, {int64(2), int64(1)}},
		},
		{
			name:    "scoped filtered set",
			query:   "MATCH (n:T) CALL (n) { WITH n WHERE n.id = 2 SET n.k = 1 } RETURN n.id AS id ORDER BY id",
			columns: []string{"id"},
			rows:    [][]interface{}{{int64(1)}, {int64(2)}},
			stored:  [][]interface{}{{int64(1), nil}, {int64(2), int64(1)}},
		},
		{
			name:    "legacy inner match filter",
			query:   "MATCH (n:T) CALL { WITH n MATCH (n) WHERE n.id = 2 SET n.k = 1 } RETURN count(*) AS c",
			columns: []string{"c"},
			rows:    [][]interface{}{{int64(2)}},
			stored:  [][]interface{}{{int64(1), nil}, {int64(2), int64(1)}},
		},
		{
			name:    "all inner rows filtered",
			query:   "MATCH (n:T) CALL (n) { WITH n WHERE false SET n.k = 1 } RETURN n.id AS id ORDER BY id",
			columns: []string{"id"},
			rows:    [][]interface{}{{int64(1)}, {int64(2)}},
			stored:  [][]interface{}{{int64(1), nil}, {int64(2), nil}},
		},
		{
			name:    "multiple inner rows do not multiply outer rows",
			query:   "MATCH (n:T) CALL (n) { WITH n UNWIND [1, 2] AS value WITH n, value WHERE n.id = 2 SET n.k = value } RETURN n.id AS id ORDER BY id",
			columns: []string{"id"},
			rows:    [][]interface{}{{int64(1)}, {int64(2)}},
			stored:  [][]interface{}{{int64(1), nil}, {int64(2), int64(2)}},
		},
		{
			name:    "returning subquery still filters outer rows",
			query:   "MATCH (n:T) CALL (n) { WITH n WHERE n.id = 2 RETURN n.id AS inner } RETURN n.id AS id, inner",
			columns: []string{"id", "inner"},
			rows:    [][]interface{}{{int64(2), int64(2)}},
			stored:  [][]interface{}{{int64(1), nil}, {int64(2), nil}},
		},
		{
			name:    "empty outer input stays empty",
			query:   "MATCH (n:Missing) CALL (n) { WITH n WHERE n.id = 2 SET n.k = 1 } RETURN count(*) AS c",
			columns: []string{"c"},
			rows:    [][]interface{}{{int64(0)}},
			stored:  [][]interface{}{{int64(1), nil}, {int64(2), nil}},
		},
	} {
		for _, explicit := range []bool{false, true} {
			mode := "autocommit"
			if explicit {
				mode = "explicit transaction"
			}
			t.Run(testCase.name+"/"+mode, func(t *testing.T) {
				exec := newGh648Executor(t)
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (:T {id: 1, x: 5}), (:T {id: 2})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				result, err := exec.Execute(ctx, testCase.query, nil)
				require.NoError(t, err)
				require.Equal(t, testCase.columns, result.Columns)
				require.Equal(t, testCase.rows, result.Rows)
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				stored, err := exec.Execute(ctx, "MATCH (n:T) RETURN n.id AS id, n.k AS k ORDER BY id", nil)
				require.NoError(t, err)
				require.Equal(t, testCase.stored, stored.Rows)
			})
		}
	}
}

func TestPR771TransactionalUnitCallPreservesFilteredOuterRows(t *testing.T) {
	for _, body := range []string{
		"CALL (n) { WITH n WHERE n.id = 2 SET n.k = 1 }",
		"CALL { WITH n WITH n WHERE n.id = 2 SET n.k = 1 }",
	} {
		t.Run(body, func(t *testing.T) {
			exec := newGh648Executor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:T {id: 1}), (:T {id: 2})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH (n:T) "+body+" IN TRANSACTIONS OF 1 ROW RETURN n.id AS id ORDER BY id", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"id"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
			stored, err := exec.Execute(ctx, "MATCH (n:T) RETURN n.id AS id, n.k AS k ORDER BY id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), nil}, {int64(2), int64(1)}}, stored.Rows)
		})
	}
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
	t.Run("independent_return_bindings", func(t *testing.T) {
		exec := newGh648Executor(t)
		rows, _, handled, err := exec.pipelineApplyCallSubquery(context.Background(), []pipelineRow{{"v": int64(1)}}, "CALL { RETURN 2 AS c }")
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, []pipelineRow{{"v": int64(1), "c": int64(2)}}, rows)
	})
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
		// Unscoped bodies (Neo4j 5.26.30, #907): WITH * imports, a leading
		// WITH of literals imports nothing, and each UNION branch decides.
		{"WITH 1 AS v CALL { WITH * RETURN v * 2 AS d } RETURN v, d", []string{"v", "d"}, [][]interface{}{{int64(1), int64(2)}}},
		{"WITH 1 AS v CALL { WITH 2 AS x RETURN x } RETURN v, x", []string{"v", "x"}, [][]interface{}{{int64(1), int64(2)}}},
		{"WITH 1 AS v CALL { WITH 2 AS x RETURN x UNION WITH * RETURN v AS x } RETURN v, x", []string{"v", "x"}, [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(1)}}},
		{"WITH 1 AS v CALL { RETURN 2 AS x UNION WITH v RETURN v AS x } RETURN v, x", []string{"v", "x"}, [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(1)}}},
		{"WITH 1 AS v CALL { RETURN 0 AS x UNION WITH v CALL db.labels() YIELD label WITH v LIMIT 1 RETURN v AS x } RETURN v, x", []string{"v", "x"}, [][]interface{}{{int64(1), int64(0)}, {int64(1), int64(1)}}},
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

func TestPR771TransactionalCallChainedRows(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	query := "CALL { UNWIND [1,2,3] AS i RETURN i } CALL (i) { CREATE (:X {i:i}) } IN TRANSACTIONS OF 2 ROWS RETURN i ORDER BY i"
	clauses, split := splitPipelineClausesAllowingProcedureCalls(query)
	t.Logf("clauses: split=%v values=%+v", split, clauses)
	callRows, callStats, callHandled, callErr := newGh648Executor(t).pipelineApplyCallSubquery(ctx, []pipelineRow{{"i": int64(1)}}, "CALL (i) { CREATE (:X {i:i}) } IN TRANSACTIONS OF 2 ROWS")
	t.Logf("call probe: rows=%+v stats=%+v handled=%v err=%v", callRows, callStats, callHandled, callErr)
	runner := newGh648Executor(t)
	firstRows, firstStats, firstHandled, firstErr := runner.pipelineApplyCallSubquery(ctx, []pipelineRow{{}}, clauses[0].text)
	secondRows, secondStats, secondHandled, secondErr := runner.pipelineApplyCallSubquery(ctx, firstRows, clauses[1].text)
	t.Logf("step probes: first=%+v stats=%+v handled=%v err=%v second=%+v stats=%+v handled=%v err=%v", firstRows, firstStats, firstHandled, firstErr, secondRows, secondStats, secondHandled, secondErr)
	manual, manualHandled, manualErr := runner.runPipelineClauses(ctx, []pipelineRow{{}}, map[string]struct{}{}, clauses, clauses)
	t.Logf("runner probe: handled=%v err=%v result=%+v", manualHandled, manualErr, manual)
	probe := newGh648Executor(t).executePipeline(ctx, query)
	t.Logf("pipeline probe: handled=%v err=%v result=%+v", probe.terminal(), probe.err, probe.result)
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)
	require.Equal(t, 3, result.Stats.NodesCreated)
	stored, err := exec.Execute(ctx, "MATCH (x:X) RETURN x.i AS i ORDER BY i", nil)
	require.NoError(t, err)
	require.Equal(t, result.Rows, stored.Rows)
}

func TestPR771CallOrderModifiersCanonicalExpressions(t *testing.T) {
	exec := newGh648Executor(t)
	input := &ExecuteResult{
		Columns: []string{"value"},
		Rows:    [][]interface{}{{int64(1)}, {int64(4)}, {int64(3)}, {int64(2)}},
		Stats:   &QueryStats{NodesCreated: 4},
	}
	result, err := exec.sharedCallTailForTest(context.Background(), input, "ORDER BY value % 2, value DESC SKIP 1 LIMIT 2")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, result.Rows)
	require.Same(t, input.Stats, result.Stats)
}

func TestPR771TransactionalCallBodyIsOneOuterRow(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Src {v:2}), (:Src {v:1}), (:Src {v:0})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CALL { MATCH (s:Src) WITH s ORDER BY s.v DESC CREATE (:Dst {v:1/s.v}) } IN TRANSACTIONS OF 2 ROWS", nil)
	require.Error(t, err)
	stored, err := exec.Execute(ctx, "MATCH (d:Dst) RETURN count(d)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
}

func TestPR771TransactionalCallPriorExplicitWrites(t *testing.T) {
	for _, query := range []string{
		"CALL { CREATE (:X) } IN TRANSACTIONS",
		"MATCH (n:Prior) CALL (n) { CREATE (:X) } IN TRANSACTIONS",
		"MATCH (n:Prior) CALL { WITH n CREATE (:X) } IN TRANSACTIONS",
		"UNWIND [1] AS i CALL (i) { CREATE (:X) } IN TRANSACTIONS",
	} {
		t.Run(query, func(t *testing.T) {
			exec := newGh648Executor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE (:Prior)", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, query, nil)
			require.Error(t, err)
			var classified interface{ BoltErrorCode() string }
			require.ErrorAs(t, err, &classified)
			require.Equal(t, "Neo.DatabaseError.Statement.ExecutionFailed", classified.BoltErrorCode())
			require.Equal(t, "Expected transaction state to be empty when calling transactional subquery. (Transactions committed: 0)", err.Error())
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.Error(t, err)
			stored, err := exec.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
		})
	}
}

func TestPR771TransactionalCallExplicitInputBoundary(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		query string
		empty bool
	}{
		{"standalone", "CALL { CREATE (:X) } IN TRANSACTIONS", false},
		{"scoped match", "MATCH (n:Seed) CALL (n) { CREATE (:X) } IN TRANSACTIONS", false},
		{"legacy match", "MATCH (n:Seed) CALL { WITH n CREATE (:X) } IN TRANSACTIONS", false},
		{"unwind returning", "UNWIND [1] AS i CALL (i) { CREATE (:X) RETURN i AS value } IN TRANSACTIONS RETURN value", false},
		{"empty scoped match", "MATCH (n:Missing) CALL (n) { CREATE (:X) } IN TRANSACTIONS", true},
		{"empty legacy match", "MATCH (n:Missing) CALL { WITH n CREATE (:X) } IN TRANSACTIONS RETURN count(*) AS c", true},
		{"empty unwind", "UNWIND [] AS i CALL (i) { CREATE (:X) } IN TRANSACTIONS", true},
		{"empty unwind returning", "UNWIND [] AS i CALL (i) { CREATE (:X) RETURN i AS value } IN TRANSACTIONS RETURN count(*) AS c", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exec := newGh648Executor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:Seed)", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, testCase.query, nil)
			if testCase.empty {
				require.NoError(t, err)
				if len(result.Columns) == 0 {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
				}
			} else {
				var classified interface{ BoltErrorCode() string }
				require.ErrorAs(t, err, &classified)
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionStartFailed", classified.BoltErrorCode())
			}
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			stored, err := exec.Execute(ctx, "MATCH (x:X) RETURN count(x)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
		})
	}
}

func TestPR771TransactionalCallOuterBatchPersistence(t *testing.T) {
	for _, suffix := range []string{"", " RETURN i AS value"} {
		t.Run(suffix, func(t *testing.T) {
			exec := newGh648Executor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "UNWIND [2,1,0] AS i CALL (i) { CREATE (:Dst {v:1/i})"+suffix+" } IN TRANSACTIONS OF 2 ROWS", nil)
			require.Error(t, err)
			stored, err := exec.Execute(ctx, "MATCH (d:Dst) RETURN count(d)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, stored.Rows)
		})
	}
}

// An unscoped body's importing WITH (Neo4j 5.26.30, #907): plain references
// only, also when an expression reads the outer variable, and a branch
// that only imports is not a query of its own.
func TestGh648_UnscopedImportForms(t *testing.T) {
	exec := newGh648Executor(t)
	ctx := context.Background()
	for _, query := range []string{
		"WITH 1 AS v CALL { WITH [v] AS l, v RETURN v AS x } RETURN x",
		"WITH 1 AS v CALL { WITH v, size([1, 2]) AS n RETURN n AS x } RETURN x",
		"WITH 1 AS v CALL { RETURN 1 AS x UNION WITH v } RETURN x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
	result, err := exec.Execute(ctx, "WITH [1, 2] AS l CALL { WITH l RETURN size(l) AS x } RETURN x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}
