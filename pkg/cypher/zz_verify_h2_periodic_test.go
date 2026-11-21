package cypher

// Verifier of hunt leads L-H2-006 and L-H2-007 (v1.5.2 verification campaign,
// test-side only). Public Cypher path (StorageExecutor.Execute) on the server
// storage stack, both parsers. A "fail" case must disagree on a build that has
// the defect (it reports DISAGREE); a "control" case must hold on every build.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func verifyH2Row(result *ExecuteResult) map[string]interface{} {
	out := map[string]interface{}{}
	if result == nil || len(result.Rows) == 0 {
		return out
	}
	for i, c := range result.Columns {
		out[c] = result.Rows[0][i]
	}
	return out
}

func verifyH2Log(t *testing.T, fields map[string]interface{}) {
	t.Helper()
	data, _ := json.Marshal(fields)
	t.Logf("VERIFY_H2 %s", data)
}

func verifyH2Count(t *testing.T, exec *StorageExecutor, query string) int64 {
	t.Helper()
	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	require.Len(t, res.Rows, 1)
	v, ok := res.Rows[0][0].(int64)
	require.True(t, ok, "%T", res.Rows[0][0])
	return v
}

func verifyH2Setup(t *testing.T, exec *StorageExecutor, statements ...string) {
	t.Helper()
	for _, s := range statements {
		_, err := exec.Execute(context.Background(), s, nil)
		require.NoError(t, err, s)
	}
}

// L-H2-006: apoc.periodic.commit whose statement fails on a uniqueness
// constraint. APOC reports a failed batch (failedBatches/batchErrors/
// failedCommits/commitErrors) or the call fails; a successful call whose row
// shows no failure hides it.
func TestVerifyH2PeriodicCommit(t *testing.T) {
	const commitFailing = "CALL apoc.periodic.commit('MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT $limit SET n.done = true CREATE (:CU {k: 1}) RETURN count(*)', {limit: 1})"
	forEachParser(t, func(t *testing.T) {
		t.Run("L-H2-006/fail-constraint-failure-is-visible", func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			verifyH2Setup(t, exec,
				"CREATE CONSTRAINT h2vcu IF NOT EXISTS FOR (u:CU) REQUIRE u.k IS UNIQUE",
				"CREATE (:CU {k: 1})",
				"UNWIND range(1,3) AS i CREATE (:Todo {i: i})")
			// The statement on its own fails on the constraint (premise).
			_, directErr := exec.Execute(context.Background(), "MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT 1 SET n.done = true CREATE (:CU {k: 1}) RETURN count(*)", nil)
			res, err := exec.Execute(context.Background(), commitFailing, nil)
			row := verifyH2Row(res)
			done := verifyH2Count(t, exec, "MATCH (n:Todo) WHERE n.done IS NOT NULL RETURN count(n)")
			fields := map[string]interface{}{"lead": "L-H2-006", "case": "fail-constraint-failure-is-visible", "direct_error": verifyH2ErrString(directErr), "call_error": verifyH2ErrString(err), "row": row, "todo_done": done}
			if res != nil {
				fields["columns"] = res.Columns
			}
			verifyH2Log(t, fields)
			require.Error(t, directErr, "premise: the statement alone violates the constraint")
			if err != nil {
				return // failure visible as an error
			}
			for _, c := range []string{"failedBatches", "failedCommits"} {
				if n, ok := row[c].(int64); ok && n > 0 {
					return // failure visible in the row
				}
			}
			t.Errorf("DISAGREE apoc.periodic.commit succeeded and its row reports no failure: columns %v row %v (statement alone fails: %v)", res.Columns, row, directErr)
		})
		t.Run("L-H2-006/control-commit-succeeds", func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			verifyH2Setup(t, exec, "UNWIND range(1,3) AS i CREATE (:Todo {i: i})")
			res, err := exec.Execute(context.Background(), "CALL apoc.periodic.commit('MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT $limit SET n.done = true RETURN count(*)', {limit: 1}) YIELD updates, executions RETURN updates, executions", nil)
			require.NoError(t, err)
			done := verifyH2Count(t, exec, "MATCH (n:Todo) WHERE n.done IS NOT NULL RETURN count(n)")
			verifyH2Log(t, map[string]interface{}{"lead": "L-H2-006", "case": "control-commit-succeeds", "row": verifyH2Row(res), "todo_done": done})
			require.Equal(t, int64(3), done)
		})
	})
}

// L-H2-007: apoc.periodic.iterate whose action fails with an arithmetic
// error. APOC counts the failed batches and reports their messages; the call
// itself succeeds.
func TestVerifyH2PeriodicIterate(t *testing.T) {
	run := func(t *testing.T, exec *StorageExecutor, action string) (map[string]interface{}, error) {
		res, err := exec.Execute(context.Background(), "CALL apoc.periodic.iterate('UNWIND [1,2] AS i RETURN i', '"+action+"', {batchSize:1}) YIELD batches, committedOperations, failedBatches, errorMessages RETURN batches, committedOperations, failedBatches, errorMessages", nil)
		return verifyH2Row(res), err
	}
	forEachParser(t, func(t *testing.T) {
		t.Run("L-H2-007/fail-division-by-zero-is-a-failed-batch", func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			row, err := run(t, exec, "CREATE (:ProcessedItem {v: i / 0})")
			stored := verifyH2Count(t, exec, "MATCH (n:ProcessedItem) RETURN count(n)")
			verifyH2Log(t, map[string]interface{}{"lead": "L-H2-007", "case": "fail-division-by-zero-is-a-failed-batch", "call_error": verifyH2ErrString(err), "row": row, "stored": stored})
			if err != nil {
				t.Fatalf("DISAGREE the whole call failed instead of counting failed batches: %v", err)
			}
			require.Equal(t, int64(2), row["failedBatches"])
			msgs, _ := row["errorMessages"].(map[string]interface{})
			require.NotEmpty(t, msgs)
		})
		t.Run("L-H2-007/control-constraint-failure-is-a-failed-batch", func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			verifyH2Setup(t, exec,
				"CREATE CONSTRAINT h2vpiu IF NOT EXISTS FOR (u:PIU) REQUIRE u.k IS UNIQUE",
				"CREATE (:PIU {k: 1}), (:PIU {k: 2})")
			row, err := run(t, exec, "CREATE (:PIU {k: i})")
			verifyH2Log(t, map[string]interface{}{"lead": "L-H2-007", "case": "control-constraint-failure-is-a-failed-batch", "call_error": verifyH2ErrString(err), "row": row})
			require.NoError(t, err)
			require.Equal(t, int64(2), row["failedBatches"])
			msgs, _ := row["errorMessages"].(map[string]interface{})
			require.NotEmpty(t, msgs)
		})
		t.Run("L-H2-007/control-action-succeeds", func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			row, err := run(t, exec, "CREATE (:ProcessedItem {v: i / 1})")
			stored := verifyH2Count(t, exec, "MATCH (n:ProcessedItem) RETURN count(n)")
			verifyH2Log(t, map[string]interface{}{"lead": "L-H2-007", "case": "control-action-succeeds", "call_error": verifyH2ErrString(err), "row": row, "stored": stored})
			require.NoError(t, err)
			require.Equal(t, int64(0), row["failedBatches"])
			require.Equal(t, int64(2), stored)
		})
	})
}

func verifyH2ErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
