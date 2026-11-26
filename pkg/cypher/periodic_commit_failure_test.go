package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeriodicCommitReportsConstraintFailure(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newAsyncStackTestExecutor(t)
		ctx := context.Background()
		for _, query := range []string{
			"CREATE CONSTRAINT periodic_commit_unique IF NOT EXISTS FOR (n:PeriodicCommitUnique) REQUIRE n.k IS UNIQUE",
			"CREATE (:PeriodicCommitUnique {k: 1})",
			"CREATE (:PeriodicCommitTodo {done: false})",
		} {
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
		}

		statement := "MATCH (n:PeriodicCommitTodo) WHERE n.done = false WITH n LIMIT 1 SET n.done = true CREATE (:PeriodicCommitUnique {k: 1}) RETURN count(*)"
		_, directErr := exec.Execute(ctx, statement, nil)
		require.ErrorContains(t, directErr, "constraint", "premise: the inner statement must fail")

		result, err := exec.Execute(ctx, "CALL apoc.periodic.commit('"+statement+"', {})", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, 9, len(result.Columns), "Neo4j APOC exposes failure fields")
		row := make(map[string]interface{}, len(result.Columns))
		for i, column := range result.Columns {
			row[column] = result.Rows[0][i]
		}
		require.Equal(t, int64(1), row["batches"])
		require.Equal(t, int64(1), row["failedBatches"])
		require.Equal(t, int64(0), row["executions"])
		require.Equal(t, int64(0), row["failedCommits"])
		require.Empty(t, row["commitErrors"])
		require.Equal(t, false, row["wasTerminated"])
		batchErrors, ok := row["batchErrors"].(map[string]interface{})
		require.True(t, ok, "%T", row["batchErrors"])
		require.Len(t, batchErrors, 1)
		for message, count := range batchErrors {
			require.True(t, strings.Contains(strings.ToLower(message), "constraint"), message)
			require.Equal(t, int64(1), count)
		}
		remaining, err := exec.Execute(ctx, "MATCH (n:PeriodicCommitTodo) WHERE n.done = false RETURN count(n)", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), remaining.Rows[0][0], "failed batch must roll back")
	})
}

func TestPeriodicCommitSuccessfulBatchesKeepTheirCounts(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newAsyncStackTestExecutor(t)
		ctx := context.Background()
		_, err := exec.Execute(ctx, "UNWIND range(1,2) AS i CREATE (:PeriodicCommitTodo {done: false, i: i})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, "CALL apoc.periodic.commit('MATCH (n:PeriodicCommitTodo) WHERE n.done = false WITH n LIMIT 1 SET n.done = true RETURN count(*)', {})", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		row := make(map[string]interface{}, len(result.Columns))
		for i, column := range result.Columns {
			row[column] = result.Rows[0][i]
		}
		require.Equal(t, int64(2), row["updates"])
		require.Equal(t, int64(2), row["executions"])
		require.Equal(t, int64(3), row["batches"], "terminal empty batch is counted")
		require.Equal(t, int64(0), row["failedBatches"])
		require.Empty(t, row["batchErrors"])
		require.Equal(t, int64(0), row["failedCommits"])
		require.Empty(t, row["commitErrors"])
		require.Equal(t, false, row["wasTerminated"])
	})
}

func TestPeriodicCommitReportsFailureAfterCommittedBatch(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newAsyncStackTestExecutor(t)
		ctx := context.Background()
		for _, query := range []string{
			"CREATE CONSTRAINT periodic_commit_partial IF NOT EXISTS FOR (n:PeriodicCommitUnique) REQUIRE n.k IS UNIQUE",
			"CREATE (:PeriodicCommitUnique {k: 2})",
			"CREATE (:PeriodicCommitTodo {k: 1, done: false}), (:PeriodicCommitTodo {k: 2, done: false})",
		} {
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
		}
		result, err := exec.Execute(ctx, "CALL apoc.periodic.commit('MATCH (n:PeriodicCommitTodo) WHERE n.done = false WITH n ORDER BY n.k LIMIT 1 SET n.done = true CREATE (:PeriodicCommitUnique {k: n.k}) RETURN count(*)', {})", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		row := make(map[string]interface{}, len(result.Columns))
		for i, column := range result.Columns {
			row[column] = result.Rows[0][i]
		}
		require.Equal(t, int64(1), row["executions"])
		require.Equal(t, int64(2), row["batches"])
		require.Equal(t, int64(1), row["failedBatches"])
		require.NotEmpty(t, row["batchErrors"])
		result, err = exec.Execute(ctx, "MATCH (n:PeriodicCommitTodo) RETURN n.k, n.done ORDER BY n.k", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1), true}, {int64(2), false}}, result.Rows)
	})
}
