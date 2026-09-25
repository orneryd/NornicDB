package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestFailedStatementMarksExplicitTransactionFailed verifies a statement
// failing inside an explicit transaction marks it failed, as in Neo4j (#683):
// later statements are refused, ROLLBACK discards everything the transaction
// wrote, and COMMIT rolls it back and reports the failure. Before, a runtime
// error rolled the transaction back silently, so later statements
// auto-committed and ROLLBACK failed with "no active transaction".
func TestFailedStatementMarksExplicitTransactionFailed(t *testing.T) {
	for _, failing := range []string{
		"RETURN 1 / 0 AS x",
		"RETRUN 1",
		"RETURN nosuch(1) AS x",
	} {
		for _, end := range []string{"ROLLBACK", "COMMIT"} {
			t.Run(end+" after "+failing, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "tx683"))
				ctx := context.Background()
				run := func(query string) error {
					_, err := exec.Execute(ctx, query, nil)
					return err
				}
				require.NoError(t, run("BEGIN"))
				require.NoError(t, run("CREATE (:A683)"))
				require.Error(t, run(failing))

				err := run("CREATE (:C683)")
				require.Error(t, err)
				require.Contains(t, err.Error(), "Neo.TransientError.Transaction.QueryExecutionFailedOnTransaction")

				err = run(end)
				if end == "COMMIT" {
					require.Error(t, err)
					require.Contains(t, err.Error(), "Neo.ClientError.Transaction.TransactionMarkedAsFailed")
				} else {
					require.NoError(t, err)
				}

				result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "nothing the transaction wrote is kept")
				require.NoError(t, run("BEGIN"), "the failed transaction has ended")
				require.NoError(t, run("ROLLBACK"))
			})
		}
	}

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "tx683ok"))
	ctx := context.Background()
	for _, query := range []string{"BEGIN", "CREATE (:A683)", "CREATE (:C683)", "COMMIT"} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	result, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}
