package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestClientTransactionControlPreservesOwnedTransaction(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "client_boundary")
	executor := NewStorageExecutor(store)
	ctx := context.Background()
	client := WithClientStatement(ctx)
	owner := WithTransactionControl(client)

	_, err := executor.Execute(owner, "BEGIN", nil)
	require.NoError(t, err)
	_, err = executor.Execute(client, "CREATE (:ClientBoundary)", nil)
	require.NoError(t, err)
	for _, command := range []string{"COMMIT", "ROLLBACK TRANSACTION", "BEGIN"} {
		_, err = executor.Execute(client, command, nil)
		var syntax *SemanticError
		require.ErrorAs(t, err, &syntax, command)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", syntax.Code)
		require.True(t, executor.HasActiveTransaction(), command)
	}
	_, err = executor.Execute(owner, "COMMIT", nil)
	require.NoError(t, err)
	require.False(t, executor.HasActiveTransaction())
	result, err := executor.Execute(client, "MATCH (n:ClientBoundary) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
}

// TestClientTransactionCommandsAreSyntaxErrors: a client statement can't be a
// bare BEGIN, COMMIT or ROLLBACK (Neo4j 5.26: SyntaxError "Invalid input
// 'BEGIN'"); it leaves the executor it ran on as it found it. The
// one-statement script form still runs and an embedded caller keeps the bare
// commands (the owner of a transaction: TestClientTransactionControlPreservesOwnedTransaction).
func TestClientTransactionCommandsAreSyntaxErrors(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "client_tx")
	exec := NewStorageExecutor(store)
	client := WithClientStatement(context.Background())

	for _, statement := range []string{"BEGIN", "begin", "BEGIN TRANSACTION", "COMMIT", "ROLLBACK", "  Rollback  Transaction ", "USE client_tx BEGIN"} {
		_, err := exec.Execute(client, statement, nil)
		require.Error(t, err, statement)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "%s: %v", statement, err)
		require.Nil(t, exec.txContext, statement)
	}
	// The error quotes the word as typed, as Neo4j's does.
	_, err := exec.Execute(client, "  Rollback  Transaction ", nil)
	require.Contains(t, err.Error(), "Invalid input 'Rollback'")

	// The next client statement runs on its own and commits.
	_, err = exec.Execute(client, "CREATE (:ClientTx {v: 1})", nil)
	require.NoError(t, err)
	result, err := NewStorageExecutor(store).Execute(context.Background(), "MATCH (n:ClientTx) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])

	// The one-statement script opens and ends its own transaction.
	_, err = exec.Execute(client, "BEGIN CREATE (:ClientTx {v: 2}) COMMIT", nil)
	require.NoError(t, err)
	require.Nil(t, exec.txContext)

	// An embedded caller that owns its executor keeps the bare commands.
	_, err = exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)

	result, err = NewStorageExecutor(store).Execute(context.Background(), "MATCH (n:ClientTx) RETURN n.v AS v ORDER BY v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
}
