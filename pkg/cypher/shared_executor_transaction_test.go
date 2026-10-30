package cypher

// Shared-executor transaction-control boundary: a cached per-database executor
// is shared by every auto-commit client, so a statement must never leave it
// inside a transaction. Bare BEGIN/COMMIT/ROLLBACK are rejected there; the
// one-statement script form keeps working on a private clone, and
// per-session executors (never marked shared) keep the embedded explicit
// transaction pattern.

import (
	"context"
	"sync"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func requireTransactionCommandRejected(t *testing.T, err error, query string) {
	t.Helper()
	require.Error(t, err, query)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
}

func TestSharedExecutorRejectsBareTransactionCommands(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "shared_tx_cmd"))
	exec.SetSharedExecutor(true)
	ctx := context.Background()

	for _, query := range []string{"BEGIN", "begin", "BEGIN TRANSACTION", "COMMIT", "commit transaction", "ROLLBACK", "rollback TRANSACTION"} {
		_, err := exec.Execute(ctx, query, nil)
		requireTransactionCommandRejected(t, err, query)
		require.False(t, exec.HasActiveTransaction(), query)
	}

	// A transaction-owner marker does not override the shared boundary.
	_, err := exec.Execute(WithTransactionControl(ctx), "BEGIN", nil)
	requireTransactionCommandRejected(t, err, "WithTransactionControl BEGIN")
	require.False(t, exec.HasActiveTransaction())

	// Auto-commit statements keep working on the shared executor afterwards.
	_, err = exec.Execute(ctx, "CREATE (:SharedProbe {id: 1})", nil)
	require.NoError(t, err)
	res, err := exec.Execute(ctx, "MATCH (n:SharedProbe) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows[0][0])

	// The one-statement script form still works: it runs on a private clone
	// and never leaves the shared executor inside a transaction.
	res, err = exec.Execute(ctx, "BEGIN CREATE (:SharedScriptProbe) COMMIT", nil)
	require.NoError(t, err)
	require.False(t, exec.HasActiveTransaction())
	count, err := exec.Execute(ctx, "MATCH (n:SharedScriptProbe) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), count.Rows[0][0])
}

func TestSharedExecutorScriptsAndAutocommitDoNotCollide(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "shared_tx_race"))
	exec.SetSharedExecutor(true)
	ctx := context.Background()

	const scripters = 8
	const victims = 12
	const rounds = 25
	var wg sync.WaitGroup
	errs := make(chan error, scripters+victims)
	for i := 0; i < scripters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if _, err := exec.Execute(ctx, "BEGIN CREATE (:RaceScript) COMMIT", nil); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := 0; i < victims; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if _, err := exec.Execute(ctx, "CREATE (:RaceVictim)", nil); err != nil {
					errs <- err
					return
				}
				if _, err := exec.Execute(ctx, "MATCH (n:RaceVictim) RETURN count(n)", nil); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent statement failed: %v", err)
	}
	require.False(t, exec.HasActiveTransaction())
	res, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(scripters*rounds+victims*rounds), res.Rows[0][0],
		"every acknowledged write must survive")
}

func TestNonSharedExecutorKeepsSessionTransactionPattern(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "session_tx_pattern"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:SessionProbe {v: 1})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	require.False(t, exec.HasActiveTransaction())
	res, err := exec.Execute(ctx, "MATCH (n:SessionProbe) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows[0][0])
}

func TestClientStatementBareTransactionCommandRejectedOnAnyExecutor(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "client_tx_cmd"))
	ctx := WithClientStatement(context.Background())
	for _, query := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
		_, err := exec.Execute(ctx, query, nil)
		requireTransactionCommandRejected(t, err, query)
		require.False(t, exec.HasActiveTransaction(), query)
	}
}
