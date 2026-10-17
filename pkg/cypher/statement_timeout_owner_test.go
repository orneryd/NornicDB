package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// The executor owns the statement's deadline: a statement its own context
// stopped fails with Neo4j's transaction timeout (#1339).
func TestExecuteMarksItsOwnDeadlineAsTransactionTimeout(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := executor.Execute(ctx, "RETURN 1 AS x", nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	code, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, nornicerrors.TransactionTimedOut, code)
	require.Equal(t, nornicerrors.TransactionTimedOutMessage, message)
}

// A cancelled statement (RESET, a closed connection) is not a timeout.
func TestExecuteDoesNotMarkCancellationAsTimeout(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := executor.Execute(ctx, "RETURN 1 AS x", nil)
	require.ErrorIs(t, err, context.Canceled)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.NotEqual(t, nornicerrors.TransactionTimedOut, code)
}

// A dependency's own deadline (an embedder behind db.retrieve, a remote call)
// is the procedure's failure, not the statement's timeout (review
// 4300775974): it keeps the ProcedureCallFailed wrapper.
func TestDependencyDeadlineKeepsProcedureCallFailed(t *testing.T) {
	err := procedureRuntimeError("db.retrieve", fmt.Errorf("embedder: %w", context.DeadlineExceeded))
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", code)
}
