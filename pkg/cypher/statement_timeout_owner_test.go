package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// expiredDatabaseLimit admits a statement under a database query time limit
// that has already ended, and returns only once the caller's own deadline has
// ended too: the database's deadline won, but both have expired by the time
// the statement returns.
type expiredDatabaseLimit struct{}

func (expiredDatabaseLimit) CheckQueryRate() error       { return nil }
func (expiredDatabaseLimit) GetQueryLimits() interface{} { return nil }
func (expiredDatabaseLimit) CheckQueryLimits(ctx context.Context) (context.Context, context.CancelFunc, error) {
	limited, cancel := context.WithDeadlineCause(ctx, time.Now(), nornicerrors.ErrDatabaseQueryTimeLimit)
	<-ctx.Done()
	return limited, cancel, nil
}

type expiredDatabaseLimitEngine struct{ *storage.NamespacedEngine }

func (engine expiredDatabaseLimitEngine) GetInnerEngine() storage.Engine {
	return engine.NamespacedEngine
}
func (engine expiredDatabaseLimitEngine) GetQueryLimitChecker() interface {
	CheckQueryRate() error
	CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
	GetQueryLimits() interface{}
} {
	return expiredDatabaseLimit{}
}

// A statement an owned deadline stopped fails with Neo4j's transaction
// timeout, with the status of the owner whose deadline ended it (#1339): the
// client's tx_timeout, the server's statement timeout.
func TestExecuteMarksItsOwnDeadlineAsTransactionTimeout(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, test := range []struct {
		cause         error
		code, message string
	}{
		{nornicerrors.ErrClientTransactionTimeout, nornicerrors.TransactionTimedOutClientConfiguration, nornicerrors.TransactionTimedOutClientConfigurationMessage},
		{nornicerrors.ErrServerStatementTimeout, nornicerrors.TransactionTimedOut, nornicerrors.TransactionTimedOutMessage},
	} {
		t.Run(test.cause.Error(), func(t *testing.T) {
			ctx, cancel := context.WithDeadlineCause(context.Background(), time.Now().Add(-time.Second), test.cause)
			defer cancel()
			_, err := executor.Execute(ctx, "RETURN 1 AS x", nil)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			code, message := nornicerrors.Neo4jStatus(err)
			require.Equal(t, test.code, code)
			require.Equal(t, test.message, message)
		})
	}
}

// The deadline that ended the statement first decides its status, not which
// deadlines have expired by the time it returns (sol-r2-1392 P2): the
// database's limit won, so the statement reports the server's timeout though
// the client's tx_timeout has expired as well.
func TestExecuteReportsTheDeadlineThatWon(t *testing.T) {
	executor := NewStorageExecutor(expiredDatabaseLimitEngine{storage.NewNamespacedEngine(newTestMemoryEngine(t), "deadline_winner")})
	// Long enough that the database's limit is installed (and ends the
	// statement) before the client's deadline expires.
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Second, nornicerrors.ErrClientTransactionTimeout)
	defer cancel()
	_, err := executor.Execute(ctx, "UNWIND range(1, 100000000) AS x WITH x WHERE x < 0 RETURN count(x) AS c", nil)
	require.Error(t, ctx.Err(), "the client's deadline expired before the statement returned")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	code, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, nornicerrors.TransactionTimedOut, code)
	require.Equal(t, nornicerrors.TransactionTimedOutMessage, message)
}

// A caller's own request budget (an embedded caller's context, an inherited
// HTTP request deadline) is not a transaction timeout when no transaction or
// statement timeout is configured (sol-r2-1392 P2).
func TestExecuteKeepsAnUnownedCallerDeadline(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := executor.Execute(ctx, "RETURN 1 AS x", nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.NotEqual(t, nornicerrors.TransactionTimedOut, code)
	require.NotEqual(t, nornicerrors.TransactionTimedOutClientConfiguration, code)
	require.False(t, nornicerrors.HasNeo4jStatus(err), "an unowned deadline carries no status of its own")
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
