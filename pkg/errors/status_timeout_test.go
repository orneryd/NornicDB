package errors

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A statement its own deadline stopped (marked by its owner) is Neo4j's
// transaction timeout, not a SyntaxError (#1339).
func TestNeo4jStatusDeadlineIsTransactionTimeout(t *testing.T) {
	for _, err := range []error{
		MarkStatementTimeout(context.DeadlineExceeded, true),
		fmt.Errorf("route: %w", MarkStatementTimeout(fmt.Errorf("pipeline stage: %w", context.DeadlineExceeded), false)),
	} {
		code, message := Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", code)
		require.Equal(t, TransactionTimedOutMessage, message)
		require.True(t, HasNeo4jStatus(err))
		require.False(t, IsCompileTimeError(err), "a timed-out statement compiled, so HTTP keeps its result columns")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	require.Nil(t, MarkStatementTimeout(nil, true))
	marked := MarkStatementTimeout(context.DeadlineExceeded, true)
	require.Same(t, marked, MarkStatementTimeout(marked, false), "marking is idempotent")
	require.True(t, CallerDeadlineTimeout(marked))
	require.False(t, CallerDeadlineTimeout(MarkStatementTimeout(context.DeadlineExceeded, false)), "a database limit is not the caller's deadline")
	require.False(t, CallerDeadlineTimeout(context.DeadlineExceeded))
}

// A dependency's own deadline (an embedder, a remote call) is not the
// statement's timeout: unmarked, it keeps the default status and stays
// unclassified, so a procedure still reports ProcedureCallFailed for it
// (review 4300775974).
func TestNeo4jStatusDependencyDeadlineIsNotTransactionTimeout(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		fmt.Errorf("embedder: %w", context.DeadlineExceeded),
	} {
		code, _ := Neo4jStatus(err)
		require.NotEqual(t, TransactionTimedOut, code)
		require.False(t, HasNeo4jStatus(err), "an unmarked dependency deadline carries no status of its own")
	}
	// An error that carries its own status keeps it.
	coded := fmt.Errorf("%w", &codedTestError{code: TransactionTimedOutClientConfiguration, err: context.DeadlineExceeded})
	code, _ := Neo4jStatus(coded)
	require.Equal(t, TransactionTimedOutClientConfiguration, code)
}

type codedTestError struct {
	code string
	err  error
}

func (e *codedTestError) Error() string         { return e.err.Error() }
func (e *codedTestError) Unwrap() error         { return e.err }
func (e *codedTestError) BoltErrorCode() string { return e.code }
