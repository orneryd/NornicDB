package errors

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A statement an owned deadline stopped is Neo4j's transaction timeout, not
// a SyntaxError (#1339), with the status of the owner whose deadline ended
// it first: the context's cause.
func TestNeo4jStatusDeadlineIsTransactionTimeout(t *testing.T) {
	for _, test := range []struct {
		cause         error
		code, message string
	}{
		{ErrDatabaseQueryTimeLimit, TransactionTimedOut, TransactionTimedOutMessage},
		{ErrServerStatementTimeout, TransactionTimedOut, TransactionTimedOutMessage},
		{ErrClientTransactionTimeout, TransactionTimedOutClientConfiguration, TransactionTimedOutClientConfigurationMessage},
		{fmt.Errorf("wrapped: %w", ErrDatabaseQueryTimeLimit), TransactionTimedOut, TransactionTimedOutMessage},
	} {
		t.Run(test.cause.Error(), func(t *testing.T) {
			require.ErrorIs(t, test.cause, context.DeadlineExceeded, "an owned cause is a deadline")
			for _, err := range []error{
				MarkStatementTimeout(context.DeadlineExceeded, test.cause),
				fmt.Errorf("route: %w", MarkStatementTimeout(fmt.Errorf("pipeline stage: %w", context.DeadlineExceeded), test.cause)),
			} {
				code, message := Neo4jStatus(err)
				require.Equal(t, test.code, code)
				require.Equal(t, test.message, message)
				require.True(t, HasNeo4jStatus(err))
				require.False(t, IsCompileTimeError(err), "a timed-out statement compiled, so HTTP keeps its result columns")
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			marked := MarkStatementTimeout(context.DeadlineExceeded, test.cause)
			require.Same(t, marked, MarkStatementTimeout(marked, ErrClientTransactionTimeout), "marking is idempotent: the first owner stands")
		})
	}
	require.Nil(t, MarkStatementTimeout(nil, ErrDatabaseQueryTimeLimit))
}

// A deadline no owner installed is not the statement's timeout: a caller's
// own budget (cause context.DeadlineExceeded), a live context (nil cause), a
// cancelled one, or a statement error that is not a deadline at all.
func TestMarkStatementTimeoutNeedsAnOwnedDeadline(t *testing.T) {
	for _, cause := range []error{nil, context.DeadlineExceeded, context.Canceled, fmt.Errorf("budget: %w", context.DeadlineExceeded)} {
		err := MarkStatementTimeout(context.DeadlineExceeded, cause)
		require.Equal(t, context.DeadlineExceeded, err)
		require.False(t, HasNeo4jStatus(err))
	}
	cancelled := MarkStatementTimeout(context.Canceled, ErrDatabaseQueryTimeLimit)
	require.Equal(t, context.Canceled, cancelled, "a statement error that is not a deadline keeps its status")
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
