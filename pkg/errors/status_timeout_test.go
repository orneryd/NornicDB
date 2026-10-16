package errors

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A statement stopped at its deadline is Neo4j's transaction timeout, not a
// SyntaxError (R12: "context deadline exceeded" surfaced as SyntaxError).
func TestNeo4jStatusDeadlineIsTransactionTimeout(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		fmt.Errorf("pipeline stage: %w", context.DeadlineExceeded),
	} {
		code, message := Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", code)
		require.Equal(t, TransactionTimedOutMessage, message)
		require.True(t, HasNeo4jStatus(err))
		require.False(t, IsCompileTimeError(err), "a timed-out statement compiled, so HTTP keeps its result columns")
	}
	// An error that carries its own status keeps it.
	coded := fmt.Errorf("%w", &codedTestError{code: TransactionTimedOutClientConfiguration, err: context.DeadlineExceeded})
	code, _ := Neo4jStatus(coded)
	require.Equal(t, TransactionTimedOutClientConfiguration, code)
	// Cancellation is not a timeout.
	code, _ = Neo4jStatus(context.Canceled)
	require.NotEqual(t, TransactionTimedOut, code)
}

type codedTestError struct {
	code string
	err  error
}

func (e *codedTestError) Error() string         { return e.err.Error() }
func (e *codedTestError) Unwrap() error         { return e.err }
func (e *codedTestError) BoltErrorCode() string { return e.code }
