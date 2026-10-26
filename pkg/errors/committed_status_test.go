// Author: Claude, NOR-180 committed-outcome retry contract, 30 October 2026.
package errors

import (
	stderrors "errors"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// IsRetriableStatus is the pinned driver's own retry decision.
func TestIsRetriableStatusIsTheDriversDecision(t *testing.T) {
	for _, code := range []string{
		TransientDatabaseUnavailable, TransientOutdated, TransientDeadlockDetected,
		"Neo.TransientError.General.MemoryPoolOutOfMemoryError",
		"Neo.TransientError.Transaction.Terminated", "Neo.TransientError.Transaction.LockClientStopped",
		"Neo.ClientError.Cluster.NotALeader", "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase",
		"Neo.ClientError.Security.AuthorizationExpired", "Neo.ClientError.Security.Unauthorized",
		TransactionCommitFailed, TransactionRollbackFailed, ConstraintValidationFailed, StatementSyntaxError, "",
	} {
		require.Equal(t, (&db.Neo4jError{Code: code}).IsRetriable(), IsRetriableStatus(code), code)
	}
}

// An outcome containing committed work never carries a retriable status,
// single or aggregate, in either child order; each record keeps its own code.
func TestCommittedOutcomeIsNeverRetriable(t *testing.T) {
	walClosed := stderrors.New("wal closed")
	diag := localization.ExternalDiagnostic(walClosed)
	transient := func() error {
		return &statusError{code: "Neo.TransientError.General.MemoryPoolOutOfMemoryError", message: "memory"}
	}

	completion := CommittedFailure(storage.ErrStorageRestoring)
	code, message := Neo4jStatus(completion)
	require.Equal(t, TransactionCommitFailed, code, "a committed post-commit failure (WAL open)")
	require.Contains(t, message, storage.ErrStorageRestoring.Error())
	require.ErrorIs(t, completion, storage.ErrStorageRestoring)
	classified := &statusError{code: ConstraintValidationFailed, message: "kept"}
	require.Same(t, classified, CommittedFailure(classified), "a non-retriable status is the failure's own")

	committed := NewTerminalFailure(TerminalCommit, true, completion, walClosed, diag)
	require.Equal(t, TransactionCommitFailed, committed.BoltErrorCode(), "WAL closed after a transient post-commit failure")
	direct := NewTerminalFailure(TerminalCommit, true, storage.ErrStorageRestoring, walClosed, diag)
	require.Equal(t, TransactionCommitFailed, direct.BoltErrorCode())
	require.Contains(t, direct.Error(), TransientDatabaseUnavailable+": ", "the record keeps its own classification")
	aborted := NewTerminalFailure(TerminalAbort, false, transient(), walClosed, diag)
	require.True(t, IsRetriableStatus(aborted.BoltErrorCode()), "an outcome with no committed work keeps its retriable status")

	for _, order := range [][]error{
		{NewTerminalFailure(TerminalAbort, false, transient(), walClosed, diag), NewTerminalFailure(TerminalCommit, true, nil, walClosed, diag)},
		{NewTerminalFailure(TerminalCommit, true, nil, walClosed, diag), NewTerminalFailure(TerminalAbort, false, transient(), walClosed, diag)},
		{transient(), NewTerminalFailure(TerminalCommit, true, nil, walClosed, diag)},
	} {
		aggregate := CombineTerminalOutcomes(order[0], order[1:]...)
		code, message := Neo4jStatus(aggregate)
		require.False(t, IsRetriableStatus(code), "aggregate with committed work: %s", code)
		require.Equal(t, 1, strings.Count(message, "Neo.TransientError.General.MemoryPoolOutOfMemoryError"), "the aborted child keeps its own status")
		for _, child := range order {
			require.ErrorIs(t, aggregate, child)
		}
	}
}
