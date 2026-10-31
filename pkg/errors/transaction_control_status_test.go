// Author: Claude, NOR-180 transaction-control status owner, 2026-10-30.
package errors

import (
	stderrors "errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// A transaction-control operation's failure is classified where the operation
// produces it; every consumer renders it with the one status renderer.
func TestTransactionControlStatusOwner(t *testing.T) {
	unclassified := stderrors.New("transaction session has expired or closed")
	classified := &statusError{code: "Neo.ClientError.Transaction.TransactionNotFound", message: "no such transaction"}
	diag := localization.ExternalDiagnostic(stderrors.New("wal closed"))
	terminal := NewTerminalFailure(TerminalRollback, false, nil, stderrors.New("wal closed"), diag)
	for _, tc := range []struct {
		op   TerminalOperation
		want string
	}{{TerminalBegin, TransactionStartFailed}, {TerminalCommit, DatabaseTransactionCommitFailed}, {TerminalRollback, TransactionRollbackFailed}} {
		t.Run(tc.want, func(t *testing.T) {
			failure := TransactionControlFailure(tc.op, unclassified)
			code, message := Neo4jStatus(failure)
			require.Equal(t, tc.want, code, "an unclassified failure gets the operation's status, not SyntaxError")
			require.Contains(t, message, unclassified.Error())
			require.ErrorIs(t, failure, unclassified)
			require.Same(t, classified, TransactionControlFailure(tc.op, classified), "an error keeps its own status")
			require.Same(t, terminal, TransactionControlFailure(tc.op, terminal), "a produced terminal outcome keeps its frozen status")
			require.NoError(t, TransactionControlFailure(tc.op, nil))
		})
	}
	// Composition after classification: the operation's primary decides, and
	// a terminal child's status never replaces it.
	for _, tc := range []struct {
		primary error
		want    string
	}{{TransactionControlFailure(TerminalBegin, unclassified), TransactionStartFailed}, {classified, classified.code}} {
		composed := CombineTerminalOutcomes(tc.primary, terminal)
		code, message := RenderStatus(composed, nil)
		require.Equal(t, tc.want, code)
		require.Contains(t, message, tc.primary.Error())
		require.Contains(t, message, "wal closed")
		require.ErrorIs(t, composed, tc.primary)
	}
	// A produced record is frozen: a statement abort keeps its statement's
	// status wherever it is delivered later, and an explicit ROLLBACK's own
	// failure (no statement primary) is a rollback failure.
	abort := NewTerminalFailure(TerminalAbort, false, unclassified, stderrors.New("wal closed"), diag)
	code, _ := Neo4jStatus(TransactionControlFailure(TerminalCommit, abort))
	require.Equal(t, StatementSyntaxError, code)
	code, _ = Neo4jStatus(NewTerminalFailure(TerminalRollback, false, unclassified, stderrors.New("wal closed"), diag))
	require.Equal(t, TransactionRollbackFailed, code)
	// Neo4j 5.26 Status.Transaction: the generic control failures are DatabaseErrors.
	require.Equal(t, "Neo.DatabaseError.Transaction.TransactionStartFailed", TransactionStartFailed)
	require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", DatabaseTransactionCommitFailed)
	require.Equal(t, "Neo.DatabaseError.Transaction.TransactionRollbackFailed", TransactionRollbackFailed)
}

type statusError struct{ code, message string }

func (e *statusError) Error() string         { return e.message }
func (e *statusError) BoltErrorCode() string { return e.code }
