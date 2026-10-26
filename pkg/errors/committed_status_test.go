// Author: Claude, NOR-180 committed-outcome retry contract and the one
// committed representation, 30 October 2026.
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

	// A committed transaction whose COMMIT record was written but whose
	// post-commit step failed: the same committed terminal value, without a WAL
	// failure (finishCommitted).
	written := func() *TerminalFailure {
		return NewTerminalFailure(TerminalCommit, true, storage.ErrStorageRestoring, nil, localization.Message{})
	}
	completion := written()
	code, message := Neo4jStatus(completion)
	require.Equal(t, TransactionCommitFailed, code, "a committed post-commit failure (WAL open)")
	require.Equal(t, TransientDatabaseUnavailable+": "+storage.ErrStorageRestoring.Error(), message, "its own classification and cause, no WAL diagnostic")
	require.ErrorIs(t, completion, storage.ErrStorageRestoring)
	require.True(t, HasTerminalOutcome(completion), "committed truth has one representation")

	direct := NewTerminalFailure(TerminalCommit, true, storage.ErrStorageRestoring, walClosed, diag)
	require.Equal(t, TransactionCommitFailed, direct.BoltErrorCode())
	require.Contains(t, direct.Error(), TransientDatabaseUnavailable+": ", "the record keeps its own classification")
	aborted := NewTerminalFailure(TerminalAbort, false, transient(), walClosed, diag)
	require.True(t, IsRetriableStatus(aborted.BoltErrorCode()), "an outcome with no committed work keeps its retriable status")

	for _, order := range [][]error{
		{NewTerminalFailure(TerminalAbort, false, transient(), walClosed, diag), NewTerminalFailure(TerminalCommit, true, nil, walClosed, diag)},
		{NewTerminalFailure(TerminalCommit, true, nil, walClosed, diag), NewTerminalFailure(TerminalAbort, false, transient(), walClosed, diag)},
		{transient(), NewTerminalFailure(TerminalCommit, true, nil, walClosed, diag)},
		// The 044a review's composition: a transient aborted child whose WAL
		// failed and a committed child whose WAL record was written, both orders.
		{NewTerminalFailure(TerminalAbort, false, transient(), walClosed, diag), written()},
		{written(), NewTerminalFailure(TerminalAbort, false, transient(), walClosed, diag)},
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

// A discarded child return is retained by the statement journal only as a
// terminal value: the committed writable-WAL outcome is one, so a count-only
// consumer's parent still reports it, with committed truth, in either order.
func TestCommittedOutcomeSurvivesRetention(t *testing.T) {
	walClosed := stderrors.New("wal closed")
	diag := localization.ExternalDiagnostic(walClosed)
	aborted := func() error {
		return NewTerminalFailure(TerminalAbort, false, &statusError{code: "Neo.TransientError.General.MemoryPoolOutOfMemoryError", message: "memory"}, walClosed, diag)
	}
	written := func() error {
		return NewTerminalFailure(TerminalCommit, true, storage.ErrStorageRestoring, nil, localization.Message{})
	}
	for _, order := range [][]error{{aborted(), written()}, {written(), aborted()}} {
		var journal TerminalOutcomes
		for _, child := range order {
			var changed bool
			journal, changed = journal.Record(child)
			require.True(t, changed, "every terminal child is retained")
		}
		retained := journal.Retain(nil)
		code, message := Neo4jStatus(retained)
		require.False(t, IsRetriableStatus(code), "promoted aggregate with committed work: %s", code)
		require.Contains(t, message, TransientDatabaseUnavailable+": "+storage.ErrStorageRestoring.Error())
		require.Contains(t, message, "Neo.TransientError.General.MemoryPoolOutOfMemoryError")
		for _, child := range order {
			require.ErrorIs(t, retained, child)
		}
	}
}

// Committed work belongs to the invocation, not to a child's error: an
// owner that saw a commit settles its returned terminal outcome even when the
// committing child returned nothing, in either order of recording. Healthy
// completion and ordinary errors are unchanged.
func TestInvocationCommittedWorkSettlesTheOutcome(t *testing.T) {
	walClosed := stderrors.New("wal closed")
	diag := localization.ExternalDiagnostic(walClosed)
	memory := "Neo.TransientError.General.MemoryPoolOutOfMemoryError"
	aborted := func() error {
		return NewTerminalFailure(TerminalAbort, false, &statusError{code: memory, message: "memory"}, walClosed, diag)
	}
	record := func(o TerminalOutcomes, err error) TerminalOutcomes { o, _ = o.Record(err); return o }
	commit := func(o TerminalOutcomes) TerminalOutcomes { o, _ = o.RecordCommit(); return o }
	for name, journal := range map[string]TerminalOutcomes{
		"commit_then_abort": record(commit(TerminalOutcomes{}), aborted()),
		"abort_then_commit": commit(record(TerminalOutcomes{}, aborted())),
	} {
		retained := journal.Retain(nil)
		code, message := Neo4jStatus(retained)
		require.False(t, IsRetriableStatus(code), "%s: a successful committed sibling: %s", name, code)
		require.Equal(t, TransactionCommitFailed, code, name)
		require.Equal(t, 1, strings.Count(message, memory+": memory"), "%s: the aborted child keeps its own status and cause", name)
		require.True(t, HasTerminalOutcome(retained))
		// A later composition keeps the invocation's committed truth.
		outer := CombineTerminalOutcomes(stderrors.New("enclosing"), retained)
		outerCode, _ := Neo4jStatus(outer)
		require.False(t, IsRetriableStatus(outerCode), "%s: composed again: %s", name, outerCode)
	}
	committedOnly := commit(TerminalOutcomes{})
	require.True(t, committedOnly.Empty(), "committed work records no failure")
	require.NoError(t, committedOnly.Retain(nil), "healthy completion is unchanged")
	ordinary := &statusError{code: memory, message: "memory"}
	require.Same(t, ordinary, committedOnly.Retain(ordinary), "an ordinary error keeps its own status policy")
	withoutCommit, _ := TerminalOutcomes{}.Record(aborted())
	code, _ := Neo4jStatus(withoutCommit.Retain(nil))
	require.Equal(t, memory, code, "no committed work: the aborted outcome stays retriable")
}
