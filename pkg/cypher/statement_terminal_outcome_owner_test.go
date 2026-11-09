package cypher

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// terminalOwnerValueKey is an originating request value a completion must keep
// reachable through the exact context its owner passes.
type terminalOwnerValueKey struct{}

func newTerminalOwnerFailure(primary, terminal error) error {
	return nornicerrors.NewTerminalFailure(nornicerrors.TerminalRollback, false, primary, terminal, localization.Message{})
}

// A statement that never reached creation owns no terminal journal. Its
// originating request context must still project the display value exactly
// once, and its outcome must not reach an enclosing statement's journal or an
// enclosing client operation's terminal scope.
func TestCompleteStatementTerminalOutcomeWithoutJournalProjectsOnly(t *testing.T) {
	base := context.WithValue(context.Background(), terminalOwnerValueKey{}, "originating request")
	scope := nornicerrors.NewTerminalOutcomeScope(base)
	enclosing := &statementExecutionContext{Context: scope, outcome: &statementOutcome{}}
	enclosingFailure := newTerminalOwnerFailure(nil, stderrors.New("enclosing wal append failed"))
	enclosing.outcome.record(false, false, enclosingFailure)

	result := &ExecuteResult{Columns: []string{"n"}}
	originalResult := result
	retErr := newTerminalOwnerFailure(stderrors.New("statement panicked before creation"), nil)
	originalErr := retErr

	projections := 0
	completeStatementTerminalOutcome(enclosing, false, &result, &retErr, func() {
		projections++
		// No journal exists, so compose changed nothing and recording has not
		// started yet.
		require.Nil(t, scope.Complete(nil))
		require.Same(t, originalResult, result)
		require.Same(t, originalErr, retErr)
	})

	require.Equal(t, 1, projections, "the display projection must run exactly once")
	require.Same(t, originalResult, result, "a no-journal completion keeps the original result")
	require.Same(t, originalErr, retErr, "a no-journal completion keeps the original error")
	require.Nil(t, scope.Complete(nil), "a no-journal completion records nothing into the enclosing scope")
	require.True(t, stderrors.Is(enclosing.outcome.retainTerminalFailures(nil), enclosingFailure),
		"a no-journal completion retains nothing on the enclosing statement")
}

// A panic before statement creation unwinds through the same completion: the
// display projection still runs once, and nothing reaches the enclosing scope.
func TestCompleteStatementTerminalOutcomeWithoutJournalProjectsDuringPanic(t *testing.T) {
	base := context.WithValue(context.Background(), terminalOwnerValueKey{}, "originating request")
	scope := nornicerrors.NewTerminalOutcomeScope(base)

	result := &ExecuteResult{Columns: []string{"n"}}
	originalResult := result
	retErr := newTerminalOwnerFailure(stderrors.New("panic before statement creation"), nil)
	originalErr := retErr

	projections := 0
	func() {
		defer func() {
			require.Equal(t, "statement panicked before creation", recover())
		}()
		defer func() {
			completeStatementTerminalOutcome(scope, false, &result, &retErr, func() { projections++ })
		}()
		panic("statement panicked before creation")
	}()

	require.Equal(t, 1, projections, "panic unwind must still project exactly once")
	require.Same(t, originalResult, result)
	require.Same(t, originalErr, retErr)
	require.Nil(t, scope.Complete(nil))
}

// An initialized statement journal keeps its own context: the originating
// cancellation cause, deadline and values stay reachable, compose precedes the
// single display projection, and recording follows it.
func TestCompleteStatementTerminalOutcomeWithJournalKeepsOriginatingContext(t *testing.T) {
	base := context.WithValue(context.Background(), terminalOwnerValueKey{}, "originating request")
	scope := nornicerrors.NewTerminalOutcomeScope(base)
	deadlineCtx, stopDeadline := context.WithTimeout(scope, time.Minute)
	defer stopDeadline()
	causeErr := stderrors.New("statement timeout")
	cancelled, cancel := context.WithCancelCause(deadlineCtx)
	cancel(causeErr)

	statement := &statementExecutionContext{Context: cancelled, outcome: &statementOutcome{}}
	statement.outcome.record(false, false, newTerminalOwnerFailure(nil, stderrors.New("wal append failed")))

	result := &ExecuteResult{Columns: []string{"n"}}
	retErr := context.Canceled

	projections := 0
	completeStatementTerminalOutcome(statement, true, &result, &retErr, func() {
		projections++
		// Compose already promoted the retained journal, and recording has not
		// happened yet.
		require.Nil(t, scope.Complete(nil))
		require.Nil(t, result)
		require.True(t, nornicerrors.HasTerminalOutcome(retErr))
	})

	require.Equal(t, 1, projections, "the display projection must run exactly once")
	require.Nil(t, result, "a recorded journal failure replaces the result")
	require.True(t, nornicerrors.HasTerminalOutcome(retErr))
	require.True(t, stderrors.Is(retErr, context.Canceled), "the original cancellation stays a cause")
	require.NotNil(t, scope.Complete(nil), "the completion records into its enclosing client operation")

	require.Equal(t, causeErr, context.Cause(statement), "the statement's originating cause is unchanged")
	require.Equal(t, context.Canceled, statement.Err())
	deadline, ok := statement.Deadline()
	require.True(t, ok, "the originating deadline stays reachable")
	wantDeadline, _ := deadlineCtx.Deadline()
	require.Equal(t, wantDeadline, deadline)
	require.Equal(t, "originating request", statement.Value(terminalOwnerValueKey{}),
		"the originating request value stays reachable")
}

// Coordinator and Fabric participant markers still own their terminal outcome:
// a journaled completion projects but neither retains nor records it.
func TestCompleteStatementTerminalOutcomeJournalRespectsCoordinatorAndFabricScope(t *testing.T) {
	markers := map[string]func(context.Context) context.Context{
		"coordinator-owned": storage.WithCoordinatorOwnedTransaction,
		"fabric-participant": func(ctx context.Context) context.Context {
			return fabric.WithFabricTransaction(ctx, fabric.NewFabricTransaction("terminal-owner-test"))
		},
	}
	for name, mark := range markers {
		t.Run(name, func(t *testing.T) {
			base := context.WithValue(context.Background(), terminalOwnerValueKey{}, name)
			scope := nornicerrors.NewTerminalOutcomeScope(base)
			statement := &statementExecutionContext{Context: mark(scope), outcome: &statementOutcome{}}
			statement.outcome.record(false, false, newTerminalOwnerFailure(nil, stderrors.New("wal append failed")))

			result := &ExecuteResult{Columns: []string{"n"}}
			originalResult := result
			retErr := stderrors.New("participant failure")
			originalErr := retErr

			projections := 0
			completeStatementTerminalOutcome(statement, true, &result, &retErr, func() { projections++ })

			require.Equal(t, 1, projections)
			require.Same(t, originalResult, result)
			require.Same(t, originalErr, retErr)
			require.Nil(t, scope.Complete(nil))
		})
	}
}
