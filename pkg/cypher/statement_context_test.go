package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStatementContextCancellation: a running statement's context behaves as
// a context.WithCancel of its parent. The parent's cancellation reaches Err
// and Done (whether Done was taken before or after it), the statement's own
// cancellation (TERMINATE, the statement's end) reaches Done and contexts
// derived from it, and the first cancellation wins.
func TestStatementContextCancellation(t *testing.T) {
	waitDone := func(t *testing.T, ctx context.Context) {
		t.Helper()
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("context not done")
		}
	}

	t.Run("parent cancelled after Done", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		statement := &statementContext{parent: parent}
		done := statement.Done()
		require.NoError(t, statement.Err())
		cancel()
		waitDone(t, statement)
		require.Equal(t, done, statement.Done())
		require.ErrorIs(t, statement.Err(), context.Canceled)
	})

	t.Run("parent cancelled before Done", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		statement := &statementContext{parent: parent}
		cancel()
		require.ErrorIs(t, statement.Err(), context.Canceled)
		waitDone(t, statement)
	})

	t.Run("parent deadline", func(t *testing.T) {
		parent, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		statement := &statementContext{parent: parent}
		deadline, ok := statement.Deadline()
		require.True(t, ok)
		wantDeadline, _ := parent.Deadline()
		require.Equal(t, wantDeadline, deadline)
		waitDone(t, statement)
		require.ErrorIs(t, statement.Err(), context.DeadlineExceeded)
	})

	t.Run("statement cancelled reaches derived contexts", func(t *testing.T) {
		statement := &statementContext{parent: context.Background()}
		derived, cancelDerived := context.WithCancel(statement)
		defer cancelDerived()
		statement.cancel(context.Canceled)
		waitDone(t, derived)
		require.ErrorIs(t, derived.Err(), context.Canceled)
		statement.cancel(context.DeadlineExceeded)
		require.ErrorIs(t, statement.Err(), context.Canceled, "the first cancellation wins")
	})

	t.Run("cancelled before Done", func(t *testing.T) {
		statement := &statementContext{parent: context.Background()}
		require.Nil(t, context.Background().Done())
		statement.cancel(context.Canceled)
		waitDone(t, statement)
	})

	t.Run("values", func(t *testing.T) {
		type key struct{}
		tx := &runningTransaction{}
		statement := &statementContext{parent: context.WithValue(context.Background(), key{}, "v"), tx: tx}
		require.Equal(t, "v", statement.Value(key{}))
		require.Same(t, tx, statement.Value(ctxKeyRunningTransaction{}))
	})
}
