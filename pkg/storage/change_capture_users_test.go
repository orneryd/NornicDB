package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A transaction's change events carry who it ran for (SetUsers): the
// executing user, whose privileges applied, and the authenticated user, who
// differs from it only while impersonating; a transaction without them
// records neither. SetUsers is refused once the transaction has ended.
func TestChangeEventsCarryTransactionUsers(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetChangeCapture("users", ChangeCaptureFull)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetUsers("alice", "svc"))
	_, err = tx.CreateNode(&Node{ID: "users:impersonated"})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Error(t, tx.SetUsers("bob", "bob"), "an ended transaction records no users")

	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "users:anonymous"})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	events, err := engine.ChangeEvents("users", 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "alice", events[0].ExecutingUser)
	require.Equal(t, "svc", events[0].AuthenticatedUser)
	require.Empty(t, events[1].ExecutingUser)
	require.Empty(t, events[1].AuthenticatedUser)
}
