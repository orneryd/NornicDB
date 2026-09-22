package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// commitRecovering runs tx.Commit and reports a panic as a value instead of
// crashing the test binary, so a torn commit tail shows up as a failed
// assertion with its message rather than as a lost test run.
func commitRecovering(tx *BadgerTransaction) (err error, panicked any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = recovered
		}
	}()
	return tx.Commit(), nil
}

// closeWhileTailRuns installs a commit-tail hook that starts engine.Close on
// another goroutine right after the Badger commit returns and then gives it
// time to run. Before the fix Close released the engine's state (b.db and the
// caches) under the running tail; after the fix Close waits for the tail.
func closeWhileTailRuns(t *testing.T, engine *BadgerEngine) <-chan error {
	t.Helper()
	closeDone := make(chan error, 1)
	restore := setCommitTailHook(func() {
		go func() { closeDone <- engine.Close() }()
		time.Sleep(150 * time.Millisecond)
	})
	t.Cleanup(restore)
	return closeDone
}

// TestBadgerTransaction_CommitTailSurvivesConcurrentClose pins the shutdown
// ordering an explicit transaction is owed: once badgerTx.Commit() has
// returned, the data is durable, so Close must wait for the post-commit tail
// (label counts, MVCC sequence, ID counters, caches, callbacks) instead of
// nil-ing the handles it uses. The client must see success, and the counter
// high-water mark the tail persists must be on disk for the next open.
func TestBadgerTransaction_CommitTailSurvivesConcurrentClose(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "race:1", Labels: []string{"Race"}, Properties: map[string]any{"k": "v"}})
	require.NoError(t, err)

	closeDone := closeWhileTailRuns(t, engine)
	commitErr, panicked := commitRecovering(tx)
	require.Nil(t, panicked, "commit tail panicked after the Badger commit returned: %v", panicked)
	require.NoError(t, commitErr, "a commit whose Badger commit already returned must be reported as success")
	require.NoError(t, <-closeDone)

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	node, err := reopened.GetNode("race:1")
	require.NoError(t, err)
	require.Equal(t, "v", node.Properties["k"])
	num, ok := reopened.idDict.lookupNodeNumID("race:1")
	require.True(t, ok)
	require.GreaterOrEqual(t, persistedIDCounter(t, reopened, idCounterNodeKey), num,
		"the counter high-water mark is written by the tail Close must wait for")
}

// TestBadgerEngine_NonTransactionalWriteTailSurvivesConcurrentClose is the
// withUpdate analogue: CreateNode outside an explicit transaction persists the
// ID counters after db.Update returns, and Close must wait for that too.
func TestBadgerEngine_NonTransactionalWriteTailSurvivesConcurrentClose(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	closeDone := closeWhileTailRuns(t, engine)
	_, err = engine.CreateNode(&Node{ID: "race:2", Labels: []string{"Race"}})
	require.NoError(t, err)
	require.NoError(t, <-closeDone)

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	num, ok := reopened.idDict.lookupNodeNumID("race:2")
	require.True(t, ok)
	require.GreaterOrEqual(t, persistedIDCounter(t, reopened, idCounterNodeKey), num,
		"CreateNode's counter write ran against a released db handle and was silently dropped")
}

// TestBadgerTransaction_CommitAfterCloseFailsCleanly is the control for the
// other side of the barrier: a commit that reaches the Badger commit only
// after Close has finished must fail with ErrStorageClosed, without touching
// Badger and without panicking.
func TestBadgerTransaction_CommitAfterCloseFailsCleanly(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "late:1", Labels: []string{"Late"}})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	commitErr, panicked := commitRecovering(tx)
	require.Nil(t, panicked, "commit after Close panicked: %v", panicked)
	require.ErrorIs(t, commitErr, ErrStorageClosed)
	require.Equal(t, TxStatusRolledBack, tx.Status)
}
