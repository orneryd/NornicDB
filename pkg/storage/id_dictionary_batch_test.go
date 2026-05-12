package storage

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIDDict_CounterSurvivesRestart: a fresh engine with persisted
// data reloads the node/edge counters even though the counter keys
// are now written once per commit (not once per alloc).
func TestIDDict_CounterSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dict")
	eng, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)

	// Allocate three node numIDs via three separate txns (each goes
	// through b.withUpdate → flushTxnCounters once per commit).
	for i := 0; i < 3; i++ {
		id := NodeID("t:n" + string(rune('0'+i)))
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	nodeMaxBefore, _ := eng.idDict.CounterUsage()
	require.Equal(t, uint64(3), nodeMaxBefore)
	require.NoError(t, eng.Close())

	// Reopen. loadFromBadger must pick the counter up.
	eng2, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng2.Close() })
	nodeMaxAfter, _ := eng2.idDict.CounterUsage()
	require.Equal(t, nodeMaxBefore, nodeMaxAfter, "counter must persist across reopen despite batched writes")

	// Next alloc continues from the loaded counter — no collision.
	_, err = eng2.CreateNode(&Node{ID: "t:n9", Labels: []string{"L"}})
	require.NoError(t, err)
	got, ok := eng2.idDict.lookupNodeNumID("t:n9")
	require.True(t, ok)
	require.Equal(t, nodeMaxBefore+1, got)
}

// TestIDDict_BulkTxnCommitsCounterOnce: many allocations in a single
// explicit transaction write exactly ONE counter key at commit time
// (not one per alloc).
func TestIDDict_BulkTxnCommitsCounterOnce(t *testing.T) {
	eng, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)

	const n = 10
	for i := 0; i < n; i++ {
		id := NodeID("t:b" + string(rune('0'+i)))
		_, err := tx.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	// Before commit: txnCounters should have a single entry tracking
	// the high-water nodeMax.
	eng.idDict.txnMu.Lock()
	st, ok := eng.idDict.txnCounters[tx.badgerTx]
	eng.idDict.txnMu.Unlock()
	require.True(t, ok, "txn-scoped counter state should be staged before commit")
	require.Equal(t, uint64(n), st.nodeMax)

	require.NoError(t, tx.Commit())

	// After commit: staged entry is drained.
	eng.idDict.txnMu.Lock()
	_, stillThere := eng.idDict.txnCounters[tx.badgerTx]
	eng.idDict.txnMu.Unlock()
	require.False(t, stillThere, "txn-scoped counter state must be dropped at commit")
}

// TestIDDict_RollbackDoesNotPersistCounter: a rolled-back txn must
// drop any staged counter updates (atomic counter still advances for
// uniqueness, but the persisted key stays at the last committed value).
func TestIDDict_RollbackDoesNotPersistCounter(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dict-rollback")
	eng, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)

	// Commit one node so the persisted counter is 1.
	_, err = eng.CreateNode(&Node{ID: "t:c1", Labels: []string{"L"}})
	require.NoError(t, err)

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		id := NodeID("t:r" + string(rune('0'+i)))
		_, err := tx.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.NoError(t, tx.Rollback())

	// Staged state must be dropped.
	eng.idDict.txnMu.Lock()
	_, stillThere := eng.idDict.txnCounters[tx.badgerTx]
	eng.idDict.txnMu.Unlock()
	require.False(t, stillThere)

	require.NoError(t, eng.Close())

	// Reopen: persisted counter should reflect only the committed node.
	eng2, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng2.Close() })
	n, _ := eng2.idDict.CounterUsage()
	require.Equal(t, uint64(1), n, "rollback must not bump persisted counter key")
}
