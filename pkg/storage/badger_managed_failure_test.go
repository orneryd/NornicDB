package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Failure paths of managed-mode commits (#703): conflicts Badger finds while
// committing, commits refused after a rollback could not complete, and
// failures part-way through a large commit.

// overwriteRaw rewrites a key with its own value outside any transaction
// validation, so only Badger's conflict check can see the change.
func overwriteRaw(t *testing.T, engine *BadgerEngine, key []byte) {
	t.Helper()
	var raw []byte
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		raw, err = item.ValueCopy(nil)
		return err
	}))
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error { return txn.Set(key, raw) }))
}

// A peer that commits between a transaction's snapshot validation and its
// write is caught by Badger's conflict check: the commit fails as a
// conflict, ordinary or large, and writes nothing.
func TestCommit_BadgerConflictAfterValidation(t *testing.T) {
	for _, large := range []bool{false, true} {
		engine := openLargeCommitTestEngine(t, t.TempDir())
		_, err := engine.CreateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		if large {
			stageLargeCreate(t, tx, 6000)
		}
		require.NoError(t, tx.UpdateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(2)}}))
		overwriteRaw(t, engine, nodeKey("test:base"))

		err = tx.Commit()
		require.ErrorIs(t, err, ErrConflict, "large=%v", large)
		require.ErrorIs(t, err, badger.ErrConflict, "large=%v", large)
		require.Zero(t, countBigNodes(t, engine))
		base, err := engine.GetNode("test:base")
		require.NoError(t, err)
		require.Equal(t, int64(1), base.Properties["v"])
		requireNoLargeCommitIntent(t, engine)
		require.NoError(t, engine.Close())
	}
}

// When a failed large commit cannot be rolled back, the store refuses every
// write until it is reopened, reads continue without the commit, and the
// reopen completes the rollback.
func TestLargeCommit_UnrecoverableRollbackRefusesWritesUntilReopen(t *testing.T) {
	for _, batchFails := range []bool{true, false} {
		t.Run(map[bool]string{true: "a batch fails", false: "a later batch is refused"}[batchFails], func(t *testing.T) {
			requireUnrecoverableRollback(t, batchFails)
		})
	}
}

func requireUnrecoverableRollback(t *testing.T, batchFails bool) {
	dir := t.TempDir()
	engine := openLargeCommitTestEngine(t, dir)
	_, err := engine.CreateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)

	injected := errors.New("injected batch failure")
	restore := setLargeCommitBatchHook(func(batch int) error {
		if batch == 2 {
			// The store stops writes: the rollback cannot get a timestamp
			// either, and neither can a further batch.
			engine.db.oracle.fail(errors.New("disk gone"))
			if batchFails {
				return injected
			}
		}
		return nil
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:base", Labels: []string{"Base"}, Properties: map[string]any{"v": int64(2)}}))
	stageLargeCreate(t, tx, 6000)
	err = tx.Commit()
	restore()
	if batchFails {
		require.ErrorIs(t, err, injected)
	}
	require.Contains(t, err.Error(), "refuses writes until restart")

	// Reads continue at the last published state.
	require.Zero(t, countBigNodes(t, engine))
	base, err := engine.GetNode("test:base")
	require.NoError(t, err)
	require.Equal(t, int64(1), base.Properties["v"])

	// Every kind of write is refused: engine writes, ordinary transaction
	// commits (their new property names included) and large commits.
	_, err = engine.CreateNode(&Node{ID: "test:refused", Labels: []string{"Base"}})
	require.ErrorContains(t, err, "refuses writes until restart")
	small, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = small.CreateNode(&Node{ID: "test:refused-tx", Labels: []string{"Base"}, Properties: map[string]any{"never_seen_before": int64(1)}})
	require.NoError(t, err)
	require.ErrorContains(t, small.Commit(), "refuses writes until restart")
	big, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, big, 6000)
	require.ErrorContains(t, big.Commit(), "refuses writes until restart")
	require.NoError(t, engine.Close())

	engine = openLargeCommitTestEngine(t, dir)
	defer engine.Close()
	requireNoLargeCommitIntent(t, engine)
	require.Zero(t, countBigNodes(t, engine))
	base, err = engine.GetNode("test:base")
	require.NoError(t, err)
	require.Equal(t, int64(1), base.Properties["v"])
	_, err = engine.CreateNode(&Node{ID: "test:after", Labels: []string{"Base"}})
	require.NoError(t, err)
}

// largeCommitWithLateName stages a large create and, last, a node with a
// property name of its own.
func largeCommitWithLateName(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, 6000)
	_, err = tx.CreateNode(&Node{ID: "test:late", Labels: []string{"Late"}, Properties: map[string]any{"late_name": int64(1)}})
	require.NoError(t, err)
	return tx
}

// A large commit that fails at any of its batches — the property-name
// tokens ahead of the first batch, a later batch, or the last batch, which
// deletes the intent — leaves nothing behind, and the same commit succeeds
// afterwards.
func TestLargeCommit_FailureAtAnyBatchRollsBack(t *testing.T) {
	probe := openLargeCommitTestEngine(t, t.TempDir())
	batches := countBatches(t)
	require.NoError(t, largeCommitWithLateName(t, probe).Commit())
	total := int(batches.Load())
	require.NoError(t, probe.Close())
	require.Greater(t, total, 3)

	for _, failAt := range []int{1, total - 1, total} {
		engine := openLargeCommitTestEngine(t, t.TempDir())
		injected := errors.New("injected batch failure")
		restore := setLargeCommitBatchHook(func(batch int) error {
			if batch == failAt {
				return injected
			}
			return nil
		})
		err := largeCommitWithLateName(t, engine).Commit()
		restore()
		require.ErrorIs(t, err, injected, "batch %d of %d", failAt, total)
		if failAt == 1 {
			require.ErrorIs(t, err, errPersistingTokens, "the first batch writes the statement's new property names")
		}
		require.Zero(t, countBigNodes(t, engine))
		_, err = engine.GetNode("test:late")
		require.ErrorIs(t, err, ErrNotFound)
		requireNoLargeCommitIntent(t, engine)

		require.NoError(t, largeCommitWithLateName(t, engine).Commit())
		require.Equal(t, 6000, countBigNodes(t, engine))
		late, err := engine.GetNode("test:late")
		require.NoError(t, err)
		require.Equal(t, int64(1), late.Properties["late_name"])
		require.NoError(t, engine.Close())
	}
}

// New property names that need several batches are not marked persisted
// when the store refuses their writes: the next commit stages them again.
func TestPropertyKeyDict_ManyNamesRefusedStayUnpersisted(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()
	txn, readTs := engine.db.beginTxn(true)
	defer engine.db.endRead(readTs)
	defer txn.Discard()
	for i := 0; i < 40000; i++ {
		_, err := engine.propKeyDict.resolveOrAllocateInTxn(txn, "test", fmt.Sprintf("refused_%d", i))
		require.NoError(t, err)
	}
	drain := engine.propKeyDict.flushTxnCounters(txn)
	engine.db.oracle.fail(errors.New("disk gone"))
	require.ErrorContains(t, engine.propKeyDict.persistTxnCounters(engine.db, drain), "refuses writes until restart")
	again := engine.db.DB.NewTransactionAt(engine.db.oracle.published.Load(), true)
	defer again.Discard()
	_, err := engine.propKeyDict.resolveOrAllocateInTxn(again, "test", "refused_0")
	require.NoError(t, err)
	require.Len(t, engine.propKeyDict.flushTxnCounters(again).pending, 1)
}

// The commit oracle refuses timestamps after it failed, and a committer
// waiting for its own write to publish is released with the failure.
func TestCommitOracle_FailureReleasesWaiters(t *testing.T) {
	o := newCommitOracle(5)
	ts, err := o.assign()
	require.NoError(t, err)
	cause := errors.New("disk gone")
	o.fail(cause)
	_, err = o.assign()
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, o.waitPublished(ts), cause)
	require.ErrorIs(t, o.failure(), cause)
}

// A store whose interrupted large commit cannot be rolled back (here: it is
// opened read-only) refuses to open; opened writable, it recovers.
func TestManagedBadger_OpenFailsWhenRecoveryCannotWrite(t *testing.T) {
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	m, err := openManagedBadger(opts)
	require.NoError(t, err)
	require.NoError(t, m.Update(func(txn *badger.Txn) error { return txn.Set([]byte("k"), []byte("a")) }))
	lc, err := m.beginLargeCommit()
	require.NoError(t, err)
	batch := lc.openBatch()
	require.NoError(t, batch.Set([]byte("k"), []byte("x")))
	require.NoError(t, lc.commitBatch(batch))
	require.NoError(t, m.DB.Close())

	_, err = openManagedBadger(opts.WithReadOnly(true))
	require.ErrorContains(t, err, "rolling back interrupted large commit")

	m, err = openManagedBadger(opts)
	require.NoError(t, err)
	defer m.Close()
	require.NoError(t, m.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "k", "a")
		return nil
	}))
}

// A large commit that cannot write its intent record, and cannot roll that
// back either, fails and stops further commits.
func TestManagedBadger_LargeCommitCannotStartOnReadOnlyStore(t *testing.T) {
	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	m, err := openManagedBadger(opts)
	require.NoError(t, err)
	require.NoError(t, m.Update(func(txn *badger.Txn) error { return txn.Set([]byte("k"), []byte("a")) }))
	require.NoError(t, m.Close())

	m, err = openManagedBadger(opts.WithReadOnly(true))
	require.NoError(t, err)
	defer m.Close()
	_, err = m.beginLargeCommit()
	require.ErrorContains(t, err, "rolling back")
	require.Error(t, m.oracle.failure())
	require.NoError(t, m.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "k", "a")
		return nil
	}))
}

func TestManagedBadger_ClosedHandleRefusesTransactions(t *testing.T) {
	m, err := openManagedBadger(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, m.Close())
	require.ErrorIs(t, m.View(func(*badger.Txn) error { return nil }), badger.ErrDBClosed)
	require.ErrorIs(t, m.Update(func(*badger.Txn) error { return nil }), badger.ErrDBClosed)
}

// The Badger methods that bypass the oracle are not available on the
// managed handle.
func TestManagedBadger_UnregisteredAccessPanics(t *testing.T) {
	engine := newTestEngine(t)
	require.Panics(t, func() { engine.db.NewTransaction(false) })
	require.Panics(t, func() { engine.db.NewStream() })
	require.Panics(t, func() { _, _ = engine.db.GetSequence([]byte("seq"), 1) })
}

// The maintenance write batch commits what is pending on every Flush and
// stays usable afterwards; Cancel drops writes not yet flushed.
func TestManagedWriteBatch_FlushKeepsWriterUsable(t *testing.T) {
	engine := newTestEngine(t)
	wb := engine.db.NewWriteBatch()
	require.NoError(t, wb.Set([]byte("wb-1"), []byte("a")))
	require.NoError(t, wb.Flush())
	require.NoError(t, wb.Set([]byte("wb-2"), []byte("b")))
	require.NoError(t, wb.Delete([]byte("wb-1")))
	require.NoError(t, wb.Flush())
	require.NoError(t, wb.Set([]byte("wb-3"), []byte("c")))
	wb.Cancel()
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("wb-1"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		requireValue(t, txn, "wb-2", "b")
		_, err = txn.Get([]byte("wb-3"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestManagedBadger_LoadRejectsInvalidBackup(t *testing.T) {
	engine := newTestEngine(t)
	require.Error(t, engine.db.Load(iotest.ErrReader(errors.New("read failed")), 1))
	_, err := engine.CreateNode(&Node{ID: "test:after-load", Labels: []string{"X"}})
	require.NoError(t, err, "a failed load releases the commit gate")
}

// A commit that cannot take what publication needs before turning large
// fails without taking the exclusive commit gate.
func TestCommitWriter_BeforeLargeFailureStopsTheCommit(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()
	txn, readTs := engine.db.beginTxn(true)
	cw := engine.newCommitWriter(engine.db, txn)
	refused := errors.New("engine closing")
	cw.beforeLarge = func() error { return refused }
	value := []byte(strings.Repeat("x", 100))
	var err error
	for i := 0; err == nil && i < 1_000_000; i++ {
		key := []byte(fmt.Sprintf("before-large-%06d", i))
		err = cw.write(func(txn *badger.Txn) error { return txn.Set(key, value) })
	}
	require.ErrorIs(t, err, refused)
	require.NoError(t, cw.abort())
	cw.discard()
	engine.db.endRead(readTs)
	_, err = engine.CreateNode(&Node{ID: "test:after", Labels: []string{"X"}})
	require.NoError(t, err, "the commit gate is free")
}

func TestIsPropertyKeyDictionaryKey(t *testing.T) {
	require.False(t, isPropertyKeyDictionaryKey(nil))
	require.True(t, isPropertyKeyDictionaryKey(propKeyForwardKey("ns", "name")))
	require.True(t, isPropertyKeyDictionaryKey(propKeyReverseKey("ns", 1)))
	require.True(t, isPropertyKeyDictionaryKey(propKeyCounterKey("ns")))
	require.False(t, isPropertyKeyDictionaryKey(nodeKey("test:n")))
}
