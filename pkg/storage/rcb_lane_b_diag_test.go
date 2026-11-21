package storage

// Root-cause diagnostic, v1.5.2 verification campaign, lane B (test-only).
// G-http-autocommit-runner (NOR-358), the panic member: an explicit
// transaction's snapshot read after its engine has closed.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestRCBClosedEngineSnapshotReadGuard(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
	if err != nil {
		t.Fatal(err)
	}
	read := func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		it.Close()
		return nil
	}
	t.Logf("RCB358 TX snapshotTx pinned at begin=%v", tx.snapshotTx != nil)
	t.Logf("RCB358 TX snapshot read before close err=%v", tx.withSnapshotViewLocked(read))
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	unguarded := func() (recovered interface{}, err error) {
		defer func() { recovered = recover() }()
		err = tx.withSnapshotViewLocked(read)
		return nil, err
	}
	panicValue, readErr := unguarded()
	t.Logf("RCB358 TX withSnapshotViewLocked after close (transaction path, badger_transaction_snapshot_reads.go:572): panic=%v err=%v", panicValue, readErr)
	var guardPanic interface{}
	guarded := func() error {
		defer func() { guardPanic = recover() }()
		return recoverBadgerClosedPanic(func() error { return tx.withSnapshotViewLocked(read) })
	}()
	t.Logf("RCB358 TX same read inside recoverBadgerClosedPanic (variant): panic=%v err=%v isStorageClosed=%v", guardPanic, guarded, errors.Is(guarded, ErrStorageClosed))
	var viewPanic interface{}
	viewErr := func() error {
		defer func() { viewPanic = recover() }()
		return engine.withView(read)
	}()
	t.Logf("RCB358 TX engine.withView after close (auto-commit helper, badger_txn_helpers.go:87): panic=%v err=%v", viewPanic, viewErr)
	if panicValue == nil || fmt.Sprint(panicValue) != "DB Closed" {
		t.Errorf("RCB358 expected the unguarded transaction read to panic with DB Closed, got %v", panicValue)
	}
}
