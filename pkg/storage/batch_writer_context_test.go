package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type exactBatchContextTarget struct {
	db       *managedBadgerDB
	expected context.Context
	commits  int
}

func (target *exactBatchContextTarget) openBatch() *badger.Txn { return target.db.openBatch() }
func (target *exactBatchContextTarget) commitBatch(ctx context.Context, txn *badger.Txn) error {
	if ctx != target.expected {
		panic("batch writer substituted the selected operation context")
	}
	target.commits++
	return target.db.commitBatch(ctx, txn)
}

// Force the repeatable-unit spill deterministically while executing actual
// native metadata commits: the same caller context owns both the spill and
// final flush; its later cancellation also prevents another publication.
func TestBatchWriterForwardsExactContextAcrossSpillAndFlush(t *testing.T) {
	engine := newTestEngine(t)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), struct{}{}, "caller-value"))
	defer cancel()
	target := &exactBatchContextTarget{db: engine.db, expected: ctx}
	w := &batchWriter{to: target}
	defer w.discard()
	calls := 0
	require.NoError(t, w.write(ctx, func(txn *badger.Txn) error {
		calls++
		if calls == 1 {
			require.NoError(t, txn.Set([]byte("ctx:spill"), []byte("first")))
			return badger.ErrTxnTooBig
		}
		return txn.Set([]byte("ctx:final"), []byte("second"))
	}))
	require.Equal(t, 2, calls)
	require.Equal(t, 1, target.commits)
	require.NoError(t, w.flush(ctx))
	require.Equal(t, 2, target.commits)
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		requireValue(t, txn, "ctx:spill", "first")
		requireValue(t, txn, "ctx:final", "second")
		return nil
	}))
	require.NoError(t, w.write(ctx, func(txn *badger.Txn) error { return txn.Set([]byte("ctx:canceled"), []byte("never")) }))
	cancel()
	require.ErrorIs(t, w.flush(ctx), context.Canceled)
	require.Equal(t, 3, target.commits)
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("ctx:canceled"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}
