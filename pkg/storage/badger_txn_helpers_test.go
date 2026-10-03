package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerHelperTransactionsDrainOnFailure(t *testing.T) {
	for _, name := range []string{"view", "update"} {
		t.Run(name, func(t *testing.T) {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			helper := engine.withView
			if name == "update" {
				helper = engine.withUpdate
			}
			sentinel := errors.New("callback failed")
			require.ErrorIs(t, helper(func(*badger.Txn) error { return sentinel }), sentinel)
			require.PanicsWithValue(t, "unexpected", func() { _ = helper(func(*badger.Txn) error { panic("unexpected") }) })
			require.ErrorIs(t, helper(func(*badger.Txn) error { panic("DB Closed") }), ErrStorageClosed)
			require.NoError(t, engine.Close())
			require.ErrorIs(t, helper(func(*badger.Txn) error { t.Error("callback called after Close"); return nil }), ErrStorageClosed)
		})
	}
}

func TestRecoverBadgerClosedPanic_ReturnsErrStorageClosed(t *testing.T) {
	err := recoverBadgerClosedPanic(func() error {
		panic("DB Closed")
	})
	require.ErrorIs(t, err, ErrStorageClosed)
}

func TestRecoverBadgerClosedPanic_RepanicsUnexpectedPanic(t *testing.T) {
	assert.PanicsWithValue(t, "boom", func() {
		_ = recoverBadgerClosedPanic(func() error {
			panic("boom")
		})
	})
}

func TestIsTransactionTooBig(t *testing.T) {
	assert.False(t, IsTransactionTooBig(nil))
	assert.True(t, IsTransactionTooBig(fmt.Errorf("preface: %w", badger.ErrTxnTooBig)), "wrapped")
	assert.True(t, IsTransactionTooBig(errors.New("the message: "+badger.ErrTxnTooBig.Error())), "re-formatted")
	assert.False(t, IsTransactionTooBig(errors.New("unrelated failure")))
}
