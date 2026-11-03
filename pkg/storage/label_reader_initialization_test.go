package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Initialization errors have no iterable value or reader-owned resources.
// A discarded real Badger transaction exercises the marker-read failure.
func TestLabelReaderInitializationRejectsBeforeIteration(t *testing.T) {
	engine := createTestBadgerEngine(t)
	engine.labelIndexReadyAt.Store(0)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	deadline, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		discard bool
		want    error
	}{
		{"canceled", canceled, false, context.Canceled},
		{"deadline", deadline, false, context.DeadlineExceeded},
		{"marker-read", t.Context(), true, badger.ErrDiscardedTxn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := engine.ActiveReaders()
			txn, readTs, openErr := engine.db.beginTxn(false)
			require.NoError(t, openErr)
			defer engine.db.endRead(readTs)
			defer txn.Discard()
			if tc.discard {
				txn.Discard()
			}
			reader, err := engine.newLabelIndexReader(tc.ctx, txn, "test:", "Record", "", visibleRead, false)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, reader, "initialization failure cannot supply an iterable value")
			require.Equal(t, before, engine.ActiveReaders())
		})
	}
	for _, ctx := range []context.Context{canceled, deadline} {
		before := engine.ActiveReaders()
		visits := 0
		err := engine.StreamNodesByLabelProjectedInScope(ctx, "test:", "Record", nil, func(*Node) error { visits++; return nil })
		require.ErrorIs(t, err, ctx.Err())
		require.Zero(t, visits)
		require.Equal(t, before, engine.ActiveReaders(), "public canceled stream must release its read scope")
	}
}

// Component-boundary schedule: cancel the real context at the post-marker
// check, without introducing a production hook or claiming public scheduling.
type cancelAtSecondLabelCheck struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelAtSecondLabelCheck) Err() error {
	c.checks++
	if c.checks == 2 {
		c.cancel()
	}
	return c.Context.Err()
}
func TestLabelReaderPostMarkerCancellation(t *testing.T) {
	engine := createTestBadgerEngine(t)
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &cancelAtSecondLabelCheck{Context: parent, cancel: cancel}
	txn, readTs, openErr := engine.db.beginTxn(false)
	require.NoError(t, openErr)
	defer engine.db.endRead(readTs)
	defer txn.Discard()
	before := engine.ActiveReaders()
	reader, err := engine.newLabelIndexReader(ctx, txn, "test:", "Record", "", visibleRead, false)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, reader)
	require.Equal(t, 2, ctx.checks)
	select {
	case <-ctx.Done():
	default:
		t.Fatal("real parent cancellation must close Done")
	}
	require.ErrorIs(t, parent.Err(), context.Canceled)
	require.Equal(t, before, engine.ActiveReaders())
}
