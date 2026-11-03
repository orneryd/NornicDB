package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// TestCountDeltaFoldSourceHonorsOwningContext pins the fold kernel's context
// contract at the three admission owners countFoldSource selects. A fold is
// handed one owning context by its caller and must hand that same context to
// the transaction owner it commits through, so the cause the caller observes
// is its own context.Canceled or context.DeadlineExceeded.
//
// foldRestoredCountDeltas and foldRemainingCountDeltasOnClose are the exact
// owners Restore and Close call; both pass an explicit engine-lifecycle root
// rather than a request context, so this test exercises the private admission
// contract those owners rely on and does not claim a production request ever
// reaches the database-source branches.
//
// A fold that stops at admission publishes nothing: it fails in the commit
// gate or in DecidePublication before a timestamp is assigned, so the deltas
// it was handed and their base count keys stay exactly as the last commit left
// them, a concurrent count read still sums the same rows, and the next fold
// with a live owning context folds the same backlog.
func TestCountDeltaFoldSourceHonorsOwningContext(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	const (
		ns    = "ns1"
		label = "CtxFold"
	)
	for _, id := range []NodeID{ns + ":ctx1", ns + ":ctx2"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{label}})
		require.NoError(t, err)
	}

	pendingDeltas := func(t *testing.T) int {
		t.Helper()
		var pending int
		require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
			prefix := countDeltaPrefixFor(countDeltaKindLabel, ns, label)
			it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
			defer it.Close()
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				pending++
			}
			return nil
		}))
		return pending
	}
	haveBaseCount := func(t *testing.T) bool {
		t.Helper()
		var found bool
		require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
			_, getErr := txn.Get(labelCountKey(ns, label))
			found = getErr == nil
			if getErr != nil && !errors.Is(getErr, badger.ErrKeyNotFound) {
				return getErr
			}
			return nil
		}))
		return found
	}
	baseCount := func(t *testing.T) int64 {
		t.Helper()
		var count int64
		require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
			decoded, err := readDerivedCountInTxn(txn, labelCountKey(ns, label))
			count = decoded
			return err
		}))
		return count
	}
	requireLabelCount := func(t *testing.T, want int64) {
		t.Helper()
		count, err := engine.NodeCountByLabelInNamespace(t.Context(), ns, label)
		require.NoError(t, err)
		require.Equal(t, want, count)
	}

	// Two acknowledged creates leave one pending delta each and no base key.
	require.NotZero(t, pendingDeltas(t), "each acknowledged create leaves its derived-count delta pending")
	require.False(t, haveBaseCount(t), "the base count key exists only after a fold")
	requireLabelCount(t, 2)

	t.Run("engine admission already stops on the owning canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.ErrorIs(t, engine.foldLabelCountDeltas(ctx, ns, label), context.Canceled)
		require.NotZero(t, pendingDeltas(t))
		require.False(t, haveBaseCount(t))
		requireLabelCount(t, 2)
	})

	t.Run("restore hold owner keeps the owning cancellation cause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := engine.foldRestoredCountDeltas(ctx, engine.db)
		require.ErrorIs(t, err, context.Canceled)
		require.NotZero(t, pendingDeltas(t), "a fold stopped at admission leaves every delta to the next fold")
		require.False(t, haveBaseCount(t), "a fold stopped at admission writes no base count")
		requireLabelCount(t, 2)
	})

	t.Run("close owner keeps the owning deadline cause", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		err := engine.foldRemainingCountDeltasOnClose(ctx, engine.db)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotZero(t, pendingDeltas(t))
		require.False(t, haveBaseCount(t))
		requireLabelCount(t, 2)
	})

	t.Run("a live owning context folds the same backlog", func(t *testing.T) {
		require.NoError(t, engine.foldRestoredCountDeltas(context.Background(), engine.db))

		require.Zero(t, pendingDeltas(t), "the retried fold consumes the backlog the stopped folds left")
		require.True(t, haveBaseCount(t))
		require.Equal(t, int64(2), baseCount(t))
		requireLabelCount(t, 2)
	})
}
