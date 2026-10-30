package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestCurrentMVCCReadVersion_ClampsToHighWater regresses the
// non-monotonic clock issue: when allocateMVCCVersion stamps a head
// version with a timestamp that, due to a NTP correction or
// containerized-clock drift, is later than the wall-clock value
// observed in a subsequent BeginTransaction, currentMVCCReadVersion
// must clamp the returned readTS to the high-water mark so the
// per-row write-conflict check (head.Version > tx.readTS) does not
// fire on straight-line single-goroutine code paths.
//
// This regresses the
//
//	commit failed: conflict: node X changed after transaction start
//
// failure observed on Linux CI runners under
// TestExecuteCypher_SetInvalidatesManagedEmbeddings.
func TestCurrentMVCCReadVersion_ClampsToHighWater(t *testing.T) {
	e := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = e.Close() })

	// Force one namespace's high-water mark to a timestamp ~1s in the
	// future. This simulates a head being stamped with a clock that
	// briefly ran ahead of the wall-clock observed by the next
	// BeginTransaction in that namespace.
	state, err := e.namespaceMVCC("test")
	require.NoError(t, err)
	future := time.Now().Add(time.Second).UnixNano()
	state.highWaterNanos.Store(future)

	got := e.currentMVCCReadVersion("test")
	require.GreaterOrEqual(t, got.CommitTimestamp.UnixNano(), future,
		"currentMVCCReadVersion must not return a timestamp earlier than the high-water mark")
}

func TestAllocateMVCCVersion_AdvancesHighWater(t *testing.T) {
	e := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = e.Close() })

	commitAt := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		_, err := e.allocateMVCCVersion(txn, "test", commitAt)
		return err
	}))
	state, err := e.namespaceMVCC("test")
	require.NoError(t, err)
	require.Equal(t, commitAt.UnixNano(), state.highWaterNanos.Load(),
		"allocateMVCCVersion must publish its commit timestamp to the namespace's high-water mark")

	// A subsequent allocate at an EARLIER timestamp must not lower the
	// high-water mark — monotonicity is the whole point.
	earlier := commitAt.Add(-time.Hour)
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		_, err := e.allocateMVCCVersion(txn, "test", earlier)
		return err
	}))
	require.Equal(t, commitAt.UnixNano(), state.highWaterNanos.Load(),
		"high-water mark must never go backward")
}

func TestBeginTransaction_DoesNotConflictAfterClockSkew(t *testing.T) {
	// End-to-end variant of the regression: simulate a head whose
	// version timestamp lies in the future relative to wall-clock,
	// then open a transaction and exercise the write path. Without
	// the clamp, the SET would fail with a "changed after transaction
	// start" conflict; with it, the tx commits cleanly.
	e := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = e.Close() })

	_, err := e.CreateNode(&Node{
		ID:         "test:skew",
		Labels:     []string{"L"},
		Properties: map[string]any{"v": int64(1)},
	})
	require.NoError(t, err)

	// Pretend a future commit happened in the "test" namespace: bump
	// its high-water past wall-clock.
	state, err := e.namespaceMVCC("test")
	require.NoError(t, err)
	state.highWaterNanos.Store(time.Now().Add(2 * time.Second).UnixNano())

	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{
		ID:         "test:skew",
		Labels:     []string{"L"},
		Properties: map[string]any{"v": int64(2)},
	}, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit(), "skewed clock must not cause a phantom conflict")

	got, err := e.GetNode("test:skew")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])
}

func TestSnapshotIsolationConflict_UsesTimestampWhenSequenceSaturated(t *testing.T) {
	tx := &BadgerTransaction{
		readTS: MVCCVersion{
			CommitTimestamp: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
			CommitSequence:  maxMVCCCommitSequence,
		},
	}

	require.True(t, tx.snapshotIsolationConflict(MVCCVersion{
		CommitTimestamp: tx.readTS.CommitTimestamp.Add(time.Nanosecond),
		CommitSequence:  maxMVCCCommitSequence,
	}), "later saturated timestamp should still count as a concurrent write")

	require.False(t, tx.snapshotIsolationConflict(MVCCVersion{
		CommitTimestamp: tx.readTS.CommitTimestamp,
		CommitSequence:  maxMVCCCommitSequence,
	}), "same saturated timestamp should not count as a conflict")
}
