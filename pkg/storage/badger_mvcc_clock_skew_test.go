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
//	failed to commit implicit transaction: conflict: node X changed
//	after transaction start
//
// failure observed on Linux CI runners under
// TestExecuteCypher_SetInvalidatesManagedEmbeddings.
func TestCurrentMVCCReadVersion_ClampsToHighWater(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	// Force the high-water mark to a timestamp ~1s in the future. This
	// simulates a head being stamped with a clock that briefly ran
	// ahead of the wall-clock observed by the next BeginTransaction.
	future := time.Now().Add(time.Second).UnixNano()
	e.mvccHighWaterNanos.Store(future)

	got := e.currentMVCCReadVersion()
	require.GreaterOrEqual(t, got.CommitTimestamp.UnixNano(), future,
		"currentMVCCReadVersion must not return a timestamp earlier than the high-water mark")
}

func TestAllocateMVCCVersion_AdvancesHighWater(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	commitAt := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		_, err := e.allocateMVCCVersion(txn, commitAt)
		return err
	}))
	require.Equal(t, commitAt.UnixNano(), e.mvccHighWaterNanos.Load(),
		"allocateMVCCVersion must publish its commit timestamp to the high-water mark")

	// A subsequent allocate at an EARLIER timestamp must not lower the
	// high-water mark — monotonicity is the whole point.
	earlier := commitAt.Add(-time.Hour)
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		_, err := e.allocateMVCCVersion(txn, earlier)
		return err
	}))
	require.Equal(t, commitAt.UnixNano(), e.mvccHighWaterNanos.Load(),
		"high-water mark must never go backward")
}

func TestBeginTransaction_DoesNotConflictAfterClockSkew(t *testing.T) {
	// End-to-end variant of the regression: simulate a head whose
	// version timestamp lies in the future relative to wall-clock,
	// then open a transaction and exercise the write path. Without
	// the clamp, the SET would fail with a "changed after transaction
	// start" conflict; with it, the tx commits cleanly.
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })

	_, err := e.CreateNode(&Node{
		ID:         "test:skew",
		Labels:     []string{"L"},
		Properties: map[string]any{"v": int64(1)},
	})
	require.NoError(t, err)

	// Pretend a future commit happened: bump high-water past wall-clock.
	e.mvccHighWaterNanos.Store(time.Now().Add(2 * time.Second).UnixNano())

	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{
		ID:         "test:skew",
		Labels:     []string{"L"},
		Properties: map[string]any{"v": int64(2)},
	}))
	require.NoError(t, tx.Commit(), "skewed clock must not cause a phantom conflict")

	got, err := e.GetNode("test:skew")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])
}
