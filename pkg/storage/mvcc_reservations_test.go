package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func takeReservationCut(t *testing.T, ledger *mvccReservations, cut uint64) *mvccCut {
	t.Helper()
	_, _, copied, err := ledger.cutTaken(cut, func(time.Time) (map[string]MVCCVersion, error) { return nil, nil })
	require.NoError(t, err)
	return copied
}

// A cut copies the versions in flight and the ones published since it
// started; a version published before it started is in its snapshot and is
// neither copied nor kept.
func TestReservationCutCopiesWhatItsSnapshotMayNotHold(t *testing.T) {
	ledger := &mvccReservations{}
	state := &namespaceMVCCState{}
	var before, during, inflight badger.Txn
	reserve := func(txn *badger.Txn) MVCCVersion {
		version, err := ledger.reserve(txn, "test", state, time.Now(), false)
		require.NoError(t, err)
		return version
	}
	publishedBefore := reserve(&before)
	older := ledger.cutStarted()
	ledger.publishTxn(&before)
	cut := ledger.cutStarted()
	publishedDuring, unpublished := reserve(&during), reserve(&inflight)
	ledger.publishTxn(&during)
	ledger.publishTxn(&during)

	copied := takeReservationCut(t, ledger, cut)
	require.True(t, copied.holds("test", publishedDuring))
	require.True(t, copied.holds("test", unpublished))
	require.False(t, copied.holds("test", publishedBefore), "published before the cut started")
	require.Len(t, copied.entries, 2)
	copied.release()

	// The older cut, still in progress, started before both publications.
	copied = takeReservationCut(t, ledger, older)
	require.True(t, copied.holds("test", publishedBefore))
	require.True(t, copied.holds("test", publishedDuring))
	copied.release()
	require.Empty(t, ledger.published)
	require.Empty(t, ledger.cuts)
	ledger.publishTxn(&inflight)
	require.Empty(t, ledger.inflight)
	require.Empty(t, ledger.at)
	require.Nil(t, takeReservationCut(t, ledger, ledger.cutStarted()))
}

// Cuts that keep overlapping, so that one is always in progress, retain only
// the versions published since the oldest of them started.
func TestOverlappingReservationCutsDoNotRetainThePublishedVersions(t *testing.T) {
	ledger := &mvccReservations{}
	state := &namespaceMVCCState{}
	previous := ledger.cutStarted()
	for round := 0; round < 1000; round++ {
		var txn badger.Txn
		_, err := ledger.reserve(&txn, "test", state, time.Now(), false)
		require.NoError(t, err)
		ledger.publishTxn(&txn)
		next := ledger.cutStarted()
		copied := takeReservationCut(t, ledger, previous)
		require.Len(t, copied.entries, 1)
		copied.release()
		require.LessOrEqual(t, len(ledger.published), 1)
		previous = next
	}
	ledger.cutAbandoned(previous)
	require.Empty(t, ledger.published)
}

// Ending a later cut must not retire the publications an earlier cut needs.
func TestReservationCutsRetainPublicationsUntilOldestFinishes(t *testing.T) {
	ledger := &mvccReservations{}
	state := &namespaceMVCCState{}
	older, newer := ledger.cutStarted(), ledger.cutStarted()
	var txn badger.Txn
	version, err := ledger.reserve(&txn, "test", state, time.Now(), false)
	require.NoError(t, err)
	ledger.publishTxn(&txn)
	ledger.cutAbandoned(newer)
	require.Len(t, ledger.published, 1)
	copied := takeReservationCut(t, ledger, older)
	require.True(t, copied.holds("test", version))
	copied.release()
	require.Empty(t, ledger.published)
	require.Empty(t, ledger.cuts)
}

func TestPublicationBoundaryForSnapshotAndRetirement(t *testing.T) {
	ledger := &mvccReservations{published: []publishedReservation{{cut: 2}, {cut: 2}, {cut: 5}, {cut: 9}}}
	for _, test := range []struct {
		cut  uint64
		want int
	}{{0, 0}, {2, 0}, {3, 2}, {5, 2}, {9, 3}, {10, 4}} {
		require.Equal(t, test.want, ledger.publishedLowerBoundLocked(test.cut), "cut %d", test.cut)
	}
	ledger.published = nil
	require.Zero(t, ledger.publishedLowerBoundLocked(1))
}
