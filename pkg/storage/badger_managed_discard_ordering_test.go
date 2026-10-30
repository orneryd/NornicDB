package storage

import (
	"runtime"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestManagedDB_DiscardPublishOrdering pins the hazard fixed in
// advanceDiscardTs: the discard accept and its publish to Badger must run
// under one critical section. Before the fix, a goroutine that accepted a
// lower timestamp could be preempted and publish AFTER a higher one,
// feeding Badger a decreasing discardTs and tripping its fatal
// cleanupCommittedTransactions assert under concurrent commits.
//
// With the recording seam the test asserts the publish sequence directly;
// without the fix it fails deterministically-ish under the race detector.
func TestManagedDB_DiscardPublishOrdering(t *testing.T) {
	e := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = e.Close() })
	db := e.BadgerEngine.db

	var mu sync.Mutex
	published := make([]uint64, 0, 64)
	db.publishDiscard = func(ts uint64) {
		// Widen the accept→publish window: without the fix, a goroutine that
		// accepted a lower ts parks here while a later accept publishes
		// first, and the recorded sequence shows the inversion.
		runtime.Gosched()
		mu.Lock()
		published = append(published, ts)
		mu.Unlock()
	}

	const goroutines = 8
	const perGoroutine = 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := []byte{1, byte(g), byte(i / 256), byte(i % 256)}
				err := db.Update(func(txn *badger.Txn) error { return txn.Set(key, nil) })
				require.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, published)
	for i := 1; i < len(published); i++ {
		require.Greater(t, published[i], published[i-1],
			"discard publishes must be strictly increasing; got %d after %d", published[i], published[i-1])
	}
}

// TestManagedDB_ConcurrentUpdatesNoBadgerAssert is the crash-form regression:
// the same concurrent workload against the real SetDiscardTs publish. Before
// the ordering fix this intermittently tripped Badger's fatal
// "Assert failed" in cleanupCommittedTransactions.
func TestManagedDB_ConcurrentUpdatesNoBadgerAssert(t *testing.T) {
	e := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = e.Close() })
	db := e.BadgerEngine.db

	const goroutines = 8
	const perGoroutine = 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := []byte{2, byte(g), byte(i / 256), byte(i % 256)}
				err := db.Update(func(txn *badger.Txn) error { return txn.Set(key, nil) })
				require.NoError(t, err)
			}
		}(g)
	}
	wg.Wait()
}
