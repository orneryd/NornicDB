package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type fixedTransactionMemoryTrackerGetter struct {
	tracker *transactionMemoryTracker
}

func (g fixedTransactionMemoryTrackerGetter) Get() any {
	return g.tracker
}

func TestNewTestMemoryTrackerResetsAllLimitSnapshots(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(17, TransactionMemoryLimits{TransactionMax: 23, DatabaseTotalMax: 29})

	tracker := &transactionMemoryTracker{limit: 1, databaseLimit: 2, globalLimit: 3}
	tracker.root.tracker = tracker
	tracker.failure.Store(&transactionMemoryFailure{err: errors.New("previous statement failed")})

	reused := newTestMemoryTrackerFromPool(fixedTransactionMemoryTrackerGetter{tracker: tracker}, "snapshot")
	require.Same(t, tracker, reused, "the deterministic test pool must return the seeded tracker")
	reused.mu.Lock()
	limit := reused.limit
	databaseLimit := reused.databaseLimit
	globalLimit := reused.globalLimit
	reused.mu.Unlock()
	require.Equal(t, int64(23), limit)
	require.Equal(t, int64(29), databaseLimit)
	require.Equal(t, int64(17), globalLimit)
	require.Nil(t, reused.failure.Load())
	t.Cleanup(reused.end)
}
