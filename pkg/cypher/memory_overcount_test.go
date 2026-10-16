package cypher

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The estimate follows the memory a statement holds (#1343): on a
// 5,280-document library it counted 4.9 GiB while the statement held about
// 0.3 GiB, because every clause's output was charged and none released, and
// because shared values were sized again for every row and every operator.

// countedPeak runs query with tracking on under a limit it never reaches
// and returns the highest the global pool's use rose to (the statement's
// estimate, in 1 MiB reservations).
func countedPeak(t *testing.T, executor *StorageExecutor, query string, params map[string]interface{}) int64 {
	t.Helper()
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	var peak atomic.Int64
	done := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		for {
			if global, _ := TransactionMemoryPoolUsage("nornic"); global > peak.Load() {
				peak.Store(global)
			}
			select {
			case <-done:
				return
			case <-time.After(50 * time.Microsecond):
			}
		}
	}()
	_, err := executor.Execute(context.Background(), query, params)
	close(done)
	sampler.Wait()
	require.NoError(t, err)
	requirePoolsEmpty(t, "nornic")
	return peak.Load()
}

// (a) Each clause's output replaces its input: the estimate of a pipeline of
// eight WITH clauses over the same rows stays near one clause's, where it
// was eight times it.
func TestEstimateFollowsTheRowsHeldNow(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	head := "UNWIND range(1, 20000) AS x WITH x, 'carried text value ' + toString(x) AS s ORDER BY x "
	one := countedPeak(t, executor, head+"RETURN count(*) AS n", nil)
	eight := countedPeak(t, executor, head+strings.Repeat("WITH x, s ORDER BY x ", 7)+"RETURN count(*) AS n", nil)
	require.Positive(t, one)
	require.Lessf(t, eight, 2*one, "eight clauses counted %d bytes, one clause %d", eight, one)
}

// (a) A CALL subquery's outer row is shared by every row its body returns:
// its values count once, not once per returned row.
func TestEstimateCountsCallOuterRowOnce(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	view := make(map[string]interface{}, 2000)
	for i := 0; i < 2000; i++ {
		view["key"+strings.Repeat("x", i%7)+string(rune('a'+i%26))+strings.Repeat("y", i%5)+":"+string(rune('A'+i%26))] = "value text " + strings.Repeat("v", 40)
	}
	params := map[string]interface{}{"view": view}
	few := countedPeak(t, executor, "WITH $view AS view CALL (view) { UNWIND range(1, 10) AS i RETURN i } RETURN count(*) AS n", params)
	many := countedPeak(t, executor, "WITH $view AS view CALL (view) { UNWIND range(1, 5000) AS i RETURN i } RETURN count(*) AS n", params)
	// 5,000 joined rows of slots, not 5,000 copies of a 2,000-entry map.
	require.Lessf(t, many, few+(16<<20), "the outer row's map was counted per returned row: %d vs %d", many, few)
}

// (b) A large value every row of every per-row CALL run carries is sized
// once per statement: the statement's size cache holds it.
func TestSharedValueSizedOncePerStatement(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	tracker := transactionMemoryTrackers.Get().(*transactionMemoryTracker)
	tracker.database = transactionMemory.database("sized")
	t.Cleanup(tracker.end)
	big := make([]interface{}, 10000)
	for i := range big {
		big[i] = "element"
	}
	var walks atomic.Int64
	estimateObserver = func() { walks.Add(1) }
	t.Cleanup(func() { estimateObserver = nil })
	first := rowEstimator{known: tracker}
	size := first.bytes(pipelineRow{"view": big})
	afterFirst := walks.Load()
	for i := 0; i < 100; i++ {
		other := rowEstimator{known: tracker}
		require.Equal(t, size, other.bytes(pipelineRow{"view": big}))
	}
	require.Equal(t, afterFirst, walks.Load(), "the large list was walked again")
}

// (c) With no limit configured nothing sizes a value: grouping rows that
// carry a large list doesn't walk it.
func TestNoEstimationWithoutLimit(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	big := make([]interface{}, 50000)
	for i := range big {
		big[i] = int64(i)
	}
	query := "UNWIND range(1, 2000) AS g WITH g, $big AS b RETURN g % 50 AS k, count(*) AS n, collect(DISTINCT size(b)) AS sizes ORDER BY k"
	var walks atomic.Int64
	estimateObserver = func() { walks.Add(1) }
	t.Cleanup(func() { estimateObserver = nil })

	ConfigureTransactionMemory(0, TransactionMemoryLimits{})
	untracked, err := executor.Execute(context.Background(), query, map[string]interface{}{"big": big})
	require.NoError(t, err)
	require.Zero(t, walks.Load(), "a value was sized with no limit configured")

	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	tracked, err := executor.Execute(context.Background(), query, map[string]interface{}{"big": big})
	require.NoError(t, err)
	require.Positive(t, walks.Load(), "with a limit the values are sized")
	require.Equal(t, untracked.Rows, tracked.Rows)
}
