package cypher

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"unsafe"
	"weak"

	"github.com/stretchr/testify/require"
)

// The statement's size caches don't keep the values they sized alive: a
// list the statement dropped is collected while the statement still runs,
// whatever was cached about it (#1343).
func TestStatementSizeCachesDoNotKeepValuesAlive(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	tracker := newTestMemoryTracker("pinned")
	t.Cleanup(tracker.end)

	newList := func() []interface{} {
		list := make([]interface{}, 100000)
		for i := range list {
			list[i] = "element " + strconv.Itoa(i)
		}
		return list
	}
	sized := newList()
	sizedRef := weak.Make(unsafe.SliceData(sized))
	require.Positive(t, tracker.sizedValue(unsafe.Pointer(unsafe.SliceData(sized)), len(sized), sized))
	collected := newList()
	collectedRef := weak.Make(unsafe.SliceData(collected))
	tracker.noteList(collected, 1<<20)
	sized, collected = nil, nil
	runtime.GC()
	runtime.GC()
	require.Nil(t, sizedRef.Value(), "the size cache kept a dropped list alive")
	require.Nil(t, collectedRef.Value(), "the collected-list registry kept a dropped list alive")

	// A list sized again is still sized from the cache, and a different list
	// of the same length is not mistaken for it.
	again := newList()
	first := tracker.sizedValue(unsafe.Pointer(unsafe.SliceData(again)), len(again), again)
	var walks int
	estimateObserver = func() { walks++ }
	t.Cleanup(func() { estimateObserver = nil })
	require.Equal(t, first, tracker.sizedValue(unsafe.Pointer(unsafe.SliceData(again)), len(again), again))
	require.Zero(t, walks, "a cached list was walked again")
	other := make([]interface{}, len(again))
	for i := range other {
		other[i] = int64(i)
	}
	require.Equal(t, estimateValueBytes(other), tracker.sizedValue(unsafe.Pointer(unsafe.SliceData(other)), len(other), other))
	runtime.KeepAlive(again)
}

// A WITH that keeps some of the rows it projects (DISTINCT, WHERE, SKIP or
// LIMIT, or rows kept only for ORDER BY) is charged for the rows it keeps,
// not for every row it built (#1343).
func TestWithChargesOnlyTheRowsItKeeps(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	executor := newTransactionMemoryExecutor(t)
	rows := make([]pipelineRow, 20000)
	for i := range rows {
		rows[i] = pipelineRow{"x": int64(i), "s": "carried text value " + strconv.Itoa(i)}
	}
	for _, clause := range []string{
		"WITH DISTINCT 1 AS one",
		"WITH x, s WHERE x = 7",
		"WITH x, s ORDER BY x DESC LIMIT 1",
		"WITH x, s SKIP 19999",
		"WITH count(*) AS n, collect(s)[0] AS first",
	} {
		t.Run(clause, func(t *testing.T) {
			tracker := newTestMemoryTracker("nornic")
			defer tracker.end()
			ctx := context.WithValue(context.Background(), memoryScopeKey{}, &tracker.root)
			before := tracker.root.mark()
			fixtureAdmission961Context, fixtureAdmission961 := statementTestContext(ctx, clause)
			_ = fixtureAdmission961Context
			out, ok := executor.pipelineApplyWith(fixtureAdmission961, fixtureAdmission961Context, rows, clause)
			require.True(t, ok)
			require.Len(t, out, 1)
			taken := tracker.root.mark() - before
			require.Lessf(t, taken, int64(64<<10), "%s kept 1 row but stayed charged %d bytes", clause, taken)
		})
	}
}

// A statement keeps the memory limits it started with when they change
// while it runs: lowered, it doesn't fail on the next reservation; cleared,
// it stays counted to its end (#1343).
func TestRunningStatementKeepsItsStartingLimits(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	// A first clause of a few rows, sized while the limit changes; then about
	// 5 MB of rows built by operators that start after the change.
	const rowsQuery = "UNWIND range(1, 10) AS x WITH x, 'first' AS s WITH count(*) AS n UNWIND range(1, 30000) AS y WITH y, 'carried text value ' + toString(y) AS s ORDER BY y RETURN count(*) AS n"
	changeOnFirstSizing := func(t *testing.T, change func()) {
		var once sync.Once
		sizingObserver = func() { once.Do(change) }
		t.Cleanup(func() { sizingObserver = nil })
	}

	t.Run("cleared", func(t *testing.T) {
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
		changeOnFirstSizing(t, func() { ConfigureTransactionMemory(0, TransactionMemoryLimits{}) })
		_, err := executor.Execute(context.Background(), rowsQuery, nil)
		sizingObserver = nil
		requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		requirePoolsEmpty(t, "nornic")
		_, err = executor.Execute(context.Background(), rowsQuery, nil)
		require.NoError(t, err, "a statement started after the limits were cleared has none")
	})

	t.Run("lowered", func(t *testing.T) {
		ConfigureTransactionMemory(1<<30, TransactionMemoryLimits{})
		changeOnFirstSizing(t, func() { ConfigureTransactionMemory(1<<20, TransactionMemoryLimits{}) })
		_, err := executor.Execute(context.Background(), rowsQuery, nil)
		sizingObserver = nil
		require.NoError(t, err, "the statement failed under a limit lowered after it started")
		requirePoolsEmpty(t, "nornic")
		_, err = executor.Execute(context.Background(), rowsQuery, nil)
		requireMemoryError(t, err, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", "dbms.memory.transaction.total.max")
		requirePoolsEmpty(t, "nornic")
	})
}
