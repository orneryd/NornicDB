package cypher

import (
	"context"
	"math"
	"runtime"
	"strconv"
	"testing"
	"unsafe"
	"weak"

	"github.com/stretchr/testify/require"
)

// The rows a WITH drops (ORDER BY … LIMIT, SKIP, WHERE) are not reachable
// from the rows it hands on: their charge goes back, so what they hold must
// be garbage too (#1343).
func TestWithDropsTheRowsItDoesNotKeep(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	executor := newTransactionMemoryExecutor(t)
	for _, clause := range []string{
		"WITH x, xs ORDER BY x DESC LIMIT 1",
		"WITH x, xs SKIP 199",
		"WITH * ORDER BY x LIMIT 1",
		"WITH DISTINCT * WHERE x = 7",
	} {
		t.Run(clause, func(t *testing.T) {
			tracker := newTestMemoryTracker("nornic")
			defer tracker.end()
			ctx := context.WithValue(context.Background(), memoryScopeKey{}, &tracker.root)
			rows := make([]pipelineRow, 200)
			refs := make([]weak.Pointer[interface{}], len(rows))
			for i := range rows {
				xs := make([]interface{}, 1000)
				for j := range xs {
					xs[j] = "element " + strconv.Itoa(j)
				}
				refs[i] = weak.Make(unsafe.SliceData(xs))
				rows[i] = pipelineRow{"x": int64(i), "xs": xs}
			}
			fixtureAdmission960Context, fixtureAdmission960 := statementTestContext(ctx, clause)
			_ = fixtureAdmission960Context
			out, ok := executor.pipelineApplyWith(fixtureAdmission960, fixtureAdmission960Context, rows, clause)
			require.True(t, ok)
			require.Len(t, out, 1)
			rows = nil
			runtime.GC()
			runtime.GC()
			live := 0
			for _, ref := range refs {
				if ref.Value() != nil {
					live++
				}
			}
			require.LessOrEqualf(t, live, 1, "%s kept 1 row; %d rows' lists stayed reachable", clause, live)
			runtime.KeepAlive(out)
		})
	}
}

// The keys ORDER BY evaluates count while the rows are sorted: a key that
// builds a list per row is kept by the sort until it is done (#1343).
func TestOrderByChargesTheKeysItBuilds(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	_, err := executor.Execute(context.Background(), "UNWIND range(1, 1000) AS i WITH i ORDER BY range(i, i + 9999) RETURN count(*) AS n", nil)
	requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	requirePoolsEmpty(t, "nornic")

	// Keys the rows already hold count once, in the rows.
	result, err := executor.Execute(context.Background(), "UNWIND range(1, 1000) AS i WITH i, 'carried text value ' + toString(i) AS s ORDER BY s, i RETURN count(*) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1000)}}, result.Rows)
	requirePoolsEmpty(t, "nornic")
}

// range()'s preflight counts a range as wide as the integers in full: its
// count saturates rather than wrapping to zero (#1343). A range it can
// count has the length its values have.
func TestRangeLengthSaturatesAtFullWidth(t *testing.T) {
	require.Equal(t, maxReservation, cypherRange{start: math.MinInt64, end: math.MaxInt64, step: 1}.length())
	require.Equal(t, maxReservation, cypherRange{start: math.MaxInt64, end: math.MinInt64, step: -1}.length())
	require.Equal(t, maxReservation, cypherRange{start: math.MinInt64, end: math.MaxInt64, step: 2}.length())
	for _, sequence := range []cypherRange{
		{start: math.MinInt64, end: math.MaxInt64, step: math.MaxInt64},
		{start: math.MaxInt64, end: math.MinInt64, step: math.MinInt64},
		{start: math.MaxInt64, end: math.MaxInt64, step: 1},
		{start: math.MinInt64, end: math.MinInt64, step: -1},
		{start: 1, end: 10, step: 3},
		{start: 10, end: 1, step: -4},
	} {
		var count int64
		for range sequence.values() {
			count++
		}
		require.Equal(t, count, sequence.length(), "%+v", sequence)
	}
}

// A full-width range fails the limit before it is built.
func TestFullWidthRangeFailsTheLimit(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	for _, bounds := range [][2]int64{{math.MinInt64, math.MaxInt64}, {math.MaxInt64, math.MinInt64}} {
		step := int64(1)
		if bounds[0] > bounds[1] {
			step = -1
		}
		_, err := executor.Execute(context.Background(), "RETURN size(range($lo, $hi, $step)) AS n", map[string]interface{}{"lo": bounds[0], "hi": bounds[1], "step": step})
		requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		requirePoolsEmpty(t, "nornic")
	}
}

// collect() of a value every row shares counts the value once and a slot
// per reference: the list it builds keeps one payload (#1343).
func TestCollectCountsASharedValueOnce(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 32 << 20})
	big := make([]interface{}, 100000)
	for i := range big {
		big[i] = "element " + strconv.Itoa(i)
	}
	bigMap := make(map[string]interface{}, 20000)
	for i := 0; i < 20000; i++ {
		bigMap["key"+strconv.Itoa(i)] = "value " + strconv.Itoa(i)
	}
	require.Greater(t, estimateValueBytes(big), int64(1<<20))
	require.Greater(t, estimateValueBytes(bigMap), int64(1<<20))
	for _, query := range []string{
		"UNWIND range(1, 1000) AS i WITH collect($big) AS xs RETURN size(xs) AS n",
		"UNWIND range(1, 1000) AS i WITH collect($bigMap) AS xs RETURN size(xs) AS n",
		"UNWIND range(1, 1000) AS i RETURN size(collect($big)) AS n",
	} {
		result, err := executor.Execute(context.Background(), query, map[string]interface{}{"big": big, "bigMap": bigMap})
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{int64(1000)}}, result.Rows, query)
		requirePoolsEmpty(t, "nornic")
	}
	// Distinct values still count in full.
	_, err := executor.Execute(context.Background(), "UNWIND range(1, 1000) AS i WITH collect(range(i, i + 9999)) AS xs RETURN size(xs) AS n", nil)
	requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	requirePoolsEmpty(t, "nornic")
}

// What the statement's caches record about a list refers to that list
// alone: once it is collected the records refer to no list, so a new list
// allocated at its address, whatever elements the two share, is not sized
// from them (#1343).
func TestStatementSizeCachesDoNotTrustAReusedAddress(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	tracker := newTestMemoryTracker("reused")
	t.Cleanup(tracker.end)

	func() {
		dropped := make([]interface{}, 100)
		tracker.sizedValue(unsafe.Pointer(unsafe.SliceData(dropped)), len(dropped), dropped)
		tracker.noteList(dropped, estimateValueBytes(dropped))
	}()
	require.Len(t, tracker.sizes, 1)
	runtime.GC()
	runtime.GC()
	for key, size := range tracker.sizes {
		require.Nil(t, size.list.array.Value(), "the size recorded at %#x still refers to a list", key.address)
	}
	recorded := 0
	for _, known := range tracker.known {
		if known.key.length > 0 {
			recorded++
			require.Nil(t, known.list.array.Value(), "the collected list recorded at %#x still refers to a list", known.key.address)
		}
	}
	require.Equal(t, 1, recorded)

	// A live list is still sized from its records, and another list of its
	// length and elements is not.
	live := make([]interface{}, 100)
	for i := range live {
		live[i] = "element " + strconv.Itoa(i)
	}
	data := unsafe.Pointer(unsafe.SliceData(live))
	tracker.noteList(live, 12345)
	require.Equal(t, int64(12345), tracker.sizedValue(data, len(live), live))
	other := append([]interface{}(nil), live...)
	require.Equal(t, estimateValueBytes(other), tracker.sizedValue(unsafe.Pointer(unsafe.SliceData(other)), len(other), other))
	runtime.KeepAlive(live)
}
