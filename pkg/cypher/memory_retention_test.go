package cypher

import (
	"context"
	"fmt"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Growth sites reserve before they allocate (#1343): each statement below
// built its large state without counting it. Under a 1 MiB limit each now
// fails with TransactionMemoryLimit while the heap stays far below what the
// statement would build; with no limit (and with tracking on under a limit
// it never reaches) each returns the same rows as before.

const retentionLimit = 1 << 20

// retentionPeakBound is the most the heap may grow while a statement fails
// at retentionLimit: the limit, the engine's per-statement working state,
// and GC slack, against the hundreds of MiB each statement builds unchecked.
const retentionPeakBound = 96 << 20

// peakHeapDuring is the most the live heap grew over its level before fn,
// sampled every 100 µs while fn runs.
func peakHeapDuring(fn func()) uint64 {
	const name = "/memory/classes/heap/objects:bytes"
	read := func() uint64 {
		sample := []metrics.Sample{{Name: name}}
		metrics.Read(sample)
		return sample[0].Value.Uint64()
	}
	runtime.GC()
	base := read()
	var peak atomic.Uint64
	peak.Store(base)
	done := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		for {
			if current := read(); current > peak.Load() {
				peak.Store(current)
			}
			select {
			case <-done:
				return
			case <-time.After(100 * time.Microsecond):
			}
		}
	}()
	fn()
	close(done)
	sampler.Wait()
	if peak.Load() < base {
		return 0
	}
	return peak.Load() - base
}

// requireFailsBeforeAllocating runs query under retentionLimit: it fails
// with TransactionMemoryLimit, the heap stays under retentionPeakBound, the
// executor keeps working and the pools are empty.
func requireFailsBeforeAllocating(t *testing.T, executor *StorageExecutor, query string) {
	t.Helper()
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: retentionLimit})
	var err error
	peak := peakHeapDuring(func() { _, err = executor.Execute(context.Background(), query, nil) })
	requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	require.Lessf(t, peak, uint64(retentionPeakBound), "the heap grew %d MiB before the limit stopped the statement", peak>>20)
	result, err := executor.Execute(context.Background(), "RETURN 1 AS x", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
	requirePoolsEmpty(t, "nornic")
}

// requireSameRowsTracked runs query with no limit and with tracking on
// under a limit it never reaches: the rows are the same.
func requireSameRowsTracked(t *testing.T, executor *StorageExecutor, query string) [][]interface{} {
	t.Helper()
	ConfigureTransactionMemory(0, TransactionMemoryLimits{})
	untracked, err := executor.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{TransactionMax: 1 << 40})
	tracked, err := executor.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, untracked.Columns, tracked.Columns)
	require.Equal(t, untracked.Rows, tracked.Rows)
	requirePoolsEmpty(t, "nornic")
	return untracked.Rows
}

func TestRetentionIndependentCallProduct(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	requireFailsBeforeAllocating(t, executor, `UNWIND range(1,1000) AS x
CALL () { UNWIND range(1,1000) AS y RETURN y }
RETURN count(*) AS n`)
	rows := requireSameRowsTracked(t, executor, `UNWIND range(1,200) AS x
CALL () { UNWIND range(1,200) AS y RETURN y }
RETURN count(*) AS n, sum(x * y) AS s`)
	require.Equal(t, int64(40000), rows[0][0])
}

func TestRetentionProcedureOutputRows(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	for i := 0; i < 200; i++ {
		_, err := executor.Execute(context.Background(), fmt.Sprintf("CREATE (:RetentionLabel%03d)", i), nil)
		require.NoError(t, err)
	}
	requireFailsBeforeAllocating(t, executor, "UNWIND range(1, 20000) AS x CALL db.labels() YIELD label RETURN count(*) AS n")
	rows := requireSameRowsTracked(t, executor, "UNWIND range(1, 50) AS x CALL db.labels() YIELD label RETURN count(*) AS n")
	require.Equal(t, int64(50*200), rows[0][0])
}

func TestRetentionPercentileInputs(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	for _, function := range []string{"percentileCont(x, 0.5)", "percentileDisc(x, 0.5)", "percentileCont(DISTINCT x, 0.5)"} {
		t.Run(function, func(t *testing.T) {
			requireFailsBeforeAllocating(t, executor, "UNWIND range(1,1000000) AS x RETURN "+function+" AS p")
			rows := requireSameRowsTracked(t, executor, "UNWIND range(1,10001) AS x RETURN "+function+" AS p")
			require.EqualValues(t, 5001, rows[0][0])
		})
	}
}

func TestRetentionExpressionLists(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	for name, statements := range map[string][2]string{
		"range in an expression": {
			"UNWIND coalesce(range(1,10000000),[]) AS x RETURN count(*) AS n",
			"UNWIND coalesce(range(1,100000),[]) AS x RETURN count(*) AS n",
		},
		"reduce string accumulator": {
			"RETURN size(reduce(s='x', i IN range(1,26) | s+s)) AS n",
			"RETURN size(reduce(s='x', i IN range(1,16) | s+s)) AS n",
		},
		"reduce list accumulator": {
			"RETURN size(reduce(acc=[], i IN range(1,200000) | acc + [{i: i, text: 'abcdefghijklmnopqrstuvwxyz'}])) AS n",
			"RETURN size(reduce(acc=[], i IN range(1,300) | acc + [{i: i, text: 'abcdefghijklmnopqrstuvwxyz'}])) AS n",
		},
		"comprehension": {
			"WITH range(1,40000) AS l RETURN size([x IN l | toString(x) + 'abcdefghijklmnopqrstuvwxyz0123456789']) AS n",
			"WITH range(1,400) AS l RETURN size([x IN l | toString(x) + 'abcdefghijklmnopqrstuvwxyz0123456789']) AS n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFailsBeforeAllocating(t, executor, statements[0])
			requireSameRowsTracked(t, executor, statements[1])
		})
	}
}

func TestRetentionUnionAndReturnRows(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	requireFailsBeforeAllocating(t, executor, `UNWIND range(1,1000000) AS x RETURN x
UNION ALL
UNWIND range(1000001,2000000) AS x RETURN x`)
	requireFailsBeforeAllocating(t, executor, `UNWIND range(1,1000000) AS x RETURN x
UNION
UNWIND range(500001,1500000) AS x RETURN x`)
	requireFailsBeforeAllocating(t, executor, "UNWIND range(1,2000000) AS x RETURN x")
	rows := requireSameRowsTracked(t, executor, `UNWIND range(1,1000) AS x RETURN x
UNION
UNWIND range(501,1500) AS x RETURN x`)
	require.Len(t, rows, 1500)
}

func TestRetentionWithAndOptionalMatchOutputs(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	_, err := executor.Execute(context.Background(), `CREATE (h:RetentionHub {id: 'h'})
WITH h UNWIND range(1, 30000) AS i
CREATE (h)-[:R]->(:RetentionLeaf {i: i, text: 'leaf text ' + toString(i)})`, nil)
	require.NoError(t, err)
	requireFailsBeforeAllocating(t, executor, `UNWIND range(1,1000) AS x
WITH x ORDER BY x
WITH x, range(x,x+9999) AS xs ORDER BY x
RETURN count(*) AS n`)
	requireFailsBeforeAllocating(t, executor, `MATCH (h:RetentionHub)
OPTIONAL MATCH (h)-[:R]->(leaf)
WITH h, leaf, leaf.text AS text ORDER BY text
RETURN count(*) AS n`)
	rows := requireSameRowsTracked(t, executor, `UNWIND range(1,100) AS x
WITH x ORDER BY x
WITH x, range(x,x+99) AS xs ORDER BY x
RETURN count(*) AS n, sum(size(xs)) AS s`)
	require.Equal(t, []interface{}{int64(100), int64(10000)}, rows[0])
	rows = requireSameRowsTracked(t, executor, `MATCH (h:RetentionHub)
OPTIONAL MATCH (h)-[:R]->(leaf) WHERE leaf.i <= 100
RETURN count(leaf) AS n`)
	require.Equal(t, int64(100), rows[0][0])
}

func TestRetentionShortestPathSearch(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	// a's component is a 40,000-node chain; b is in another component, so
	// the search visits a's whole component and finds no path.
	_, err := executor.Execute(context.Background(), `CREATE (:RetentionEnd {id: 'a', i: 0}), (:RetentionEnd {id: 'b'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(context.Background(), `UNWIND range(1, 40000) AS i CREATE (:RetentionChain {i: i})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(context.Background(), `MATCH (a:RetentionEnd {id: 'a'}), (c:RetentionChain {i: 1}) CREATE (a)-[:R]->(c)`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(context.Background(), `UNWIND range(1, 39999) AS i
MATCH (x:RetentionChain {i: i}), (y:RetentionChain {i: i + 1})
CREATE (x)-[:R]->(y)`, nil)
	require.NoError(t, err)
	for _, selector := range []string{"shortestPath", "allShortestPaths"} {
		t.Run(selector, func(t *testing.T) {
			requireFailsBeforeAllocating(t, executor, `MATCH (a:RetentionEnd {id:'a'}), (b:RetentionEnd {id:'b'})
MATCH p=`+selector+`((a)-[:R*]-(b))
RETURN p`)
			rows := requireSameRowsTracked(t, executor, `MATCH (a:RetentionEnd {id:'a'}), (c:RetentionChain {i: 50})
MATCH p=`+selector+`((a)-[:R*]-(c))
RETURN length(p) AS n`)
			require.Equal(t, [][]interface{}{{int64(50)}}, rows)
		})
	}
}

func TestRetentionQuantifiedPathMatches(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	_, err := executor.Execute(context.Background(), `UNWIND range(0, 3000) AS i CREATE (:RetentionStep {i: i})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(context.Background(), `UNWIND range(0, 2999) AS i
MATCH (x:RetentionStep {i: i}), (y:RetentionStep {i: i + 1})
CREATE (x)-[:NEXT]->(y)`, nil)
	require.NoError(t, err)
	requireFailsBeforeAllocating(t, executor, `MATCH (a:RetentionStep {i: 0})
MATCH p = (a)(()-[:NEXT]->())*(b)
RETURN count(*) AS n`)
	rows := requireSameRowsTracked(t, executor, `MATCH (a:RetentionStep {i: 2900})
MATCH p = (a)(()-[:NEXT]->())*(b)
RETURN count(*) AS n, max(length(p)) AS longest`)
	require.Equal(t, []interface{}{int64(101), int64(100)}, rows[0])
}

// TestResultMemoryHeldUntilReleased: a caller that keeps a result
// (WithResultMemory) holds its reservation in the pools until it releases
// it; a caller that doesn't, holds nothing once the statement ends.
func TestResultMemoryHeldUntilReleased(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<30, TransactionMemoryLimits{})
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	query := "UNWIND range(1, 100000) AS x RETURN x, 'row ' + toString(x) AS text"

	memory := &ResultMemory{}
	result, err := executor.Execute(WithResultMemory(context.Background(), memory), query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 100000)
	global, database := TransactionMemoryPoolUsage("nornic")
	require.Positive(t, memory.Bytes())
	require.Equal(t, memory.Bytes(), global, "the held result is all the pool holds")
	require.Equal(t, memory.Bytes(), database)
	memory.Release()
	memory.Release()
	requirePoolsEmpty(t, "nornic")

	_, err = executor.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	requirePoolsEmpty(t, "nornic")
}
