package cypher

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Transaction memory limits (#1343).

// heavyCollectQuery collects 200,000 integers: ~3 MiB of estimated
// list memory.
const heavyCollectQuery = "UNWIND range(1, 200000) AS x RETURN size(collect(x)) AS n"

// topicGroupsQuery is a TOPIC_GROUPS-style aggregation (Personal Documents):
// collected documents, a CALL subquery collecting maps per document, a
// second one grouping them with collect(DISTINCT …), and ORDER BY.
const topicGroupsQuery = `MATCH (d:MemDoc)
WITH d ORDER BY d.id
WITH collect(d) AS documents
CALL (documents) {
  UNWIND documents AS document
  MATCH (document)-[:HAS]->(thing:MemThing)
  RETURN collect({document: document.id, thing: thing.id, text: thing.text}) AS incidence
}
CALL (incidence) {
  UNWIND incidence AS each
  WITH each.thing AS thing, collect(DISTINCT each.document) AS holders, collect(each.text) AS texts
  WITH thing, holders, texts ORDER BY size(holders) DESC, thing
  RETURN collect({thing: thing, holders: holders, texts: texts}) AS groups
}
RETURN size(groups) AS n`

// resetTransactionMemory restores unlimited memory after a test.
func resetTransactionMemory(t testing.TB) {
	t.Helper()
	t.Cleanup(func() {
		r := transactionMemory
		r.mu.Lock()
		r.databases = make(map[string]*databaseMemory)
		r.mu.Unlock()
		ConfigureTransactionMemory(0, TransactionMemoryLimits{})
	})
}

// newTestMemoryTracker is a pooled tracker for database, reset as
// beginTransactionMemory resets one for a statement: the limits configured
// now, and no failure. A tracker from the pool keeps the limits and failure
// of the statement it last served, which a test sizing through it directly
// would otherwise inherit.
func newTestMemoryTracker(database string) *transactionMemoryTracker {
	tracker := transactionMemoryTrackers.Get().(*transactionMemoryTracker)
	memory := transactionMemory.database(database)
	tracker.mu.Lock()
	tracker.database = memory
	tracker.limit = memory.transactionMax.Load()
	tracker.databaseLimit = memory.pool.limit.Load()
	tracker.globalLimit = transactionMemory.global.limit.Load()
	tracker.mu.Unlock()
	tracker.failure.Store(nil)
	return tracker
}

// newTransactionMemoryEngine is a store namespaced as the default database,
// as the server runs it.
func newTransactionMemoryEngine(t testing.TB) storage.Engine {
	t.Helper()
	return storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
}

// newTransactionMemoryExecutor has no result cache, so a statement that
// succeeded once runs again under a lower limit.
func newTransactionMemoryExecutor(t testing.TB) *StorageExecutor {
	t.Helper()
	return NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
}

func requireMemoryError(t testing.TB, err error, code, setting string) {
	t.Helper()
	require.Error(t, err)
	require.True(t, nornicerrors.HasNeo4jStatus(err), "%v", err)
	actual, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, code, actual, message)
	require.True(t, strings.HasPrefix(message, "The allocation of an extra "), message)
	require.Contains(t, message, " would use more than the limit ")
	require.Contains(t, message, ". Currently using ")
	require.True(t, strings.HasSuffix(message, ". "+setting+" threshold reached"), message)
	var detailed interface{ BoltErrorDetail() string }
	require.ErrorAs(t, err, &detailed)
	require.Equal(t, "50N42", detailed.BoltErrorDetail())
}

func requirePoolsEmpty(t testing.TB, database string) {
	t.Helper()
	global, databaseUsed := TransactionMemoryPoolUsage(database)
	require.Zero(t, global, "global pool still holds memory")
	require.Zero(t, databaseUsed, "database pool still holds memory")
}

func seedTopicGroupsGraph(t testing.TB, executor *StorageExecutor, documents, thingsPerDocument int) {
	t.Helper()
	_, err := executor.Execute(context.Background(), `UNWIND range(1, $documents) AS d
CREATE (doc:MemDoc {id: 'doc-' + toString(d)})
WITH doc, d
UNWIND range(1, $things) AS k
MERGE (thing:MemThing {id: 'thing-' + toString((d * 7 + k) % 300)})
ON CREATE SET thing.text = 'text of thing ' + toString(k) + ' ' + reduce(s = '', i IN range(1, 20) | s + 'lorem ')
CREATE (doc)-[:HAS]->(thing)`, map[string]interface{}{"documents": int64(documents), "things": int64(thingsPerDocument)})
	require.NoError(t, err)
}

func TestFormatMemoryBytesMatchesNeo4j(t *testing.T) {
	// Neo4j 5.26 on neo-ref: "The allocation of an extra 197.7 MiB would use
	// more than the limit 1.0 GiB. Currently using 922.8 MiB."
	require.Equal(t, "0 B", formatMemoryBytes(0))
	require.Equal(t, "512 B", formatMemoryBytes(512))
	require.Equal(t, "1.0 KiB", formatMemoryBytes(1024))
	require.Equal(t, "197.7 MiB", formatMemoryBytes(207306752))
	require.Equal(t, "922.8 MiB", formatMemoryBytes(967622656))
	require.Equal(t, "1.0 GiB", formatMemoryBytes(1<<30))
	require.Equal(t, "1.5 TiB", formatMemoryBytes(3<<39))
}

func TestTransactionMemoryNoLimitTracksNothing(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{})
	require.False(t, transactionMemoryActive.Load())
	executor := newTransactionMemoryExecutor(t)
	result, err := executor.Execute(context.Background(), heavyCollectQuery, nil)
	require.NoError(t, err)
	require.Equal(t, int64(200000), result.Rows[0][0])
}

func TestTransactionMemoryLimitFailsHeavyStatement(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	seedTopicGroupsGraph(t, executor, 120, 40)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	for name, query := range map[string]string{
		"collect":      heavyCollectQuery,
		"topic groups": topicGroupsQuery,
		"expansion":    "MATCH (d:MemDoc)-[:HAS]->(t:MemThing)<-[:HAS]-(other:MemDoc) RETURN d.id, other.id, t.text",
		"order by":     "UNWIND range(1, 200000) AS x WITH x ORDER BY x DESC RETURN x LIMIT 1",
		"distinct":     "UNWIND range(1, 200000) AS x WITH DISTINCT x % 100000 AS y RETURN count(y) AS n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := executor.Execute(context.Background(), query, nil)
			requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
			requirePoolsEmpty(t, "nornic")
			// The executor keeps working: a small statement succeeds.
			result, err := executor.Execute(context.Background(), "RETURN 1 AS x", nil)
			require.NoError(t, err)
			require.Equal(t, int64(1), result.Rows[0][0])
		})
	}
	// Under the limit the same statements succeed.
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 30})
	result, err := executor.Execute(context.Background(), topicGroupsQuery, nil)
	require.NoError(t, err)
	require.Positive(t, result.Rows[0][0])
	requirePoolsEmpty(t, "nornic")
}

func TestTransactionMemoryPoolFailsWhileSmallStatementsSucceed(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ConfigureTransactionMemory(16<<20, TransactionMemoryLimits{})
	// Other transactions hold all but 2 MiB of the global pool.
	ok, _ := transactionMemory.global.reserve(14<<20, 16<<20)
	require.True(t, ok)
	_, err := executor.Execute(context.Background(), heavyCollectQuery, nil)
	requireMemoryError(t, err, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", "dbms.memory.transaction.total.max")
	require.Contains(t, err.Error(), "would use more than the limit 16.0 MiB")
	result, err := executor.Execute(context.Background(), "UNWIND range(1, 100) AS x RETURN collect(x) AS xs", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows[0][0], 100)
	// The failed and the small statements gave back what they reserved.
	global, _ := TransactionMemoryPoolUsage("nornic")
	require.Equal(t, int64(14<<20), global)
	transactionMemory.global.release(14 << 20)
	result, err = executor.Execute(context.Background(), heavyCollectQuery, nil)
	require.NoError(t, err)
	require.Equal(t, int64(200000), result.Rows[0][0])
	requirePoolsEmpty(t, "nornic")

	// The database pool, db.memory.transaction.total.max, fails the same way.
	SetDatabaseTransactionMemoryLimits("nornic", TransactionMemoryLimits{DatabaseTotalMax: 1 << 20})
	_, err = executor.Execute(context.Background(), heavyCollectQuery, nil)
	requireMemoryError(t, err, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", "db.memory.transaction.total.max")
	requirePoolsEmpty(t, "nornic")
}

// TestTransactionMemoryConcurrentHeavyAggregationsUnderPool runs six
// TOPIC_GROUPS-style aggregations at once under a pool that holds about
// two of them, with small statements alongside: each heavy statement
// either returns its full result or fails with MemoryPoolOutOfMemoryError,
// every small statement succeeds, and the pool is empty afterwards.
func TestTransactionMemoryConcurrentHeavyAggregationsUnderPool(t *testing.T) {
	resetTransactionMemory(t)
	engine := newTransactionMemoryEngine(t)
	seed := NewStorageExecutorWithQueryCachePolicy(engine, 0, 0)
	seedTopicGroupsGraph(t, seed, 120, 40)

	// Measure one statement's peak under a generous limit.
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	expected, err := seed.Execute(context.Background(), topicGroupsQuery, nil)
	require.NoError(t, err)
	peak := measureStatementPeak(t, seed, topicGroupsQuery)
	require.Positive(t, peak)
	ConfigureTransactionMemory(peak*5/2, TransactionMemoryLimits{})

	const heavy = 6
	succeeded, failed := 0, 0
	for round := 0; round < 20 && (succeeded == 0 || failed == 0); round++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		results := make([]error, heavy)
		values := make([]interface{}, heavy)
		smallErrors := make([]error, heavy)
		for worker := 0; worker < heavy; worker++ {
			wg.Add(2)
			go func(worker int) {
				defer wg.Done()
				<-start
				result, err := NewStorageExecutor(engine).Execute(context.Background(), topicGroupsQuery+fmt.Sprintf(", %d AS tag", round*heavy+worker), nil)
				results[worker] = err
				if err == nil {
					values[worker] = result.Rows[0][0]
				}
			}(worker)
			go func(worker int) {
				defer wg.Done()
				<-start
				_, smallErrors[worker] = NewStorageExecutor(engine).Execute(context.Background(), "MATCH (d:MemDoc {id: 'doc-1'}) RETURN d.id", nil)
			}(worker)
		}
		close(start)
		wg.Wait()
		for worker := 0; worker < heavy; worker++ {
			require.NoError(t, smallErrors[worker], "a small statement failed")
			if results[worker] == nil {
				succeeded++
				require.Equal(t, expected.Rows[0][0], values[worker])
				continue
			}
			failed++
			requireMemoryError(t, results[worker], "Neo.TransientError.General.MemoryPoolOutOfMemoryError", "dbms.memory.transaction.total.max")
		}
		requirePoolsEmpty(t, "nornic")
	}
	require.Positive(t, succeeded, "no heavy statement completed under the pool")
	require.Positive(t, failed, "the pool never refused a heavy statement")
}

// measureStatementPeak is the highest estimate a statement reached: the
// pool's reservations while it runs alone, read through the tracker.
func measureStatementPeak(t testing.TB, executor *StorageExecutor, query string) int64 {
	t.Helper()
	var peak int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := executor.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}()
	for {
		select {
		case <-done:
			if peak == 0 {
				// Too fast to sample: the statement's memory is at least
				// what one collect of its rows needs.
				peak = 4 << 20
			}
			return peak
		default:
			if used := transactionMemory.global.used.Load(); used > peak {
				peak = used
			}
		}
	}
}

func TestTransactionMemoryReleasedAfterCommitRollbackAndFailure(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(256<<20, TransactionMemoryLimits{TransactionMax: 2 << 20})
	ctx := context.Background()
	executor := newTransactionMemoryExecutor(t)
	underLimit := "UNWIND range(1, 20000) AS x RETURN size(collect(x)) AS n"

	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		_, err := executor.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = executor.Execute(ctx, "CREATE (:MemTx {end: $end})", map[string]interface{}{"end": end})
		require.NoError(t, err)
		_, err = executor.Execute(ctx, underLimit, nil)
		require.NoError(t, err)
		// A statement's memory goes back when it ends.
		requirePoolsEmpty(t, "nornic")
		_, err = executor.Execute(ctx, end, nil)
		require.NoError(t, err)
		requirePoolsEmpty(t, "nornic")
	}
	result, err := executor.Execute(ctx, "MATCH (n:MemTx) RETURN n.end AS end", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"COMMIT"}}, result.Rows)

	// A statement over the limit fails its transaction: later statements
	// are refused, and its writes are rolled back.
	_, err = executor.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "CREATE (:MemTx {end: 'failed'})", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, heavyCollectQuery, nil)
	requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	requirePoolsEmpty(t, "nornic")
	_, err = executor.Execute(ctx, "RETURN 1", nil)
	require.Error(t, err)
	_, _ = executor.Execute(ctx, "ROLLBACK", nil)
	requirePoolsEmpty(t, "nornic")
	result, err = executor.Execute(ctx, "MATCH (n:MemTx) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])

	// Auto-commit statements leave nothing behind either.
	for i := 0; i < 3; i++ {
		_, err = executor.Execute(ctx, heavyCollectQuery, nil)
		require.Error(t, err)
		_, err = executor.Execute(ctx, underLimit, nil)
		require.NoError(t, err)
	}
	requirePoolsEmpty(t, "nornic")
}

func TestTransactionMemoryLimitsAreDynamic(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	SetDatabaseTransactionMemoryLimits("nornic", TransactionMemoryLimits{TransactionMax: 1 << 20})
	_, err := executor.Execute(context.Background(), heavyCollectQuery, nil)
	requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	// Process defaults don't override a database's own limit.
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 30})
	_, err = executor.Execute(context.Background(), heavyCollectQuery, nil)
	require.Error(t, err)
	SetDatabaseTransactionMemoryLimits("nornic", TransactionMemoryLimits{})
	result, err := executor.Execute(context.Background(), heavyCollectQuery, nil)
	require.NoError(t, err)
	require.Equal(t, int64(200000), result.Rows[0][0])
}

func TestShowTransactionsEstimatedUsedHeapMemory(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	executor := newTransactionMemoryExecutor(t)
	// The registry is shared: other tests' open transactions are listed too,
	// so the statement finds its own row by its text.
	const query = "SHOW TRANSACTIONS YIELD currentQuery, estimatedUsedHeapMemory RETURN currentQuery AS q, estimatedUsedHeapMemory AS ownHeapMemory"
	result, err := executor.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	var own [][]interface{}
	for _, row := range result.Rows {
		if text, ok := row[0].(string); ok && strings.Contains(text, "ownHeapMemory") {
			own = append(own, row)
		}
	}
	require.Len(t, own, 1, "rows: %v", result.Rows)
	require.IsType(t, int64(0), own[0][1])
}

func TestEstimateValueBytes(t *testing.T) {
	require.Equal(t, int64(valueSlotBytes), estimateValueBytes(int64(1)))
	require.Equal(t, int64(valueSlotBytes+stringHeaderBytes+5), estimateValueBytes("hello"))
	list := []interface{}{int64(1), "ab"}
	require.Equal(t, int64(valueSlotBytes+sliceHeaderBytes)+int64(cap(list)-len(list))*valueSlotBytes+valueSlotBytes+valueSlotBytes+stringHeaderBytes+2, estimateValueBytes(list))
	nested := map[string]interface{}{"k": list}
	require.Greater(t, estimateValueBytes(nested), estimateValueBytes(list))
	deep := interface{}(int64(0))
	for i := 0; i < 100; i++ {
		deep = []interface{}{deep}
	}
	require.Positive(t, estimateValueBytes(deep))
}

// transactionMemoryBenchRounds is how many interleaved off/on rounds
// BenchmarkTransactionMemoryTracking runs (-args -txmem.rounds=10).
var transactionMemoryBenchRounds = flag.Int("txmem.rounds", 1, "interleaved off/on rounds of BenchmarkTransactionMemoryTracking")

// BenchmarkTransactionMemoryTracking is the cost of tracking (#1343): the
// same aggregations with no limit configured (nothing tracked) and with a
// limit no statement reaches (everything tracked).
func BenchmarkTransactionMemoryTracking(b *testing.B) {
	resetTransactionMemory(b)
	executor := newTransactionMemoryExecutor(b)
	seedTopicGroupsGraph(b, executor, 60, 20)
	queries := map[string]string{
		"collect":      "UNWIND range(1, 10000) AS x RETURN size(collect(x)) AS n",
		"group":        "UNWIND range(1, 10000) AS x WITH x % 100 AS k, collect(x) AS xs RETURN k, size(xs) ORDER BY k",
		"topic_groups": topicGroupsQuery,
		"point":        "MATCH (d:MemDoc {id: 'doc-1'}) RETURN d.id",
	}
	// Off and on run next to each other, query by query, the order
	// alternating per round, so drift on a shared machine falls on both
	// (go test -count repeats one sub-benchmark back to back instead).
	limits := map[string]int64{"off": 0, "on": 1 << 40}
	for round := 0; round < *transactionMemoryBenchRounds; round++ {
		modes := []string{"off", "on"}
		if round%2 == 1 {
			modes = []string{"on", "off"}
		}
		for _, name := range []string{"collect", "group", "topic_groups", "point"} {
			query := queries[name]
			for _, mode := range modes {
				limit := limits[mode]
				b.Run(mode+"/"+name, func(b *testing.B) {
					ConfigureTransactionMemory(limit, TransactionMemoryLimits{})
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, err := executor.Execute(context.Background(), query, nil); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

// TestMemoryAccountKeepsTheTransactionLimitExact: an account takes credit
// ahead, but never past the per-transaction limit, so the allocation that
// would cross the limit is the one that fails, with the bytes in use being
// exactly what was spent. Closing returns the unspent credit.
func TestMemoryAccountKeepsTheTransactionLimitExact(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1000})
	tracker := &transactionMemoryTracker{database: transactionMemory.database("exact"), limit: 1000}
	tracker.root.tracker = tracker
	t.Cleanup(tracker.end)

	account := memoryAccount{scope: &tracker.root}
	for i := 0; i < 100; i++ {
		require.NoError(t, account.grow(10), "allocation %d", i)
		require.LessOrEqual(t, tracker.used.Load(), int64(1000))
	}
	require.Equal(t, int64(1000), tracker.used.Load())
	err := account.grow(10)
	requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
	require.Contains(t, err.Error(), "The allocation of an extra 10 B would use more than the limit 1000 B. Currently using 1000 B.")

	// Credit taken ahead goes back on close; a nested execution gives back
	// what it took since its mark.
	other := &transactionMemoryTracker{database: transactionMemory.database("exact")}
	other.root.tracker = other
	t.Cleanup(other.end)
	mark := other.root.mark()
	spender := memoryAccount{scope: &other.root}
	for i := 0; i < 1000; i++ {
		require.NoError(t, spender.grow(100))
	}
	require.Greater(t, other.used.Load(), int64(100000), "credit was taken ahead")
	spender.close()
	require.Equal(t, int64(100000), other.used.Load())
	other.root.releaseSince(mark)
	require.Zero(t, other.used.Load())
}

// TestRowEstimatorCountsEveryRow: rows are sized in full, every one. A
// list built per row counts in each row; a list rows share counts once.
func TestRowEstimatorCountsEveryRow(t *testing.T) {
	var estimate rowEstimator
	small := pipelineRow{"i": int64(1), "s": "x"}
	smallBytes := estimate.rowBytes(small)
	big := strings.Repeat("y", 100000)
	require.Equal(t, smallBytes+int64(len(big))-1, estimate.rowBytes(pipelineRow{"i": int64(2), "s": big}),
		"a long string late in the stream counts in full")

	shared := make([]interface{}, 1000)
	for i := range shared {
		shared[i] = int64(i)
	}
	first := estimate.rowBytes(pipelineRow{"l": shared})
	again := estimate.rowBytes(pipelineRow{"l": shared})
	require.Greater(t, first, int64(1000*valueSlotBytes))
	require.Less(t, again, int64(200), "a shared list counts once")
	fresh := make([]interface{}, len(shared))
	copy(fresh, shared)
	require.Equal(t, first, estimate.rowBytes(pipelineRow{"l": fresh}), "a list built per row counts in full")
}

// TestTransactionMemoryLimitCatchesRowsThatGrowLate: a stream of small
// rows followed by huge ones (long strings, lists built per row) still
// fails with TransactionMemoryLimit, and the bytes in use when it fails are
// within the limit: nothing is sampled, so nothing is undercounted.
func TestTransactionMemoryLimitCatchesRowsThatGrowLate(t *testing.T) {
	resetTransactionMemory(t)
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	const limit = 4 << 20
	for name, query := range map[string]string{
		// 1000 one-character rows, then 1000 rows of distinct
		// 10,000-character strings (~10 MB), kept for ORDER BY.
		"long strings": `WITH reduce(s = '', k IN range(1, 1000) | s + 'xxxxxxxxxx') AS long
UNWIND range(1, 2000) AS i
WITH i, CASE WHEN i > 1000 THEN long + toString(i) ELSE 'x' END AS s
WITH i, s ORDER BY i DESC
RETURN count(*) AS c, sum(size(s)) AS total`,
		// 1000 rows of an empty list, then 1000 rows of a 2,000-element list
		// built per row (~32 MB).
		"lists built per row": `UNWIND range(1, 2000) AS i
WITH i, CASE WHEN i > 1000 THEN range(1, 2000) ELSE [] END AS l
WITH i, l ORDER BY i DESC
RETURN count(*) AS c, sum(size(l)) AS total`,
	} {
		t.Run(name, func(t *testing.T) {
			ConfigureTransactionMemory(0, TransactionMemoryLimits{})
			result, err := executor.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, int64(2000), result.Rows[0][0])

			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: limit})
			_, err = executor.Execute(context.Background(), query, nil)
			requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
			require.Contains(t, err.Error(), "would use more than the limit 4.0 MiB")
			requirePoolsEmpty(t, "nornic")
		})
	}
}

// TestTransactionMemoryCreditReturnedOnFailure: operators that fail mid-way
// (over the limit) and operators that finish leave nothing in the pools.
func TestTransactionMemoryCreditReturnedOnFailure(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(512<<20, TransactionMemoryLimits{TransactionMax: 2 << 20})
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	seedTopicGroupsGraph(t, executor, 40, 10)
	for i := 0; i < 5; i++ {
		_, err := executor.Execute(context.Background(), heavyCollectQuery, nil)
		require.Error(t, err)
		requirePoolsEmpty(t, "nornic")
		_, err = executor.Execute(context.Background(), topicGroupsQuery, nil)
		require.NoError(t, err)
		requirePoolsEmpty(t, "nornic")
		_, err = executor.Execute(context.Background(), "UNWIND range(1, 5000) AS x WITH x % 50 AS k, collect(x) AS xs RETURN k, size(xs) ORDER BY k", nil)
		require.NoError(t, err)
		requirePoolsEmpty(t, "nornic")
	}
}

// TestKnownListsSizeCollectedListsOnce: a list collect() counted is sized
// from what it counted where rows keep it; the same values in a list built
// any other way are walked in full.
func TestKnownListsSizeCollectedListsOnce(t *testing.T) {
	resetTransactionMemory(t)
	tracker := newTestMemoryTracker("known")
	t.Cleanup(tracker.end)
	state := pipelineAggregateState{name: "collect", expression: "x", memory: retentionIn(&tracker.root)}
	for i := 0; i < 100; i++ {
		require.True(t, state.add(context.Background(), nil, pipelineRow{"x": strings.Repeat("z", 50)}))
	}
	collected, ok := state.result(context.Background(), nil)
	require.True(t, ok)
	list := collected.([]interface{})
	counted, known := tracker.knownListBytes(list)
	require.True(t, known)
	require.GreaterOrEqual(t, counted, estimateValueBytes(list)-int64(cap(list)-len(list))*valueSlotBytes)
	require.Equal(t, int64(pipelineRowExtraBytes+sliceHeaderBytes)+counted, estimateResultRowBytes([]interface{}{list}, tracker))

	copied := make([]interface{}, len(list))
	copy(copied, list)
	_, known = tracker.knownListBytes(copied)
	require.False(t, known, "a list built otherwise is walked")
	require.Equal(t, int64(pipelineRowExtraBytes+sliceHeaderBytes)+estimateValueBytes(copied), estimateResultRowBytes([]interface{}{copied}, tracker))
}
