package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The memory estimate of a statement that keeps rows (#1343) is discarded when
// no limit is configured: nothing is tracked. Neither the aggregate operator
// nor any other operator may walk a row's imported lists and maps for such an
// estimate, because a correlated CALL runs its body once per outer row and
// would walk the same imported values again in every run.

// estimatorOffCorrelatedAggregateQuery aggregates inside a CALL body, so the
// body's aggregate operator runs once per outer row. The unused import travels
// in every row of the body without the aggregate reading it.
const estimatorOffCorrelatedAggregateQuery = `
UNWIND $ids AS id
MATCH (c:EstimatorCase {id: id})
CALL {
	WITH c
	RETURN count(c) AS hits
}
RETURN c.id AS id, hits
ORDER BY id`

// estimatorOffControlQuery is the same correlated body without an aggregate:
// it has no aggregate operator to estimate the imported rows for.
const estimatorOffControlQuery = `
UNWIND $ids AS id
MATCH (c:EstimatorCase {id: id})
CALL {
	WITH c
	WITH c AS carried RETURN carried.id AS caseId
}
RETURN caseId
ORDER BY caseId`

// estimatorOffUnusedImport is the parameter the queries above never read: one
// large list and one large map, as an application imports into a correlated
// body it aggregates over.
func estimatorOffUnusedImport(size int) map[string]interface{} {
	list := make([]interface{}, size)
	for index := range list {
		list[index] = map[string]interface{}{
			"id":    fmt.Sprintf("imported-%06d", index),
			"text":  "imported text the aggregate never reads",
			"link":  "https://example.invalid/imported/" + fmt.Sprint(index),
			"depth": index,
		}
	}
	mapping := make(map[string]interface{}, size)
	for index := 0; index < size; index++ {
		mapping[fmt.Sprintf("key-%06d", index)] = "imported value the aggregate never reads"
	}
	return map[string]interface{}{"docs": list, "byKey": mapping}
}

// estimatorOffSeed creates the outer rows and returns the ids the queries
// unwind.
func estimatorOffSeed(t testing.TB, exec *StorageExecutor, cases int) map[string]interface{} {
	t.Helper()
	ids := make([]interface{}, cases)
	rows := make([]interface{}, cases)
	for index := range ids {
		ids[index] = fmt.Sprintf("case-%05d", index)
		rows[index] = map[string]interface{}{"id": ids[index]}
	}
	_, err := exec.Execute(context.Background(),
		`UNWIND $rows AS row CREATE (:EstimatorCase {id: row.id})`,
		map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	return map[string]interface{}{"ids": ids}
}

// TestCorrelatedCallAggregateKeepsResultsWithoutBudget: the public statement
// with a large unused import returns what the same statement with a small one
// returns, without a configured memory limit.
func TestCorrelatedCallAggregateKeepsResultsWithoutBudget(t *testing.T) {
	resetTransactionMemory(t)
	require.False(t, transactionMemoryActive.Load(), "the test runs with no configured memory budget")
	exec := newTransactionMemoryExecutor(t)
	const cases = 64
	ids := estimatorOffSeed(t, exec, cases)

	small, err := exec.Execute(context.Background(), estimatorOffCorrelatedAggregateQuery,
		map[string]interface{}{"ids": ids["ids"], "unused": estimatorOffUnusedImport(8)})
	require.NoError(t, err)
	large, err := exec.Execute(context.Background(), estimatorOffCorrelatedAggregateQuery,
		map[string]interface{}{"ids": ids["ids"], "unused": estimatorOffUnusedImport(20000)})
	require.NoError(t, err)

	require.Equal(t, []string{"id", "hits"}, small.Columns)
	require.Len(t, small.Rows, cases)
	require.Equal(t, small.Columns, large.Columns)
	require.Equal(t, small.Rows, large.Rows, "the unused import must not change the result")
	for index, row := range large.Rows {
		require.Equal(t, fmt.Sprintf("case-%05d", index), row[0])
		require.Equal(t, int64(1), row[1], "one aggregate group per outer row")
	}
}

// TestAggregateGroupEstimateRunsOnlyWhileTracking: the group estimate the
// aggregate operator sizes from a row is the memory owner's charge only while
// the statement tracks memory. Enabled, the first group of a row carrying an
// imported list charges the whole row, and a limit below that charge fails the
// aggregation instead of discarding the estimate.
func TestAggregateGroupEstimateRunsOnlyWhileTracking(t *testing.T) {
	resetTransactionMemory(t)
	exec := newTransactionMemoryExecutor(t)
	projections := returnProjectionPlanFor(context.Background(), "RETURN count(*) AS total").projections
	list := make([]interface{}, 256)
	for index := range list {
		list[index] = int64(index)
	}
	rows := []pipelineRow{{"docs": list}, {"docs": list}}

	t.Run("configured budget charges the grouped row", func(t *testing.T) {
		resetTransactionMemory(t)
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 30})
		tracker := newTestMemoryTracker("nornic")
		t.Cleanup(tracker.end)
		tracker.limit = 1 << 30
		ctx := context.WithValue(context.Background(), memoryScopeKey{}, &tracker.root)

		groups, ok := exec.pipelineAggregateGroups(ctx, pipelineRowsSource(rows), projections, false)
		require.True(t, ok)
		require.Len(t, groups, 1)
		require.GreaterOrEqual(t, tracker.used.Load(), estimateValueBytes(list),
			"an enabled aggregate charges the imported list in the row it groups")
		require.NoError(t, tracker.err())
	})

	t.Run("limit below the grouped row fails", func(t *testing.T) {
		resetTransactionMemory(t)
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 10})
		tracker := newTestMemoryTracker("nornic")
		t.Cleanup(tracker.end)
		tracker.limit = 1 << 10
		ctx := context.WithValue(context.Background(), memoryScopeKey{}, &tracker.root)

		groups, ok := exec.pipelineAggregateGroups(ctx, pipelineRowsSource(rows), projections, false)
		require.False(t, ok, "the enabled aggregate charges the grouped row and goes over the limit")
		require.Nil(t, groups)
		requireMemoryError(t, tracker.err(), transactionMemoryLimitCode, transactionMemoryMaxSetting)
	})
}

// BenchmarkCorrelatedCallAggregateWithoutBudget measures the public statement
// with no configured memory limit, as an application that imports a large
// unused list and map into a correlated aggregate runs it. The aggregate-free
// statement is the control: its rows carry the same import without an
// aggregate operator estimating them.
func BenchmarkCorrelatedCallAggregateWithoutBudget(b *testing.B) {
	for _, size := range []int{8, 40000} {
		b.Run(fmt.Sprintf("unused-import-%d/aggregate", size), func(b *testing.B) {
			benchmarkEstimatorOffStatement(b, estimatorOffCorrelatedAggregateQuery, size)
		})
		b.Run(fmt.Sprintf("unused-import-%d/control", size), func(b *testing.B) {
			benchmarkEstimatorOffStatement(b, estimatorOffControlQuery, size)
		})
	}
}

func benchmarkEstimatorOffStatement(b *testing.B, query string, size int) {
	b.Helper()
	resetTransactionMemory(b)
	exec := newTransactionMemoryExecutor(b)
	ids := estimatorOffSeed(b, exec, 128)
	params := map[string]interface{}{"ids": ids["ids"], "unused": estimatorOffUnusedImport(size)}
	if _, err := exec.Execute(context.Background(), query, params); err != nil {
		b.Fatalf("warm-up execute failed: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := exec.Execute(context.Background(), query, params); err != nil {
			b.Fatalf("execute failed at iteration %d: %v", iteration, err)
		}
	}
}
