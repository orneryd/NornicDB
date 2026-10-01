package cypher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRangeValidationDoesNotMaterializeValues(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := executor.validateRangeCalls("range(1, 200000)", pipelineRow{})
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(256*1024))
}

func TestUnwindRangeFiltersBeforeMaterializingRows(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	result, err := executor.Execute(context.Background(), "UNWIND range(1, 30000) AS x WITH x WHERE x < 0 RETURN count(*) AS c", nil)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4*1024*1024))
}

func TestUnwindRangeAggregationDoesNotRetainInputRows(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, test := range []struct {
			query string
			want  int64
		}{
			{"UNWIND range(1, 30000) AS x RETURN count(*) AS c", 30000},
			{"UNWIND range(1, 30000) AS x RETURN sum(x) AS s", 450015000},
			{"UNWIND range(1, 30000) AS x WITH x RETURN count(*) AS c", 30000},
			{"UNWIND range(1, 30000) AS x WITH sum(x) AS s RETURN s", 450015000},
		} {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, test.query), func(t *testing.T) {
				executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "stream_aggregate"), 0, 0)
				if explicit {
					_, err := executor.handleBegin()
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = executor.handleRollback() })
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				var result *ExecuteResult
				var err error
				if explicit {
					result, err = executor.executeInTransaction(context.Background(), test.query, test.query)
				} else {
					result, err = executor.Execute(context.Background(), test.query, nil)
				}
				runtime.ReadMemStats(&after)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{test.want}}, result.Rows)
				require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4*1024*1024), "aggregate state must not retain the UNWIND input")
			})
		}
	}
}

func TestUnwindRangeExplicitTransactionFiltersBeforeMaterializingRows(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_explicit"))
	_, err := executor.handleBegin()
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = executor.handleRollback() })
	query := "UNWIND range(1, 30000) AS x WITH x WHERE x < 0 RETURN count(*) AS c"
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	result, err := executor.executeInTransaction(context.Background(), query, query)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4*1024*1024))
}

func TestUnwindStreamingAggregateSemantics(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, test := range []struct {
			query string
			rows  [][]interface{}
		}{
			{"UNWIND [1, null, 2, 2] AS x RETURN count(x), count(*), sum(DISTINCT x), avg(x), min(x), max(x), collect(DISTINCT x)", [][]interface{}{{int64(3), int64(4), int64(3), float64(5) / 3, int64(1), int64(2), []interface{}{int64(1), int64(2)}}}},
			{"UNWIND [] AS x RETURN count(*), sum(x), avg(x), min(x), max(x), collect(x), stDev(x), stDevP(x)", [][]interface{}{{int64(0), int64(0), nil, nil, nil, []interface{}{}, nil, nil}}},
			{"UNWIND range(1, 4) AS x RETURN x % 2 AS parity, sum(x) AS total ORDER BY parity", [][]interface{}{{int64(0), int64(6)}, {int64(1), int64(4)}}},
			{"UNWIND range(1, 4) AS x WITH x % 2 AS parity, sum(x) AS total WHERE total > 4 RETURN parity, total", [][]interface{}{{int64(0), int64(6)}}},
			{"UNWIND range(1, 4) AS x WITH x % 2 AS parity, sum(x) AS total ORDER BY total DESC LIMIT 1 RETURN parity, total", [][]interface{}{{int64(0), int64(6)}}},
			{"UNWIND [1, 2, 3] AS x RETURN {total: sum(x), counts: [count(*), count(DISTINCT x)]} AS stats, sum(x) + count(*) AS combined", [][]interface{}{{map[string]interface{}{"total": int64(6), "counts": []interface{}{int64(3), int64(3)}}, int64(9)}}},
			{"UNWIND [1, 2.5, null] AS x RETURN sum(x), avg(x)", [][]interface{}{{3.5, 1.75}}},
			{"UNWIND [1, 2, 3] AS x RETURN stDev(x), stDevP(x), percentileCont(x, 0.5), percentileDisc(x, 0.5)", [][]interface{}{{float64(1), math.Sqrt(float64(2) / 3), int64(2), int64(2)}}},
			{"UNWIND range(1, 2) AS x RETURN percentileCont(x, x)", [][]interface{}{{int64(2)}}},
		} {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, test.query), func(t *testing.T) {
				executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate_semantics"), 0, 0)
				var result *ExecuteResult
				var err error
				if explicit {
					_, err = executor.handleBegin()
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = executor.handleRollback() })
					result, err = executor.executeInTransaction(context.Background(), test.query, test.query)
				} else {
					result, err = executor.Execute(context.Background(), test.query, nil)
				}
				require.NoError(t, err)
				require.Equal(t, test.rows, result.Rows)
			})
		}
	}
}

func TestUnwindStreamingAggregateArgumentErrors(t *testing.T) {
	for _, projection := range []string{
		"collect(size(x))", "collect(labels(x))", "collect(toInteger([x]))",
		"collect(range(1, 2, x - 2))", "collect([1][x / 2.0])",
		"percentileCont(x, x + 1)",
	} {
		for _, clause := range []string{"RETURN " + projection, "WITH " + projection + " AS result RETURN result"} {
			t.Run(clause, func(t *testing.T) {
				executor := NewStorageExecutorWithQueryCachePolicy(newTestMemoryEngine(t), 0, 0)
				result, err := executor.Execute(context.Background(), "UNWIND range(1, 2) AS x "+clause, nil)
				require.Error(t, err)
				require.Nil(t, result)
			})
		}
	}
}

func TestRangeRequiresIntegerArgumentsAndNonzeroStep(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_argument_semantics"))
	for _, query := range []string{
		"RETURN range(0.0, 1, 1)",
		"RETURN range(0, true, 1)",
		"RETURN range(0, 1, '1')",
	} {
		_, err := executor.Execute(context.Background(), query, nil)
		require.Error(t, err)
		var semanticError *SemanticError
		require.True(t, errors.As(err, &semanticError))
		require.Equal(t, "InvalidArgumentType", semanticError.Detail)
	}

	_, err := executor.Execute(context.Background(), "RETURN range(0, 1, 0)", nil)
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "NumberOutOfRange", semanticError.Detail)
}

func TestUnwindRangeSharedPipelineSemantics(t *testing.T) {
	for _, test := range []struct {
		name, query string
		params      map[string]interface{}
		rows        [][]interface{}
	}{
		{"descending", "UNWIND range(5, 1, -2) AS x WITH x WHERE x > 1 RETURN collect(x) AS xs", nil, [][]interface{}{{[]interface{}{int64(5), int64(3)}}}},
		{"empty direction", "UNWIND range(1, 5, -1) AS x WITH x RETURN count(*) AS c", nil, [][]interface{}{{int64(0)}}},
		{"parameters", "UNWIND range($start, $end) AS x WITH x AS y WHERE y > $start RETURN y ORDER BY y", map[string]interface{}{"start": int64(2), "end": int64(4)}, [][]interface{}{{int64(3)}, {int64(4)}}},
		{"multiple projections", "UNWIND range(1, 4) AS x WITH x + 1 AS y WHERE y > 2 WITH y * 2 AS z WHERE z < 9 RETURN collect(z) AS zs", nil, [][]interface{}{{[]interface{}{int64(6), int64(8)}}}},
		{"wildcard", "UNWIND range(1, 2) AS x WITH * WHERE x > 1 RETURN *", nil, [][]interface{}{{int64(2)}}},
		{"empty wildcard", "UNWIND range(1, 2) AS x WITH * WHERE x < 0 RETURN *", nil, [][]interface{}{}},
		{"window before filter", "UNWIND range(1, 4) AS x WITH x LIMIT 2 WHERE x > 2 RETURN count(*) AS c", nil, [][]interface{}{{int64(0)}}},
		{"ordered window", "UNWIND range(1, 4) AS x WITH x ORDER BY x DESC LIMIT 2 RETURN collect(x) AS xs", nil, [][]interface{}{{[]interface{}{int64(4), int64(3)}}}},
		{"distinct", "UNWIND range(1, 4) AS x WITH x % 2 AS y WITH DISTINCT y RETURN y ORDER BY y", nil, [][]interface{}{{int64(0)}, {int64(1)}}},
		{"aggregation", "UNWIND range(1, 4) AS x WITH count(x) AS c RETURN c", nil, [][]interface{}{{int64(4)}}},
		{"nested unwind", "UNWIND [1, 2] AS start UNWIND range(start, 2) AS x WITH x WHERE x > 1 RETURN count(*) AS c", nil, [][]interface{}{{int64(2)}}},
		{"writes", "UNWIND range(1, 3) AS x WITH x WHERE x > 1 CREATE (n:RangeRow {v: x}) RETURN n.v ORDER BY n.v", nil, [][]interface{}{{int64(2)}, {int64(3)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_shared_pipeline"))
			result, err := executor.Execute(context.Background(), test.query, test.params)
			require.NoError(t, err)
			require.Equal(t, test.rows, result.Rows)
			if test.name == "empty wildcard" {
				require.Equal(t, []string{"x"}, result.Columns)
			}
		})
	}
}

func TestUnwindRangeSharedPipelineErrors(t *testing.T) {
	for _, query := range []string{
		"UNWIND range(1, 2, 0) AS x RETURN x",
		"UNWIND range($start, 2) AS x RETURN x",
		"UNWIND [1, 0] AS step UNWIND range(1, 2, step) AS x RETURN x",
		"UNWIND range(1, 2) AS x WITH range(x, 3, 0) AS xs RETURN xs",
		"UNWIND range(1, 2) AS x WITH x WHERE x < 0 RETURN range(1, 2, 0)",
	} {
		t.Run(query, func(t *testing.T) {
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			_, err := executor.Execute(context.Background(), query, map[string]interface{}{"start": 1.0})
			require.Error(t, err)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	_, err := executor.Execute(ctx, "UNWIND range(1, 300000000) AS x WITH x WHERE x < 0 RETURN count(*)", nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRangeReturnsEmptyWhenDirectionAndStepDisagree(t *testing.T) {
	for _, arguments := range [][]interface{}{
		{int64(0), int64(1), int64(-1)},
		{int64(0), int64(-1), int64(1)},
	} {
		values, err := evaluateCypherRange(arguments)
		require.NoError(t, err)
		require.Empty(t, values)
	}
}

func TestRangeIteratorStopsAtIntegerBoundaries(t *testing.T) {
	for _, test := range []struct {
		start, end, step int64
		values           []int64
	}{
		{math.MaxInt64 - 1, math.MaxInt64, 1, []int64{math.MaxInt64 - 1, math.MaxInt64}},
		{math.MinInt64 + 1, math.MinInt64, -1, []int64{math.MinInt64 + 1, math.MinInt64}},
		{0, math.MinInt64, math.MinInt64, []int64{0, math.MinInt64}},
	} {
		sequence, err := newCypherRange([]interface{}{test.start, test.end, test.step})
		require.NoError(t, err)
		var values []int64
		for value := range sequence.values() {
			values = append(values, value)
			require.LessOrEqual(t, len(values), len(test.values))
		}
		require.Equal(t, test.values, values)
	}
	sequence, err := newCypherRange([]interface{}{int64(1), int64(math.MaxInt64)})
	require.NoError(t, err)
	for value := range sequence.values() {
		require.Equal(t, int64(1), value)
		break
	}
}

func TestPipelineUnwindIteratorStopsAndErrors(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, expression := range []string{"range(1, 3)", "[1, 2, 3]"} {
		values, resolved := executor.pipelineUnwindValues(context.Background(), expression, pipelineRow{})
		require.True(t, resolved)
		for value := range values {
			require.Equal(t, int64(1), value)
			break
		}
	}
	for _, expression := range []string{"range(1, 2, 0)", "range(missing_function(), 2)", "missing_function()"} {
		ctx := withExpressionFailureSlot(context.Background())
		_, resolved := executor.pipelineUnwindValues(ctx, expression, pipelineRow{})
		require.False(t, resolved, expression)
		require.Error(t, getExpressionFailure(ctx))
	}
	for _, query := range []string{
		"UNWIND range(1, 2, 0) AS x RETURN x",
		"UNWIND range(1, 2) AS x WITH range(1, 2, 0) AS xs RETURN xs",
		"UNWIND range(1, 2) AS x WITH range(x, 2, 0) AS xs RETURN xs",
	} {
		ctx := withExpressionFailureSlot(context.Background())
		clauses, parsed := canExecuteAsPipeline(query)
		require.True(t, parsed)
		_, _, resolved := executor.pipelineApplyUnwindPrefix(ctx, []pipelineRow{{}}, clauses)
		require.False(t, resolved)
		require.Error(t, getExpressionFailure(ctx))
	}
	ctx, cancel := context.WithCancel(withExpressionFailureSlot(context.Background()))
	cancel()
	clauses, parsed := canExecuteAsPipeline("UNWIND range(1, 2) AS x RETURN x")
	require.True(t, parsed)
	_, _, resolved := executor.pipelineApplyUnwindPrefix(ctx, []pipelineRow{{}}, clauses)
	require.False(t, resolved)
	require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
}

func TestUnwindRangeReportedWorkload(t *testing.T) {
	if os.Getenv("NORNICDB_RUN_LARGE_UNWIND_RANGE") != "1" {
		t.Skip("large issue #772 workload")
	}
	for _, end := range []int64{3000000, 30000000, 300000000} {
		t.Run(fmt.Sprint(end), func(t *testing.T) {
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			query := fmt.Sprintf("UNWIND range(1, %d) AS x WITH x WHERE x < 0 RETURN count(*) AS c", end)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			started := time.Now()
			result, err := executor.Execute(context.Background(), query, nil)
			runtime.ReadMemStats(&after)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			t.Logf("values=%d elapsed=%s total_alloc_bytes=%d heap_alloc_bytes=%d", end, time.Since(started), after.TotalAlloc-before.TotalAlloc, after.HeapAlloc)
		})
	}
}

func TestUnwindRangeReportedAggregateWorkload(t *testing.T) {
	if os.Getenv("NORNICDB_RUN_LARGE_UNWIND_AGGREGATE") != "1" {
		t.Skip("large issue #772 aggregate correctness workload")
	}
	for _, explicit := range []bool{false, true} {
		for _, test := range []struct {
			query string
			want  int64
		}{
			{"UNWIND range(1, 30000000) AS x RETURN count(*) AS c", 30000000},
			{"UNWIND range(1, 30000000) AS x RETURN sum(x) AS s", 450000015000000},
			{"UNWIND range(1, 30000000) AS x WITH x RETURN count(*) AS c", 30000000},
		} {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, test.query), func(t *testing.T) {
				executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "large_aggregate"), 0, 0)
				var result *ExecuteResult
				var err error
				if explicit {
					_, err = executor.handleBegin()
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = executor.handleRollback() })
					result, err = executor.executeInTransaction(context.Background(), test.query, test.query)
				} else {
					result, err = executor.Execute(context.Background(), test.query, nil)
				}
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{test.want}}, result.Rows)
			})
		}
	}
}

func TestPipelineAggregateSourceFailureDoesNotReturnPartialGroups(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	projections := returnProjectionPlanFor("RETURN count(*) AS total").projections
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(withExpressionFailureSlot(context.Background()))
			defer cancel()
			source := func(yield func(pipelineRow) bool) bool {
				if !yield(pipelineRow{"x": int64(1)}) {
					t.Error("first row must be accepted before producer failure")
					return true
				}
				if canceled {
					cancel()
					require.False(t, yield(pipelineRow{"x": int64(2)}))
					return true
				}
				return false
			}
			groups, resolved := executor.pipelineAggregateGroups(ctx, source, projections)
			require.False(t, resolved)
			require.Nil(t, groups)
			if canceled {
				require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
			}
		})
	}
}

func TestPipelineVectorWriteKeepsTypedValues(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "typed_vector_write")
	counting := &graphitiCopyCountingEngine{Engine: store}
	executor := NewStorageExecutor(counting)
	vector := []float64{0.12345678901234567, 0.9876543210987654}
	result, err := executor.Execute(context.Background(), `UNWIND $rows AS row
MERGE (n:TypedVector {key: row.key}) SET n = row
WITH n, row CALL db.create.setNodeVectorProperty(n, "embedding", row.embedding)
RETURN n.embedding AS embedding`, map[string]interface{}{
		"rows": []interface{}{map[string]interface{}{"key": "typed", "embedding": vector}},
	})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{vector[0], vector[1]}}}, result.Rows)
	require.Equal(t, int64(1), counting.NodeUpdateCount())
}

func BenchmarkUnwindRangeFilteredCount(b *testing.B) {
	for _, end := range []int64{3000, 30000} {
		b.Run(fmt.Sprint(end), func(b *testing.B) {
			executor := NewStorageExecutorWithQueryCachePolicy(storage.NewMemoryEngine(), 0, 0)
			query := fmt.Sprintf("UNWIND range(1, %d) AS x WITH x WHERE x < 0 RETURN count(*) AS c", end)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := executor.Execute(context.Background(), query, nil)
				if err != nil || result.Rows[0][0] != int64(0) {
					b.Fatalf("result=%v error=%v", result, err)
				}
			}
		})
	}
}

func TestQuantifiedAggregateConfirmsEveryInconsistentRangeIsEmpty(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_quantified_aggregate"))
	result, err := executor.Execute(context.Background(), `
		WITH 0 AS start, [1, 2, 500, 1000, 1500] AS stopList, [-1000, -3, -2, -1, 1, 2, 3, 1000] AS stepList
		UNWIND stopList AS stop
		UNWIND stepList AS step
		WITH start, stop, step, range(start, stop, step) AS list
		WITH start, stop, step, list, sign(stop-start) <> sign(step) AS empty
		RETURN ALL(ok IN collect((size(list) = 0) = empty) WHERE ok) AS okay
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"okay"}, result.Columns)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}
