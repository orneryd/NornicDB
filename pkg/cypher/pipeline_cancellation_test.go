package cypher

import (
	"archive/zip"
	"context"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineStagesRejectCancelledContext(t *testing.T) {
	rows := []pipelineRow{{"x": int64(2)}, {"x": int64(1)}}
	for _, clause := range []string{
		"WITH x AS y", "WITH DISTINCT x AS y", "WITH x AS y ORDER BY y",
		"WITH collect(x) AS y", "WITH * ORDER BY x",
	} {
		t.Run(clause, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			ctx = withExpressionFailureSlot(ctx)
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			_, ok := executor.pipelineApplyWith(ctx, rows, clause)
			require.False(t, ok)
			require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
		})
	}
}

type pipelineCountingContext struct {
	context.Context
	calls int
	stop  int
}

func (ctx *pipelineCountingContext) Err() error {
	ctx.calls++
	if ctx.stop > 0 && ctx.calls >= ctx.stop {
		return context.Canceled
	}
	return ctx.Context.Err()
}

func TestPipelineCancellationAmortized(t *testing.T) {
	for _, rows := range []int{1, 100000} {
		t.Run(strconv.Itoa(rows), func(t *testing.T) {
			ctx := &pipelineCountingContext{Context: context.Background()}
			probe := pipelineCancellation{ctx: ctx}
			for index := 0; index < rows; index++ {
				require.True(t, probe.check())
			}
			require.Equal(t, 1+(rows-1)/pipelineCancellationInterval, ctx.calls)
		})
	}
	ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
	probe := pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
	for index := 0; index < pipelineCancellationInterval; index++ {
		require.True(t, probe.check())
	}
	require.False(t, probe.check())
	for index := 0; index < 100000; index++ {
		require.False(t, probe.check())
	}
	require.Equal(t, 2, ctx.calls, "cancellation remains latched")
	require.ErrorIs(t, probe.err, context.Canceled)
}

func TestPipelineCancellationAtEndOfSource(t *testing.T) {
	ctx, cancel := context.WithCancel(withExpressionFailureSlot(context.Background()))
	defer cancel()
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	source := func(yield func(pipelineRow) bool) bool {
		require.True(t, yield(pipelineRow{"x": int64(1)}))
		cancel()
		return true
	}
	groups, ok := executor.pipelineAggregateGroups(ctx, source, returnProjectionPlanFor("RETURN count(*)").projections, true)
	require.False(t, ok)
	require.Nil(t, groups)
	require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
}

func TestPipelineCancellationNestedValue(t *testing.T) {
	values := make([]interface{}, 100000)
	for index := range values {
		values[index] = map[string]interface{}{"value": int64(index), "items": []interface{}{nil, true, "text"}}
	}
	value := map[string]interface{}{"nested": values}
	probe := pipelineCancellation{ctx: context.Background()}
	require.Equal(t, cypherEquivalenceKey(value), cypherEquivalenceKeyWithCancellation(value, &probe))
	ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
	probe = pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
	_ = cypherEquivalenceKeyWithCancellation(value, &probe)
	require.ErrorIs(t, probe.err, context.Canceled)
	require.Equal(t, 2, ctx.calls, "one large value must be interruptible")
}

func TestPipelineCancellationSort(t *testing.T) {
	values := make([]int, 100000)
	for index := range values {
		values[index] = len(values) - index
	}
	ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
	probe := pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
	comparisons := 0
	require.False(t, probe.sort(values, func(i, j int) bool { comparisons++; return values[i] < values[j] }, true))
	require.LessOrEqual(t, comparisons, pipelineCancellationInterval)
	require.ErrorIs(t, probe.err, context.Canceled)
	probe = pipelineCancellation{ctx: context.Background()}
	require.PanicsWithValue(t, "comparison failure", func() {
		probe.sort(values, func(i, j int) bool { panic("comparison failure") }, false)
	})
}

func TestPipelineCancellationWithinConversionsAndComparisons(t *testing.T) {
	for _, value := range []interface{}{
		make([]map[string]interface{}, 1000), make([]string, 1000), make([]int, 1000), make([]int64, 1000),
		make([]float64, 1000), make([]float32, 1000), make([]bool, 1000), [1000]int16{},
	} {
		ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
		probe := pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
		require.Nil(t, toAnySliceWithCancellation(value, &probe))
		require.ErrorIs(t, probe.err, context.Canceled)
	}
	for _, convert := range []func(*pipelineCancellation){
		func(probe *pipelineCancellation) { _ = toAnySliceWithCancellation(make([]int64, 100000), probe) },
		func(probe *pipelineCancellation) {
			_, _ = evaluateCypherRangeWithCancellation([]interface{}{int64(1), int64(100000)}, probe)
		},
		func(probe *pipelineCancellation) {
			values := make([]interface{}, 100000)
			_ = compareValuesForSortWithCancellation(values, values, probe)
		},
		func(probe *pipelineCancellation) {
			fields := make(map[interface{}]interface{}, 100000)
			for index := 0; index < 100000; index++ {
				fields[strconv.Itoa(index)] = index
			}
			_, _ = toStringAnyMapWithCancellation(fields, probe)
		},
		func(probe *pipelineCancellation) {
			fields := make(map[string]interface{}, 100000)
			for index := 0; index < 100000; index++ {
				fields[strconv.Itoa(index)] = index
			}
			_ = compareValuesForSortWithCancellation(fields, fields, probe)
		},
	} {
		ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
		probe := pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
		convert(&probe)
		require.ErrorIs(t, probe.err, context.Canceled)
		require.Equal(t, 2, ctx.calls)
	}
}

func TestPipelineMaterializedStagesLatchCancellation(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	rows := []pipelineRow{{"x": int64(2)}, {"x": int64(1)}}
	stages := []struct {
		name string
		run  func(context.Context)
	}{
		{"distinct_rows", func(ctx context.Context) { _ = deduplicatePipelineRows(ctx, rows, []string{"x"}) }},
		{"distinct_values", func(ctx context.Context) { _ = deduplicatePipelineResultRows(ctx, [][]interface{}{{int64(1)}}) }},
		{"columns", func(ctx context.Context) { _ = pipelineColumns(ctx, rows, true); _ = pipelineWildcardColumns(ctx, rows) }},
		{"materialize", func(ctx context.Context) {
			_, ok := materializePipelineSource(ctx, pipelineRowsSource(rows))
			require.False(t, ok)
		}},
		{"window_filter", func(ctx context.Context) { _ = executor.filterPipelineWindow(ctx, rows, rows, 0, -1, "x > 0") }},
		{"filter", func(ctx context.Context) { _ = executor.filterPipelineRows(ctx, rows, "x > 0") }},
		{"plain_return", func(ctx context.Context) { _, _ = executor.pipelineApplyReturn(ctx, rows, "RETURN x") }},
		{"wildcard_return", func(ctx context.Context) { _, _ = executor.pipelineApplyReturn(ctx, rows, "RETURN *") }},
		{"simple_projection", func(ctx context.Context) {
			_ = projectPipelineSimpleNodeRead(ctx, []*storage.Node{{ID: "n"}}, []returnItem{{expr: "n"}}, "n")
		}},
		{"call_result", func(ctx context.Context) {
			_ = callPipelineRowsFromResult(ctx, &ExecuteResult{Columns: []string{"x"}, Rows: [][]interface{}{{int64(1)}}})
		}},
		{"call_columns", func(ctx context.Context) { _ = callPipelineResultFromRows(ctx, rows, nil, nil, nil) }},
		{"call_join", func(ctx context.Context) {
			_ = appendCallReturnRows(ctx, nil, rows[0], []string{"x"}, [][]interface{}{{int64(1)}})
		}},
		{"optional_call", func(ctx context.Context) {
			ok := true
			_, _ = pipelineApplyOptionally(ctx, rows, func([]pipelineRow) ([]pipelineRow, []string, bool, error) {
				t.Fatal("cancelled OPTIONAL CALL executed a row")
				return nil, nil, false, nil
			}, &ok)
		}},
		{"pattern_validation", func(ctx context.Context) { _ = validateBoundPatternNodes(ctx, rows, "(x)") }},
		{"percentile", func(ctx context.Context) {
			_, _ = executor.evaluatePipelinePercentile(ctx, rows, "percentilecont", "x, 0.5", false)
		}},
		{"aggregate", func(ctx context.Context) {
			_, _ = executor.evaluatePipelineAggregateWithContext(ctx, rows, "collect", "x", false)
		}},
		{"filter_source", func(ctx context.Context) {
			source, err := executor.pipelineSharedClauseSource(ctx, pipelineRowsSource(rows), pipelineClause{kind: pipelineClauseFilter, text: "FILTER x > 0"})
			require.NoError(t, err)
			require.False(t, source(func(pipelineRow) bool { t.Fatal("cancelled FILTER yielded a row"); return true }))
		}},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			ctx = withExpressionFailureSlot(ctx)
			stage.run(ctx)
			require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
		})
	}
}

// assertPipelineQueryExited verifies the particular query goroutine is gone,
// instead of counting unrelated storage/cache goroutines in a busy process.
func assertPipelineQueryExited(t *testing.T, id uint64) {
	t.Helper()
	buffer := make([]byte, 8<<20)
	require.Eventually(t, func() bool {
		length := runtime.Stack(buffer, true)
		return length < len(buffer) && !strings.Contains(string(buffer[:length]), "goroutine "+strconv.FormatUint(id, 10)+" [")
	}, 250*time.Millisecond, time.Millisecond, "query goroutine survived its returned error")
}

func pipelineCurrentGoroutine() uint64 {
	var buffer [128]byte
	length := runtime.Stack(buffer[:], false)
	id, _ := strconv.ParseUint(strings.Fields(string(buffer[:length]))[1], 10, 64)
	return id
}

// Each stage runs enough real work that its own loop must observe the deadline.
// A fixture with shared immutable rows keeps setup small, while the nested
// value also exercises cancellation within a single DISTINCT/grouping key.
func TestPipelineStageDeadlineProbes(t *testing.T) {
	nested := make([]interface{}, 128)
	for index := range nested {
		nested[index] = map[string]interface{}{"v": int64(index)}
	}
	rows := make([]pipelineRow, 400000)
	for index := range rows {
		rows[index] = pipelineRow{"x": int64(len(rows) - index), "nested": nested}
	}
	// Compiled per-row expressions make the cheap projection stages fast; they
	// read the rows four times over so the deadline still falls inside them.
	cheap := make([]pipelineRow, 0, 4*len(rows))
	for range 4 {
		cheap = append(cheap, rows...)
	}
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	stages := []struct {
		name string
		run  func(context.Context) bool
	}{
		{"distinct", func(ctx context.Context) bool {
			_, _ = deduplicatePipelineRowsWithScopes(ctx, rows, rows, []string{"nested", "x"})
			return getExpressionFailure(ctx) == nil
		}},
		{"order_by", func(ctx context.Context) bool {
			return executor.orderPipelineRowsWithScopes(ctx, rows, rows, parseOrderByTerms("ORDER BY x + 1 DESC"))
		}},
		{"collect", func(ctx context.Context) bool {
			_, ok := executor.pipelineAggregateGroups(ctx, pipelineRowsSource(cheap), []returnProjection{{expr: "collect(x + 1)", alias: "xs", isAggr: true}}, true)
			return ok
		}},
		{"grouping", func(ctx context.Context) bool {
			_, ok := executor.pipelineAggregateGroups(ctx, pipelineRowsSource(rows), []returnProjection{{expr: "nested", alias: "n"}, {expr: "count(*)", alias: "c", isAggr: true}}, true)
			return ok
		}},
		{"with_projection", func(ctx context.Context) bool {
			_, ok := executor.pipelineApplyWith(ctx, cheap, "WITH x + 1 AS y")
			return ok
		}},
		{"return_projection", func(ctx context.Context) bool {
			_, ok := executor.pipelineApplyReturn(ctx, cheap, "RETURN x + 1 AS y")
			return ok
		}},
		{"unwind", func(ctx context.Context) bool {
			_, ok := executor.pipelineApplyUnwind(ctx, rows[:1], "UNWIND range(1, 10000000) AS n")
			return ok
		}},
		{"filter", func(ctx context.Context) bool {
			_ = executor.filterPipelineRows(ctx, rows, "size([v IN nested WHERE v.v < x]) > 0")
			return getExpressionFailure(ctx) == nil
		}},
		{"percentile", func(ctx context.Context) bool {
			_, ok := executor.evaluatePipelinePercentile(ctx, rows, "percentilecont", "x + 1, 0.5", false)
			return ok
		}},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			ctx = withExpressionFailureSlot(ctx)
			deadline, _ := ctx.Deadline()
			var id atomic.Uint64
			done := make(chan bool, 1)
			go func() { id.Store(pipelineCurrentGoroutine()); done <- stage.run(ctx) }()
			select {
			case ok := <-done:
				elapsed := time.Until(deadline) * -1
				require.False(t, ok, "fixture completed before deadline; increase work to probe this stage")
				require.ErrorIs(t, getExpressionFailure(ctx), context.DeadlineExceeded)
				require.Less(t, elapsed, 250*time.Millisecond)
				t.Logf("deadline=%s returned=%s overdue=%s", deadline.Format(time.RFC3339Nano), time.Now().Format(time.RFC3339Nano), elapsed)
			case <-time.After(2 * time.Second):
				t.Fatal("stage did not stop promptly")
			}
			// The channel send precedes the goroutine's return by a few instructions.
			runtime.Gosched()
			assertPipelineQueryExited(t, id.Load())
		})
	}
}

func pipelineDeadlineControl(t *testing.T) (string, map[string]interface{}) {
	t.Helper()
	query, err := os.ReadFile("testdata/pipeline_deadline/statement.cypher")
	require.NoError(t, err)
	archive, err := zip.OpenReader("testdata/pipeline_deadline/parameters.zip")
	require.NoError(t, err)
	defer archive.Close()
	require.Len(t, archive.File, 1)
	reader, err := archive.File[0].Open()
	require.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	var params map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &params))
	require.Len(t, params["payload"], 4188)
	return string(query), params
}

func TestPipelineScopedDistinctExactDeadline(t *testing.T) {
	query, params := pipelineDeadlineControl(t)
	for _, explicit := range []bool{false, true} {
		t.Run(strconv.FormatBool(explicit), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			baselineReaders := base.ActiveReaders()
			store := storage.NewNamespacedEngine(base, "pipeline_deadline")
			executor := NewStorageExecutor(store)
			if explicit {
				_, err := executor.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
				defer executor.handleRollback()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			started := time.Now()
			type outcome struct {
				result *ExecuteResult
				err    error
			}
			var id atomic.Uint64
			done := make(chan outcome, 1)
			go func() {
				id.Store(pipelineCurrentGoroutine())
				result, err := executor.Execute(ctx, query, params)
				done <- outcome{result, err}
			}()
			var result *ExecuteResult
			var err error
			select {
			case returned := <-done:
				result, err = returned.result, returned.err
			case <-time.After(2250 * time.Millisecond):
				t.Fatal("exact control exceeded its deadline plus 250 ms")
			}
			returned := time.Now()
			if err == nil {
				require.Equal(t, [][]interface{}{{int64(996)}}, result.Rows)
				require.True(t, returned.Before(deadline), "early success must precede deadline")
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Less(t, returned.Sub(deadline), 250*time.Millisecond)
			}
			t.Logf("source=%s parser=%s explicit=%t start=%s context_deadline=%s return=%s elapsed=%s overdue=%s error=%v", os.Getenv("PIPELINE_SOURCE_HEAD"), os.Getenv("NORNICDB_PARSER"), explicit, started.Format(time.RFC3339Nano), deadline.Format(time.RFC3339Nano), returned.Format(time.RFC3339Nano), returned.Sub(started), returned.Sub(deadline), err)
			assertPipelineQueryExited(t, id.Load())
			if explicit {
				_, _ = executor.handleRollback()
			}
			require.Equal(t, baselineReaders, base.ActiveReaders(), "exact control left a snapshot reader")
			result, err = executor.Execute(context.Background(), "RETURN 1 AS healthy", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}

func TestPipelineCancellationRollsBackWrites(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(strconv.FormatBool(explicit), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "pipeline_rollback"))
			baselineReaders := base.ActiveReaders()
			if explicit {
				_, err := executor.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			_, err := executor.Execute(ctx, "CREATE (:DeadlineWrite) WITH 1 AS seed UNWIND range(1, 10000000) AS x RETURN sum(x + seed)", nil)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			if explicit {
				_, _ = executor.handleRollback()
			}
			require.Less(t, time.Since(deadline), 250*time.Millisecond, "rollback must finish before response")
			require.Equal(t, baselineReaders, base.ActiveReaders(), "snapshot reader leaked")
			result, err := NewStorageExecutor(storage.NewNamespacedEngine(base, "pipeline_rollback")).Execute(context.Background(), "MATCH (n:DeadlineWrite) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
		})
	}
}

// Unlike the exact historical control, this statement deliberately repeats
// the nested DISTINCT value so early completion cannot mask cancellation.
// The row source is streamed into the aggregate; it does not retain a billion rows.
func TestPipelineForcedTwoSecondDeadline(t *testing.T) {
	_, params := pipelineDeadlineControl(t)
	query := "WITH $payload AS payload UNWIND range(1, 1000000000) AS each RETURN count(DISTINCT payload)"
	for _, explicit := range []bool{false, true} {
		t.Run(strconv.FormatBool(explicit), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "pipeline_forced_deadline"))
			baselineReaders := base.ActiveReaders()
			if explicit {
				_, err := executor.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			started := time.Now()
			var id atomic.Uint64
			done := make(chan error, 1)
			go func() {
				id.Store(pipelineCurrentGoroutine())
				_, err := executor.Execute(ctx, query, params)
				done <- err
			}()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(2250 * time.Millisecond):
				t.Fatal("forced work exceeded its deadline plus 250 ms")
			}
			if explicit {
				_, _ = executor.handleRollback()
			}
			returned := time.Now()
			require.Less(t, returned.Sub(deadline), 250*time.Millisecond)
			assertPipelineQueryExited(t, id.Load())
			require.Equal(t, baselineReaders, base.ActiveReaders())
			t.Logf("source=%s parser=%s explicit=%t start=%s context_deadline=%s return=%s elapsed=%s overdue=%s", os.Getenv("PIPELINE_SOURCE_HEAD"), os.Getenv("NORNICDB_PARSER"), explicit, started.Format(time.RFC3339Nano), deadline.Format(time.RFC3339Nano), returned.Format(time.RFC3339Nano), returned.Sub(started), returned.Sub(deadline))
		})
	}
}

func TestPipelineCancellationHealthyStatements(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, query := range []string{
		"UNWIND [3,1,2,2] AS x WITH DISTINCT x ORDER BY x RETURN collect(x) AS xs",
		"UNWIND [3,1,2,2] AS x RETURN collect(DISTINCT x) AS xs",
		"UNWIND [3,1,2,2] AS x WITH x + 1 AS y WHERE y > 2 RETURN y ORDER BY y",
		"UNWIND [3,1,2,2] AS x WITH x % 2 AS k, count(*) AS c RETURN k,c ORDER BY k",
		"RETURN 1 AS x UNION RETURN 1.0 AS x UNION RETURN 2 AS x",
	} {
		t.Run(query, func(t *testing.T) {
			base, err := executor.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			bounded, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, base.Columns, bounded.Columns)
			require.Equal(t, base.Rows, bounded.Rows)
		})
	}
}
