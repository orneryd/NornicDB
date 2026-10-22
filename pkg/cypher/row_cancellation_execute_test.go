package cypher

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This context arms a real cancellation/deadline only when Execute reaches the
// named consumer. Looking at the immediate Cypher caller avoids canceling in a
// producer nested beneath it. No production test hook or private operator call
// is involved; a missing checkpoint makes the public query incorrectly succeed.
type rowConsumerContext struct {
	context.Context
	mu        sync.Mutex
	consumer  string
	deadline  bool
	after     int
	line      int
	checks    int
	triggered time.Time
	done      chan struct{}
	failure   error
}

func (c *rowConsumerContext) Done() <-chan struct{} { return c.done }
func (c *rowConsumerContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	owner := ""
	ownerLine := 0
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "github.com/orneryd/nornicdb/pkg/cypher.") &&
			!strings.HasSuffix(frame.Function, ".Err") &&
			!strings.Contains(frame.Function, "materializedRowCheckpoint") &&
			!strings.Contains(frame.Function, "materializedBatches") &&
			!strings.Contains(frame.Function, "storageScan") &&
			!strings.HasSuffix(frame.Function, ".rowCancellationError") &&
			!strings.HasSuffix(frame.Function, ".materializedRowCancellationError") {
			owner = frame.Function
			ownerLine = frame.Line
			break
		}
		if !more {
			break
		}
	}
	if !strings.Contains(owner, c.consumer) || (c.line > 0 && c.line != ownerLine) {
		return nil
	}
	c.checks++
	if c.checks < c.after {
		return nil
	}
	c.triggered = time.Now()
	if c.deadline {
		expired, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		<-expired.Done()
		c.failure = expired.Err()
	} else {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		c.failure = canceled.Err()
	}
	close(c.done)
	return c.failure
}

type rowConsumerCase struct {
	name, query, consumer string
	after                 int
}

func TestMaterializedRowConsumersExecuteCancellation(t *testing.T) {
	const input = "UNWIND range(1,$n) AS x WITH x ORDER BY x "
	cases := []rowConsumerCase{
		{"with_plain", input + "WITH x AS y RETURN y", "pipelineApplyWithSource", 2},
		{"with_wildcard", input + "WITH * ORDER BY x RETURN x", "pipelineApplyWithSource", 2},
		{"with_modifiers", input + "WITH x AS y ORDER BY y RETURN y", "pipelineApplyWithSource", 2},
		{"with_groups", "UNWIND range(1,$n) AS x WITH x, count(*) AS c RETURN count(*)", "pipelineApplyWithSource", 2},
		{"return_wildcard", input + "RETURN *", "pipelineApplyReturnPlan", 2},
		{"return_groups", "UNWIND range(1,$n) AS x RETURN x, count(*) AS c", "pipelineApplyReturnPlan", 2},
		{"return_group_order", "UNWIND range(1,$n) AS x RETURN x, count(*) AS c ORDER BY x", "pipelineApplyReturnPlan", 2},
		{"return_modifiers", input + "RETURN x AS y LIMIT $n", "pipelineApplyReturnPlan", 2},
		{"return_plain", input + "RETURN x AS y", "pipelineProjectPlainReturn", 2},
		{"final_assembly", input + "RETURN x AS y LIMIT $n", "finishPipelineReturn", 2},
		{"distinct_with", input + "WITH DISTINCT x RETURN x", "deduplicatePipelineRowsWithScopes", 2},
		{"distinct_return", input + "RETURN DISTINCT x", "deduplicatePipelineRowsWithScopes", 2},
		{"distinct_wildcard", input + "RETURN DISTINCT *", "deduplicatePipelineResultRows", 2},
		{"distinct_groups", "UNWIND range(1,$n) AS x RETURN DISTINCT x, count(*) AS c", "deduplicatePipelineResultRows", 2},
		{"wildcard_columns", input + "RETURN *", "pipelineWildcardColumns", 2},
		{"with_filter", input + "WITH DISTINCT * WHERE x > 0 RETURN x", "filterPipelineRows", 2},
		{"with_window_filter", input + "WITH x LIMIT $n WHERE x > 0 RETURN x", "filterPipelineWindow", 2},
		{"sort", input + "RETURN x", "stableSortRows[", 2},
		{"collect", "UNWIND range(1,$n) AS x RETURN collect(x)", "pipelineAggregateGroups", 2},
		{"aggregate", "UNWIND range(1,$n) AS x RETURN sum(x)", "pipelineAggregateGroups", 2},
		{"percentile", "UNWIND range(1,$n) AS x RETURN percentileCont(x,0.5)", "evaluatePipelinePercentile", 2},
		{"result_conversion", input + "RETURN *", "applyCallResultOrderWindow", 2},
		{"sort_keys", input + "RETURN x", "orderPipelineRowsWithScopes", 3},
		{"sort_copyback", input + "RETURN x", "orderPipelineRowsWithScopes", 7},
		{"result_copyback", input + "RETURN *", "applyCallResultOrderWindow", 6},
		{"call_append", "UNWIND [0] AS marker CALL { UNWIND range(1,$n) AS x RETURN x } RETURN x", "appendCallReturnRows", 2},
		{"call_product", "UNWIND range(1,2) AS marker CALL { UNWIND range(1,$n) AS x RETURN x } RETURN x", "pipelineApplyCallSubqueryWithMetadata", 2},
		{"call_terminal", "UNWIND [0] AS marker CALL { UNWIND range(1,$n) AS x RETURN x }", "callPipelineResultFromRows", 2},
		{"optional_input", input + "OPTIONAL MATCH (m:AbsentRowCancel) RETURN x", "pipelineApplyOptionalMatch", 2},
		{"optional_output", input + "OPTIONAL MATCH (m:AbsentRowCancel) RETURN x", "pipelineApplyOptionalMatch", 2},
		{"key_equality", "UNWIND $values AS k RETURN DISTINCT k", "cypherValuesEquivalent", 2},
		{"map_order_snapshot", "UNWIND range(1,$n) AS x RETURN {value:x}", "snapshotMapKeyOrders", 2},
		{"execute_success", input + "RETURN x LIMIT 0", "execute", 1},
		{"pipeline_success", input + "RETURN x LIMIT 0", "runPipelineClauseRows", 1},
	}
	runRowConsumerExecuteCases(t, cases, func(t *testing.T) *StorageExecutor { return rowCancellationFixture(t, 1) })
}

// Explicit transactions take the deferred snapshot route. Its metadata comes
// from the ended statement, but its cancellation budget remains the caller's.
func TestMaterializedRowDeferredSnapshotExecuteCancellation(t *testing.T) {
	cases := []rowConsumerCase{{"deferred_map_order_snapshot", "UNWIND range(1,$n) AS x RETURN {value:x}", "snapshotMapKeyOrders", 2}}
	runRowConsumerExecuteCases(t, cases, func(t *testing.T) *StorageExecutor {
		executor := rowCancellationFixture(t, 1)
		_, err := executor.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := executor.Execute(context.Background(), "ROLLBACK", nil)
			require.NoError(t, err)
		})
		return executor
	})
}

func runRowConsumerExecuteCases(t *testing.T, cases []rowConsumerCase, fixture func(*testing.T) *StorageExecutor) {
	for _, tc := range cases {
		t.Run(tc.name+"/success", func(t *testing.T) {
			executor := fixture(t)
			result, err := executor.Execute(context.Background(), tc.query, rowConsumerParameters(tc.name))
			require.NoError(t, err)
			expected := 4096
			switch tc.name {
			case "with_groups", "collect", "aggregate", "percentile", "key_equality":
				expected = 1
			case "pipeline_success", "execute_success":
				expected = 0
			case "call_product":
				expected = 8192
			}
			require.Len(t, result.Rows, expected)
		})
		for _, deadline := range []bool{false, true} {
			mode := "cancel"
			if deadline {
				mode = "deadline"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				executor := fixture(t)
				ctx := &rowConsumerContext{Context: context.Background(), consumer: tc.consumer, deadline: deadline, after: tc.after, done: make(chan struct{})}
				if pass := map[string]string{
					"yield_input":                "\tfor batch, batchErr := range materializedBatches(ctx, len(result.Rows)) {",
					"yield_output":               "\tfor batch, batchErr := range materializedBatches(ctx, len(rows)) {",
					"optional_output":            "\tfor batch, batchErr := range materializedBatches(ctx, len(expanded)) {",
					"call_tail_procedure_output": "\t\t\tfor innerBatch, innerErr := range materializedBatches(ctx, len(procedureResult.Rows)) {",
				}[tc.name]; pass != "" {
					file := "pipeline_aggregate_stream.go"
					if strings.HasPrefix(tc.name, "yield_") || tc.name == "call_tail_procedure_output" {
						file = "call.go"
					}
					if tc.name == "optional_output" {
						file = "pipeline_executor.go"
					}
					ctx.line = rowConsumerPassLine(t, file, tc.consumer, pass)
				}
				if tc.name == "execute_success" {
					ctx.line = rowConsumerPassLine(t, "executor_entry.go", tc.consumer, "\t\tif err := deadlineCtx.Err(); err != nil {")
				}
				_, err := executor.Execute(ctx, tc.query, rowConsumerParameters(tc.name))
				want := context.Canceled
				if deadline {
					want = context.DeadlineExceeded
				}
				require.ErrorIs(t, err, want, "consumer must not report success")
				require.Equal(t, tc.after, ctx.checks, "must cancel inside the requested consumer")
				require.False(t, ctx.triggered.IsZero())
				require.Less(t, time.Since(ctx.triggered), 2*time.Second, "canceled pass must stop promptly")
			})
		}
	}
}

// Resolve a named pass (its source line, indentation included) once, outside
// query execution; source line shifts do not turn a merge/copyback regression
// into cancellation at an earlier checkpoint.
func rowConsumerPassLine(t *testing.T, file, consumer, pass string) int {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	require.True(t, ok)
	source, err := os.ReadFile(filepath.Join(filepath.Dir(here), file))
	require.NoError(t, err)
	inside := false
	for i, line := range strings.Split(string(source), "\n") {
		if strings.HasPrefix(line, "func ") {
			inside = strings.Contains(line, consumer+"(")
		}
		if inside && strings.TrimRight(line, "\r") == pass {
			return i + 1
		}
	}
	t.Fatalf("consumer pass not found: %s %s", consumer, pass)
	return 0
}

func TestMaterializedCallAdaptersExecuteCancellation(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "custom.row_cancel_rows", Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "x", Type: "INTEGER"}}}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		rows := make([][]interface{}, 4096)
		for i := range rows {
			rows[i] = []interface{}{int64(i + 1)}
		}
		return &ExecuteResult{Columns: []string{"x"}, Rows: rows}, nil
	}))
	const call = "CALL custom.row_cancel_rows() YIELD x "
	cases := []rowConsumerCase{
		{"call_tail_procedure_input", "CALL custom.row_cancel_rows() YIELD x AS incoming WITH incoming LIMIT 1 CALL custom.row_cancel_rows() YIELD x CREATE (:RowCancelOwned {value:x}) RETURN x", "tryExecuteCallTailProcedurePipeline", 2},
		{"call_tail_procedure_output", "CALL custom.row_cancel_rows() YIELD x AS incoming WITH incoming LIMIT 1 CALL custom.row_cancel_rows() YIELD x CREATE (:RowCancelOwned {value:x}) RETURN x", "tryExecuteCallTailProcedurePipeline", 2},
		{"call_tail_star", call + "RETURN *", "projectCallTailReturnAll", 2},
		{"call_tail_projection_input", call + "WITH x AS y RETURN y", "tryExecuteCallTailProjectionFilter", 2},
		{"call_tail_pipeline_input", call + "WITH DISTINCT x RETURN x", "executeCallTailPipeline", 2},
		{"yield_alias", "CALL custom.row_cancel_rows() YIELD x AS y RETURN y", "applyYieldFilter", 2},
		{"yield_input", call + "WHERE x>0 RETURN x", "applyYieldFilter", 2},
		{"yield_output", call + "WHERE x>0 RETURN x", "applyYieldFilter", 2},
	}
	runRowConsumerExecuteCases(t, cases, func(t *testing.T) *StorageExecutor { return rowCancellationFixture(t, 1) })
}

func rowConsumerParameters(name string) map[string]interface{} {
	parameters := map[string]interface{}{"n": int64(4096)}
	if name == "key_equality" {
		values := make([]interface{}, 4096)
		for i := range values {
			values[i] = int64(i)
		}
		value := map[string]interface{}{"payload": values}
		parameters["values"] = []interface{}{value, value}
	}
	return parameters
}
