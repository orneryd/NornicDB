package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/queryprofile"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObservedPlanKeepsExclusiveCountersAndUnknownRows(t *testing.T) {
	c := newProfileRuntimeCollector()
	ctx := withProfileRuntimeCollector(t.Context(), c)
	matchCtx := profileRuntimeClauseContext(ctx, c, 0, pipelineClause{kind: pipelineClauseMatch, text: "MATCH (n) WHERE n.pass"})
	queryprofile.Access(matchCtx, queryprofile.KindAllNodesScan, "all nodes").Add(5, 5)
	profileRuntimeClauseOutputRows(matchCtx, c, 1)
	profileRuntimeClauseContext(ctx, c, 0, pipelineClause{kind: pipelineClauseReturn, text: "RETURN n"})
	plan := observedExecutionPlan("query", &ExecuteResult{Rows: [][]interface{}{{1}}}, time.Millisecond, c)
	require.Equal(t, int64(5), plan.TotalDBHits, "sum actual exclusive read counters once")
	require.Equal(t, int64(1), plan.TotalRows)
	require.Zero(t, plan.Root.DBHits, "ProduceResults performs no storage access itself")
	scan := planOperatorOfType(plan.Root, "AllNodesScan")
	require.NotNil(t, scan)
	require.Equal(t, int64(5), scan.ActualRows)
	require.Equal(t, int64(5), scan.DBHits)
	projection := planOperatorOfType(plan.Root, "Projection")
	require.NotNil(t, projection)
	require.False(t, projection.RowsObserved, "an unobserved clause is never assigned the final cardinality")
	view := neo4jPlanOperatorMap(projection, true)
	require.NotContains(t, view, "rows")
	require.Contains(t, formatPlan(plan), "Actual: unobserved")
}

func TestProfileConstantHasNoFabricatedReadOperators(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		result, err := exec.Execute(t.Context(), "PROFILE RETURN 1 AS n", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.Zero(t, plan.TotalDBHits)
		require.Equal(t, int64(1), plan.TotalRows)
		require.Nil(t, planOperatorOfType(plan.Root, "AllNodesScan"))
		require.Nil(t, planOperatorOfType(plan.Root, "NodeIndexSeek"))
		require.Contains(t, formatPlan(plan), "Total DB Hits: 0")
	})
}

func TestProfileObserverCountsStreamedRowsWithoutMaterializingThem(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newStreamTestExecutor(t, 12)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		collector := newProfileRuntimeCollector()
		query := "MATCH (n:Item) RETURN n.k AS k"
		run := executeStreamed(withProfileRuntimeCollector(ctx, collector), exec, query, 1)
		defer func() { cancel(); <-run.stream.Done() }()
		require.NoError(t, run.errIfDone())
		if !run.stream.streamed() {
			require.NoError(t, run.err)
			t.Fatalf("the public plain RETURN streaming route must stay streamed; result=%v", run.result)
		}
		var count int
		for {
			rows, done := run.stream.Next(3)
			count += len(rows)
			if done {
				break
			}
		}
		<-run.stream.Done()
		require.NoError(t, run.err)
		require.Equal(t, 12, count)
		require.Empty(t, run.result.Rows)
		plan := observedExecutionPlan(query, run.result, 0, collector)
		require.Equal(t, int64(12), plan.TotalRows)
		require.Equal(t, int64(12), plan.Root.ActualRows)
		for _, reason := range plan.Unobserved {
			require.False(t, strings.Contains(reason, "streamed"), reason)
		}
	})
}

func TestObservedPlanDoesNotInventRowsForUnobservedNilResult(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     [][]interface{}
		final    bool
		observed bool
	}{
		{"nil without observation", nil, false, false},
		{"explicit empty result", [][]interface{}{}, false, true},
		{"observed empty stream", nil, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collector := newProfileRuntimeCollector()
			if tc.final {
				collector.observeFinalRows(0)
			}
			plan := observedExecutionPlan("query", &ExecuteResult{Rows: tc.rows}, 0, collector)
			require.Equal(t, tc.observed, plan.Root.RowsObserved)
			require.Zero(t, plan.TotalRows)
		})
	}
}
