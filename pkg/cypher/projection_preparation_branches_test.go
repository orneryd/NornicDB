package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAggregateExpressionReaderBudgets(t *testing.T) {
	executor, _ := newTestExecutor(t)
	rows := []pipelineRow{{"x": int64(1)}, {"x": int64(2)}}
	for _, expression := range []string{"count(*)", "sum(x)", "sum(x)+1", "sum(x)+missing", "{n:sum(x), k:1}", "{}", "{invalid}", "{n:missing}", "[sum(x),count(*),1]", "[]", "[missing]", "[x IN [1,2] | x+1]", "x", "missing", "sum("} {
		t.Run(expression, func(t *testing.T) {
			want, wok := executor.evaluatePipelineAggregateExpressionWithContext(context.Background(), rows, expression)
			runPreparationBudgetSweep(t, func(ctx context.Context, p *pipelineCancellation) {
				actual, ok := executor.evaluatePipelineAggregateExpressionWithContext(ctx, rows, expression)
				if p.err == nil {
					require.Equal(t, wok, ok)
					require.Equal(t, want, actual)
				} else {
					require.False(t, ok)
				}
			})
		})
	}
	value, ok := executor.evaluatePipelineAggregateExpressionWithContext(context.Background(), rows, "sum(x)+1")
	require.True(t, ok)
	require.Equal(t, int64(4), value)
	runPreparationBudgetSweep(t, func(ctx context.Context, p *pipelineCancellation) {
		value, ok := executor.evaluatePipelineAggregateExpressionWithContext(ctx, nil, "1")
		if p.err == nil {
			require.True(t, ok)
			require.Equal(t, int64(1), value)
		} else {
			require.False(t, ok)
		}
	})
}

func TestProjectionColumnPreparationBudgets(t *testing.T) {
	columns := []string{"x", "a b", "a`b"}
	items := []string{"x+1 AS x", "count(*) AS c"}
	plan := parseReturnProjectionPlan("RETURN *, x+1 AS x, count(*) AS c")
	scope := map[string]struct{}{"x": {}, "a b": {}, "$param": {}, generatedVariablePrefix + "0": {}}
	runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
		actual := plan.withStarExpandedWithCancellation(columns, p)
		if p.err == nil {
			require.Equal(t, plan.withStarExpanded(columns), actual)
		}
	})
	runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
		actual := starProjectionItemsWithCancellation(columns, items, p)
		if p.err == nil {
			require.Equal(t, starProjectionItems(columns, items), actual)
		}
	})
	for _, clause := range []string{"RETURN *", "RETURN *, x+1 AS next", "RETURN x AS value ORDER BY value", "RETURN x"} {
		for _, original := range []string{"", "RETURN x AS original"} {
			want := &ExecuteResult{Columns: []string{"value"}}
			pipelineNameReturnColumns(want, clause, original, scope)
			runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
				actual := &ExecuteResult{Columns: []string{"value"}}
				pipelineNameReturnColumnsWithCancellation(actual, clause, original, scope, p)
				if p.err == nil {
					require.Equal(t, want, actual)
				}
			})
		}
	}
	for _, name := range []string{"", "`", "x", " x ", "1x", "x!", "a b", "a`b", "`x`", strings.Repeat("x", 129)} {
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			actual := projectionVariableTextWithCancellation(name, p)
			if p.err == nil {
				require.Equal(t, projectionVariableText(name), actual)
			}
		})
	}
}

func TestProjectionCacheLockDeadline(t *testing.T) {
	returnProjectionPlans.Lock()
	defer returnProjectionPlans.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	p := &pipelineCancellation{ctx: ctx}
	plan := returnProjectionPlanForWithCancellation("RETURN 1 AS value", p)
	returned := time.Now()
	require.False(t, plan.valid)
	require.ErrorIs(t, p.err, context.DeadlineExceeded)
	require.Less(t, returned.Sub(deadline), 250*time.Millisecond)
}
