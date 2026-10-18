package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineAggregateCopiesCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate_copies"))
	row := pipelineRow{"x": int64(7)}
	templates := make([]pipelineAggregateProjection, 1025)
	projections := make([]returnProjection, 1025)
	for i := range templates {
		row[fmt.Sprintf("column%d", i)] = int64(i)
		templates[i] = pipelineAggregateProjection{expression: "__nornic_agg_0", states: []pipelineAggregateState{{name: "count", expression: "*", count: 1}}}
		projections[i] = returnProjection{expr: "count(*)", alias: fmt.Sprintf("count%d", i), isAggr: true}
	}
	wideStates := []pipelineAggregateProjection{{expression: "x", states: make([]pipelineAggregateState, 1025)}}
	group := newPipelineAggregateGroup(row, templates, nil)
	for _, stage := range []struct {
		name string
		run  func(context.Context)
	}{
		{"first_row_and_templates", func(ctx context.Context) {
			newPipelineAggregateGroup(row, templates, pipelineCancellationForContext(ctx))
		}},
		{"state_template_copy", func(ctx context.Context) {
			newPipelineAggregateGroup(nil, wideStates, pipelineCancellationForContext(ctx))
		}},
		{"group_value_scope", func(ctx context.Context) { group.value(ctx, executor, 0) }},
		{"wide_projection_templates", func(ctx context.Context) {
			executor.pipelineAggregateGroups(ctx, pipelineRowsSource([]pipelineRow{row}), projections, true)
		}},
	} {
		t.Run(stage.name, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error {
				stage.run(withPipelineCancellation(ctx))
				return getExpressionFailure(ctx)
			})
		})
	}
}

func TestPipelineOutputGrowthCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "output_copies"))
	rows := make([]pipelineRow, 1025)
	for i := range rows {
		rows[i] = pipelineRow{"x": int64(i)}
	}
	wide := pipelineRow{}
	for i := 0; i < 1025; i++ {
		wide[fmt.Sprintf("column%d", i)] = int64(i)
	}
	plain := &returnProjectionPlan{valid: true, columns: []string{"x"}, projections: []returnProjection{{expr: "x", alias: "x"}}}
	for _, stage := range []struct {
		name string
		run  func(context.Context)
	}{
		{"return_star_wide", func(ctx context.Context) {
			executor.pipelineApplyReturnPlan(ctx, []pipelineRow{wide}, &returnProjectionPlan{valid: true, star: true}, nil, true)
		}},
		{"return_plain_rows", func(ctx context.Context) { executor.pipelineApplyReturnPlan(ctx, rows, plain, nil, true) }},
		{"return_source_rows", func(ctx context.Context) {
			executor.pipelineApplyReturnPlan(ctx, nil, plain, pipelineRowsSource(rows), true)
		}},
		{"return_order_distinct_rows", func(ctx context.Context) {
			plan := *plain
			plan.distinct = true
			plan.modifiers = "ORDER BY x DESC"
			executor.pipelineApplyReturnPlan(ctx, rows, &plan, nil, true)
		}},
		{"with_star_wide", func(ctx context.Context) {
			executor.pipelineApplyWithSource(ctx, []pipelineRow{wide}, "WITH DISTINCT *", nil, true)
		}},
		{"with_projected_rows", func(ctx context.Context) { executor.pipelineApplyWithSource(ctx, rows, "WITH x AS x", nil, true) }},
	} {
		t.Run(stage.name, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error {
				stage.run(withPipelineCancellation(ctx))
				return getExpressionFailure(ctx)
			})
		})
	}
}

func TestPipelineAggregateCopiesHealthy(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate_copy_parity"))
	row := pipelineRow{"x": int64(3)}
	templates := []pipelineAggregateProjection{{expression: "__nornic_agg_0", states: []pipelineAggregateState{{name: "count", expression: "*", count: 1}}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := pipelineCancellationForContext(ctx)
	copy := newPipelineAggregateGroup(row, templates, probe)
	require.NoError(t, probe.err)
	require.Equal(t, row, copy.first)
	copy.first["x"] = int64(9)
	copy.projections[0].states[0].count = 5
	require.Equal(t, int64(3), row["x"])
	require.Equal(t, int64(1), templates[0].states[0].count)
	for _, parent := range []context.Context{context.Background(), ctx} {
		groups, ok := executor.pipelineAggregateGroups(parent, pipelineRowsSource([]pipelineRow{row, row}), []returnProjection{{expr: "count(*)", alias: "c", isAggr: true}}, true)
		require.True(t, ok)
		require.Len(t, groups, 1)
		value, ok := groups[0].value(parent, executor, 0)
		require.True(t, ok)
		require.Equal(t, int64(2), value)
	}
}

func TestPipelineOutputBudgetBoundaries(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "output_budget"))
	// Sweep the position of the existing amortized sample, including copies
	// whose length is smaller than one interval and their enclosing frames.
	phases := make([]int, pipelineCancellationInterval)
	for i := range phases {
		phases[i] = i + 1
	}
	rows := make([]pipelineRow, 129)
	for i := range rows {
		rows[i] = pipelineRow{"k": int64(i), "x": int64(i)}
	}
	for _, clause := range []string{
		"WITH k, collect(DISTINCT x) AS xs ORDER BY k",
		"WITH k, x%3 AS m, collect(x) AS xs ORDER BY k,m",
		"RETURN k, collect(x) AS xs ORDER BY k",
		"RETURN k, count(*) AS c",
		"RETURN k, count(*) AS c ORDER BY c,k",
		"RETURN DISTINCT k, count(*) AS c ORDER BY c,k",
		"RETURN x+1 AS next ORDER BY x+1",
		"RETURN DISTINCT k AS key ORDER BY k",
		"RETURN DISTINCT k, sum(x) AS s",
		"RETURN percentileCont(x,0.5) AS p",
		"WITH DISTINCT *, x+1 AS next ORDER BY x SKIP 1 LIMIT 3 WHERE next>0",
		"WITH k, count(*) AS c",
		"WITH k, count(*) AS c ORDER BY k WHERE c>0",
	} {
		t.Run(clause, func(t *testing.T) {
			runPreparationBudgetSweep(t, func(ctx context.Context, probe *pipelineCancellation) {
				var ok bool
				if strings.HasPrefix(clause, "WITH") {
					_, ok = executor.pipelineApplyWith(ctx, rows, clause)
				} else {
					_, ok = executor.pipelineApplyReturn(ctx, rows, clause)
				}
				sampled := ctx.Value(preparationBudgetSampleKey{}).(*queryCacheSampleContext)
				_, cancelled := sampled.receipt()
				if !cancelled.IsZero() {
					require.False(t, ok)
					require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
				} else {
					require.True(t, ok, "healthy aggregate fixture, sample=%d", sampled.stop)
				}
			}, phases...)
		})
	}
	runPreparationBudgetSweep(t, func(ctx context.Context, probe *pipelineCancellation) {
		_, ok := executor.pipelineApplyReturn(ctx, nil, "RETURN count(*) AS c")
		sampled := ctx.Value(preparationBudgetSampleKey{}).(*queryCacheSampleContext)
		_, cancelled := sampled.receipt()
		if !cancelled.IsZero() {
			require.False(t, ok)
			require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
		} else {
			require.True(t, ok)
		}
	}, phases...)
}
