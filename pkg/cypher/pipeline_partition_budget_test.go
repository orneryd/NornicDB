package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelinePartitionSourceBudget(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "partition_source_budget"))
	patterns := make([]struct {
		variable string
		nodes    []*storage.Node
	}, 129)
	for i := range patterns {
		patterns[i].variable = fmt.Sprintf("n%d", i)
		patterns[i].nodes = []*storage.Node{{ID: storage.NodeID(patterns[i].variable)}}
	}
	plan := returnProjectionPlanFor("RETURN count(*) AS count")
	runPreparationBudgetSweep(t, func(ctx context.Context, _ *pipelineCancellation) {
		groups, handled, err := executor.tryCartesianAggregatePartitions(ctx, patterns, plan, 1)
		require.True(t, handled)
		_, cancelled := ctx.Value(preparationBudgetSampleKey{}).(*queryCacheSampleContext).receipt()
		if !cancelled.IsZero() {
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, groups)
		} else {
			require.NoError(t, err)
			require.Len(t, groups, 1)
		}
	})
}

func TestPipelineCallCombinationBudget(t *testing.T) {
	columns := make([]string, 129)
	rows := make([][]interface{}, 129)
	for i := range columns {
		columns[i] = fmt.Sprintf("col%d", i)
		rows[i] = []interface{}{int64(i)}
	}
	phases := make([]int, pipelineCancellationInterval)
	for i := range phases {
		phases[i] = i + 1
	}
	for _, results := range [][]callTailRowResult{nil, {{res: &ExecuteResult{Columns: columns, Rows: rows}}}, {{res: &ExecuteResult{Columns: columns, Rows: rows}}, {res: &ExecuteResult{Columns: columns, Rows: rows}}}} {
		runPreparationBudgetSweep(t, func(ctx context.Context, probe *pipelineCancellation) {
			result, err := combineCallTailRowResultsWithCancellation(results, columns, probe)
			_, cancelled := ctx.Value(preparationBudgetSampleKey{}).(*queryCacheSampleContext).receipt()
			if !cancelled.IsZero() {
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, columns, result.Columns)
			}
		}, phases...)
	}
}

func TestPipelineCallRowBudget(t *testing.T) {
	wide := pipelineRow{}
	columns := make([]string, 129)
	values := make([]interface{}, 129)
	for i := range columns {
		columns[i] = fmt.Sprintf("column%d", i)
		values[i] = int64(i)
		wide[columns[i]] = values[i]
	}
	seed := &ExecuteResult{Columns: columns, Rows: [][]interface{}{values, values}}
	phases := make([]int, pipelineCancellationInterval)
	for i := range phases {
		phases[i] = i + 1
	}
	for _, stage := range []struct {
		name string
		run  func(context.Context, *pipelineCancellation)
	}{
		{"append", func(ctx context.Context, p *pipelineCancellation) {
			appendCallReturnRows(ctx, nil, wide, columns, seed.Rows)
		}},
		{"merge", func(_ context.Context, p *pipelineCancellation) { mergeCallPipelineRowsWithCancellation(wide, wide, p) }},
		{"decode", func(ctx context.Context, p *pipelineCancellation) { callPipelineRowsFromResult(ctx, seed) }},
		{"encode", func(ctx context.Context, p *pipelineCancellation) {
			callPipelineResultFromRows(ctx, []pipelineRow{wide, wide}, seed, nil, columns)
		}},
		{"discover_columns", func(ctx context.Context, p *pipelineCancellation) {
			callPipelineResultFromRows(ctx, []pipelineRow{wide, wide}, nil, nil, nil)
		}},
		{"optional_missing", func(ctx context.Context, p *pipelineCancellation) {
			ok := true
			_, err := pipelineApplyOptionally(ctx, []pipelineRow{wide, wide}, func([]pipelineRow) ([]pipelineRow, []string, bool, error) { return nil, columns, true, nil }, &ok)
			if p.err != nil {
				require.ErrorIs(t, err, context.Canceled)
			}
		}},
		{"optional_produced", func(ctx context.Context, p *pipelineCancellation) {
			ok := true
			_, err := pipelineApplyOptionally(ctx, []pipelineRow{wide, wide}, func([]pipelineRow) ([]pipelineRow, []string, bool, error) {
				return []pipelineRow{wide, wide}, columns, true, nil
			}, &ok)
			if p.err != nil {
				require.ErrorIs(t, err, context.Canceled)
			}
		}},
	} {
		t.Run(stage.name, func(t *testing.T) { runPreparationBudgetSweep(t, stage.run, phases...) })
	}
}

func TestPipelineCallScopeBudget(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_scope_budget"))
	rows := make([]pipelineRow, 3)
	for i := range rows {
		rows[i] = pipelineRow{"x": int64(i)}
		for j := 0; j < 129; j++ {
			rows[i][fmt.Sprintf("col%d", j)] = int64(j)
		}
	}
	for _, query := range []string{"CALL { WITH x RETURN x AS y }", "CALL { WITH * RETURN * }", "CALL { WITH x CREATE (:CallBudget) }", "CALL { RETURN 1 AS y }"} {
		t.Run(query, func(t *testing.T) {
			runPreparationBudgetSweep(t, func(ctx context.Context, probe *pipelineCancellation) {
				metadata := &pipelineCallMetadata{scope: map[string]struct{}{}}
				_, _, handled, err := executor.pipelineApplyCallSubqueryWithMetadata(ctx, rows, query, metadata)
				require.True(t, handled)
				_, cancelled := ctx.Value(preparationBudgetSampleKey{}).(*queryCacheSampleContext).receipt()
				if !cancelled.IsZero() {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}
