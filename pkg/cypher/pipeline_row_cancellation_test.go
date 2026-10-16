package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPipelineRowReadersCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_readers"))
	wide := pipelineRow{"value": int64(7)}
	projections := make([]pipelineRowProjection, 1025)
	returns := make([]returnProjection, 1025)
	for i := range projections {
		name := fmt.Sprintf("column%d", i)
		wide[name] = int64(i)
		projections[i] = pipelineRowProjection{"value", name}
		returns[i] = returnProjection{expr: "value", alias: name}
	}
	clone := func() pipelineRow {
		out := pipelineRow{}
		for k, v := range wide {
			out[k] = v
		}
		return out
	}
	for _, stage := range []struct {
		name string
		run  func(context.Context)
	}{
		{"star_projection_and_clear", func(ctx context.Context) {
			executor.pipelineProjectWithRow(ctx, wide, pipelineRowWith{star: true}, clone(), nil)
		}},
		{"wide_projections_and_scope", func(ctx context.Context) {
			executor.pipelineProjectWithRow(ctx, wide, pipelineRowWith{projections: projections, where: "value=7"}, nil, clone())
		}},
		{"return_projection", func(ctx context.Context) { executor.pipelineProjectReturnRow(ctx, returns, wide) }},
		{"wide_materialization", func(ctx context.Context) { materializePipelineSource(ctx, pipelineRowsSource([]pipelineRow{wide})) }},
		{"growing_materialization", func(ctx context.Context) {
			rows := make([]pipelineRow, 1025)
			for i := range rows {
				rows[i] = pipelineRow{"value": i}
			}
			materializePipelineSource(ctx, pipelineRowsSource(rows))
		}},
		{"unwind_wide_copy", func(ctx context.Context) {
			source, _, ok := executor.pipelineUnwindSource(ctx, []pipelineRow{wide}, []pipelineClause{{kind: pipelineClauseUnwind, text: "UNWIND [1] AS x"}})
			if ok {
				source(func(pipelineRow) bool { return true })
			}
		}},
		{"node_match_wide_copy", func(ctx context.Context) {
			source, _, err := executor.pipelineNodeMatchSourceWithHint(ctx, pipelineRowsSource([]pipelineRow{wide, wide}), "MATCH (n)", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
			if err == nil && source != nil {
				source(func(pipelineRow) bool { return true })
			}
		}},
		{"streamed_node_match_wide_copy", func(ctx context.Context) {
			source, _, err := executor.pipelineNodeMatchSourceWithHint(ctx, pipelineRowsSource([]pipelineRow{wide, wide}), "MATCH (n)", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1, streamScan: true})
			if err == nil && source != nil {
				source(func(pipelineRow) bool { return true })
			}
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

func TestPipelineRowReadersHealthyParity(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_parity"))
	row := pipelineRow{"value": int64(7)}
	plan := pipelineRowWith{star: true, projections: []pipelineRowProjection{{"value", "alias"}}, where: "value=7"}
	want, wantAccepted, wantOK := executor.pipelineProjectWithRow(context.Background(), row, plan, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actual, accepted, ok := executor.pipelineProjectWithRow(withPipelineCancellation(ctx), row, plan, pipelineRow{"stale": 1}, pipelineRow{"stale": 2})
	require.Equal(t, want, actual)
	require.Equal(t, wantAccepted, accepted)
	require.Equal(t, wantOK, ok)
	stale := pipelineRow{"stale": 1}
	require.True(t, clearPipelineRow(stale, nil))
	require.Empty(t, stale)
}

func TestPipelineExpressionBindingCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "expression_bindings"))
	wide := pipelineRow{"value": int64(7)}
	for i := 0; i < 1025; i++ {
		wide[fmt.Sprintf("binding%d", i)] = int64(i)
	}
	for _, stage := range []struct {
		name string
		run  func(context.Context)
	}{
		{"copy_for_context_binding", func(ctx context.Context) {
			executor.evaluateRowExpressionWithContext(withValueBindings(ctx, map[string]interface{}{"extra": 1}), "value+extra", wide)
		}},
		{"many_context_bindings", func(ctx context.Context) {
			executor.evaluateRowExpressionWithContext(withValueBindings(ctx, wide), "value+1", pipelineRow{})
		}},
		{"map_order_scope", func(ctx context.Context) { executor.evaluateRowExpressionWithContext(ctx, "{value:value}", wide) }},
	} {
		t.Run(stage.name, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error { stage.run(ctx); return getExpressionFailure(ctx) })
		})
	}
}

func TestPipelineExpressionChildrenBorrowExplicitProbe(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "explicit_expression_probe"))
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	sampled := &queryCacheSampleContext{Context: base, cancel: cancel, stop: 2}
	ctx := withPipelineCancellation(withExpressionFailureSlot(sampled))
	outer := pipelineCancellationForContext(ctx)
	explicit := &pipelineCancellation{ctx: ctx}
	ctx = withQueryParams(ctx, map[string]interface{}{"items": make([]interface{}, 1025)})
	_, ok := executor.evaluateRowExpressionWithContext(ctx, "size(items)", pipelineRow{}, explicit)
	require.False(t, ok)
	require.ErrorIs(t, explicit.err, context.Canceled)
	require.NoError(t, outer.err)
	require.Zero(t, outer.remaining, "nested parameter preparation sampled a second probe")
}
