package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParameterMergeCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "parameter_merges"))
	params := make(map[string]interface{}, 1025)
	nodes := make(binding, 1025)
	for i := 0; i < 1025; i++ {
		name := fmt.Sprintf("value%d", i)
		params[name] = int64(i)
		nodes[name] = &storage.Node{ID: storage.NodeID(name)}
	}
	executor.shellParams = params
	for _, stage := range []struct {
		name string
		run  func(context.Context)
	}{
		{"inherited", func(ctx context.Context) {
			mergeInheritedQueryParams(withQueryParams(ctx, params), map[string]interface{}{"extra": 1})
		}},
		{"shell", func(ctx context.Context) { executor.mergeShellParams(ctx, map[string]interface{}{"extra": 1}) }},
		{"bind_row", func(ctx context.Context) { bindParameterRow(withQueryParams(ctx, params), pipelineRow{}) }},
		{"binding_predicate", func(ctx context.Context) {
			executor.evaluateBindingExpressionAsBoolean(ctx, nodes, "value0 IS NOT NULL", params)
		}},
	} {
		t.Run(stage.name, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error { stage.run(ctx); return getExpressionFailure(ctx) })
		})
	}
	require.True(t, executor.shellParamsMu.TryLock(), "cancelled shell copy stranded its lock")
	executor.shellParamsMu.Unlock()
}

func TestParameterMergeHealthyPrecedence(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_precedence"))
	executor.shellParams = map[string]interface{}{"shared": 1, "shell": 2}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = withPipelineCancellation(withQueryParams(ctx, map[string]interface{}{"inherited": 3, "shared": 4}))
	merged := executor.mergeShellParams(ctx, mergeInheritedQueryParams(ctx, map[string]interface{}{"shared": 5}))
	require.Equal(t, map[string]interface{}{"shared": 5, "shell": 2, "inherited": 3}, merged)
	require.Equal(t, map[string]interface{}{"shared": 1, "shell": 2}, executor.shellParams)
}
