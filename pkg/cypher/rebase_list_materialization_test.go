package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRebaseListMaterializationPreservesBindingsAndFailures(t *testing.T) {
	withPluginFunctions(t, map[string]interface{}{
		"rebase.failed": func() (map[string]interface{}, error) {
			return nil, fmt.Errorf("producer failed")
		},
	})
	e := NewStorageExecutor(newTestMemoryEngine(t))
	nodes := map[string]*storage.Node{"n": {ID: "n", Properties: map[string]interface{}{"v": int64(7)}}}
	ctx := withExpressionFailureSlot(context.Background())
	value := e.evaluateExpressionWithContextFull(ctx, "[n.v, [1, 2][1]]", nodes, nil, nil, nil, nil, 0)
	require.NoError(t, getExpressionFailure(ctx))
	require.Equal(t, []interface{}{int64(7), int64(2)}, value)

	ctx = withExpressionFailureSlot(context.Background())
	e.evaluateExpressionWithContextFull(ctx, "[rebase.failed()]", nodes, nil, nil, nil, nil, 0)
	require.ErrorContains(t, getExpressionFailure(ctx), "producer failed")
}
