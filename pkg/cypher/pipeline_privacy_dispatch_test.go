package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPipelineRegistryDispatcherCancellation(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	items := make([]interface{}, 513)
	pairs := make([]interface{}, len(items))
	fields := map[string]interface{}{}
	for i := range items {
		items[i] = int64(i)
		pairs[i] = []interface{}{fmt.Sprint(i), int64(i)}
		fields[fmt.Sprint(i)] = i
	}
	row := pipelineRow{"items": items, "pairs": pairs, "fields": fields}
	for _, expression := range []string{
		"apoc.coll.pairs(items)", "apoc.coll.frequencies(items)",
		"apoc.map.merge(fields,fields)", "apoc.map.fromPairs(pairs)", "apoc.map.fromLists(items,items)",
		"apoc.map.removeKey(fields,'missing')", "apoc.map.setKey(fields,'extra',1)",
		"apoc.map.clean(fields,[],items)", "apoc.map.clean(fields,['missing'],[])",
	} {
		t.Run(expression, func(t *testing.T) {
			runPipelineCancelPointStage(t, func(ctx context.Context) error {
				ctx = withPipelineCancellation(withValueBindings(ctx, row))
				_ = e.evaluateExpressionWithContextFullFunctions(ctx, expression, nil, nil, nil, nil, nil, 0)
				return getExpressionFailure(ctx)
			})
		})
	}
	t.Run("compiled_registry", func(t *testing.T) {
		runPipelineCancelPointStage(t, func(ctx context.Context) error {
			ctx = withPipelineCancellation(ctx)
			compiled, ok := e.tryCompileTraversalFunctionCall(ctx, "keys(n)")
			require.True(t, ok)
			_ = compiled(traversalOptRow{nodes: binding{"n": &storage.Node{Properties: fields}}})
			return getExpressionFailure(ctx)
		})
	})
}
