package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Planning may have no receiver yet. That is unresolved, distinct from a
// bound NULL; materialization still requires a resolved value.
func TestRowPropertyResolutionPreservesPartialScope(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, evaluate := range []struct {
		name string
		read func(string, map[string]interface{}) (interface{}, bool, error)
	}{
		{"compiled", executor.evaluateRowValue},
		{"text", executor.evaluateRowValueText},
	} {
		t.Run(evaluate.name, func(t *testing.T) {
			ctx := withExpressionFailureSlot(context.Background())
			value, resolved, err := evaluate.read("record.value", pipelineRow{rowStatementContextKey: ctx})
			require.NoError(t, err)
			require.False(t, resolved)
			require.Nil(t, value)
			require.NoError(t, getExpressionFailure(ctx))
			value, resolved, err = evaluate.read("record.value", pipelineRow{"record": nil, rowStatementContextKey: ctx})
			require.NoError(t, err)
			require.True(t, resolved)
			require.Nil(t, value)
			outer := withValueBindings(ctx, pipelineRow{"record": map[string]interface{}{"value": "outer"}})
			value, resolved, err = evaluate.read("record.value", pipelineRow{rowStatementContextKey: outer})
			require.NoError(t, err)
			require.True(t, resolved)
			require.Equal(t, "outer", value)
			value, resolved, err = evaluate.read("record.value", pipelineRow{"record": map[string]interface{}{"value": "inner"}, rowStatementContextKey: outer})
			require.NoError(t, err)
			require.True(t, resolved)
			require.Equal(t, "inner", value)
		})
	}
	ctx := withExpressionFailureSlot(context.Background())
	value, err := executor.checkedPropertyExpression(ctx, "record.value", nil)
	require.Nil(t, value)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}
