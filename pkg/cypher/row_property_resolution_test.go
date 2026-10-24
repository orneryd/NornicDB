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
	_, admission := expressionTestContext(context.Background(), "record.value")
	for _, evaluate := range []struct {
		name string
		read func(admittedRelationshipStatement, string, map[string]interface{}) (interface{}, bool, error)
	}{
		{"compiled", executor.evaluateRowValue},
		{"text", executor.evaluateRowValueText},
	} {
		t.Run(evaluate.name, func(t *testing.T) {
			ctx := withExpressionFailureSlot(context.Background())
			value, resolved, err := evaluate.read(admission, "record.value", pipelineRow{rowStatementContextKey: ctx})
			require.NoError(t, err)
			require.False(t, resolved)
			require.Nil(t, value)
			require.NoError(t, getExpressionFailure(ctx))
			value, resolved, err = evaluate.read(admission, "record.value", pipelineRow{"record": nil, rowStatementContextKey: ctx})
			require.NoError(t, err)
			require.True(t, resolved)
			require.Nil(t, value)
			outer := withValueBindings(ctx, pipelineRow{"record": map[string]interface{}{"value": "outer"}})
			value, resolved, err = evaluate.read(admission, "record.value", pipelineRow{rowStatementContextKey: outer})
			require.NoError(t, err)
			require.True(t, resolved)
			require.Equal(t, "outer", value)
			value, resolved, err = evaluate.read(admission, "record.value", pipelineRow{"record": map[string]interface{}{"value": "inner"}, rowStatementContextKey: outer})
			require.NoError(t, err)
			require.True(t, resolved)
			require.Equal(t, "inner", value)
		})
	}
	ctx := withExpressionFailureSlot(context.Background())
	fixtureAdmission1911Context, fixtureAdmission1911 := expressionTestContext(ctx, "record.value")
	_ = fixtureAdmission1911Context
	value, err := executor.checkedPropertyExpression(fixtureAdmission1911, fixtureAdmission1911Context, "record.value", nil)
	require.Nil(t, value)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}
