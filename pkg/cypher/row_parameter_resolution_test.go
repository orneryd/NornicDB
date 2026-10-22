package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowParameterResolutionPreservesBindingPrecedence(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	request := withParams(context.Background(), map[string]interface{}{"value": "request"})
	outer := withValueBindings(request, pipelineRow{"$value": "outer"})
	for _, test := range []struct {
		name string
		ctx  context.Context
		row  pipelineRow
		want interface{}
	}{
		{"request", request, nil, "request"},
		{"outer", outer, nil, "outer"},
		{"row", outer, pipelineRow{"$value": "row"}, "row"},
		{"outer-null", withValueBindings(request, pipelineRow{"$value": nil}), nil, nil},
		{"row-null", outer, pipelineRow{"$value": nil}, nil},
		{"typed-list", withParams(context.Background(), map[string]interface{}{"value": []string{"a", "b"}}), nil, []string{"a", "b"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixtureAdmission1721Context, fixtureAdmission1721 := expressionTestContext(test.ctx, "$value")
			_ = fixtureAdmission1721Context
			value, resolved := executor.evaluateRowExpressionWithContext(fixtureAdmission1721, fixtureAdmission1721Context, "$value", test.row)
			require.True(t, resolved)
			require.Equal(t, test.want, value)
		})
	}
	ctx, admission := expressionTestContext(withParams(context.Background(), map[string]interface{}{"value": int64(3)}), "$value + 1")
	value, resolved := executor.evaluateRowExpressionWithContext(admission, ctx, "$value + 1", nil)
	require.True(t, resolved)
	require.Equal(t, int64(4), value)
}
