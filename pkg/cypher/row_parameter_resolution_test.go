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
			value, resolved := executor.evaluateRowExpressionWithContext(test.ctx, "$value", test.row)
			require.True(t, resolved)
			require.Equal(t, test.want, value)
		})
	}
	value, resolved := executor.evaluateRowExpressionWithContext(withParams(context.Background(), map[string]interface{}{"value": int64(3)}), "$value + 1", nil)
	require.True(t, resolved)
	require.Equal(t, int64(4), value)
}
