package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineIndexValuePreparation(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	for _, value := range []interface{}{nil, true, int64(1), uint(2), 1.5, "small"} {
		require.True(t, boundedPipelineIndexValue(value))
	}
	for _, value := range []interface{}{strings.Repeat("x", 4097), []int{1}, map[string]interface{}{"x": 1}} {
		p := pipelineCancellation{ctx: context.Background()}
		_, _, ok := e.parseSimpleIndexedInParamWithCancellation("n", "n.x IN $xs", map[string]interface{}{"xs": []interface{}{value}}, &p)
		require.False(t, ok, "expensive keys use the cancellable predicate reader")
		_, used, err := e.collectIndexedPropertyValues(nodePatternInfo{}, nil, e.storage.GetSchema(), []indexedPropertyValues{{[]string{"L"}, "x", []interface{}{value}}}, &p)
		require.NoError(t, err)
		require.False(t, used)
	}
	_, values, ok := e.parseSimpleIndexedInParam("n", "n.x IN $xs", map[string]interface{}{"xs": []interface{}{nil, int64(1), int64(1)}})
	require.True(t, ok)
	require.Equal(t, []interface{}{int64(1)}, values)
	_, values, ok = e.parseSimpleIndexedInParam("n", "n.x IN $xs", map[string]interface{}{"xs": []int{}})
	require.True(t, ok)
	require.Empty(t, values)
	for _, probe := range []*pipelineCancellation{nil, {ctx: context.Background()}} {
		nodes, used, err := e.collectIndexedPropertyValues(nodePatternInfo{}, nil, e.storage.GetSchema(), nil, probe)
		require.NoError(t, err)
		require.True(t, used)
		require.Empty(t, nodes)
	}
	// A long label list must also sample before any individual lookup.
	labels := make([]string, 257)
	for i := range labels {
		labels[i] = "L"
	}
	ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
	p := pipelineCancellation{ctx: ctx}
	_, used, err := e.collectIndexedPropertyValues(nodePatternInfo{}, nil, e.storage.GetSchema(), []indexedPropertyValues{{labels, "x", nil}}, &p)
	require.True(t, used)
	require.ErrorIs(t, err, context.Canceled)
}
