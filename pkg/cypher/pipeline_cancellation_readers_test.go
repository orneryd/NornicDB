package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"strings"
	"testing"
)

func TestPipelineCancellationReaderBoundaries(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	typed := make([]int, 257)
	list := make([]interface{}, 257)
	fields := map[string]int{}
	for index := range typed {
		typed[index] = index
		list[index] = int64(index)
		fields[string(rune(index+256))] = index
	}
	path := PathResult{Nodes: make([]*storage.Node, 257)}
	vector := CypherVector{Type: VectorFloat64, Floats: make([]float64, 257)}
	cases := []struct {
		name string
		run  func(*pipelineCancellation)
	}{
		{"size", func(p *pipelineCancellation) {
			_, _, _ = evaluateCypherSizeWithCancellation(strings.Repeat("é", 4097), p)
		}},
		{"compiled_add", func(p *pipelineCancellation) {
			operand := rowOperand{compiled: &compiledRowOperand{operator: '+', left: rowOperand{kind: rowOperandVariable, variable: "left"}, right: rowOperand{kind: rowOperandVariable, variable: "right"}}}
			_, _, _ = operand.evaluateWithCancellation(e, compiledRowScope{values: map[string]interface{}{"left": typed, "right": typed}}, p)
		}},
		{"add_left_conversion", func(p *pipelineCancellation) { _ = e.addWithCancellation(typed, list, p) }},
		{"add_right_conversion", func(p *pipelineCancellation) { _ = e.addWithCancellation(list, typed, p) }},
		{"add_scalar", func(p *pipelineCancellation) { _ = e.addWithCancellation(list, int64(1), p) }},
		{"small_concat", func(p *pipelineCancellation) { _ = concatPipelineStrings("a", "b", p) }},
		{"sort_rank", func(p *pipelineCancellation) { _ = compareValuesForSortWithCancellation(fields, fields, p) }},
		{"typed_key", func(p *pipelineCancellation) { _ = cypherEquivalenceKeyWithCancellation(fields, p) }},
		{"nested_equality", func(p *pipelineCancellation) {
			_ = cypherEqualityWithCancellation([]interface{}{typed}, []interface{}{typed}, p)
		}},
		{"nested_membership", func(p *pipelineCancellation) {
			_, _ = rowMembershipOfValuesWithCancellation(typed, []interface{}{typed}, false, p)
		}},
		{"vector", func(p *pipelineCancellation) { _ = compareVectorsWithCancellation(vector, vector, p) }},
		{"path", func(p *pipelineCancellation) {
			_, _, _ = compareCypherStructuredValuesWithCancellation(path, path, func(a, b interface{}) (int, bool) {
				if !p.check() {
					return 0, false
				}
				return 0, true
			}, p)
		}},
		{"projection_conversion", func(p *pipelineCancellation) {
			_, _, _, _ = e.evaluateRowMapProjectionWithCancellation("value{.*}", map[string]interface{}{"value": fields}, p)
		}},
		{"pad_left", func(p *pipelineCancellation) {
			_, _ = padRowStringWithCancellation(strings.Repeat("x", 257), 513, strings.Repeat("é", 257), true, p)
		}},
		{"pad_right", func(p *pipelineCancellation) {
			_, _ = padRowStringWithCancellation(strings.Repeat("x", 257), 513, strings.Repeat("é", 257), false, p)
		}},
		{"pad_truncate", func(p *pipelineCancellation) {
			_, _ = padRowStringWithCancellation(strings.Repeat("x", 257), 129, "x", false, p)
		}},
		{"reverse_conversion", func(p *pipelineCancellation) { _, _ = evaluateListAccessFunctionWithCancellation("reverse", typed, p) }},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			// Vary the existing shared budget so even a one-off copy following a
			// recursive reader lands on a sample. No production hooks are needed.
			for remaining := 0; remaining <= pipelineCancellationInterval; remaining++ {
				healthy := &pipelineCountingContext{Context: context.Background()}
				probe := pipelineCancellation{ctx: healthy, remaining: remaining}
				entry.run(&probe)
				for sample := 1; sample <= healthy.calls; sample++ {
					ctx := &pipelineCountingContext{Context: context.Background(), stop: sample}
					probe := pipelineCancellation{ctx: withExpressionFailureSlot(ctx), remaining: remaining}
					entry.run(&probe)
					if ctx.calls >= sample {
						require.ErrorIs(t, probe.err, context.Canceled)
					} else {
						require.NoError(t, probe.err)
					}
				}
			}
		})
	}
}

func TestPipelineReaderWrapperResults(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	value, _, ok, err := e.evaluateRowMapProjection("value{.*, other}", map[string]interface{}{"value": map[string]interface{}{"x": int64(1)}, "other": int64(2)})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, map[string]interface{}{"x": int64(1), "other": int64(2)}, value)
	_, ok = rowProjectionProperties(map[string]interface{}{})
	require.True(t, ok)
	require.Equal(t, []string{"x"}, appendSortedMapKeys(nil, map[string]interface{}{"x": 1}))
	_, ok = compareCypherComparableValues(int64(1), int64(2))
	require.True(t, ok)
	state := pipelineAggregateState{name: "collect", expression: "x"}
	require.True(t, state.add(context.Background(), e, pipelineRow{"x": int64(1)}))
	require.Equal(t, []interface{}{int64(1)}, state.values)
	form := reduceForm{}
	v, err := runReduceForm(form, int64(0), []interface{}{int64(1)}, func(a, b interface{}) (interface{}, error) { return a.(int64) + b.(int64), nil }, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), v)
	plan := planRowPredicate("x IN $items AND x>=0")
	memberships := compiledRowMemberships{}
	memberships.prepare(&plan.root, compiledRowScope{parameters: map[string]interface{}{"items": []interface{}{int64(1)}}})
	require.Positive(t, memberships.count)
	var empty interface{}
	var integer interface{} = int64(1)
	nilValue := reflect.ValueOf(&empty).Elem()
	integerValue := reflect.ValueOf(&integer).Elem()
	require.Zero(t, compareEquivalenceGoMapKeys(nilValue, nilValue, nil))
	require.Negative(t, compareEquivalenceGoMapKeys(nilValue, integerValue, nil))
	require.Positive(t, compareEquivalenceGoMapKeys(integerValue, nilValue, nil))
	for _, pair := range [][2]bool{{false, false}, {true, false}} {
		_ = compareEquivalenceGoMapKeys(reflect.ValueOf(pair[0]), reflect.ValueOf(pair[1]), nil)
	}
	for _, expr := range []string{"'x'=~'x'", "'é' IS NORMALIZED", "2^3", "1 NOT IN [2]", "properties(value)", "labels(value)", "date(null)"} {
		_, _, _ = e.evaluateRowValue(expr, map[string]interface{}{"value": map[string]interface{}{"labels": []string{"L"}}})
	}
	_, _, _ = e.evaluateRowValue("length(value)", map[string]interface{}{"value": map[string]interface{}{"relationships": []int{1, 2}}})
	_, _ = evaluateStaticListForPipeline("value.items", pipelineRow{"value": map[string]interface{}{"items": []int{1}}})
	_, _ = e.evaluateListForPipelineWithContext(context.Background(), "range(start,stop,step)", pipelineRow{"start": int64(1), "stop": int64(2), "step": int64(1)})
	_, err = e.runCorrelatedSubquery(context.Background(), "RETURN 1 AS x UNION RETURN 2 AS x", nil)
	require.NoError(t, err)
}
