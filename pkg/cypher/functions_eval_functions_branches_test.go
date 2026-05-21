package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateExpression_FunctionUtilityBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	nodes := map[string]*storage.Node{
		"n": {
			ID:     "n1",
			Labels: []string{"T"},
			Properties: map[string]interface{}{
				"name": "  Alice  ",
				"list": []interface{}{"a", nil, 1},
			},
		},
	}
	ctx := context.Background()

	// toStringList
	assert.Equal(t, []interface{}{"a", nil, "1"}, exec.evaluateExpressionWithContext(ctx, "toStringList(n.list)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "toStringList(n.name)", nodes, nil))

	// valueType
	assert.Equal(t, "NULL", exec.evaluateExpressionWithContext(ctx, "valueType(null)", nodes, nil))
	assert.Equal(t, "BOOLEAN", exec.evaluateExpressionWithContext(ctx, "valueType(true)", nodes, nil))
	assert.Equal(t, "INTEGER", exec.evaluateExpressionWithContext(ctx, "valueType(1)", nodes, nil))
	assert.Equal(t, "FLOAT", exec.evaluateExpressionWithContext(ctx, "valueType(1.5)", nodes, nil))
	assert.Equal(t, "STRING", exec.evaluateExpressionWithContext(ctx, "valueType('x')", nodes, nil))
	assert.Equal(t, "LIST", exec.evaluateExpressionWithContext(ctx, "valueType([1,2])", nodes, nil))
	assert.Equal(t, "MAP", exec.evaluateExpressionWithContext(ctx, "valueType({a:1})", nodes, nil))

	// aggregation passthrough in expression context
	assert.EqualValues(t, 7, exec.evaluateExpressionWithContext(ctx, "sum(7)", nodes, nil))
	assert.EqualValues(t, 8, exec.evaluateExpressionWithContext(ctx, "avg(8)", nodes, nil))
	assert.EqualValues(t, 9, exec.evaluateExpressionWithContext(ctx, "min(9)", nodes, nil))
	assert.EqualValues(t, 10, exec.evaluateExpressionWithContext(ctx, "max(10)", nodes, nil))
	assert.Equal(t, []interface{}{}, exec.evaluateExpressionWithContext(ctx, "collect(null)", nodes, nil))
	assert.Equal(t, []interface{}{int64(1)}, exec.evaluateExpressionWithContext(ctx, "collect(1)", nodes, nil))

	// aliases
	assert.Equal(t, "alice", exec.evaluateExpressionWithContext(ctx, "lower('ALICE')", nodes, nil))
	assert.Equal(t, "ALICE", exec.evaluateExpressionWithContext(ctx, "upper('alice')", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "lower(1)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "upper(1)", nodes, nil))

	// trim family
	assert.Equal(t, "Alice", exec.evaluateExpressionWithContext(ctx, "trim(n.name)", nodes, nil))
	assert.Equal(t, "Alice  ", exec.evaluateExpressionWithContext(ctx, "ltrim(n.name)", nodes, nil))
	assert.Equal(t, "  Alice", exec.evaluateExpressionWithContext(ctx, "rtrim(n.name)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "trim(42)", nodes, nil))

	// replace / split
	assert.Equal(t, "a-c", exec.evaluateExpressionWithContext(ctx, "replace('a-b','b','c')", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "replace('a','b')", nodes, nil))
	splitVal := exec.evaluateExpressionWithContext(ctx, "split('a,b,c',',')", nodes, nil)
	splitList, ok := splitVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"a", "b", "c"}, splitList)
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "split('abc')", nodes, nil))

	// substring / left / right
	assert.Equal(t, "bc", exec.evaluateExpressionWithContext(ctx, "substring('abcd',1,2)", nodes, nil))
	assert.Equal(t, "", exec.evaluateExpressionWithContext(ctx, "substring('abcd',99)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "substring('abcd')", nodes, nil))
	assert.Equal(t, "ab", exec.evaluateExpressionWithContext(ctx, "left('abcd',2)", nodes, nil))
	assert.Equal(t, "abcd", exec.evaluateExpressionWithContext(ctx, "left('abcd',99)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "left('abcd')", nodes, nil))
	assert.Equal(t, "cd", exec.evaluateExpressionWithContext(ctx, "right('abcd',2)", nodes, nil))
	assert.Equal(t, "abcd", exec.evaluateExpressionWithContext(ctx, "right('abcd',99)", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "right('abcd')", nodes, nil))

	// lpad / rpad
	assert.Equal(t, "  ab", exec.evaluateExpressionWithContext(ctx, "lpad('ab',4)", nodes, nil))
	assert.Equal(t, "xxab", exec.evaluateExpressionWithContext(ctx, "lpad('ab',4,'x')", nodes, nil))
	assert.Equal(t, "ab  ", exec.evaluateExpressionWithContext(ctx, "rpad('ab',4)", nodes, nil))
	assert.Equal(t, "abxx", exec.evaluateExpressionWithContext(ctx, "rpad('ab',4,'x')", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "lpad('ab','bad')", nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, "rpad('ab','bad')", nodes, nil))
}

func TestEvaluateExpression_FullFunctionAdvancedBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	nodes := map[string]*storage.Node{
		"n": {
			ID:     "n1",
			Labels: []string{"Person", "Node"},
			Properties: map[string]interface{}{
				"name":  "Alice",
				"age":   int64(21),
				"text":  "hello",
				"nums":  []interface{}{int64(10), int64(20), int64(30)},
				"alive": true,
			},
		},
	}
	paths := map[string]*PathResult{
		"p": {Length: 3},
	}
	ctx := context.Background()

	// Parenthesized stripping + CASE.
	assert.Equal(t, "adult", exec.evaluateExpressionWithContextFullFunctions(ctx,
		"(CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END)",
		nodes, nil, nil, nil, nil, 0,
	))

	// Array indexing and slicing.
	assert.EqualValues(t, int64(10), exec.evaluateExpressionWithContextFullFunctions(ctx,
		"n.nums[0]",
		nodes, nil, nil, nil, nil, 0,
	))
	assert.EqualValues(t, int64(30), exec.evaluateExpressionWithContextFullFunctions(ctx,
		"n.nums[-1]",
		nodes, nil, nil, nil, nil, 0,
	))
	sliceVal := exec.evaluateExpressionWithContextFullFunctions(ctx,
		"n.nums[1..3]",
		nodes, nil, nil, nil, nil, 0,
	)
	sliceList, ok := sliceVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{int64(20), int64(30)}, sliceList)

	// Indexing on string.
	assert.Equal(t, "e", exec.evaluateExpressionWithContextFullFunctions(ctx,
		"n.text[1]",
		nodes, nil, nil, nil, nil, 0,
	))

	// Ensure list concatenation is not misdetected as indexing.
	concatVal := exec.evaluateExpressionWithContextFullFunctions(ctx,
		"n.nums + [40]",
		nodes, nil, nil, nil, nil, 0,
	)
	concatList, ok := concatVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{int64(10), int64(20), int64(30), int64(40)}, concatList)

	// Map literal evaluation.
	m := exec.evaluateExpressionWithContextFullFunctions(ctx,
		"{person: n.name, ok: n.alive, tags: ['x','y']}",
		nodes, nil, nil, nil, nil, 0,
	)
	mm, ok := m.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "Alice", mm["person"])
	assert.Equal(t, true, mm["ok"])

	// length(pathVar) with explicit path map and pathLength fallback branch.
	assert.EqualValues(t, int64(3), exec.evaluateExpressionWithContextFullFunctions(ctx,
		"length(p)",
		nodes, nil, paths, nil, nil, 0,
	))
	assert.EqualValues(t, int64(5), exec.evaluateExpressionWithContextFullFunctions(ctx,
		"length(anyPathVar)",
		nodes, nil, nil, nil, nil, 5,
	))

	// exists(prop) branch.
	assert.Equal(t, true, exec.evaluateExpressionWithContextFullFunctions(ctx,
		"exists(n.name)",
		nodes, nil, nil, nil, nil, 0,
	))
	assert.Equal(t, false, exec.evaluateExpressionWithContextFullFunctions(ctx,
		"exists(n.missing)",
		nodes, nil, nil, nil, nil, 0,
	))
}
