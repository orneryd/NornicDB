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
	assert.Equal(t, []interface{}{"a", nil, "1"}, exec.evaluateExpressionWithContext(ctx, expressionBraceView("toStringList(n.list)"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("toStringList(n.name)"), nodes, nil))

	// valueType
	assert.Equal(t, "NULL", exec.evaluateExpressionWithContext(ctx, expressionBraceView("valueType(null)"), nodes, nil))
	assert.Equal(t, "BOOLEAN NOT NULL", exec.evaluateExpressionWithContext(ctx, expressionBraceView("valueType(true)"), nodes, nil))
	assert.Equal(t, "INTEGER NOT NULL", exec.evaluateExpressionWithContext(ctx, expressionBraceView("valueType(1)"), nodes, nil))
	assert.Equal(t, "FLOAT NOT NULL", exec.evaluateExpressionWithContext(ctx, expressionBraceView("valueType(1.5)"), nodes, nil))
	assert.Equal(t, "STRING NOT NULL", exec.evaluateExpressionWithContext(ctx, expressionBraceView("valueType('x')"), nodes, nil))
	assert.Equal(t, "LIST<INTEGER NOT NULL> NOT NULL", exec.evaluateExpressionWithContext(ctx, expressionBraceView("valueType([1,2])"), nodes, nil))
	assert.Equal(t, "MAP NOT NULL", exec.evaluateExpressionWithContext(ctx, expressionBraceView("valueType({a:1})"), nodes, nil))

	// aggregation passthrough in expression context
	assert.EqualValues(t, 7, exec.evaluateExpressionWithContext(ctx, expressionBraceView("sum(7)"), nodes, nil))
	assert.EqualValues(t, 8, exec.evaluateExpressionWithContext(ctx, expressionBraceView("avg(8)"), nodes, nil))
	assert.EqualValues(t, 9, exec.evaluateExpressionWithContext(ctx, expressionBraceView("min(9)"), nodes, nil))
	assert.EqualValues(t, 10, exec.evaluateExpressionWithContext(ctx, expressionBraceView("max(10)"), nodes, nil))
	assert.Equal(t, []interface{}{}, exec.evaluateExpressionWithContext(ctx, expressionBraceView("collect(null)"), nodes, nil))
	assert.Equal(t, []interface{}{int64(1)}, exec.evaluateExpressionWithContext(ctx, expressionBraceView("collect(1)"), nodes, nil))

	// aliases
	assert.Equal(t, "alice", exec.evaluateExpressionWithContext(ctx, expressionBraceView("lower('ALICE')"), nodes, nil))
	assert.Equal(t, "ALICE", exec.evaluateExpressionWithContext(ctx, expressionBraceView("upper('alice')"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("lower(1)"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("upper(1)"), nodes, nil))

	// trim family
	assert.Equal(t, "Alice", exec.evaluateExpressionWithContext(ctx, expressionBraceView("trim(n.name)"), nodes, nil))
	assert.Equal(t, "Alice  ", exec.evaluateExpressionWithContext(ctx, expressionBraceView("ltrim(n.name)"), nodes, nil))
	assert.Equal(t, "  Alice", exec.evaluateExpressionWithContext(ctx, expressionBraceView("rtrim(n.name)"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("trim(42)"), nodes, nil))

	// replace / split
	assert.Equal(t, "a-c", exec.evaluateExpressionWithContext(ctx, expressionBraceView("replace('a-b','b','c')"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("replace('a','b')"), nodes, nil))
	splitVal := exec.evaluateExpressionWithContext(ctx, expressionBraceView("split('a,b,c',',')"), nodes, nil)
	splitList, ok := splitVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"a", "b", "c"}, splitList)
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("split('abc')"), nodes, nil))

	// substring / left / right
	assert.Equal(t, "bc", exec.evaluateExpressionWithContext(ctx, expressionBraceView("substring('abcd',1,2)"), nodes, nil))
	assert.Equal(t, "", exec.evaluateExpressionWithContext(ctx, expressionBraceView("substring('abcd',99)"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("substring('abcd')"), nodes, nil))
	assert.Equal(t, "ab", exec.evaluateExpressionWithContext(ctx, expressionBraceView("left('abcd',2)"), nodes, nil))
	assert.Equal(t, "abcd", exec.evaluateExpressionWithContext(ctx, expressionBraceView("left('abcd',99)"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("left('abcd')"), nodes, nil))
	assert.Equal(t, "cd", exec.evaluateExpressionWithContext(ctx, expressionBraceView("right('abcd',2)"), nodes, nil))
	assert.Equal(t, "abcd", exec.evaluateExpressionWithContext(ctx, expressionBraceView("right('abcd',99)"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("right('abcd')"), nodes, nil))

	// lpad / rpad
	assert.Equal(t, "  ab", exec.evaluateExpressionWithContext(ctx, expressionBraceView("lpad('ab',4)"), nodes, nil))
	assert.Equal(t, "xxab", exec.evaluateExpressionWithContext(ctx, expressionBraceView("lpad('ab',4,'x')"), nodes, nil))
	assert.Equal(t, "ab  ", exec.evaluateExpressionWithContext(ctx, expressionBraceView("rpad('ab',4)"), nodes, nil))
	assert.Equal(t, "abxx", exec.evaluateExpressionWithContext(ctx, expressionBraceView("rpad('ab',4,'x')"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("lpad('ab','bad')"), nodes, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(ctx, expressionBraceView("rpad('ab','bad')"), nodes, nil))
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
		originalBraceView("(CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END)"),
		nodes, nil, nil, nil, nil, 0,
	))
	assert.Equal(t, "adult", exec.evaluateExpressionWithContext(ctx,
		expressionBraceView("CASE\nWHEN n.age > 18 THEN 'adult' ELSE 'minor'\nEND"),
		nodes, nil,
	))

	// Subscripts dispatch through the full evaluator, before the function-only
	// evaluator. Read the same persisted fixture through public Cypher.
	createTestNode(t, exec, string(nodes["n"].ID), nodes["n"].Labels, nodes["n"].Properties)
	read := func(expression string) interface{} {
		return typedNumericRead(t, exec, "MATCH (n:Person) RETURN "+expression+" AS v", nil)
	}
	assert.EqualValues(t, int64(10), read("n.nums[0]"))
	assert.EqualValues(t, int64(30), read("n.nums[-1]"))
	sliceVal := read("n.nums[1..3]")
	sliceList, ok := sliceVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{int64(20), int64(30)}, sliceList)

	// Neo4j string characters use substring; [] requires a list receiver.
	assert.Equal(t, "e", read("substring(n.text,1,1)"))
	_, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n.text[1] AS v", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")

	// Ensure list concatenation is not misdetected as indexing.
	concatVal := exec.evaluateExpressionWithContextFullFunctions(ctx,
		expressionBraceView("n.nums + [40]"),
		nodes, nil, nil, nil, nil, 0,
	)
	concatList, ok := concatVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{int64(10), int64(20), int64(30), int64(40)}, concatList)

	// Map literal evaluation.
	m := exec.evaluateExpressionWithContextFullFunctions(ctx,
		expressionBraceView("{person: n.name, ok: n.alive, tags: ['x','y']}"),
		nodes, nil, nil, nil, nil, 0,
	)
	mm, ok := m.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "Alice", mm["person"])
	assert.Equal(t, true, mm["ok"])

	// length(pathVar) with explicit path map and pathLength fallback branch.
	assert.EqualValues(t, int64(3), exec.evaluateExpressionWithContextFullFunctions(ctx,
		expressionBraceView("length(p)"),
		nodes, nil, paths, nil, nil, 0,
	))
	assert.EqualValues(t, int64(5), exec.evaluateExpressionWithContextFullFunctions(ctx,
		expressionBraceView("length(anyPathVar)"),
		nodes, nil, nil, nil, nil, 5,
	))

	// exists(prop) branch.
	assert.Equal(t, true, exec.evaluateExpressionWithContextFullFunctions(ctx,
		expressionBraceView("exists(n.name)"),
		nodes, nil, nil, nil, nil, 0,
	))
	assert.Equal(t, false, exec.evaluateExpressionWithContextFullFunctions(ctx,
		expressionBraceView("exists(n.missing)"),
		nodes, nil, nil, nil, nil, 0,
	))
}

func BenchmarkEvaluateExpressionWithContext_HotPredicate(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench"))
	ctx := context.Background()
	nodes := map[string]*storage.Node{
		"n": {
			ID:     "n1",
			Labels: []string{"Entity"},
			Properties: map[string]interface{}{
				"uuid":  "entity-123",
				"name":  "Alice Johnson",
				"email": "alice@example.com",
			},
		},
	}
	expr := "n.uuid IS NOT NULL AND n.name CONTAINS 'Alice' AND n.email STARTS WITH 'alice'"

	// Prime one-time executor/query setup before measurement so the benchmark
	// reflects steady-state repeated row evaluation, which is the hot path.
	require.Equal(b, true, exec.evaluateExpressionWithContext(ctx, originalBraceView(expr), nodes, nil))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if exec.evaluateExpressionWithContext(ctx, originalBraceView(expr), nodes, nil) != true {
			b.Fatal("predicate should remain true")
		}
	}
}
