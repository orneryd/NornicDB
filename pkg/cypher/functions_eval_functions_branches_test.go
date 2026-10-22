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
	fixtureAdmission355Context, fixtureAdmission355 := expressionTestContext(ctx, "toStringList(n.list)")
	_ = fixtureAdmission355
	assert.Equal(t, []interface{}{"a", nil, "1"}, exec.evaluateExpressionWithContext(fixtureAdmission355, fixtureAdmission355Context, "toStringList(n.list)", nodes, nil))
	fixtureAdmission356Context, fixtureAdmission356 := expressionTestContext(ctx, "toStringList(n.name)")
	_ = fixtureAdmission356
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission356, fixtureAdmission356Context, "toStringList(n.name)", nodes, nil))

	// valueType
	fixtureAdmission357Context, fixtureAdmission357 := expressionTestContext(ctx, "valueType(null)")
	_ = fixtureAdmission357
	assert.Equal(t, "NULL", exec.evaluateExpressionWithContext(fixtureAdmission357, fixtureAdmission357Context, "valueType(null)", nodes, nil))
	fixtureAdmission358Context, fixtureAdmission358 := expressionTestContext(ctx, "valueType(true)")
	_ = fixtureAdmission358
	assert.Equal(t, "BOOLEAN NOT NULL", exec.evaluateExpressionWithContext(fixtureAdmission358, fixtureAdmission358Context, "valueType(true)", nodes, nil))
	fixtureAdmission359Context, fixtureAdmission359 := expressionTestContext(ctx, "valueType(1)")
	_ = fixtureAdmission359
	assert.Equal(t, "INTEGER NOT NULL", exec.evaluateExpressionWithContext(fixtureAdmission359, fixtureAdmission359Context, "valueType(1)", nodes, nil))
	fixtureAdmission360Context, fixtureAdmission360 := expressionTestContext(ctx, "valueType(1.5)")
	_ = fixtureAdmission360
	assert.Equal(t, "FLOAT NOT NULL", exec.evaluateExpressionWithContext(fixtureAdmission360, fixtureAdmission360Context, "valueType(1.5)", nodes, nil))
	fixtureAdmission361Context, fixtureAdmission361 := expressionTestContext(ctx, "valueType('x')")
	_ = fixtureAdmission361
	assert.Equal(t, "STRING NOT NULL", exec.evaluateExpressionWithContext(fixtureAdmission361, fixtureAdmission361Context, "valueType('x')", nodes, nil))
	fixtureAdmission362Context, fixtureAdmission362 := expressionTestContext(ctx, "valueType([1,2])")
	_ = fixtureAdmission362
	assert.Equal(t, "LIST<INTEGER NOT NULL> NOT NULL", exec.evaluateExpressionWithContext(fixtureAdmission362, fixtureAdmission362Context, "valueType([1,2])", nodes, nil))
	fixtureAdmission363Context, fixtureAdmission363 := expressionTestContext(ctx, "valueType({a:1})")
	_ = fixtureAdmission363
	assert.Equal(t, "MAP NOT NULL", exec.evaluateExpressionWithContext(fixtureAdmission363, fixtureAdmission363Context, "valueType({a:1})", nodes, nil))

	// aggregation passthrough in expression context
	fixtureAdmission364Context, fixtureAdmission364 := expressionTestContext(ctx, "sum(7)")
	_ = fixtureAdmission364
	assert.EqualValues(t, 7, exec.evaluateExpressionWithContext(fixtureAdmission364, fixtureAdmission364Context, "sum(7)", nodes, nil))
	fixtureAdmission365Context, fixtureAdmission365 := expressionTestContext(ctx, "avg(8)")
	_ = fixtureAdmission365
	assert.EqualValues(t, 8, exec.evaluateExpressionWithContext(fixtureAdmission365, fixtureAdmission365Context, "avg(8)", nodes, nil))
	fixtureAdmission366Context, fixtureAdmission366 := expressionTestContext(ctx, "min(9)")
	_ = fixtureAdmission366
	assert.EqualValues(t, 9, exec.evaluateExpressionWithContext(fixtureAdmission366, fixtureAdmission366Context, "min(9)", nodes, nil))
	fixtureAdmission367Context, fixtureAdmission367 := expressionTestContext(ctx, "max(10)")
	_ = fixtureAdmission367
	assert.EqualValues(t, 10, exec.evaluateExpressionWithContext(fixtureAdmission367, fixtureAdmission367Context, "max(10)", nodes, nil))
	fixtureAdmission368Context, fixtureAdmission368 := expressionTestContext(ctx, "collect(null)")
	_ = fixtureAdmission368
	assert.Equal(t, []interface{}{}, exec.evaluateExpressionWithContext(fixtureAdmission368, fixtureAdmission368Context, "collect(null)", nodes, nil))
	fixtureAdmission369Context, fixtureAdmission369 := expressionTestContext(ctx, "collect(1)")
	_ = fixtureAdmission369
	assert.Equal(t, []interface{}{int64(1)}, exec.evaluateExpressionWithContext(fixtureAdmission369, fixtureAdmission369Context, "collect(1)", nodes, nil))

	// aliases
	fixtureAdmission370Context, fixtureAdmission370 := expressionTestContext(ctx, "lower('ALICE')")
	_ = fixtureAdmission370
	assert.Equal(t, "alice", exec.evaluateExpressionWithContext(fixtureAdmission370, fixtureAdmission370Context, "lower('ALICE')", nodes, nil))
	fixtureAdmission371Context, fixtureAdmission371 := expressionTestContext(ctx, "upper('alice')")
	_ = fixtureAdmission371
	assert.Equal(t, "ALICE", exec.evaluateExpressionWithContext(fixtureAdmission371, fixtureAdmission371Context, "upper('alice')", nodes, nil))
	fixtureAdmission372Context, fixtureAdmission372 := expressionTestContext(ctx, "lower(1)")
	_ = fixtureAdmission372
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission372, fixtureAdmission372Context, "lower(1)", nodes, nil))
	fixtureAdmission373Context, fixtureAdmission373 := expressionTestContext(ctx, "upper(1)")
	_ = fixtureAdmission373
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission373, fixtureAdmission373Context, "upper(1)", nodes, nil))

	// trim family
	fixtureAdmission374Context, fixtureAdmission374 := expressionTestContext(ctx, "trim(n.name)")
	_ = fixtureAdmission374
	assert.Equal(t, "Alice", exec.evaluateExpressionWithContext(fixtureAdmission374, fixtureAdmission374Context, "trim(n.name)", nodes, nil))
	fixtureAdmission375Context, fixtureAdmission375 := expressionTestContext(ctx, "ltrim(n.name)")
	_ = fixtureAdmission375
	assert.Equal(t, "Alice  ", exec.evaluateExpressionWithContext(fixtureAdmission375, fixtureAdmission375Context, "ltrim(n.name)", nodes, nil))
	fixtureAdmission376Context, fixtureAdmission376 := expressionTestContext(ctx, "rtrim(n.name)")
	_ = fixtureAdmission376
	assert.Equal(t, "  Alice", exec.evaluateExpressionWithContext(fixtureAdmission376, fixtureAdmission376Context, "rtrim(n.name)", nodes, nil))
	fixtureAdmission377Context, fixtureAdmission377 := expressionTestContext(ctx, "trim(42)")
	_ = fixtureAdmission377
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission377, fixtureAdmission377Context, "trim(42)", nodes, nil))

	// replace / split
	fixtureAdmission378Context, fixtureAdmission378 := expressionTestContext(ctx, "replace('a-b','b','c')")
	_ = fixtureAdmission378
	assert.Equal(t, "a-c", exec.evaluateExpressionWithContext(fixtureAdmission378, fixtureAdmission378Context, "replace('a-b','b','c')", nodes, nil))
	fixtureAdmission379Context, fixtureAdmission379 := expressionTestContext(ctx, "replace('a','b')")
	_ = fixtureAdmission379
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission379, fixtureAdmission379Context, "replace('a','b')", nodes, nil))
	fixtureAdmission380Context, fixtureAdmission380 := expressionTestContext(ctx, "split('a,b,c',',')")
	_ = fixtureAdmission380
	splitVal := exec.evaluateExpressionWithContext(fixtureAdmission380, fixtureAdmission380Context, "split('a,b,c',',')", nodes, nil)
	splitList, ok := splitVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{"a", "b", "c"}, splitList)
	fixtureAdmission381Context, fixtureAdmission381 := expressionTestContext(ctx, "split('abc')")
	_ = fixtureAdmission381
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission381, fixtureAdmission381Context, "split('abc')", nodes, nil))

	// substring / left / right
	fixtureAdmission382Context, fixtureAdmission382 := expressionTestContext(ctx, "substring('abcd',1,2)")
	_ = fixtureAdmission382
	assert.Equal(t, "bc", exec.evaluateExpressionWithContext(fixtureAdmission382, fixtureAdmission382Context, "substring('abcd',1,2)", nodes, nil))
	fixtureAdmission383Context, fixtureAdmission383 := expressionTestContext(ctx, "substring('abcd',99)")
	_ = fixtureAdmission383
	assert.Equal(t, "", exec.evaluateExpressionWithContext(fixtureAdmission383, fixtureAdmission383Context, "substring('abcd',99)", nodes, nil))
	fixtureAdmission384Context, fixtureAdmission384 := expressionTestContext(ctx, "substring('abcd')")
	_ = fixtureAdmission384
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission384, fixtureAdmission384Context, "substring('abcd')", nodes, nil))
	fixtureAdmission385Context, fixtureAdmission385 := expressionTestContext(ctx, "left('abcd',2)")
	_ = fixtureAdmission385
	assert.Equal(t, "ab", exec.evaluateExpressionWithContext(fixtureAdmission385, fixtureAdmission385Context, "left('abcd',2)", nodes, nil))
	fixtureAdmission386Context, fixtureAdmission386 := expressionTestContext(ctx, "left('abcd',99)")
	_ = fixtureAdmission386
	assert.Equal(t, "abcd", exec.evaluateExpressionWithContext(fixtureAdmission386, fixtureAdmission386Context, "left('abcd',99)", nodes, nil))
	fixtureAdmission387Context, fixtureAdmission387 := expressionTestContext(ctx, "left('abcd')")
	_ = fixtureAdmission387
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission387, fixtureAdmission387Context, "left('abcd')", nodes, nil))
	fixtureAdmission388Context, fixtureAdmission388 := expressionTestContext(ctx, "right('abcd',2)")
	_ = fixtureAdmission388
	assert.Equal(t, "cd", exec.evaluateExpressionWithContext(fixtureAdmission388, fixtureAdmission388Context, "right('abcd',2)", nodes, nil))
	fixtureAdmission389Context, fixtureAdmission389 := expressionTestContext(ctx, "right('abcd',99)")
	_ = fixtureAdmission389
	assert.Equal(t, "abcd", exec.evaluateExpressionWithContext(fixtureAdmission389, fixtureAdmission389Context, "right('abcd',99)", nodes, nil))
	fixtureAdmission390Context, fixtureAdmission390 := expressionTestContext(ctx, "right('abcd')")
	_ = fixtureAdmission390
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission390, fixtureAdmission390Context, "right('abcd')", nodes, nil))

	// lpad / rpad
	fixtureAdmission391Context, fixtureAdmission391 := expressionTestContext(ctx, "lpad('ab',4)")
	_ = fixtureAdmission391
	assert.Equal(t, "  ab", exec.evaluateExpressionWithContext(fixtureAdmission391, fixtureAdmission391Context, "lpad('ab',4)", nodes, nil))
	fixtureAdmission392Context, fixtureAdmission392 := expressionTestContext(ctx, "lpad('ab',4,'x')")
	_ = fixtureAdmission392
	assert.Equal(t, "xxab", exec.evaluateExpressionWithContext(fixtureAdmission392, fixtureAdmission392Context, "lpad('ab',4,'x')", nodes, nil))
	fixtureAdmission393Context, fixtureAdmission393 := expressionTestContext(ctx, "rpad('ab',4)")
	_ = fixtureAdmission393
	assert.Equal(t, "ab  ", exec.evaluateExpressionWithContext(fixtureAdmission393, fixtureAdmission393Context, "rpad('ab',4)", nodes, nil))
	fixtureAdmission394Context, fixtureAdmission394 := expressionTestContext(ctx, "rpad('ab',4,'x')")
	_ = fixtureAdmission394
	assert.Equal(t, "abxx", exec.evaluateExpressionWithContext(fixtureAdmission394, fixtureAdmission394Context, "rpad('ab',4,'x')", nodes, nil))
	fixtureAdmission395Context, fixtureAdmission395 := expressionTestContext(ctx, "lpad('ab','bad')")
	_ = fixtureAdmission395
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission395, fixtureAdmission395Context, "lpad('ab','bad')", nodes, nil))
	fixtureAdmission396Context, fixtureAdmission396 := expressionTestContext(ctx, "rpad('ab','bad')")
	_ = fixtureAdmission396
	assert.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission396, fixtureAdmission396Context, "rpad('ab','bad')", nodes, nil))
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
	fixtureAdmission397Context, fixtureAdmission397 := expressionTestContext(ctx, "(CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END)")
	_ = fixtureAdmission397
	assert.Equal(t, "adult", exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission397, fixtureAdmission397Context,
		"(CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END)",
		nodes, nil, nil, nil, nil, 0,
	))
	fixtureAdmission398Context, fixtureAdmission398 := expressionTestContext(ctx, "CASE\nWHEN n.age > 18 THEN 'adult' ELSE 'minor'\nEND")
	_ = fixtureAdmission398
	assert.Equal(t, "adult", exec.evaluateExpressionWithContext(fixtureAdmission398, fixtureAdmission398Context,
		"CASE\nWHEN n.age > 18 THEN 'adult' ELSE 'minor'\nEND",
		nodes, nil,
	))

	// Array indexing and slicing.
	fixtureAdmission399Context, fixtureAdmission399 := expressionTestContext(ctx, "n.nums[0]")
	_ = fixtureAdmission399
	assert.EqualValues(t, int64(10), exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission399, fixtureAdmission399Context,
		"n.nums[0]",
		nodes, nil, nil, nil, nil, 0,
	))
	fixtureAdmission400Context, fixtureAdmission400 := expressionTestContext(ctx, "n.nums[-1]")
	_ = fixtureAdmission400
	assert.EqualValues(t, int64(30), exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission400, fixtureAdmission400Context,
		"n.nums[-1]",
		nodes, nil, nil, nil, nil, 0,
	))
	fixtureAdmission401Context, fixtureAdmission401 := expressionTestContext(ctx, "n.nums[1..3]")
	_ = fixtureAdmission401
	sliceVal := exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission401, fixtureAdmission401Context,
		"n.nums[1..3]",
		nodes, nil, nil, nil, nil, 0,
	)
	sliceList, ok := sliceVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{int64(20), int64(30)}, sliceList)

	// A string has no element access: Neo4j 5.26.30 raises a TypeError for
	// n.text[1] ("String(...) is not a collection or a map"), and so does
	// every statement here. The shared subscript (evaluateCypherSubscript)
	// gives the context evaluator no value and records that error.
	stringIndexCtx := withExpressionFailureSlot(ctx)
	fixtureAdmission402Context, fixtureAdmission402 := expressionTestContext(stringIndexCtx, "n.text[1]")
	_ = fixtureAdmission402
	assert.Nil(t, exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission402, fixtureAdmission402Context,
		"n.text[1]",
		nodes, nil, nil, nil, nil, 0,
	))
	stringIndexErr := getExpressionFailure(stringIndexCtx)
	require.Error(t, stringIndexErr)
	assert.Contains(t, statusText(stringIndexErr), "Neo.ClientError.Statement.TypeError")

	// Ensure list concatenation is not misdetected as indexing.
	fixtureAdmission403Context, fixtureAdmission403 := expressionTestContext(ctx, "n.nums + [40]")
	_ = fixtureAdmission403
	concatVal := exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission403, fixtureAdmission403Context,
		"n.nums + [40]",
		nodes, nil, nil, nil, nil, 0,
	)
	concatList, ok := concatVal.([]interface{})
	require.True(t, ok)
	assert.Equal(t, []interface{}{int64(10), int64(20), int64(30), int64(40)}, concatList)

	// Map literal evaluation.
	fixtureAdmission404Context, fixtureAdmission404 := expressionTestContext(ctx, "{person: n.name, ok: n.alive, tags: ['x','y']}")
	_ = fixtureAdmission404
	m := exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission404, fixtureAdmission404Context,
		"{person: n.name, ok: n.alive, tags: ['x','y']}",
		nodes, nil, nil, nil, nil, 0,
	)
	mm, ok := m.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "Alice", mm["person"])
	assert.Equal(t, true, mm["ok"])

	// length(pathVar) with explicit path map and pathLength fallback branch.
	fixtureAdmission405Context, fixtureAdmission405 := expressionTestContext(ctx, "length(p)")
	_ = fixtureAdmission405
	assert.EqualValues(t, int64(3), exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission405, fixtureAdmission405Context,
		"length(p)",
		nodes, nil, paths, nil, nil, 0,
	))
	fixtureAdmission406Context, fixtureAdmission406 := expressionTestContext(ctx, "length(anyPathVar)")
	_ = fixtureAdmission406
	assert.EqualValues(t, int64(5), exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission406, fixtureAdmission406Context,
		"length(anyPathVar)",
		nodes, nil, nil, nil, nil, 5,
	))

	// exists(prop) branch.
	fixtureAdmission407Context, fixtureAdmission407 := expressionTestContext(ctx, "exists(n.name)")
	_ = fixtureAdmission407
	assert.Equal(t, true, exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission407, fixtureAdmission407Context,
		"exists(n.name)",
		nodes, nil, nil, nil, nil, 0,
	))
	fixtureAdmission408Context, fixtureAdmission408 := expressionTestContext(ctx, "exists(n.missing)")
	_ = fixtureAdmission408
	assert.Equal(t, false, exec.evaluateExpressionWithContextFullFunctions(fixtureAdmission408, fixtureAdmission408Context,
		"exists(n.missing)",
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

	var fixtureAdmission409 admittedRelationshipStatement
	ctx, fixtureAdmission409 = expressionTestContext(ctx, expr)
	_ = fixtureAdmission409
	// Prime one-time executor/query setup before measurement so the benchmark
	// reflects steady-state repeated row evaluation, which is the hot path.
	require.Equal(b, true, exec.evaluateExpressionWithContext(fixtureAdmission409, ctx, expr, nodes, nil))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if exec.evaluateExpressionWithContext(fixtureAdmission409, ctx, expr, nodes, nil) != true {
			b.Fatal("predicate should remain true")
		}
	}
}
