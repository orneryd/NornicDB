package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBindingWhereCompileAndEvaluate_Branches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	a := &storage.Node{ID: "10", Properties: map[string]interface{}{"name": "Alice", "age": int64(30), "tag": nil}}
	b := &storage.Node{ID: "20", Properties: map[string]interface{}{"name": "Bob", "age": int64(40), "roles": []interface{}{"admin"}}}
	bind := binding{"a": a, "b": b}
	params := map[string]interface{}{"pname": "Ali", "ages": []interface{}{int64(30), int64(50)}, "nid": "20"}

	fixtureAdmission59Context, fixtureAdmission59 := expressionTestContext(ctx, "a.name STARTS WITH $pname")
	_ = fixtureAdmission59
	pred := exec.getCompiledBindingWhere(fixtureAdmission59, fixtureAdmission59Context, "a.name STARTS WITH $pname")
	require.True(t, pred(bind, params))

	fixtureAdmission60Context, fixtureAdmission60 := expressionTestContext(ctx, "a.name ENDS WITH 'ce'")
	_ = fixtureAdmission60
	pred = exec.getCompiledBindingWhere(fixtureAdmission60, fixtureAdmission60Context, "a.name ENDS WITH 'ce'")
	require.True(t, pred(bind, params))

	fixtureAdmission61Context, fixtureAdmission61 := expressionTestContext(ctx, "a.name CONTAINS 'li'")
	_ = fixtureAdmission61
	pred = exec.getCompiledBindingWhere(fixtureAdmission61, fixtureAdmission61Context, "a.name CONTAINS 'li'")
	require.True(t, pred(bind, params))

	fixtureAdmission62Context, fixtureAdmission62 := expressionTestContext(ctx, "a.tag IS NULL")
	_ = fixtureAdmission62
	pred = exec.getCompiledBindingWhere(fixtureAdmission62, fixtureAdmission62Context, "a.tag IS NULL")
	require.True(t, pred(bind, params))
	fixtureAdmission63Context, fixtureAdmission63 := expressionTestContext(ctx, "a.tag IS NOT NULL")
	_ = fixtureAdmission63
	pred = exec.getCompiledBindingWhere(fixtureAdmission63, fixtureAdmission63Context, "a.tag IS NOT NULL")
	require.False(t, pred(bind, params))

	fixtureAdmission64Context, fixtureAdmission64 := expressionTestContext(ctx, "a.age IN [30, 31]")
	_ = fixtureAdmission64
	pred = exec.getCompiledBindingWhere(fixtureAdmission64, fixtureAdmission64Context, "a.age IN [30, 31]")
	require.True(t, pred(bind, params))
	fixtureAdmission65Context, fixtureAdmission65 := expressionTestContext(ctx, "a.age NOT IN [30, 31]")
	_ = fixtureAdmission65
	pred = exec.getCompiledBindingWhere(fixtureAdmission65, fixtureAdmission65Context, "a.age NOT IN [30, 31]")
	require.False(t, pred(bind, params))
	fixtureAdmission66Context, fixtureAdmission66 := expressionTestContext(ctx, "a.age IN $ages")
	_ = fixtureAdmission66
	pred = exec.getCompiledBindingWhere(fixtureAdmission66, fixtureAdmission66Context, "a.age IN $ages")
	require.True(t, pred(bind, params))

	fixtureAdmission67Context, fixtureAdmission67 := expressionTestContext(ctx, "a.age >= 30 AND a.age < 31")
	_ = fixtureAdmission67
	pred = exec.getCompiledBindingWhere(fixtureAdmission67, fixtureAdmission67Context, "a.age >= 30 AND a.age < 31")
	require.True(t, pred(bind, params))
	fixtureAdmission68Context, fixtureAdmission68 := expressionTestContext(ctx, "NOT (a.age < 30)")
	_ = fixtureAdmission68
	pred = exec.getCompiledBindingWhere(fixtureAdmission68, fixtureAdmission68Context, "NOT (a.age < 30)")
	require.True(t, pred(bind, params))
	fixtureAdmission69Context, fixtureAdmission69 := expressionTestContext(ctx, "a = b OR a.age = 30")
	_ = fixtureAdmission69
	pred = exec.getCompiledBindingWhere(fixtureAdmission69, fixtureAdmission69Context, "a = b OR a.age = 30")
	require.True(t, pred(bind, params))
	fixtureAdmission70Context, fixtureAdmission70 := expressionTestContext(ctx, "a <> b")
	_ = fixtureAdmission70
	pred = exec.getCompiledBindingWhere(fixtureAdmission70, fixtureAdmission70Context, "a <> b")
	require.True(t, pred(bind, params))

	fixtureAdmission71Context, fixtureAdmission71 := expressionTestContext(ctx, "id(a) = $nid")
	_ = fixtureAdmission71
	pred = exec.getCompiledBindingWhere(fixtureAdmission71, fixtureAdmission71Context, "id(a) = $nid")
	require.False(t, pred(bind, params))

	// generic fallback path
	fixtureAdmission72Context, fixtureAdmission72 := expressionTestContext(ctx, "a.age = 30")
	_ = fixtureAdmission72
	require.True(t, exec.evaluateBindingWhereGeneric(fixtureAdmission72, fixtureAdmission72Context, bind, "a.age = 30", params))
	fixtureAdmission73Context, fixtureAdmission73 := expressionTestContext(ctx, "a.name CONTAINS 'li'")
	_ = fixtureAdmission73
	require.True(t, exec.evaluateBindingWhereGeneric(fixtureAdmission73, fixtureAdmission73Context, bind, "a.name CONTAINS 'li'", params))
	fixtureAdmission74Context, fixtureAdmission74 := expressionTestContext(ctx, "a.age > 31")
	_ = fixtureAdmission74
	require.False(t, exec.evaluateBindingWhereGeneric(fixtureAdmission74, fixtureAdmission74Context, bind, "a.age > 31", params))

	fixtureAdmission1033Context, fixtureAdmission1033 := expressionTestContext(ctx, "$pname")
	_ = fixtureAdmission1033Context
	v, ok := exec.resolveBindingFallbackValueWithOk(fixtureAdmission1033, fixtureAdmission1033Context, "$pname", bind, params)
	require.True(t, ok)
	require.Equal(t, "Ali", v)
	fixtureAdmission1034Context, fixtureAdmission1034 := expressionTestContext(ctx, "$missing")
	_ = fixtureAdmission1034Context
	_, ok = exec.resolveBindingFallbackValueWithOk(fixtureAdmission1034, fixtureAdmission1034Context, "$missing", bind, params)
	require.False(t, ok)

	fixtureAdmission75Context, fixtureAdmission75 := expressionTestContext(ctx, "1 = 1")
	_ = fixtureAdmission75
	require.True(t, exec.evaluateBindingExpressionAsBoolean(fixtureAdmission75, fixtureAdmission75Context, bind, "1 = 1", nil))
	fixtureAdmission76Context, fixtureAdmission76 := expressionTestContext(ctx, "42")
	_ = fixtureAdmission76
	require.False(t, exec.evaluateBindingExpressionAsBoolean(fixtureAdmission76, fixtureAdmission76Context, bind, "42", nil))

	require.True(t, exec.compareNodeIDs("10", "2", ">"))
	require.True(t, exec.compareNodeIDs("b", "a", ">"))
	require.False(t, exec.compareNodeIDs("a", "b", ">="))
}

func TestBindingWhereResolverAndLiteralListHelpers(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	n := &storage.Node{ID: "1", Properties: map[string]interface{}{"name": "A"}}
	bind := binding{"n": n}

	vals, ok := parseBindingLiteralList("[1, 'x', true, null]")
	require.True(t, ok)
	require.Len(t, vals, 4)
	_, ok = parseBindingLiteralList("bad")
	require.False(t, ok)

	resolver, ok := exec.compileBindingValueResolver("n.name")
	require.True(t, ok)
	v, ok := resolver(bind, nil)
	require.True(t, ok)
	require.Equal(t, "A", v)

	resolver, ok = exec.compileBindingValueResolver("$x")
	require.True(t, ok)
	v, ok = resolver(bind, map[string]interface{}{"x": int64(7)})
	require.True(t, ok)
	require.EqualValues(t, 7, v)

	resolver, ok = exec.compileBindingValueResolver("n")
	require.True(t, ok)
	v, ok = resolver(bind, nil)
	require.True(t, ok)
	require.Equal(t, n, v)

	_, ok = exec.compileBindingValueResolver("n bad")
	require.False(t, ok)

	pred := exec.makeCompiledBindingMembershipPredicate(func(binding, map[string]interface{}) (interface{}, bool) {
		return "x", true
	}, []interface{}{"x", []interface{}{1}})
	require.True(t, pred.predicate()(bind, nil))

	require.True(t, normalizeBindingWhereClause("a\nAND\tb") == "a AND b")
	fixtureAdmission77Context, fixtureAdmission77 := expressionTestContext(ctx, "a.age = 1")
	_ = fixtureAdmission77
	p1 := exec.getCompiledBindingWhere(fixtureAdmission77, fixtureAdmission77Context, "a.age = 1")
	fixtureAdmission78Context, fixtureAdmission78 := expressionTestContext(ctx, "a.age = 1")
	_ = fixtureAdmission78
	p2 := exec.getCompiledBindingWhere(fixtureAdmission78, fixtureAdmission78Context, "a.age = 1")
	require.NotNil(t, p1)
	require.NotNil(t, p2)
}
