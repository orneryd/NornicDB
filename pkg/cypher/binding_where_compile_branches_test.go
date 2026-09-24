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

	pred := exec.getCompiledBindingWhere(ctx, "a.name STARTS WITH $pname")
	require.True(t, pred(bind, params))

	pred = exec.getCompiledBindingWhere(ctx, "a.name ENDS WITH 'ce'")
	require.True(t, pred(bind, params))

	pred = exec.getCompiledBindingWhere(ctx, "a.name CONTAINS 'li'")
	require.True(t, pred(bind, params))

	pred = exec.getCompiledBindingWhere(ctx, "a.tag IS NULL")
	require.True(t, pred(bind, params))
	pred = exec.getCompiledBindingWhere(ctx, "a.tag IS NOT NULL")
	require.False(t, pred(bind, params))

	pred = exec.getCompiledBindingWhere(ctx, "a.age IN [30, 31]")
	require.True(t, pred(bind, params))
	pred = exec.getCompiledBindingWhere(ctx, "a.age NOT IN [30, 31]")
	require.False(t, pred(bind, params))
	pred = exec.getCompiledBindingWhere(ctx, "a.age IN $ages")
	require.True(t, pred(bind, params))

	pred = exec.getCompiledBindingWhere(ctx, "a.age >= 30 AND a.age < 31")
	require.True(t, pred(bind, params))
	pred = exec.getCompiledBindingWhere(ctx, "NOT (a.age < 30)")
	require.True(t, pred(bind, params))
	pred = exec.getCompiledBindingWhere(ctx, "a = b OR a.age = 30")
	require.True(t, pred(bind, params))
	pred = exec.getCompiledBindingWhere(ctx, "a <> b")
	require.True(t, pred(bind, params))

	pred = exec.getCompiledBindingWhere(ctx, "id(a) = $nid")
	require.False(t, pred(bind, params))

	// generic fallback path
	require.True(t, exec.evaluateBindingWhereGeneric(ctx, bind, "a.age = 30", params))
	require.True(t, exec.evaluateBindingWhereGeneric(ctx, bind, "a.name CONTAINS 'li'", params))
	require.False(t, exec.evaluateBindingWhereGeneric(ctx, bind, "a.age > 31", params))

	v, ok := exec.resolveBindingFallbackValueWithOk(ctx, "$pname", bind, params)
	require.True(t, ok)
	require.Equal(t, "Ali", v)
	_, ok = exec.resolveBindingFallbackValueWithOk(ctx, "$missing", bind, params)
	require.False(t, ok)

	require.True(t, exec.evaluateBindingExpressionAsBoolean(ctx, bind, "1 = 1", nil))
	require.False(t, exec.evaluateBindingExpressionAsBoolean(ctx, bind, "42", nil))

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
	p1 := exec.getCompiledBindingWhere(ctx, "a.age = 1")
	p2 := exec.getCompiledBindingWhere(ctx, "a.age = 1")
	require.NotNil(t, p1)
	require.NotNil(t, p2)
}
