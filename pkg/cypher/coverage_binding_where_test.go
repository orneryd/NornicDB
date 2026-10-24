package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Coverage for the bindingWherePredicate compilers in binding_where_compile.go.
// We drive each compiler directly with carefully constructed bindings and
// param maps so no full query pipeline is needed.

func freshExecutorForBindingWhere(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "binding-where-coverage")
	return NewStorageExecutor(ns)
}

func makeBindingNode(id storage.NodeID, props map[string]any) *storage.Node {
	return &storage.Node{ID: id, Labels: []string{"X"}, Properties: props}
}

// ============================================================================
// compileBindingValueResolver — every resolution path.
// ============================================================================

func TestCompileBindingValueResolver_AllPaths(t *testing.T) {
	e := freshExecutorForBindingWhere(t)

	t.Run("empty expression returns ok=false", func(t *testing.T) {
		_, ok := e.compileBindingValueResolver("")
		require.False(t, ok)
	})

	t.Run("integer literal", func(t *testing.T) {
		r, ok := e.compileBindingValueResolver("42")
		require.True(t, ok)
		v, ok := r(nil, nil)
		require.True(t, ok)
		require.Equal(t, int64(42), v)
	})

	t.Run("string literal", func(t *testing.T) {
		r, ok := e.compileBindingValueResolver(`'alice'`)
		require.True(t, ok)
		v, ok := r(nil, nil)
		require.True(t, ok)
		require.Equal(t, "alice", v)
	})

	t.Run("$param resolves from params map", func(t *testing.T) {
		r, ok := e.compileBindingValueResolver("$name")
		require.True(t, ok)
		v, ok := r(nil, map[string]interface{}{"name": "Bob"})
		require.True(t, ok)
		require.Equal(t, "Bob", v)
	})

	t.Run("$param with nil params returns ok=false", func(t *testing.T) {
		r, ok := e.compileBindingValueResolver("$name")
		require.True(t, ok)
		_, ok = r(nil, nil)
		require.False(t, ok)
	})

	t.Run("bare $ rejected", func(t *testing.T) {
		_, ok := e.compileBindingValueResolver("$")
		require.False(t, ok)
	})

	t.Run("variable.property reads node property", func(t *testing.T) {
		r, ok := e.compileBindingValueResolver("n.name")
		require.True(t, ok)
		b := binding{"n": makeBindingNode("n1", map[string]any{"name": "Alice"})}
		v, ok := r(b, nil)
		require.True(t, ok)
		require.Equal(t, "Alice", v)
	})

	t.Run("variable.property with missing variable returns ok=false", func(t *testing.T) {
		r, ok := e.compileBindingValueResolver("missing.name")
		require.True(t, ok)
		_, ok = r(binding{}, nil)
		require.False(t, ok)
	})

	t.Run("variable.property tolerates whitespace around dot", func(t *testing.T) {
		// TrimSpace normalises "n . name" → varName="n", propName="name".
		r, ok := e.compileBindingValueResolver("n . name")
		require.True(t, ok)
		b := binding{"n": makeBindingNode("n1", map[string]any{"name": "Alice"})}
		v, ok := r(b, nil)
		require.True(t, ok)
		require.Equal(t, "Alice", v)
	})

	t.Run("variable.property with internal whitespace in property name is rejected", func(t *testing.T) {
		// "n.first name" → propName has an internal space and gets rejected
		// by the ContainsAny check.
		_, ok := e.compileBindingValueResolver("n.first name")
		require.False(t, ok)
	})

	t.Run("bare identifier returns the whole node", func(t *testing.T) {
		r, ok := e.compileBindingValueResolver("n")
		require.True(t, ok)
		node := makeBindingNode("n1", nil)
		v, ok := r(binding{"n": node}, nil)
		require.True(t, ok)
		require.Equal(t, node, v)
	})

	t.Run("bare identifier missing from binding returns ok=false", func(t *testing.T) {
		r, ok := e.compileBindingValueResolver("absent")
		require.True(t, ok)
		_, ok = r(binding{}, nil)
		require.False(t, ok)
	})

	t.Run("garbage expression rejected", func(t *testing.T) {
		_, ok := e.compileBindingValueResolver("not a real expr [&^]")
		require.False(t, ok)
	})
}

// ============================================================================
// compileBindingInPredicate — every branch (literal list, $param list,
// resolved list, negation).
// ============================================================================

func TestCompileBindingInPredicate_AllBranches(t *testing.T) {
	e := freshExecutorForBindingWhere(t)

	build := func(t *testing.T, expr, op string, negate bool) bindingWherePredicate {
		t.Helper()
		pred, ok := e.compileBindingInPredicate(expr, op, negate)
		require.True(t, ok, "compileBindingInPredicate failed for %q (op=%q)", expr, op)
		return pred.predicate()
	}

	alice := makeBindingNode("n1", map[string]any{"age": int64(30), "role": "admin"})
	b := binding{"n": alice}

	t.Run("literal list IN match", func(t *testing.T) {
		pred := build(t, "n.age IN [29, 30, 31]", " IN ", false)
		require.True(t, pred(b, nil))
	})

	t.Run("literal list IN no match", func(t *testing.T) {
		pred := build(t, "n.age IN [1, 2, 3]", " IN ", false)
		require.False(t, pred(b, nil))
	})

	t.Run("literal list NOT IN", func(t *testing.T) {
		pred := build(t, "n.age NOT IN [1, 2, 3]", " NOT IN ", true)
		require.True(t, pred(b, nil))
	})

	t.Run("$param list", func(t *testing.T) {
		pred := build(t, "n.role IN $allowed", " IN ", false)
		require.True(t, pred(b, map[string]interface{}{
			"allowed": []interface{}{"viewer", "admin"},
		}))
		require.False(t, pred(b, map[string]interface{}{
			"allowed": []interface{}{"viewer"},
		}))
	})

	t.Run("$param empty list never matches", func(t *testing.T) {
		pred := build(t, "n.role IN $allowed", " IN ", false)
		require.False(t, pred(b, map[string]interface{}{"allowed": []interface{}{}}))
	})

	t.Run("$param NOT IN inverts", func(t *testing.T) {
		pred := build(t, "n.role NOT IN $deny", " NOT IN ", true)
		require.True(t, pred(b, map[string]interface{}{"deny": []interface{}{"viewer"}}))
	})

	t.Run("$param with non-list value returns false (membership undefined)", func(t *testing.T) {
		pred := build(t, "n.role IN $not_a_list", " IN ", false)
		require.False(t, pred(b, map[string]interface{}{"not_a_list": "scalar"}))
	})

	t.Run("bare $ on right rejected", func(t *testing.T) {
		_, ok := e.compileBindingInPredicate("n.role IN $", " IN ", false)
		require.False(t, ok)
	})

	t.Run("operator absent returns ok=false", func(t *testing.T) {
		_, ok := e.compileBindingInPredicate("n.role = 'admin'", " IN ", false)
		require.False(t, ok)
	})

	t.Run("left resolver fails compile → predicate fails compile", func(t *testing.T) {
		_, ok := e.compileBindingInPredicate("    IN [1, 2]", " IN ", false)
		require.False(t, ok)
	})

	t.Run("right resolver fails compile → predicate fails compile", func(t *testing.T) {
		_, ok := e.compileBindingInPredicate("n.role IN garbage[]^_^", " IN ", false)
		require.False(t, ok)
	})

	t.Run("left resolver returns no value at runtime → false", func(t *testing.T) {
		pred := build(t, "n.missing IN [1, 2]", " IN ", false)
		require.False(t, pred(b, nil))
	})
}

// ============================================================================
// makeCompiledBindingMembershipPredicate — direct unit test for the
// comparable/non-comparable split.
// ============================================================================

func TestMakeCompiledBindingMembershipPredicate_HandlesComparableAndNonComparable(t *testing.T) {
	e := freshExecutorForBindingWhere(t)

	resolver := func(b binding, _ map[string]interface{}) (interface{}, bool) {
		v, ok := b["n"].Properties["age"]
		return v, ok
	}
	// Mix comparable scalars with a non-comparable []interface{} slice item.
	items := []interface{}{int64(30), "admin", nil, []interface{}{"sub"}}

	truth := e.makeCompiledBindingMembershipPredicate(resolver, items)

	alice := makeBindingNode("n1", map[string]any{"age": int64(30)})
	b := binding{"n": alice}
	require.Equal(t, truthTrue, truth(b, nil))

	// No match, but the list contains null: Cypher gives null (unknown), for
	// IN and for its negation alike.
	bob := makeBindingNode("n2", map[string]any{"age": int64(99)})
	require.Equal(t, truthUnknown, truth(binding{"n": bob}, nil))
	require.Equal(t, truthUnknown, notTruth(truth)(binding{"n": bob}, nil))
	require.Equal(t, truthFalse, notTruth(truth)(b, nil))

	withoutNull := e.makeCompiledBindingMembershipPredicate(resolver, []interface{}{int64(30), "admin"})
	require.Equal(t, truthFalse, withoutNull(binding{"n": bob}, nil))
	require.Equal(t, truthTrue, notTruth(withoutNull)(binding{"n": bob}, nil))
}

// ============================================================================
// evaluateBindingWhere — direct AND/OR/NOT/comparison/STARTS-WITH chains.
// ============================================================================

func TestEvaluateBindingWhere_AndOrNotAndStringPredicates(t *testing.T) {
	e := freshExecutorForBindingWhere(t)
	ctx := context.Background()

	alice := makeBindingNode("n1", map[string]any{"name": "Alice", "age": int64(30)})
	bob := makeBindingNode("n2", map[string]any{"name": "Bob", "age": int64(25)})
	b := binding{"a": alice, "b": bob}

	t.Run("empty clause is true", func(t *testing.T) {
		fixtureAdmission196Context, fixtureAdmission196 := expressionTestContext(ctx, "")
		_ = fixtureAdmission196
		require.True(t, e.evaluateBindingWhereGeneric(fixtureAdmission196, fixtureAdmission196Context, b, "", nil))
	})

	t.Run("AND composition", func(t *testing.T) {
		fixtureAdmission197Context, fixtureAdmission197 := expressionTestContext(ctx, "a.age = 30 AND b.age = 25")
		_ = fixtureAdmission197
		require.True(t, e.evaluateBindingWhereGeneric(fixtureAdmission197, fixtureAdmission197Context, b, "a.age = 30 AND b.age = 25", nil))
		fixtureAdmission198Context, fixtureAdmission198 := expressionTestContext(ctx, "a.age = 30 AND b.age = 99")
		_ = fixtureAdmission198
		require.False(t, e.evaluateBindingWhereGeneric(fixtureAdmission198, fixtureAdmission198Context, b, "a.age = 30 AND b.age = 99", nil))
	})

	t.Run("OR composition", func(t *testing.T) {
		fixtureAdmission199Context, fixtureAdmission199 := expressionTestContext(ctx, "a.age = 99 OR b.age = 25")
		_ = fixtureAdmission199
		require.True(t, e.evaluateBindingWhereGeneric(fixtureAdmission199, fixtureAdmission199Context, b, "a.age = 99 OR b.age = 25", nil))
		fixtureAdmission200Context, fixtureAdmission200 := expressionTestContext(ctx, "a.age = 99 OR b.age = 99")
		_ = fixtureAdmission200
		require.False(t, e.evaluateBindingWhereGeneric(fixtureAdmission200, fixtureAdmission200Context, b, "a.age = 99 OR b.age = 99", nil))
	})

	t.Run("NOT inversion", func(t *testing.T) {
		fixtureAdmission201Context, fixtureAdmission201 := expressionTestContext(ctx, "NOT a.age = 99")
		_ = fixtureAdmission201
		require.True(t, e.evaluateBindingWhereGeneric(fixtureAdmission201, fixtureAdmission201Context, b, "NOT a.age = 99", nil))
		fixtureAdmission202Context, fixtureAdmission202 := expressionTestContext(ctx, "NOT a.age = 30")
		_ = fixtureAdmission202
		require.False(t, e.evaluateBindingWhereGeneric(fixtureAdmission202, fixtureAdmission202Context, b, "NOT a.age = 30", nil))
	})

	t.Run("STARTS WITH literal", func(t *testing.T) {
		fixtureAdmission203Context, fixtureAdmission203 := expressionTestContext(ctx, "a.name STARTS WITH 'Al'")
		_ = fixtureAdmission203
		require.True(t, e.evaluateBindingWhereGeneric(fixtureAdmission203, fixtureAdmission203Context, b, "a.name STARTS WITH 'Al'", nil))
		fixtureAdmission204Context, fixtureAdmission204 := expressionTestContext(ctx, "a.name STARTS WITH 'Bo'")
		_ = fixtureAdmission204
		require.False(t, e.evaluateBindingWhereGeneric(fixtureAdmission204, fixtureAdmission204Context, b, "a.name STARTS WITH 'Bo'", nil))
	})

	t.Run("ENDS WITH literal", func(t *testing.T) {
		fixtureAdmission205Context, fixtureAdmission205 := expressionTestContext(ctx, "a.name ENDS WITH 'ice'")
		_ = fixtureAdmission205
		require.True(t, e.evaluateBindingWhereGeneric(fixtureAdmission205, fixtureAdmission205Context, b, "a.name ENDS WITH 'ice'", nil))
	})

	t.Run("CONTAINS literal", func(t *testing.T) {
		fixtureAdmission206Context, fixtureAdmission206 := expressionTestContext(ctx, "a.name CONTAINS 'lic'")
		_ = fixtureAdmission206
		require.True(t, e.evaluateBindingWhereGeneric(fixtureAdmission206, fixtureAdmission206Context, b, "a.name CONTAINS 'lic'", nil))
	})

	t.Run("identity comparison via <> between two node variables", func(t *testing.T) {
		fixtureAdmission207Context, fixtureAdmission207 := expressionTestContext(ctx, "a <> b")
		_ = fixtureAdmission207
		require.True(t, e.evaluateBindingWhereGeneric(fixtureAdmission207, fixtureAdmission207Context, b, "a <> b", nil))
		fixtureAdmission208Context, fixtureAdmission208 := expressionTestContext(ctx, "a <> a")
		_ = fixtureAdmission208
		require.False(t, e.evaluateBindingWhereGeneric(fixtureAdmission208, fixtureAdmission208Context, b, "a <> a", nil))
	})
}
