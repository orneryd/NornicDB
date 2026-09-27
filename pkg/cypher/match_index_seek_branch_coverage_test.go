package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchIndexSeekHelperFunctions(t *testing.T) {
	list := coerceInterfaceList([]string{"a", "b"})
	require.Equal(t, []interface{}{"a", "b"}, list)
	require.Equal(t, []interface{}{int64(1), int64(2)}, coerceInterfaceList([]int{1, 2}))
	require.Equal(t, []interface{}{int64(1), int64(2)}, coerceInterfaceList([]int64{1, 2}))
	require.Equal(t, []interface{}{1.5, 2.5}, coerceInterfaceList([]float64{1.5, 2.5}))
	require.Nil(t, coerceInterfaceList("bad"))

	v, ok := parseLiteralValue(`"x"`)
	require.True(t, ok)
	require.Equal(t, "x", v)
	v, ok = parseLiteralValue("true")
	require.True(t, ok)
	require.Equal(t, true, v)
	v, ok = parseLiteralValue("42")
	require.True(t, ok)
	require.EqualValues(t, int64(42), v)
	v, ok = parseLiteralValue("3.5")
	require.True(t, ok)
	require.Equal(t, 3.5, v)
	v, ok = parseLiteralValue("null")
	require.True(t, ok)
	require.Nil(t, v)
	_, ok = parseLiteralValue("not-a-literal")
	require.False(t, ok)

	cmp, ok := compareLiteralValues(int64(5), int64(3), ">")
	require.True(t, ok)
	require.True(t, cmp)
	cmp, ok = compareLiteralValues("x", "x", "=")
	require.True(t, ok)
	require.True(t, cmp)
	cmp, ok = compareLiteralValues("x", "y", "!=")
	require.True(t, ok)
	require.True(t, cmp)
	cmp, ok = compareLiteralValues("x", "y", ">")
	require.True(t, ok)
	require.False(t, cmp)

	require.Equal(
		t,
		"(n.flag = false OR n.flag IS NULL)",
		tryRewriteNullNormalizedPredicate("coalesce(n.flag, false) = false"),
	)
	require.Equal(
		t,
		"(n.flag <> false AND n.flag IS NOT NULL)",
		tryRewriteNullNormalizedPredicate("coalesce(n.flag, false) <> false"),
	)
	require.Equal(
		t,
		"coalesce(n.flag, false) = true",
		tryRewriteNullNormalizedPredicate("coalesce(n.flag, false) = true"),
	)

	require.Equal(t, 2, topLevelSymbolIndex("1 < 2", "<"))
	require.Equal(t, -1, topLevelSymbolIndex("'a<b'", "<"))
	require.Equal(t, 6, topLevelSymbolIndex("{k:1} = x", "="))
	require.Equal(t, -1, topLevelSymbolIndex("(a=b)", "="))

	require.Equal(t, 2, topLevelEqualsIndex("a = b"))
	require.Equal(t, 3, topLevelEqualsIndex("a == b"))
	require.Equal(t, -1, topLevelEqualsIndex("a >= b"))
	require.Equal(t, -1, topLevelEqualsIndex("func(a = b)"))

	prop, ok := parseVariableProperty("n.name", "n")
	require.True(t, ok)
	require.Equal(t, "name", prop)
	_, ok = parseVariableProperty("m.name", "n")
	require.False(t, ok)
}

func TestTryEvaluateConstantBooleanConjunct(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "seek_const_cov"))

	v, ok := exec.tryEvaluateConstantBooleanConjunct("TRUE")
	require.True(t, ok)
	require.True(t, v)

	v, ok = exec.tryEvaluateConstantBooleanConjunct("1 < 2")
	require.True(t, ok)
	require.True(t, v)

	v, ok = exec.tryEvaluateConstantBooleanConjunct("'x' = 'y'")
	require.True(t, ok)
	require.False(t, v)

	_, ok = exec.tryEvaluateConstantBooleanConjunct("n.age > 1")
	require.False(t, ok)

	_, ok = exec.tryEvaluateConstantBooleanConjunct("")
	require.False(t, ok)
}

func TestTryCollectNodesFromPropertyIndexOrEquality_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_seek")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX person_name IF NOT EXISTS FOR (n:Person) ON (n.name)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX person_code IF NOT EXISTS FOR (n:Person) ON (n.code)", nil)
	require.NoError(t, err)

	for _, n := range []struct {
		id   string
		name string
		code string
	}{
		{"p1", "Alice", "X1"},
		{"p2", "Bob", "X2"},
		{"p3", "Carol", "X3"},
	} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(n.id), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": n.name, "code": n.code}})
		require.NoError(t, err)
	}

	nodes, handled, err := exec.tryCollectNodesFromPropertyIndexOrEquality(
		ctx,
		nodePatternInfo{variable: "n", labels: []string{"Person"}},
		"n.name = 'Alice' OR n.code = 'X2'",
		nil,
	)
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, nodes, 2)

	nodes, handled, err = exec.tryCollectNodesFromPropertyIndexOrEquality(
		ctx,
		nodePatternInfo{variable: "n", labels: []string{"Person"}},
		"n.name = 'Alice'",
		nil,
	)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	nodes, handled, err = exec.tryCollectNodesFromPropertyIndexOrEquality(
		ctx,
		nodePatternInfo{variable: "n", labels: []string{"Person"}},
		"n.name > 'Alice' OR n.code = 'X2'",
		nil,
	)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)
}
