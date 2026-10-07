package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIndexedEqualityResolvesBoundRowValues(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	ctx = withValueBindings(ctx, map[string]interface{}{
		"key": "bound-key",
		"row": map[string]interface{}{"props": map[string]interface{}{"key": int64(9007199254740993)}},
	})
	for _, test := range []struct {
		predicate string
		want      interface{}
	}{
		{"n.k = key", "bound-key"},
		{"key = n.k", "bound-key"},
		{"n.k = row.props.key", int64(9007199254740993)},
	} {
		t.Run(test.predicate, func(t *testing.T) {
			property, value, admitted := executor.parseSimpleIndexedEquality(ctx, "n", test.predicate)
			require.True(t, admitted)
			require.Equal(t, "k", property)
			require.Equal(t, test.want, value)
		})
	}
	_, _, admitted := executor.parseSimpleIndexedEquality(ctx, "n", "n.k = n.other")
	require.False(t, admitted)
}

func TestIndexedWhereKeepsIncomingRowsSeparate(t *testing.T) {
	for _, expression := range []string{"key", "row.props.key"} {
		t.Run(expression, func(t *testing.T) {
			executor := transactionIndexExecutor(t, 3)
			ctx := context.Background()
			query := "UNWIND $keys AS key MATCH (n:P) WHERE n.k = key RETURN n.v AS value ORDER BY value LIMIT 2"
			params := map[string]interface{}{"keys": []interface{}{"k0", "k2"}}
			if expression == "row.props.key" {
				query = "UNWIND $rows AS row MATCH (n:P) WHERE n.k = row.props.key RETURN n.v AS value ORDER BY value LIMIT 2"
				params = map[string]interface{}{"rows": []interface{}{
					map[string]interface{}{"props": map[string]interface{}{"key": "k0"}},
					map[string]interface{}{"props": map[string]interface{}{"key": "k2"}},
				}}
			}
			result, err := executor.Execute(ctx, query, params)
			require.NoError(t, err)
			require.Equal(t, []string{"value"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(0)}, {int64(2)}}, result.Rows)
		})
	}
}

func TestMatchIndexSeek_ParserAdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "seek_parser_more_cov"))
	ctx := context.Background()

	prop, val, ok := exec.parseSimpleIndexedEquality(ctx, "n", "")
	require.False(t, ok)
	require.Empty(t, prop)
	require.Nil(t, val)

	_, _, ok = exec.parseSimpleIndexedEquality(ctx, "n", "n.a >= 1")
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedEquality(ctx, "n", "n.a IN [1]")
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedEquality(ctx, "n", "n.a IS NOT NULL")
	require.False(t, ok)

	prop, val, ok = exec.parseSimpleIndexedEquality(ctx, "n", "n.a = 'x'")
	require.True(t, ok)
	require.Equal(t, "a", prop)
	require.Equal(t, "x", val)

	prop, val, ok = exec.parseSimpleIndexedEquality(ctx, "n", "'x' = n.a")
	require.True(t, ok)
	require.Equal(t, "a", prop)
	require.Equal(t, "x", val)

	prop, vals, ok := exec.parseSimpleIndexedInParam("n", "n.k IN $keys", map[string]interface{}{"keys": []interface{}{"a", "a", nil, "b"}})
	require.True(t, ok)
	require.Equal(t, "k", prop)
	require.Equal(t, []interface{}{"a", "b"}, vals)

	_, _, ok = exec.parseSimpleIndexedInParam("n", "n.k IN keys", map[string]interface{}{"keys": []interface{}{"a"}})
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedInParam("n", "n.k IN $missing", map[string]interface{}{"keys": []interface{}{"a"}})
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedInParam("n", "n.k IN $keys", map[string]interface{}{"keys": nil})
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedInParam("n", "n.k IN $keys AND n.x = 1", map[string]interface{}{"keys": []interface{}{"a"}})
	require.False(t, ok)

	prop, vals, ok = exec.parseSimpleIndexedInLiteral(ctx, "n", "n.k IN ['a','a',null,'b']")
	require.True(t, ok)
	require.Equal(t, "k", prop)
	require.Equal(t, []interface{}{"a", "b"}, vals)

	_, _, ok = exec.parseSimpleIndexedInLiteral(ctx, "n", "n.k IN 'a'")
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedInLiteral(ctx, "n", "n.k IN ['a'] OR n.x = 1")
	require.False(t, ok)

	p, ok := exec.parseSimpleIndexedIsNotNull("n", "n.k IS NOT NULL")
	require.True(t, ok)
	require.Equal(t, "k", p)

	_, ok = exec.parseSimpleIndexedIsNotNull("n", "n.k IS NOT NULL AND n.x IS NOT NULL")
	require.False(t, ok)
	_, ok = exec.parseSimpleIndexedIsNotNull("n", "n.k IS NOT NULL AND n.x > 1")
	require.False(t, ok)

	require.Equal(t, "x", unwrapOuterParens("(((x)))"))
	require.Equal(t, []string{"a=1", "b='x AND y'", "c=2"}, splitTopLevelAndConjuncts("a=1 AND b='x AND y' AND c=2"))
}

func TestMatchIndexSeek_IDEqualityCompoundAndIDInAdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seek_id_comp_more_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, n := range []*storage.Node{
		{ID: storage.NodeID("n1"), Labels: []string{"L"}, Properties: map[string]interface{}{"k": "v1"}},
		{ID: storage.NodeID("n2"), Labels: []string{"L"}, Properties: map[string]interface{}{"k": "v2"}},
	} {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	// The value side `'n1' OR` is no constant: the seek leaves the predicate
	// to the row filter instead of looking up its text (#844).
	nodes, used, err := exec.tryCollectNodesFromIDEqualityCompound(ctx, nodePatternInfo{variable: "n"}, "(id(n) = 'n1' OR )", nil)
	require.NoError(t, err)
	require.False(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEqualityCompound(ctx, nodePatternInfo{variable: "n"}, "(id(n) = 'n1' AND elementId(n) = '4:nornic:n2')", nil)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("n1"), nodes[0].ID)

	nodes, used, err = exec.tryCollectNodesFromIDIn(ctx, nodePatternInfo{variable: "n"}, "id(n) IN $ids OR n.k='v1'", map[string]interface{}{"ids": []interface{}{"n1"}})
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDIn(ctx, nodePatternInfo{variable: "n"}, "id(n) IN $ids", map[string]interface{}{"ids": []interface{}{nil, 7, "", "n2"}})
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("n2"), nodes[0].ID)
}
