package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

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

	nodes, used, err = exec.tryCollectNodesFromIDInParam(nodePatternInfo{variable: "n"}, "id(n) IN $ids OR n.k='v1'", map[string]interface{}{"ids": []interface{}{"n1"}})
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDInParam(nodePatternInfo{variable: "n"}, "id(n) IN $ids", map[string]interface{}{"ids": []interface{}{nil, 7, "", "n2"}})
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("n2"), nodes[0].ID)
}
