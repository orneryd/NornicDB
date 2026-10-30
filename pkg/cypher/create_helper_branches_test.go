package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResolveParameterPath_Branches(t *testing.T) {
	params := map[string]interface{}{
		"row": map[string]interface{}{
			"props": map[string]interface{}{"a": int64(1)},
			"m2":    map[interface{}]interface{}{"b": "x"},
		},
	}

	ctx := withParams(context.Background(), params)
	v, ok := resolveValueReference(ctx, "$row", parameterPath)
	require.True(t, ok)
	require.NotNil(t, v)

	v, ok = resolveValueReference(ctx, "$row.props", parameterPath)
	require.True(t, ok)
	require.Equal(t, map[string]interface{}{"a": int64(1)}, v)

	v, ok = resolveValueReference(ctx, "$row.m2.b", parameterPath)
	require.True(t, ok)
	require.Equal(t, "x", v)

	v, ok = resolveValueReference(ctx, "$row['props']", parameterPath)
	require.True(t, ok)
	require.Equal(t, map[string]interface{}{"a": int64(1)}, v)

	v, ok = resolveValueReference(ctx, "$row[\"props\"].a", parameterPath)
	require.True(t, ok)
	require.EqualValues(t, int64(1), v)

	_, ok = resolveValueReference(context.Background(), "$row", parameterPath)
	require.False(t, ok)
	_, ok = resolveValueReference(ctx, "$", parameterPath)
	require.False(t, ok)
	_, ok = resolveValueReference(ctx, "$missing", parameterPath)
	require.False(t, ok)
	_, ok = resolveValueReference(ctx, "$row.props.missing", parameterPath)
	require.False(t, ok)
	_, ok = resolveValueReference(ctx, "$row.bad-char", parameterPath)
	require.False(t, ok)
	_, ok = resolveValueReference(ctx, "$row['props'", parameterPath)
	require.False(t, ok)
	_, ok = resolveValueReference(ctx, "$row[props]", parameterPath)
	require.False(t, ok)
}

func TestResolveNodeFromIDEqualityTerm_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "resolve_node_id_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:Person {name:'A'})", nil)
	require.NoError(t, err)
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	require.NotEmpty(t, nodes)
	nid := string(nodes[0].ID)

	v, node := exec.resolveNodeFromIDEqualityTerm("", nil)
	require.Empty(t, v)
	require.Nil(t, node)

	v, node = exec.resolveNodeFromIDEqualityTerm("id(n) != 'x'", nil)
	require.Empty(t, v)
	require.Nil(t, node)

	v, node = exec.resolveNodeFromIDEqualityTerm("id(n) == 'x'", nil)
	require.Empty(t, v)
	require.Nil(t, node)

	v, node = exec.resolveNodeFromIDEqualityTerm("name = 'x'", nil)
	require.Empty(t, v)
	require.Nil(t, node)

	v, node = exec.resolveNodeFromIDEqualityTerm("id(n) = $p", nil)
	require.Empty(t, v)
	require.Nil(t, node)

	v, node = exec.resolveNodeFromIDEqualityTerm("id(n) = $p", map[string]interface{}{"p": int64(1)})
	require.Empty(t, v)
	require.Nil(t, node)

	v, node = exec.resolveNodeFromIDEqualityTerm("id(n) = $p", map[string]interface{}{"p": nid})
	require.Equal(t, "n", v)
	require.NotNil(t, node)
	require.Equal(t, nid, string(node.ID))

	v, node = exec.resolveNodeFromIDEqualityTerm("id(n) = '"+nid+"'", nil)
	require.Equal(t, "n", v)
	require.NotNil(t, node)
	require.Equal(t, nid, string(node.ID))

	v, node = exec.resolveNodeFromIDEqualityTerm("elementId(n) = '4:nornic:"+nid+"'", nil)
	require.Equal(t, "n", v)
	require.NotNil(t, node)
	require.Equal(t, nid, string(node.ID))

	v, node = exec.resolveNodeFromIDEqualityTerm("elementId(n) = $p", map[string]interface{}{"p": "4:nornic:" + nid})
	require.Equal(t, "n", v)
	require.NotNil(t, node)
	require.Equal(t, nid, string(node.ID))
}
