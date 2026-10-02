package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTryCollectNodesFromIDEquality_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seek_id_eq_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     storage.NodeID("person-1"),
		Labels: []string{"Person", "Actor"},
		Properties: map[string]interface{}{
			"name": "Ada",
			"role": "lead",
		},
	})
	require.NoError(t, err)

	nodes, used, err := exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "n"}, "")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "n"}, "id(n) == 'person-1'")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "m"}, "id(n) = 'person-1'")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "n"}, "id(n) = 1")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "n"}, "id(n) = 'missing'")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "n"}, "id(n) = 'person-1'")
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("person-1"), nodes[0].ID)

	nodes, used, err = exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "n"}, "elementId(n) = '4:nornic:person-1'")
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("person-1"), nodes[0].ID)

	nodes, used, err = exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "n", labels: []string{"Movie"}}, "id(n) = 'person-1'")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEquality(ctx, nodePatternInfo{variable: "n", properties: map[string]interface{}{"role": "support"}}, "id(n) = 'person-1'")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)
}

func TestTryCollectNodesFromIDEqualityParamAndCompound_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seek_id_param_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, node := range []*storage.Node{
		{ID: storage.NodeID("n1"), Labels: []string{"Person"}, Properties: map[string]interface{}{"k": "v1"}},
		{ID: storage.NodeID("n2"), Labels: []string{"Person"}, Properties: map[string]interface{}{"k": "v2"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	nodes, used, err := exec.tryCollectNodesFromIDEqualityParam(ctx, nodePatternInfo{variable: "n"}, "id(n) = $id", nil)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEqualityParam(ctx, nodePatternInfo{variable: "n"}, "id(n) = 'n1'", map[string]interface{}{"id": "n1"})
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEqualityParam(ctx, nodePatternInfo{variable: "n"}, "id(n) = $missing", map[string]interface{}{"id": "n1"})
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEqualityParam(ctx, nodePatternInfo{variable: "n"}, "id(n) = $id", map[string]interface{}{"id": 10})
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEqualityParam(ctx, nodePatternInfo{variable: "n"}, "elementId(n) = $id", map[string]interface{}{"id": "4:nornic:n1"})
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("n1"), nodes[0].ID)

	nodes, used, err = exec.tryCollectNodesFromIDEqualityCompound(
		ctx,
		nodePatternInfo{variable: "n"},
		"(id(n) = 'n1' OR id(n) = 'n2')",
		nil,
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 2)
	require.Equal(t, storage.NodeID("n1"), nodes[0].ID)
	require.Equal(t, storage.NodeID("n2"), nodes[1].ID)

	nodes, used, err = exec.tryCollectNodesFromIDEqualityCompound(
		ctx,
		nodePatternInfo{variable: "n"},
		"(n.k = 'v1' OR id(n) = 'n1')",
		nil,
	)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDEqualityCompound(
		ctx,
		nodePatternInfo{variable: "n", labels: []string{"Movie"}},
		"(id(n) = 'n1' AND true)",
		nil,
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)
}

func TestTryCollectNodesFromIDInParam_AndIndexCandidateLabels(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seek_id_in_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, node := range []*storage.Node{
		{ID: storage.NodeID("p1"), Labels: []string{"Person"}, Properties: map[string]interface{}{"kind": "human", "name": "Ada"}},
		{ID: storage.NodeID("p2"), Labels: []string{"Person"}, Properties: map[string]interface{}{"kind": "human", "name": "Lin"}},
		{ID: storage.NodeID("m1"), Labels: []string{"Movie"}, Properties: map[string]interface{}{"kind": "film", "name": "Ada"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	nodes, used, err := exec.tryCollectNodesFromIDInParam(nodePatternInfo{variable: "n"}, "id(n) IN $ids", nil)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDInParam(nodePatternInfo{variable: "n"}, "id(n) IN $ids", map[string]interface{}{"other": []interface{}{"p1"}})
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDInParam(nodePatternInfo{variable: "n"}, "id(n) IN $ids", map[string]interface{}{"ids": "p1"})
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	nodes, used, err = exec.tryCollectNodesFromIDInParam(
		nodePatternInfo{variable: "n", labels: []string{"Person"}, properties: map[string]interface{}{"kind": "human"}},
		"elementId(n) IN $ids",
		map[string]interface{}{"ids": []interface{}{"p1", "4:nornic:p2", "p1", "", 7}},
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 2)
	require.Equal(t, storage.NodeID("p1"), nodes[0].ID)
	require.Equal(t, storage.NodeID("p2"), nodes[1].ID)

	nodes, used, err = exec.tryCollectNodesFromIDInParam(
		nodePatternInfo{variable: "n", labels: []string{"Movie"}, properties: map[string]interface{}{"kind": "human"}},
		"id(n) IN $ids",
		map[string]interface{}{"ids": []interface{}{"p1", "m1"}},
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	_, err = exec.Execute(ctx, "CREATE INDEX idx_person_name_cov IF NOT EXISTS FOR (n:Person) ON (n.name)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX idx_movie_name_cov IF NOT EXISTS FOR (n:Movie) ON (n.name)", nil)
	require.NoError(t, err)

	schema := store.GetSchema()
	require.NotNil(t, schema)

	labels := exec.indexCandidateLabels(schema, []string{"Person", "Book"}, "name")
	require.Equal(t, []string{"Person"}, labels)

	labels = exec.indexCandidateLabels(schema, nil, "name")
	require.Empty(t, labels)

	for _, nodeID := range []storage.NodeID{"p1", "p2", "m1"} {
		node, err := store.GetNode(nodeID)
		require.NoError(t, err)
		node.Labels = append(node.Labels, "All")
		require.NoError(t, store.UpdateNode(node))
	}
	_, err = exec.Execute(ctx, "CREATE INDEX idx_all_name_cov FOR (n:All) ON (n.name)", nil)
	require.NoError(t, err)
	labels = exec.indexCandidateLabels(schema, nil, "name")
	require.Equal(t, []string{"All", "Movie", "Person"}, labels)

	labels = exec.indexCandidateLabels(schema, nil, "unknown")
	require.Empty(t, labels)
}
