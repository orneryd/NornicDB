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

	fixtureAdmission1274Context, fixtureAdmission1274 := expressionTestContext(ctx, "")
	_ = fixtureAdmission1274Context
	nodes, used, err := exec.tryCollectNodesFromIDEquality(fixtureAdmission1274, fixtureAdmission1274Context, nodePatternInfo{variable: "n"}, "")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	fixtureAdmission1275Context, fixtureAdmission1275 := expressionTestContext(ctx, "id(n) == 'person-1'")
	_ = fixtureAdmission1275Context
	nodes, used, err = exec.tryCollectNodesFromIDEquality(fixtureAdmission1275, fixtureAdmission1275Context, nodePatternInfo{variable: "n"}, "id(n) == 'person-1'")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	fixtureAdmission1276Context, fixtureAdmission1276 := expressionTestContext(ctx, "id(n) = 'person-1'")
	_ = fixtureAdmission1276Context
	nodes, used, err = exec.tryCollectNodesFromIDEquality(fixtureAdmission1276, fixtureAdmission1276Context, nodePatternInfo{variable: "m"}, "id(n) = 'person-1'")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	fixtureAdmission1277Context, fixtureAdmission1277 := expressionTestContext(ctx, "id(n) = 1")
	_ = fixtureAdmission1277Context
	nodes, used, err = exec.tryCollectNodesFromIDEquality(fixtureAdmission1277, fixtureAdmission1277Context, nodePatternInfo{variable: "n"}, "id(n) = 1")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	fixtureAdmission1278Context, fixtureAdmission1278 := expressionTestContext(ctx, "id(n) = 'missing'")
	_ = fixtureAdmission1278Context
	nodes, used, err = exec.tryCollectNodesFromIDEquality(fixtureAdmission1278, fixtureAdmission1278Context, nodePatternInfo{variable: "n"}, "id(n) = 'missing'")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	fixtureAdmission1279Context, fixtureAdmission1279 := expressionTestContext(ctx, "id(n) = 'person-1'")
	_ = fixtureAdmission1279Context
	nodes, used, err = exec.tryCollectNodesFromIDEquality(fixtureAdmission1279, fixtureAdmission1279Context, nodePatternInfo{variable: "n"}, "id(n) = 'person-1'")
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("person-1"), nodes[0].ID)

	fixtureAdmission1280Context, fixtureAdmission1280 := expressionTestContext(ctx, "elementId(n) = '4:nornic:person-1'")
	_ = fixtureAdmission1280Context
	nodes, used, err = exec.tryCollectNodesFromIDEquality(fixtureAdmission1280, fixtureAdmission1280Context, nodePatternInfo{variable: "n"}, "elementId(n) = '4:nornic:person-1'")
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("person-1"), nodes[0].ID)

	fixtureAdmission1281Context, fixtureAdmission1281 := expressionTestContext(ctx, "id(n) = 'person-1'")
	_ = fixtureAdmission1281Context
	nodes, used, err = exec.tryCollectNodesFromIDEquality(fixtureAdmission1281, fixtureAdmission1281Context, nodePatternInfo{variable: "n", labels: []string{"Movie"}}, "id(n) = 'person-1'")
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	fixtureAdmission1282Context, fixtureAdmission1282 := expressionTestContext(ctx, "id(n) = 'person-1'")
	_ = fixtureAdmission1282Context
	nodes, used, err = exec.tryCollectNodesFromIDEquality(fixtureAdmission1282, fixtureAdmission1282Context, nodePatternInfo{variable: "n", properties: map[string]interface{}{"role": "support"}}, "id(n) = 'person-1'")
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

	fixtureAdmission1283Context, fixtureAdmission1283 := expressionTestContext(ctx, "(id(n) = 'n1' OR id(n) = 'n2')")
	_ = fixtureAdmission1283Context
	nodes, used, err = exec.tryCollectNodesFromIDEqualityCompound(fixtureAdmission1283,
		fixtureAdmission1283Context,
		nodePatternInfo{variable: "n"},
		"(id(n) = 'n1' OR id(n) = 'n2')",
		nil,
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 2)
	require.Equal(t, storage.NodeID("n1"), nodes[0].ID)
	require.Equal(t, storage.NodeID("n2"), nodes[1].ID)

	fixtureAdmission1284Context, fixtureAdmission1284 := expressionTestContext(ctx, "(n.k = 'v1' OR id(n) = 'n1')")
	_ = fixtureAdmission1284Context
	nodes, used, err = exec.tryCollectNodesFromIDEqualityCompound(fixtureAdmission1284,
		fixtureAdmission1284Context,
		nodePatternInfo{variable: "n"},
		"(n.k = 'v1' OR id(n) = 'n1')",
		nil,
	)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	fixtureAdmission1285Context, fixtureAdmission1285 := expressionTestContext(ctx, "(id(n) = 'n1' AND true)")
	_ = fixtureAdmission1285Context
	nodes, used, err = exec.tryCollectNodesFromIDEqualityCompound(fixtureAdmission1285,
		fixtureAdmission1285Context,
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

	fixtureAdmission1286Context, fixtureAdmission1286 := expressionTestContext(ctx, "id(n) IN $ids")
	_ = fixtureAdmission1286Context
	nodes, used, err := exec.tryCollectNodesFromIDIn(fixtureAdmission1286, fixtureAdmission1286Context, nodePatternInfo{variable: "n"}, "id(n) IN $ids", nil)
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	fixtureAdmission1287Context, fixtureAdmission1287 := expressionTestContext(ctx, "id(n) IN $ids")
	_ = fixtureAdmission1287Context
	nodes, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1287, fixtureAdmission1287Context, nodePatternInfo{variable: "n"}, "id(n) IN $ids", map[string]interface{}{"other": []interface{}{"p1"}})
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	fixtureAdmission1288Context, fixtureAdmission1288 := expressionTestContext(ctx, "id(n) IN $ids")
	_ = fixtureAdmission1288Context
	nodes, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1288, fixtureAdmission1288Context, nodePatternInfo{variable: "n"}, "id(n) IN $ids", map[string]interface{}{"ids": "p1"})
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	fixtureAdmission1289Context, fixtureAdmission1289 := expressionTestContext(ctx, "elementId(n) IN $ids")
	_ = fixtureAdmission1289Context
	nodes, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1289, fixtureAdmission1289Context,
		nodePatternInfo{variable: "n", labels: []string{"Person"}, properties: map[string]interface{}{"kind": "human"}},
		"elementId(n) IN $ids",
		map[string]interface{}{"ids": []interface{}{"p1", "4:nornic:p2", "p1", "", 7}},
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 2)
	require.Equal(t, storage.NodeID("p1"), nodes[0].ID)
	require.Equal(t, storage.NodeID("p2"), nodes[1].ID)

	fixtureAdmission1290Context, fixtureAdmission1290 := expressionTestContext(ctx, "id(n) IN $ids")
	_ = fixtureAdmission1290Context
	nodes, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1290, fixtureAdmission1290Context,
		nodePatternInfo{variable: "n", labels: []string{"Movie"}, properties: map[string]interface{}{"kind": "human"}},
		"id(n) IN $ids",
		map[string]interface{}{"ids": []interface{}{"p1", "m1"}},
	)
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)

	// A constant list seeks too: a literal, as a substituted parameter
	// arrives (#940); null selects nothing; a non-list is left to the
	// evaluator.
	fixtureAdmission1291Context, fixtureAdmission1291 := expressionTestContext(ctx, "(id(n) IN ['p2', 'm1', 'missing'])")
	_ = fixtureAdmission1291Context
	nodes, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1291, fixtureAdmission1291Context, nodePatternInfo{variable: "n"}, "(id(n) IN ['p2', 'm1', 'missing'])", nil)
	require.NoError(t, err)
	require.True(t, used)
	require.ElementsMatch(t, []storage.NodeID{"p2", "m1"}, []storage.NodeID{nodes[0].ID, nodes[1].ID})
	fixtureAdmission1292Context, fixtureAdmission1292 := expressionTestContext(ctx, "id(n) IN null")
	_ = fixtureAdmission1292Context
	nodes, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1292, fixtureAdmission1292Context, nodePatternInfo{variable: "n"}, "id(n) IN null", nil)
	require.NoError(t, err)
	require.True(t, used)
	require.Empty(t, nodes)
	fixtureAdmission1293Context, fixtureAdmission1293 := expressionTestContext(ctx, "id(n) IN 'p1'")
	_ = fixtureAdmission1293Context
	_, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1293, fixtureAdmission1293Context, nodePatternInfo{variable: "n"}, "id(n) IN 'p1'", nil)
	require.NoError(t, err)
	require.False(t, used)
	fixtureAdmission1294Context, fixtureAdmission1294 := expressionTestContext(ctx, "id(n) IN n.ids")
	_ = fixtureAdmission1294Context
	_, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1294, fixtureAdmission1294Context, nodePatternInfo{variable: "n"}, "id(n) IN n.ids", nil)
	require.NoError(t, err)
	require.False(t, used, "a list read from the node itself isn't constant")

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
