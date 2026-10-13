package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newOptionsGraph creates nodes with the given IDs and relationships
// "start type end weight" (the weight in w, and 10 - weight in weight).
func newOptionsGraph(t *testing.T, nodes []string, edges [][4]interface{}) *StorageExecutor {
	t.Helper()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for _, id := range nodes {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, Properties: map[string]interface{}{"name": id, "age": int64(len(id) * 10)}})
		require.NoError(t, err)
	}
	for i, edge := range edges {
		w := edge[3].(float64)
		require.NoError(t, engine.CreateEdge(&storage.Edge{
			ID:         storage.EdgeID(fmt.Sprintf("e%d", i)),
			StartNode:  storage.NodeID(edge[0].(string)),
			Type:       edge[1].(string),
			EndNode:    storage.NodeID(edge[2].(string)),
			Properties: map[string]interface{}{"w": w, "weight": 10 - w},
		}))
	}
	return NewStorageExecutor(engine)
}

// TestPathAlgorithmsReadRelTypesAndDirections: dijkstra, aStar and
// allSimplePaths read APOC's "TYPE>|<TYPE|TYPE" and allSimplePaths its
// maxNodes (#1185); the expected paths are APOC 5.26's on the same graph.
func TestPathAlgorithmsReadRelTypesAndDirections(t *testing.T) {
	exec := newOptionsGraph(t, []string{"a", "b", "c", "d"}, [][4]interface{}{
		{"a", "K", "b", 1.0}, {"c", "K", "b", 2.0}, {"c", "L", "d", 5.0}, {"a", "L", "d", 1.0}, {"d", "K", "c", 1.0},
	})
	ctx := context.Background()
	paths := func(query string) []string {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		var out []string
		for _, row := range result.Rows {
			out = append(out, fmt.Sprint(row[0]))
		}
		return out
	}
	for filter, want := range map[string][]string{
		"":      {"[a d c]"},
		"K":     {"[a b c]"},
		"K>|<K": {"[a b c]"},
		"K>":    nil,
		"<K":    nil,
		"L>|K>": {"[a d c]"},
		">":     {"[a d c]"},
		"L>,K>": nil,
	} {
		require.Equal(t, want, paths("CALL apoc.algo.dijkstra('a', 'c', '"+filter+"', 'w')"), "dijkstra %q", filter)
		require.Equal(t, want, paths("CALL apoc.algo.aStar('a', 'c', '"+filter+"', 'w', 'lat', 'lon')"), "aStar %q", filter)
	}

	for maxNodes, want := range map[int]int{-1: 0, 0: 0, 1: 0, 2: 3, 3: 3} {
		require.Len(t, paths(fmt.Sprintf("CALL apoc.algo.allSimplePaths('a', 'c', '', %d)", maxNodes)), want, "maxNodes %d", maxNodes)
	}
	require.Equal(t, []string{"[a d c]"}, paths("CALL apoc.algo.allSimplePaths('a', 'c', 'L>|K>', 5)"))
	require.Equal(t, []string{"[a b c]"}, paths("CALL apoc.algo.allSimplePaths('a', 'c', 'K', 3)"))

	_, err := exec.Execute(ctx, "CALL apoc.algo.allSimplePaths('a', 'c', '', 'x')", nil)
	require.Error(t, err)
}

// TestFastRPReadsItsOptions: relationshipWeightProperty names the weight,
// and iterationWeights, normalizationStrength and featureProperties change
// the embeddings (#1185).
func TestFastRPReadsItsOptions(t *testing.T) {
	exec := newOptionsGraph(t, []string{"a", "b", "c", "dd", "eee"}, [][4]interface{}{
		{"a", "R", "b", 1.0}, {"b", "R", "c", 9.0}, {"c", "R", "a", 0.5}, {"c", "R", "dd", 3.0}, {"dd", "R", "eee", 7.0},
	})
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CALL gds.graph.project('options', {P: {properties: 'age'}}, {R: {orientation: 'UNDIRECTED', properties: ['w', 'weight']}})", nil)
	require.NoError(t, err)
	embeddings := func(config string) string {
		t.Helper()
		result, err := exec.Execute(ctx, "CALL gds.fastRP.stream('options', {embeddingDimension: 8, randomSeed: 3"+config+"})", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 5)
		return fmt.Sprint(result.Rows)
	}
	refused := func(config, message string) {
		t.Helper()
		_, err := exec.Execute(ctx, "CALL gds.fastRP.stream('options', {embeddingDimension: 8"+config+"})", nil)
		require.ErrorContains(t, err, message)
	}
	plain := embeddings("")
	byW := embeddings(", relationshipWeightProperty: 'w'")
	require.NotEqual(t, plain, byW)
	require.NotEqual(t, byW, embeddings(", relationshipWeightProperty: 'weight'"))
	require.Equal(t, byW, embeddings(", relationshipWeightProperty: 'w'"))
	refused(", relationshipWeightProperty: 'missing'", "Relationship weight property `missing` not found in relationship types ['R']. Properties existing on all relationship types: ['w', 'weight']")
	require.Equal(t, plain, embeddings(", iterationWeights: [0.0, 1.0, 1.0]"))
	require.NotEqual(t, plain, embeddings(", iterationWeights: [1.0, 0.5]"))
	require.NotEqual(t, plain, embeddings(", normalizationStrength: 0.5"))
	require.NotEqual(t, plain, embeddings(", nodeSelfInfluence: 0.5"))
	// propertyRatio sizes the property subspace.
	require.NotEqual(t, plain, embeddings(", propertyRatio: 0.5, featureProperties: ['age']"))
	require.NotEqual(t, embeddings(", propertyRatio: 0.125, featureProperties: ['age']"), embeddings(", propertyRatio: 1.0, featureProperties: ['age']"))

	// More dimensions asked of the features than there are.
	result, err := exec.Execute(ctx, "CALL gds.fastRP.stream('options', {embeddingDimension: 1, propertyRatio: 1.0, featureProperties: ['age']})", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 5)

	// GDS's refusals.
	refused(", propertyRatio: 0.5", "When `propertyRatio` is non-zero, `featureProperties` may not be empty.")
	refused(", propertyRatio: 0.5, featureProperties: 'age'", "The value of `featureProperties` must be of type `List` but was `String`.")
	refused(", propertyRatio: 0.5, featureProperties: ['name']", "The feature properties ['name'] are not present for all requested labels. Requested labels: ['P']. Properties available on all requested labels: ['age']")
	for _, procedure := range []string{"stream", "stats"} {
		_, err = exec.Execute(ctx, "CALL gds.fastRP."+procedure+"('options', {embeddingDimension: 1, propertyRatio: 2, featureProperties: ['age']})", nil)
		require.ErrorContains(t, err, "Value for `propertyRatio` was `2`, but must be within the range [0.00, 1.00].")
	}
	refused(", propertyRatio: -0.5", "Value for `propertyRatio` was `-0.5`, but must be within the range [0.00, 1.00].")
}

// TestLinkPredictionReadsRelationshipTypesAndTargetNodes (#1185).
func TestLinkPredictionReadsRelationshipTypesAndTargetNodes(t *testing.T) {
	exec := newOptionsGraph(t, []string{"a", "b", "c", "d", "e"}, [][4]interface{}{
		{"a", "K", "b", 1.0}, {"b", "K", "c", 1.0}, {"a", "L", "d", 1.0}, {"d", "L", "e", 1.0},
	})
	ctx := context.Background()
	targets := func(query string) []string {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		var out []string
		for _, row := range result.Rows {
			out = append(out, row[1].(string))
		}
		return out
	}
	for _, procedure := range []string{"commonNeighbors", "adamicAdar", "resourceAllocation", "jaccard", "predict"} {
		call := "CALL gds.linkPrediction." + procedure + ".stream({sourceNode: 'a'"
		require.ElementsMatch(t, []string{"c", "e"}, targets(call+"})"), procedure)
		require.Equal(t, []string{"c"}, targets(call+", relationshipTypes: ['K']})"), procedure)
		require.Equal(t, []string{"c"}, targets(call+", relationshipTypes: 'K'})"), procedure)
		require.ElementsMatch(t, []string{"c", "e"}, targets(call+", relationshipTypes: ['*']})"), procedure)
		require.Equal(t, []string{"e"}, targets(call+", targetNodes: ['e']})"), procedure)
		require.Empty(t, targets(call+", targetNodes: ['e'], relationshipTypes: ['K']})"), procedure)
		require.Len(t, targets(call+", targetNodes: ['c', 'e'], topK: 1})"), 1, procedure)
	}
	require.Equal(t, []string{"e"}, targets("MATCH (e:P {name: 'e'}) CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 'a', targetNodes: e}) YIELD node2 RETURN 'a', node2"))
	// Preferential attachment scores every node, so the type filter shows in
	// the candidates' degrees.
	require.NotEmpty(t, targets("CALL gds.linkPrediction.preferentialAttachment.stream({sourceNode: 'a', relationshipTypes: ['K']})"))

	for _, bad := range []string{"relationshipTypes: [1]", "targetNodes: [1.5]"} {
		_, err := exec.Execute(ctx, "CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 'a', "+bad+"})", nil)
		require.Error(t, err, bad)
	}
}

// TestLinkPredictionGraphFailure: a graph that can't be built fails the call.
func TestLinkPredictionGraphFailure(t *testing.T) {
	exec := &StorageExecutor{}
	args := []interface{}{map[string]interface{}{"sourceNode": "a"}}
	_, err := exec.callGdsLinkPredictionCommonNeighbors(context.Background(), args)
	require.Error(t, err)
	_, err = exec.callGdsLinkPredictionPredict(context.Background(), args)
	require.Error(t, err)
}
