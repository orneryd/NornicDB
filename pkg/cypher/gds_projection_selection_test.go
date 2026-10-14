package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every topology reader uses the shared selector. Link prediction preserves
// its []-selects-none extension through a selection policy; GDS selects all.
func TestGDSProjectedEmptyTypeSelectionPoliciesShared(t *testing.T) {
	gdsFollowUpCatalog(t)
	exec, run := newGDSEngineGraph(t, "selection", gdsNode("a", "N", nil), gdsNode("b", "N", nil))
	_, err := run("MATCH (a), (b) WHERE id(a) = 'a' AND id(b) = 'b' CREATE (a)-[:R]->(b)")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('selection', 'N', 'R')")
	require.NoError(t, err)
	all, degreeAll, err := exec.linkPredictionGraph(context.Background(), &linkPredictionConfig{GraphName: "selection", SourceNode: "a"})
	require.NoError(t, err)
	empty, degreeEmpty, err := exec.linkPredictionGraph(context.Background(), &linkPredictionConfig{GraphName: "selection", SourceNode: "a", RelationshipTypes: []string{}})
	require.NoError(t, err)
	require.NotEqual(t, all, empty)
	require.Empty(t, empty[storage.NodeID("a")])
	require.Equal(t, 1, degreeAll(storage.NodeID("a")))
	require.Equal(t, 0, degreeEmpty(storage.NodeID("a")))
	projection, err := lookupGraphProjection("selection", "selection")
	require.NoError(t, err)
	fastAll, err := selectGDSSubgraph(projection, nil, nil, "")
	require.NoError(t, err)
	fastEmpty, err := selectGDSSubgraph(projection, nil, []string{}, "")
	require.NoError(t, err)
	require.Equal(t, fastAll, fastEmpty)
	require.Len(t, fastEmpty.outgoing[0], 1)
	noTypes, err := selectGDSSubgraph(projection, nil, []string{}, "", gdsSubgraphOptions{emptyTypesNone: true})
	require.NoError(t, err)
	require.Empty(t, noTypes.outgoing[0])
}

// Both import forms expose the same typed node/relationship values and topology
// to algorithm readers, even when a node contains a Long above 2^53.
func TestGDSNativeAggregationReaderParity(t *testing.T) {
	gdsFollowUpCatalog(t)
	exec, run := newGDSEngineGraph(t, "readerParity",
		gdsNode("a", "A", map[string]interface{}{"x": int64(9007199254740993), "features": []int64{1, 2}}),
		gdsNode("b", "B", map[string]interface{}{"x": int64(2), "features": []int64{3, 4}}),
		gdsNode("c", "A", map[string]interface{}{"x": int64(3), "features": []int64{5, 6}}))
	_, err := run("MATCH (a), (b) WHERE id(a) = 'a' AND id(b) = 'b' CREATE (a)-[:R {w: 2.5}]->(b)")
	require.NoError(t, err)
	_, err = run("MATCH (c), (b) WHERE id(c) = 'c' AND id(b) = 'b' CREATE (c)-[:R {w: 2.5}]->(b)")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('nativeReaders', {A: {properties: ['x', 'features']}, B: {properties: ['x', 'features']}}, {R: {properties: 'w'}})")
	require.NoError(t, err)
	_, err = run("MATCH (s)-[r:R]->(t) RETURN gds.graph.project('aggregationReaders', s, t, {sourceNodeLabels: labels(s), targetNodeLabels: labels(t), sourceNodeProperties: s{.x, .features}, targetNodeProperties: t{.x, .features}, relationshipType: 'R', relationshipProperties: {w: r.w}})")
	require.NoError(t, err)
	native, err := lookupGraphProjection("readerParity", "nativeReaders")
	require.NoError(t, err)
	aggregation, err := lookupGraphProjection("readerParity", "aggregationReaders")
	require.NoError(t, err)
	require.Equal(t, native.NodeProperties, aggregation.NodeProperties)
	require.Equal(t, native.NodePropertyTypes, aggregation.NodePropertyTypes)
	require.Equal(t, native.NodeLabels, aggregation.NodeLabels)
	require.Equal(t, native.Relationships[0].Properties, aggregation.Relationships[0].Properties)
	for _, config := range []FastRPConfig{{}, {NodeLabels: []string{"A"}}, {RelationshipTypes: []string{"R"}, RelationshipWeightProperty: "w"}} {
		left, err := selectGDSSubgraph(native, config.NodeLabels, config.RelationshipTypes, config.RelationshipWeightProperty)
		require.NoError(t, err)
		right, err := selectGDSSubgraph(aggregation, config.NodeLabels, config.RelationshipTypes, config.RelationshipWeightProperty)
		require.NoError(t, err)
		require.Equal(t, left, right)
	}
	for _, graph := range []string{"nativeReaders", "aggregationReaders"} {
		values, err := run("RETURN gds.util.nodeProperty('" + graph + "', 'a', 'x', 'A'), gds.util.nodeProperty('" + graph + "', 'b', 'x', 'A'), gds.util.nodeProperty('" + graph + "', 'a', 'x', 'B'), gds.util.nodeProperty('" + graph + "', 'b', 'x', 'B')")
		require.NoError(t, err)
		require.Equal(t, []interface{}{float64(9007199254740992), nil, nil, 2.0}, values.Rows[0])
		selected, degree, err := exec.linkPredictionGraph(context.Background(), &linkPredictionConfig{GraphName: graph, SourceNode: "a"})
		require.NoError(t, err)
		require.Contains(t, selected[storage.NodeID("a")], storage.NodeID("b"))
		require.Equal(t, 1, degree(storage.NodeID("a")))
	}
	left, err := run("CALL gds.fastRP.stream('nativeReaders', {embeddingDimension: 8, randomSeed: 42, propertyRatio: 1.0, featureProperties: ['features'], relationshipWeightProperty: 'w'})")
	require.NoError(t, err)
	right, err := run("CALL gds.fastRP.stream('aggregationReaders', {embeddingDimension: 8, randomSeed: 42, propertyRatio: 1.0, featureProperties: ['features'], relationshipWeightProperty: 'w'})")
	require.NoError(t, err)
	require.Equal(t, left.Rows, right.Rows)
	for _, procedure := range []string{"gds.nodeSimilarity.stream", "gds.louvain.stream"} {
		left, err := run("CALL " + procedure + "('nativeReaders', {relationshipWeightProperty: 'w'})")
		require.NoError(t, err)
		right, err := run("CALL " + procedure + "('aggregationReaders', {relationshipWeightProperty: 'w'})")
		require.NoError(t, err)
		require.NotEmpty(t, left.Rows, procedure)
		require.Equal(t, left.Rows, right.Rows, procedure)
	}
}

// GDS's label argument selects the projected node set as well as the key.
// A node outside that label returns null even when another label has the key.
func TestGDSProjectedNodePropertyHonorsLabelSelection(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "labelRead", gdsNode("a", "A", map[string]interface{}{"x": int64(1)}), gdsNode("b", "B", map[string]interface{}{"x": int64(2)}))
	_, err := run("CALL gds.graph.project('labelRead', {A: {properties: 'x'}, B: {properties: 'x'}}, '*')")
	require.NoError(t, err)
	result, err := run("RETURN gds.util.nodeProperty('labelRead', 'a', 'x', 'A'), gds.util.nodeProperty('labelRead', 'b', 'x', 'A'), gds.util.nodeProperty('labelRead', 'a', 'x', 'B'), gds.util.nodeProperty('labelRead', 'b', 'x', 'B')")
	require.NoError(t, err)
	require.Equal(t, []interface{}{1.0, nil, nil, 2.0}, result.Rows[0])
}
