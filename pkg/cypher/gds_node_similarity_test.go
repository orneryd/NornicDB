package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newNodeSimilarityGraph is GDS's probe graph: documents holding items (w
// weights), the bank held by every document.
func newNodeSimilarityGraph(t *testing.T) func(string) [][]interface{} {
	t.Helper()
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns")
	exec := NewStorageExecutor(engine)
	for _, id := range []string{"d1", "d2", "d3", "d4"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": id}})
		require.NoError(t, err)
	}
	for _, id := range []string{"i1", "i2", "i3", "bank"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Item"}})
		require.NoError(t, err)
	}
	for i, edge := range []struct {
		from, to string
		w        float64
	}{{"d1", "i1", 1}, {"d2", "i1", 1}, {"d2", "i2", 2}, {"d3", "i2", 1}, {"d3", "i3", 1}, {"d1", "bank", 0.1}, {"d2", "bank", 0.1}, {"d3", "bank", 0.1}, {"d4", "bank", 0.1}} {
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: storage.NodeID(edge.from), EndNode: storage.NodeID(edge.to), Type: "H", Properties: map[string]interface{}{"w": edge.w}}))
	}
	run := func(query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	run("CALL gds.graph.project('ns', ['Doc', 'Item'], {H: {properties: 'w'}})")
	return run
}

// similarities maps "a-b" to the similarity.
func similarities(rows [][]interface{}) map[string]float64 {
	out := map[string]float64{}
	for _, row := range rows {
		out[row[0].(string)+"-"+row[1].(string)] = row[2].(float64)
	}
	return out
}

// TestNodeSimilarityAsGDS: gds.nodeSimilarity.stream and filtered.stream
// give GDS 2.13.12's values on the same graph (#1195).
func TestNodeSimilarityAsGDS(t *testing.T) {
	run := newNodeSimilarityGraph(t)
	stream := func(config string) map[string]float64 {
		t.Helper()
		return similarities(run("CALL gds.nodeSimilarity.stream('ns', {" + config + "})"))
	}
	jaccard := map[string]float64{
		"d1-d2": 0.6666666666666666, "d1-d3": 0.25, "d1-d4": 0.5, "d2-d1": 0.6666666666666666, "d2-d3": 0.5, "d2-d4": 0.3333333333333333,
		"d3-d1": 0.25, "d3-d2": 0.5, "d3-d4": 0.3333333333333333, "d4-d1": 0.5, "d4-d2": 0.3333333333333333, "d4-d3": 0.3333333333333333,
	}
	requireSimilarities := func(want, got map[string]float64) {
		t.Helper()
		require.Len(t, got, len(want))
		for pair, value := range want {
			require.InDelta(t, value, got[pair], 1e-12, pair)
		}
	}
	requireSimilarities(jaccard, stream(""))
	requireSimilarities(map[string]float64{
		"d1-d2": 0.3548387096774194, "d1-d3": 0.03225806451612903, "d1-d4": 0.09090909090909091, "d2-d1": 0.3548387096774194,
		"d2-d3": 0.26829268292682934, "d2-d4": 0.03225806451612903, "d3-d1": 0.03225806451612903, "d3-d2": 0.26829268292682934,
		"d3-d4": 0.047619047619047616, "d4-d1": 0.09090909090909091, "d4-d2": 0.03225806451612903, "d4-d3": 0.047619047619047616,
	}, stream("relationshipWeightProperty: 'w'"))
	requireSimilarities(map[string]float64{
		"d1-d2": 1, "d1-d3": 0.5, "d1-d4": 1, "d2-d1": 1, "d2-d3": 0.6666666666666666, "d2-d4": 1,
		"d3-d1": 0.5, "d3-d2": 0.6666666666666666, "d3-d4": 1, "d4-d1": 1, "d4-d2": 1, "d4-d3": 1,
	}, stream("similarityMetric: 'OVERLAP'"))
	requireSimilarities(map[string]float64{
		"d1-d2": 0.448995330028304, "d1-d3": 0.007018451197308742, "d1-d4": 0.09950371902099893, "d2-d1": 0.448995330028304,
		"d2-d3": 0.6334016141362596, "d2-d4": 0.04467670516087703, "d3-d1": 0.007018451197308742, "d3-d2": 0.6334016141362596,
		"d3-d4": 0.07053456158585984, "d4-d1": 0.09950371902099893, "d4-d2": 0.04467670516087703, "d4-d3": 0.07053456158585984,
	}, stream("similarityMetric: 'cosine', relationshipWeightProperty: 'w'"))
	requireSimilarities(map[string]float64{"d1-d2": 0.6666666666666666, "d2-d1": 0.6666666666666666, "d3-d2": 0.5, "d4-d1": 0.5}, stream("topK: 1"))
	requireSimilarities(map[string]float64{"d1-d2": 0.6666666666666666, "d2-d1": 0.6666666666666666}, stream("topN: 2"))
	requireSimilarities(map[string]float64{"d1-d3": 0.25, "d2-d4": 0.3333333333333333, "d3-d1": 0.25, "d4-d2": 0.3333333333333333}, stream("bottomK: 1"))
	requireSimilarities(map[string]float64{"d3-d1": 0.25, "d1-d3": 0.25}, stream("bottomN: 2"))
	require.Len(t, stream("topK: 1, topN: 3"), 3)
	requireSimilarities(map[string]float64{"d1-d2": 0.6666666666666666, "d1-d4": 0.5, "d2-d1": 0.6666666666666666, "d2-d3": 0.5, "d3-d2": 0.5, "d4-d1": 0.5}, stream("similarityCutoff: 0.4"))
	requireSimilarities(map[string]float64{"d2-d3": 0.5, "d3-d2": 0.5}, stream("degreeCutoff: 3"))
	requireSimilarities(map[string]float64{"d1-d4": 0.5, "d4-d1": 0.5}, stream("upperDegreeCutoff: 2"))
	require.Empty(t, stream("nodeLabels: ['Doc']"))
	requireSimilarities(jaccard, stream("useComponents: true, concurrency: 2, jobId: 'j', logProgress: false, sudo: false"))

	// Rows: by node, then most similar first, then by the other node.
	rows := run("CALL gds.nodeSimilarity.stream('ns') YIELD node1, node2, similarity RETURN node1, node2")
	require.Equal(t, []interface{}{"d1", "d2"}, rows[0])
	require.Equal(t, []interface{}{"d1", "d4"}, rows[1])
	require.Equal(t, []interface{}{"d1", "d3"}, rows[2])

	// shared: the neighbours each pair shares, with their weight in the score.
	shared := run("CALL gds.nodeSimilarity.stream('ns', {relationshipWeightProperty: 'w'}) YIELD node1, node2, shared WHERE node1 = 'd1' AND node2 = 'd2' RETURN shared")
	require.Equal(t, [][]interface{}{{[]interface{}{map[string]interface{}{"nodeId": "i1", "weight": 1.0}, map[string]interface{}{"nodeId": "bank", "weight": 0.1}}}}, shared)

	// The undirected projection compares both sides, the items too.
	run("CALL gds.graph.project('nsu', ['Doc', 'Item'], {H: {orientation: 'UNDIRECTED'}})")
	undirected := similarities(run("CALL gds.nodeSimilarity.stream('nsu', {topK: 100})"))
	require.Len(t, undirected, 22)
	require.InDelta(t, 0.5, undirected["bank-i1"], 1e-12)
	require.InDelta(t, 0.25, undirected["bank-i3"], 1e-12)

	// filtered: a node list, an id, a label, a target; ids without a node.
	filtered := func(config string) map[string]float64 {
		t.Helper()
		return similarities(run("CALL gds.nodeSimilarity.filtered.stream('ns', {" + config + "})"))
	}
	requireSimilarities(map[string]float64{"d1-d2": 0.6666666666666666, "d1-d3": 0.25, "d1-d4": 0.5},
		similarities(run("MATCH (d {name: 'd1'}) CALL gds.nodeSimilarity.filtered.stream('ns', {sourceNodeFilter: [d]}) YIELD node1, node2, similarity, shared RETURN node1, node2, similarity, shared")))
	requireSimilarities(map[string]float64{"d1-d3": 0.25},
		similarities(run("MATCH (d {name: 'd1'}), (e {name: 'd3'}) CALL gds.nodeSimilarity.filtered.stream('ns', {sourceNodeFilter: d, targetNodeFilter: [e]}) YIELD node1, node2, similarity, shared RETURN node1, node2, similarity, shared")))
	require.Empty(t, filtered("sourceNodeFilter: 5"))
	require.Empty(t, filtered("sourceNodeFilter: [999999]"))
	require.Empty(t, filtered("sourceNodeFilter: 'Item'"))
	require.Len(t, filtered("sourceNodeFilter: 'Doc', targetNodeFilter: 'Doc'"), 12)
	got := similarities(run("MATCH (d {name: 'none'}) WITH collect(d) AS none CALL gds.nodeSimilarity.filtered.stream('ns', {sourceNodeFilter: none}) YIELD node1, node2, similarity, shared RETURN node1, node2, similarity, shared"))
	require.Empty(t, got)
}

// TestNodeSimilarityRefusesAsGDS: GDS's errors (#1195).
func TestNodeSimilarityRefusesAsGDS(t *testing.T) {
	newNodeSimilarityGraph(t)
	for config, message := range map[string]string{
		"bogus: 1":                              "Unexpected configuration key: bogus",
		"similarityMetric: 'NOPE'":              "NOPE is not a valid metric. Available metrics include Jaccard and Overlap",
		"topK: 0":                               "Value for `topK` was `0`, but must be within the range [1, 2147483647].",
		"topK: 1, bottomK: 1":                   "Invalid parameter combination: topK combined with bottomK",
		"topN: 1, bottomN: 1":                   "Invalid parameter combination: topN combined with bottomN",
		"similarityCutoff: 2":                   "Value for `similarityCutoff` was `2.0`, but must be within the range [0.00, 1.00].",
		"relationshipWeightProperty: 'nope'":    "Relationship weight property `nope` not found in relationship types ['H']. Properties existing on all relationship types: ['w']",
		"degreeCutoff: 0":                       "Value for `degreeCutoff` was `0`, but must be within the range [1, 2147483647].",
		"upperDegreeCutoff: 2, degreeCutoff: 3": "The value of upperDegreeCutoff cannot be smaller than degreeCutoff",
		"topN: -1":                              "Value for `topN` was `-1`, but must be within the range [0, 2147483647].",
		"topK: 'x'":                             "The value of `topK` must be of type `Integer` but was `String`.",
		"sourceNodeFilter: 'Doc'":               "Unexpected configuration key: sourceNodeFilter",
		"useComponents: 5":                      "The value of `useComponents` must be of type `Boolean` but was `Long`.",
	} {
		_, err := newNodeSimilarityExec(t).Execute(context.Background(), "CALL gds.nodeSimilarity.stream('ns', {"+config+"})", nil)
		require.ErrorContains(t, err, message, config)
	}
	for config, message := range map[string]string{
		"sourceNodeFilter: true":   "Invalid scalar type. Expected Long or Node but found: Boolean",
		"sourceNodeFilter: 'Nope'": "The label `Nope` does not exist in the graph",
		"targetNodeFilter: [true]": "Invalid scalar type. Expected Long or Node but found: Boolean",
	} {
		_, err := newNodeSimilarityExec(t).Execute(context.Background(), "CALL gds.nodeSimilarity.filtered.stream('ns', {"+config+"})", nil)
		require.ErrorContains(t, err, message, config)
	}
	_, err := newNodeSimilarityExec(t).Execute(context.Background(), "CALL gds.nodeSimilarity.stream('missing')", nil)
	require.ErrorContains(t, err, "Graph with name `missing` does not exist")
}

// newNodeSimilarityExec is an executor over the probe graph's database,
// whose catalog holds 'ns'.
func newNodeSimilarityExec(t *testing.T) *StorageExecutor {
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns"))
}

// TestNodeSimilarityArgumentBranches covers the remaining refusals.
func TestNodeSimilarityArgumentBranches(t *testing.T) {
	newNodeSimilarityGraph(t)
	exec := newNodeSimilarityExec(t)
	_, err := exec.callGdsNodeSimilarityStream(nil, false)
	require.ErrorContains(t, err, "graphName")
	_, err = exec.callGdsNodeSimilarityStream([]interface{}{"ns", int64(5)}, false)
	require.ErrorContains(t, err, "configuration")
	for config, message := range map[string]string{
		"similarityCutoff: 'x'":         "The value of `similarityCutoff` must be of type `Double` but was `String`.",
		"similarityCutoff: 1.5":         "Value for `similarityCutoff` was `1.5`, but must be within the range [0.00, 1.00].",
		"upperDegreeCutoff: 'x'":        "The value of `upperDegreeCutoff` must be of type `Integer` but was `String`.",
		"topN: 'x'":                     "The value of `topN` must be of type `Integer` but was `String`.",
		"similarityMetric: 5":           "The value of `similarityMetric` must be of type `String` but was `Long`.",
		"relationshipWeightProperty: 5": "The value of `relationshipWeightProperty` must be of type `String` but was `Long`.",
		"nodeLabels: 5":                 "The value of `nodeLabels` must be of type `List` but was `Long`.",
		"relationshipTypes: 5":          "The value of `relationshipTypes` must be of type `List` but was `Long`.",
		"concurrency: 0":                "Value for `concurrency` was `0`, but must be within the range [1, 2147483647].",
		"jobId: 7":                      "Expected JobId or String. Got Long.",
		"logProgress: 'x'":              "The value of `logProgress` must be of type `Boolean` but was `String`.",
	} {
		_, err := exec.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('ns', {"+config+"})", nil)
		require.ErrorContains(t, err, message, config)
	}
	_, err = exec.Execute(context.Background(), "CALL gds.nodeSimilarity.filtered.stream('ns', {targetNodeFilter: 'Nope'})", nil)
	require.ErrorContains(t, err, "The label `Nope` does not exist in the graph")
}

// TestNodeSimilarityZeroCutoffAndComponents: an explicit similarityCutoff of
// 0 keeps the pairs sharing nothing, and useComponents keeps pairs within
// one component, as GDS 2.13.12 gives on the same graph (#1197 review).
func TestNodeSimilarityZeroCutoffAndComponents(t *testing.T) {
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ns"))
	_, err := exec.Execute(context.Background(), `CREATE (d1:D {name:'d1', comp: 1}), (d2:D {name:'d2', comp: 1}), (d3:D {name:'d3', comp: 2}),
		(i1:I {name:'i1', comp: 1}), (i2:I {name:'i2', comp: 2}), (d1)-[:H]->(i1), (d2)-[:H]->(i1), (d3)-[:H]->(i2)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "CALL gds.graph.project('dis', {D: {properties: 'comp'}, I: {properties: 'comp'}}, 'H')", nil)
	require.NoError(t, err)
	byName := func(query string) map[string]float64 {
		t.Helper()
		result, err := exec.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('dis', {"+query+"}) YIELD node1, node2, similarity "+
			"MATCH (a), (b) WHERE elementId(a) ENDS WITH node1 AND elementId(b) ENDS WITH node2 OR (id(a) = node1 AND id(b) = node2) RETURN a.name, b.name, similarity", nil)
		require.NoError(t, err, query)
		return similarities(result.Rows)
	}
	require.Equal(t, map[string]float64{"d1-d2": 1, "d2-d1": 1}, byName(""))
	require.Equal(t, map[string]float64{"d1-d2": 1, "d1-d3": 0, "d2-d1": 1, "d2-d3": 0, "d3-d1": 0, "d3-d2": 0}, byName("similarityCutoff: 0.0"))
	// d3's least similar is d1 or d2 (a tie at 0, broken by node id).
	bottom := byName("similarityCutoff: 0.0, bottomK: 1")
	require.Len(t, bottom, 3)
	require.Equal(t, 0.0, bottom["d1-d3"])
	require.Equal(t, 0.0, bottom["d2-d3"])
	_, d3d1 := bottom["d3-d1"]
	_, d3d2 := bottom["d3-d2"]
	require.True(t, d3d1 != d3d2, "one of d3's ties")
	require.Equal(t, map[string]float64{"d1-d2": 1, "d2-d1": 1}, byName("useComponents: 'comp', similarityCutoff: 0.0"))
	require.Equal(t, map[string]float64{"d1-d2": 1, "d2-d1": 1}, byName("useComponents: true, similarityCutoff: 0.0"))
	// Nodes without the property (the items here) are components of their
	// own; the documents still pair within theirs.
	_, err = exec.Execute(context.Background(), "CALL gds.graph.project('dis2', {D: {properties: 'comp'}, I: {}}, 'H')", nil)
	require.NoError(t, err)
	result, err := exec.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('dis2', {useComponents: 'comp'}) YIELD node1 RETURN count(*)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Rows[0][0])
	_, err = exec.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('dis', {useComponents: 'nope'})", nil)
	require.ErrorContains(t, err, "Component property `nope` not found in graph with node properties: [comp]")
}
