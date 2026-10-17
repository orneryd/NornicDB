package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newGDSGraph is GDS's probe graph: a→b (weight 2), b→c (no weight), a→a
// (weight 1), and an isolated node; ages on a and b.
func newGDSGraph(t *testing.T) (*StorageExecutor, func(string) (*ExecuteResult, error)) {
	t.Helper()
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gds"))
	run := func(query string) (*ExecuteResult, error) {
		return exec.Execute(context.Background(), query, nil)
	}
	_, err := run(`CREATE (a:P {name:'a', age: 1}), (b:P {name:'b', age: 2}), (c:P {name:'c'}), (iso:P {name:'iso'}),
		(a)-[:K {weight: 2.0}]->(b), (b)-[:K]->(c), (a)-[:K {weight: 1.0}]->(a)`)
	require.NoError(t, err)
	return exec, run
}

// TestGraphProjectAsGDS: gds.graph.project applies orientation, properties
// and aggregation, and refuses what GDS refuses with its messages (#1190).
// The counts and messages are GDS 2.13.12's on the same graph.
func TestGraphProjectAsGDS(t *testing.T) {
	_, run := newGDSGraph(t)
	count := func(query string) int64 {
		t.Helper()
		result, err := run(query + " YIELD relationshipCount RETURN relationshipCount")
		require.NoError(t, err, query)
		return result.Rows[0][0].(int64)
	}
	require.Equal(t, int64(3), count("CALL gds.graph.project('n', 'P', 'K')"))
	require.Equal(t, int64(3), count("CALL gds.graph.project('r', 'P', {K: {orientation: 'REVERSE'}})"))
	// An undirected relationship counts twice, a self-loop too.
	require.Equal(t, int64(6), count("CALL gds.graph.project('u', 'P', {K: {orientation: 'UNDIRECTED'}})"))
	require.Equal(t, int64(6), count("CALL gds.graph.project('u2', 'P', {K: {ORIENTATION: 'undirected'}})"))

	result, err := run("CALL gds.graph.project('cols', 'P', {K: {properties: 'weight'}})")
	require.NoError(t, err)
	require.Equal(t, []string{"nodeProjection", "relationshipProjection", "graphName", "nodeCount", "relationshipCount", "projectMillis"}, result.Columns)
	require.Equal(t, map[string]interface{}{"P": map[string]interface{}{"label": "P", "properties": map[string]interface{}{}}}, result.Rows[0][0])
	require.Equal(t, map[string]interface{}{"K": map[string]interface{}{
		"type": "K", "orientation": "NATURAL", "aggregation": "DEFAULT", "indexInverse": false,
		"properties": map[string]interface{}{"weight": map[string]interface{}{"property": "weight", "aggregation": "DEFAULT", "defaultValue": nil}},
	}}, result.Rows[0][1])
	require.GreaterOrEqual(t, result.Rows[0][5].(int64), int64(0))

	for query, message := range map[string]string{
		"CALL gds.graph.project('n', 'P', 'K')":                                                              "A graph with name 'n' already exists.",
		"CALL gds.graph.project('x', 'Nope', 'K')":                                                           "Invalid node projection, one or more labels not found: 'Nope'",
		"CALL gds.graph.project('x', ['P', 'Q'], 'K')":                                                       "Invalid node projection, one or more labels not found: 'P, Q'",
		"CALL gds.graph.project('x', 'P', 'NOPE')":                                                           "Invalid relationship projection, one or more relationship types not found: 'NOPE'",
		"CALL gds.graph.project('x', 'P', ['K', 'L'])":                                                       "Invalid relationship projection, one or more relationship types not found: 'K|L'",
		"CALL gds.graph.project('x', 'P', {K: {properties: 'nope'}})":                                        "Relationship properties not found: 'nope' (if you meant to count parallel relationships, use `property:'*'`).",
		"CALL gds.graph.project('x', {P: {properties: 'nope'}}, 'K')":                                        "Node properties not found: 'nope'",
		"CALL gds.graph.project('x', 'P', {K: {orientation: 'FOO'}})":                                        "Orientation `FOO` is not supported. Must be one of: [NATURAL, REVERSE, UNDIRECTED].",
		"CALL gds.graph.project('x', 'P', {K: {aggregation: 'FOO'}})":                                        "Aggregation `FOO` is not supported. Must be one of: [DEFAULT, NONE, SINGLE, SUM, MIN, MAX, COUNT].",
		"CALL gds.graph.project('x', 'P', {K: {properties: {w: {property: 'weight', aggregation: 'BAR'}}}})": "Aggregation `BAR` is not supported. Must be one of: [DEFAULT, NONE, SINGLE, SUM, MIN, MAX, COUNT].",
		"CALL gds.graph.project('x', 'P', {K: {foo: 1}})":                                                    "Unexpected configuration key: foo",
		"CALL gds.graph.project('x', {P: {bar: 1}}, 'K')":                                                    "Unexpected configuration key: bar",
		"CALL gds.graph.project('x', 5, 'K')":                                                                "Cannot construct a node projection out of a java.lang.Long",
		"CALL gds.graph.project('x', 'P', {K: 5})":                                                           "Cannot construct a relationship projection out of a java.lang.Long",
		"CALL gds.graph.project('x', 'P', {K: {properties: 5}})":                                             "Cannot construct a property projection out of a java.lang.Long",
		"CALL gds.graph.project('x', 'P', {K: {properties: [5]}})":                                           "Cannot construct a property projection out of a java.lang.Long",
		"CALL gds.graph.project('x', 'P', {K: {properties: {w: 5}}})":                                        "Cannot construct a property projection out of a java.lang.Long",
		"CALL gds.graph.project('x', 'P', {K: {orientation: 5}})":                                            "The value of `orientation` must be of type `String` but was `Long`.",
		"CALL gds.graph.project('x', 'P', {K: {indexInverse: 'yes'}})":                                       "The value of `indexInverse` must be of type `Boolean` but was `String`.",
		"CALL gds.graph.project('x', 'P', {K: {properties: {w: {defaultValue: 'x'}}}})":                      "Expected type of default value to be `Double`. But got `String`.",
		"CALL gds.graph.project('x', ['P', 5], 'K')":                                                         "Cannot construct a node projection out of a java.lang.Long",
	} {
		_, err := run(query)
		require.ErrorContains(t, err, message, query)
	}

	// Parallel relationships merge under an aggregation; their properties
	// combine by it.
	_, err = run("CREATE (x:P {name:'x'})-[:K {weight: 3.0}]->(y:P {name:'y'}), (x)-[:K {weight: 5.0}]->(y)")
	require.NoError(t, err)
	require.Equal(t, int64(5), count("CALL gds.graph.project('none', 'P', 'K')"))
	// SINGLE keeps one of the parallel relationships' values, as GDS does.
	for aggregation, wants := range map[string][]float64{"SUM": {8}, "MIN": {3}, "MAX": {5}, "SINGLE": {3, 5}, "COUNT": {2}} {
		name := "agg" + aggregation
		property := "weight"
		if aggregation == "COUNT" {
			property = "{n: {property: 'weight', aggregation: 'COUNT'}}"
		} else {
			property = "'weight'"
		}
		require.Equal(t, int64(4), count("CALL gds.graph.project('"+name+"', 'P', {K: {aggregation: '"+aggregation+"', properties: "+property+"}})"), aggregation)
		projection := graphProjections[gdsCatalogKey("gds", name)]
		key := "weight"
		if aggregation == "COUNT" {
			key = "n"
		}
		found := false
		for _, relationship := range projection.Relationships {
			if relationship.Source == relationship.Target {
				continue
			}
			if value, ok := relationship.Properties[key]; ok && projection.nodeIndex[relationship.Source] >= 0 {
				for _, want := range wants {
					if value == want {
						found = true
					}
				}
			}
		}
		require.True(t, found, aggregation)
	}
	// A default value stands in for a missing property.
	require.Equal(t, int64(5), count("CALL gds.graph.project('def', 'P', {K: {properties: {w: {property: 'weight', defaultValue: 1.0}}}})"))
	for _, relationship := range graphProjections[gdsCatalogKey("gds", "def")].Relationships {
		require.Contains(t, relationship.Properties, "w")
	}

	// gds.graph.list and drop see the projections.
	result, err = run("CALL gds.graph.list() YIELD graphName RETURN count(*)")
	require.NoError(t, err)
	require.Equal(t, int64(len(graphProjections)), result.Rows[0][0])
}

// TestFastRPAsGDS: FastRP propagates along the projection's orientation,
// gives a zero vector to a node nothing propagates to, doesn't normalise its
// result, and refuses configurations as GDS does (#1191).
func TestFastRPAsGDS(t *testing.T) {
	_, run := newGDSGraph(t)
	for _, query := range []string{
		"CALL gds.graph.project('n', {P: {properties: 'age'}}, 'K')",
		"CALL gds.graph.project('r', 'P', {K: {orientation: 'REVERSE'}})",
		"CALL gds.graph.project('u', 'P', {K: {orientation: 'UNDIRECTED'}})",
		"CALL gds.graph.project('w', 'P', {K: {properties: 'weight'}})",
		"CALL gds.graph.project('d', 'P', {K: {properties: {weight: {property: 'weight', defaultValue: 1.0}}}})",
		"CALL gds.graph.project('na', {P: {properties: {age: {property: 'age', defaultValue: 0.0}}}}, 'K')",
	} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	embeddings := func(graph, config string) map[string][]float64 {
		t.Helper()
		result, err := run("MATCH (n:P) WITH collect(n) AS nodes CALL gds.fastRP.stream('" + graph + "', {embeddingDimension: 4, randomSeed: 7" + config + "}) YIELD nodeId, embedding " +
			"UNWIND nodes AS n WITH n, nodeId, embedding WHERE elementId(n) = nodeId OR id(n) = nodeId OR n.name = nodeId RETURN n.name, embedding")
		require.NoError(t, err)
		out := map[string][]float64{}
		for _, row := range result.Rows {
			out[row[0].(string)] = row[1].([]float64)
		}
		return out
	}
	zero := []float64{0, 0, 0, 0}
	norm := func(v []float64) float64 {
		sum := 0.0
		for _, x := range v {
			sum += x * x
		}
		return math.Sqrt(sum)
	}
	natural := embeddings("n", "")
	require.Len(t, natural, 4)
	// As GDS: in a→b→c (and a→a), b, c and the isolated node get zero.
	require.Equal(t, zero, natural["b"])
	require.Equal(t, zero, natural["c"])
	require.Equal(t, zero, natural["iso"])
	require.NotEqual(t, zero, natural["a"])
	require.Greater(t, norm(natural["a"]), 1.0, "the iterations' sum isn't normalised")
	reverse := embeddings("r", "")
	require.NotEqual(t, fmt.Sprint(natural), fmt.Sprint(reverse))
	undirected := embeddings("u", "")
	require.NotEqual(t, fmt.Sprint(natural), fmt.Sprint(undirected))
	require.NotEqual(t, zero, undirected["c"])
	require.Equal(t, zero, undirected["iso"])
	// nodeSelfInfluence adds the start vector back.
	require.NotEqual(t, zero, embeddings("n", ", nodeSelfInfluence: 0.5")["iso"])
	// Weights from a projected property, with a default for the missing one.
	require.NotEqual(t, fmt.Sprint(embeddings("d", "")), fmt.Sprint(embeddings("d", ", relationshipWeightProperty: 'weight'")))
	require.NotEqual(t, zero, embeddings("na", ", propertyRatio: 0.5, featureProperties: ['age'], nodeLabels: ['P'], relationshipTypes: ['K'], iterationWeights: [1.0]")["a"])

	refused := func(graph, config, message string) {
		t.Helper()
		_, err := run("CALL gds.fastRP.stream('" + graph + "', {" + config + "})")
		require.ErrorContains(t, err, message, config)
	}
	refused("w", "", "No value specified for the mandatory configuration parameter `embeddingDimension`")
	refused("w", "embeddingDimension: 0", "Value for `embeddingDimension` was `0`, but must be within the range [1, 8192].")
	refused("w", "embeddingDimension: 'x'", "The value of `embeddingDimension` must be of type `Integer` but was `String`.")
	refused("w", "embeddingDimension: 2.5", "The value of `embeddingDimension` must be of type `Integer` but was `Double`.")
	refused("w", "embeddingDimension: 4, nosuchKey: 1", "Unexpected configuration key: nosuchKey")
	refused("w", "embeddingDimension: 4, writeProperty: 'x'", "Unexpected configuration key: writeProperty")
	refused("w", "embeddingDimension: 4, relationshipWeightProperty: 'nope'", "Relationship weight property `nope` not found in relationship types ['K']. Properties existing on all relationship types: ['weight']")
	refused("w", "embeddingDimension: 4, relationshipWeightProperty: 'weight'", "Missing relationship property `weight` on relationship between nodes with ids `")
	refused("n", "embeddingDimension: 4, relationshipWeightProperty: 'weight'", "Relationship weight property `weight` not found in relationship types ['K']. Properties existing on all relationship types: []")
	refused("w", "embeddingDimension: 4, propertyRatio: 0.5", "When `propertyRatio` is non-zero, `featureProperties` may not be empty.")
	refused("n", "embeddingDimension: 4, propertyRatio: 0.5, featureProperties: ['age']", "has invalid feature property value `NaN` for property `age`")
	refused("w", "embeddingDimension: 4, propertyRatio: 0.5, featureProperties: ['age']", "The feature properties ['age'] are not present for all requested labels. Requested labels: ['P']. Properties available on all requested labels: []")
	refused("w", "embeddingDimension: 4, iterationWeights: []", "The value of `iterationWeights` must not be empty if `nodeSelfInfluence` is zero.")
	refused("w", "embeddingDimension: 4, iterationWeights: 'x'", "The value of `iterationWeights` must be of type `List` but was `String`.")
	refused("w", "embeddingDimension: 4, iterationWeights: [1, 'x']", "Iteration weights must be numbers, but found `x` of type `String`")
	refused("w", "embeddingDimension: 4, propertyRatio: 'x'", "The value of `propertyRatio` must be of type `Double` but was `String`.")
	refused("w", "embeddingDimension: 4, randomSeed: 'x'", "The value of `randomSeed` must be of type `Long` but was `String`.")
	refused("w", "embeddingDimension: 4, normalizationStrength: 'x'", "The value of `normalizationStrength` must be of type `Number` but was `String`.")
	refused("w", "embeddingDimension: 4, nodeSelfInfluence: -1", "The value of `nodeSelfInfluence` may not be negative.")
	refused("w", "embeddingDimension: 4, relationshipWeightProperty: 5", "The value of `relationshipWeightProperty` must be of type `String` but was `Long`.")
	refused("w", "embeddingDimension: 4, featureProperties: 'age'", "The value of `featureProperties` must be of type `List` but was `String`.")
	refused("w", "embeddingDimension: 4, featureProperties: [5]", "The value of `featureProperties` must be of type `List` but was `List`.")
	refused("w", "embeddingDimension: 4, nodeLabels: ['Nope']", "Could not find the specified `nodeLabels` of ['Nope']. Available labels are ['P'].")
	refused("w", "embeddingDimension: 4, relationshipTypes: ['Nope']", "Could not find the specified `relationshipTypes` of ['Nope']. Available relationship types are ['K'].")
	refused("w", "embeddingDimension: 4, nodeLabels: 5", "The value of `nodeLabels` must be of type `List` but was `Long`.")
	refused("w", "embeddingDimension: 4, nodeLabels: [5]", "The value of `nodeLabels` must be of type `List` but was `List`.")
	refused("w", "embeddingDimension: 4, concurrency: 0", "Value for `concurrency` was `0`, but must be within the range [1, 2147483647].")
	refused("w", "embeddingDimension: 4, logProgress: 'x'", "The value of `logProgress` must be of type `Boolean` but was `String`.")
	refused("w", "embeddingDimension: 4, sudo: 'x'", "The value of `sudo` must be of type `Boolean` but was `String`.")
	refused("missing", "embeddingDimension: 4", "Graph with name `missing` does not exist on database `gds`. It might exist on another database.")

	// stats runs FastRP and reports GDS's columns.
	result, err := run("CALL gds.fastRP.stats('n', {embeddingDimension: 4, randomSeed: -3, relationshipWeightProperty: null})")
	require.NoError(t, err)
	require.Equal(t, []string{"nodeCount", "preProcessingMillis", "computeMillis", "configuration"}, result.Columns)
	require.Equal(t, int64(4), result.Rows[0][0])
	configuration := result.Rows[0][3].(map[string]interface{})
	require.Equal(t, int64(4), configuration["embeddingDimension"])
	require.Equal(t, int64(-3), configuration["randomSeed"])
	require.Equal(t, []interface{}{0.0, 1.0, 1.0}, configuration["iterationWeights"])
	require.Equal(t, []interface{}{"*"}, configuration["nodeLabels"])
	require.NotEmpty(t, configuration["jobId"])
	_, err = run("CALL gds.fastRP.stats('w', {embeddingDimension: 4, relationshipWeightProperty: 'weight'})")
	require.ErrorContains(t, err, "Missing relationship property `weight`")
	result, err = run("CALL gds.fastRP.stats('d', {embeddingDimension: 4, relationshipWeightProperty: 'weight', jobId: 'j'})")
	require.NoError(t, err)
	require.Equal(t, "weight", result.Rows[0][3].(map[string]interface{})["relationshipWeightProperty"])
}

// TestLinkPredictionAsGDS: the stream procedures read the named projection,
// count degrees by relationships and give GDS's raw scores, the same values
// the gds.alpha.linkprediction.* functions give (#1192).
func TestLinkPredictionAsGDS(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "lp")
	exec := NewStorageExecutor(engine)
	run := func(query string) (*ExecuteResult, error) { return exec.Execute(context.Background(), query, nil) }
	for _, id := range []string{"a", "b", "c", "d", "iso"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, Properties: map[string]interface{}{"name": id}})
		require.NoError(t, err)
	}
	// a→a, a→b twice, b→c, c→b, c→d: the issue's repeated relationships.
	for i, edge := range [][3]string{{"a", "a", "K"}, {"a", "b", "K"}, {"a", "b", "K"}, {"b", "c", "K"}, {"c", "b", "K"}, {"c", "d", "L"}} {
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: storage.NodeID(edge[0]), EndNode: storage.NodeID(edge[1]), Type: edge[2]}))
	}
	scores := func(query string) map[string]float64 {
		t.Helper()
		result, err := run(query)
		require.NoError(t, err, query)
		out := map[string]float64{}
		for _, row := range result.Rows {
			out[row[1].(string)] = row[2].(float64)
		}
		return out
	}
	// GDS 2.13.12's values for (a, c).
	require.InDelta(t, 0.7213475204444817, scores("CALL gds.linkPrediction.adamicAdar.stream({sourceNode: 'a'})")["c"], 1e-12)
	require.InDelta(t, 0.25, scores("CALL gds.linkPrediction.resourceAllocation.stream({sourceNode: 'a'})")["c"], 1e-12)
	require.InDelta(t, 1.0, scores("CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 'a'})")["c"], 1e-12)
	require.InDelta(t, 6.0, scores("CALL gds.linkPrediction.preferentialAttachment.stream({sourceNode: 'a', relationshipTypes: ['K']})")["c"], 1e-12)

	// Every candidate's score is the function's value for the pair.
	for procedure, function := range map[string]string{
		"adamicAdar": "adamicAdar", "resourceAllocation": "resourceAllocation",
		"commonNeighbors": "commonNeighbors", "preferentialAttachment": "preferentialAttachment",
	} {
		for target, score := range scores("CALL gds.linkPrediction." + procedure + ".stream({sourceNode: 'b', topK: 100})") {
			result, err := run("MATCH (x {name: 'b'}), (y {name: '" + target + "'}) RETURN gds.alpha.linkprediction." + function + "(x, y)")
			require.NoError(t, err)
			require.InDelta(t, result.Rows[0][0].(float64), score, 1e-12, procedure+" "+target)
		}
	}

	// A raw 1 stays 1: preferential attachment isn't rescaled to 0 (c→d is
	// d's one L relationship, and b's one with... only K, so 0).
	pa := scores("CALL gds.linkPrediction.preferentialAttachment.stream({sourceNode: 'd', relationshipTypes: 'L'})")
	require.InDelta(t, 0.0, pa["iso"], 1e-12)

	// The named projection: its types, as projected.
	_, err := run("CALL gds.graph.project('gk', 'P', 'K')")
	require.NoError(t, err)
	require.NotContains(t, scores("CALL gds.linkPrediction.commonNeighbors.stream('gk', {sourceNode: 'b'})"), "d")
	require.Contains(t, scores("CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 'b'})"), "d")
	// Writes after projecting aren't in the graph.
	_, err = engine.CreateNode(&storage.Node{ID: "late", Labels: []string{"P"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "late-a", StartNode: "late", EndNode: "a", Type: "K"}))
	require.NotContains(t, scores("CALL gds.linkPrediction.commonNeighbors.stream('gk', {sourceNode: 'b'})"), "late")
	require.Contains(t, scores("CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 'b'})"), "late")
	// An undirected projection's relationship counts once in a degree.
	_, err = run("CALL gds.graph.project('gu', 'P', {K: {orientation: 'UNDIRECTED'}})")
	require.NoError(t, err)
	require.InDelta(t, 0.7213475204444817, scores("CALL gds.linkPrediction.adamicAdar.stream('gu', {sourceNode: 'a'})")["c"], 1e-12)
	require.Contains(t, scores("CALL gds.linkPrediction.predict.stream('gu', {sourceNode: 'a', algorithm: 'resource_allocation'})"), "c")

	for query, message := range map[string]string{
		"CALL gds.linkPrediction.adamicAdar.stream('nope', {sourceNode: 'a'})":                         "Graph with name `nope` does not exist on database `lp`. It might exist on another database.",
		"CALL gds.linkPrediction.adamicAdar.stream({sourceNode: 'no-such-id'})":                        "sourceNode `no-such-id` does not exist in the graph",
		"CALL gds.linkPrediction.adamicAdar.stream('gk', {sourceNode: 'late'})":                        "sourceNode `late` does not exist in the graph",
		"CALL gds.linkPrediction.adamicAdar.stream({sourceNode: 'a', topK: 0})":                        "Value for `topK` was `0`, but must be within the range [1, 2147483647].",
		"CALL gds.linkPrediction.adamicAdar.stream({sourceNode: 'a', topK: 'x'})":                      "The value of `topK` must be of type `Integer` but was `String`.",
		"CALL gds.linkPrediction.adamicAdar.stream({sourceNode: 'a', nosuch: 1})":                      "Unexpected configuration key: nosuch",
		"CALL gds.linkPrediction.adamicAdar.stream({sourceNode: true})":                                "The value of `sourceNode` must be of type `Node` but was `Boolean`.",
		"CALL gds.linkPrediction.adamicAdar.stream('gk', {sourceNode: 'a', relationshipTypes: ['L']})": "Could not find the specified `relationshipTypes` of ['L']. Available relationship types are ['K'].",
		"CALL gds.linkPrediction.predict.stream({sourceNode: 'a', algorithm: 5})":                      "The value of `algorithm` must be of type `String` but was `Long`.",
		"CALL gds.linkPrediction.predict.stream({sourceNode: 'a', topologyWeight: 'x'})":               "The value of `topologyWeight` must be of type `Double` but was `String`.",
		"CALL gds.linkPrediction.predict.stream({sourceNode: 'a', semanticWeight: 'x'})":               "The value of `semanticWeight` must be of type `Double` but was `String`.",
		"CALL gds.linkPrediction.predict.stream({sourceNode: 'a', minThreshold: 'x'})":                 "The value of `minThreshold` must be of type `Double` but was `String`.",
	} {
		_, err := run(query)
		require.ErrorContains(t, err, message, query)
	}
}
