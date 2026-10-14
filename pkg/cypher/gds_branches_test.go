package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestGDSProjectionBranches covers the projection forms and refusals the
// GDS tests don't reach (#1190).
func TestGDSProjectionBranches(t *testing.T) {
	_, run := newGDSGraph(t)
	for _, query := range []string{
		// A map entry naming its label or type by text, or by nothing.
		"CALL gds.graph.project('m1', {P: null}, {K: 'K'})",
		"CALL gds.graph.project('m2', {Q: 'P'}, {R: 'K', K: null})",
		// Property entries by text, by nothing, and with every setting.
		"CALL gds.graph.project('m3', {P: {properties: {years: 'age', age: null}}}, {K: {properties: {w: {property: 'weight', defaultValue: 0.5, aggregation: 'MAX'}}}})",
		// A label listed twice.
		"CALL gds.graph.project('m4', ['P', 'P'], 'K')",
		// The default value of a property no node has projects it anyway.
		"CALL gds.graph.project('m5', {P: {properties: {z: {property: 'zzz', defaultValue: 2.0}}}}, {K: {properties: {n: {property: 'nothing', aggregation: 'COUNT'}}}})",
	} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	for query, message := range map[string]string{
		"CALL gds.graph.project('x', 'P', {K: {properties: {w: {bogus: 1}}}})":       "Unexpected configuration key: bogus",
		"CALL gds.graph.project('x', 'P', {K: {properties: {w: {property: 5}}}})":    "The value of `property` must be of type `String` but was `Long`.",
		"CALL gds.graph.project('x', 'P', {K: {properties: {w: {aggregation: 5}}}})": "The value of `aggregation` must be of type `String` but was `Long`.",
		"CALL gds.graph.project('x', 'P', {K: {aggregation: 5}})":                    "The value of `aggregation` must be of type `String` but was `Long`.",
		"CALL gds.graph.project('x', 'P', {K: {type: 5}})":                           "The value of `type` must be of type `String` but was `Long`.",
		"CALL gds.graph.project('x', [['P']], 'K')":                                  "Cannot construct a node projection out of a java.util.List",
		"CALL gds.graph.project('x', [{a: 1}], 'K')":                                 "Cannot construct a node projection out of a java.util.Map",
		"CALL gds.graph.project('x', date('2026-01-01'), 'K')":                       "Cannot construct a node projection out of a DATE",
		"CALL gds.graph.project('x', true, 'K')":                                     "Cannot construct a node projection out of a java.lang.Boolean",
		"CALL gds.graph.project('x', 'P', {K: 1.5})":                                 "Cannot construct a relationship projection out of a java.lang.Double",
	} {
		_, err := run(query)
		require.ErrorContains(t, err, message, query)
	}

	// Aggregation over three parallel relationships, one without the
	// property: COUNT counts each, SUM skips the missing value.
	_, err := run("CREATE (x:P {name:'x'})-[:K {weight: 3.0}]->(y:P {name:'y'}), (x)-[:K]->(y), (x)-[:K {weight: 4.0}]->(y)")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('sum', 'P', {K: {aggregation: 'SUM', properties: 'weight'}})")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('count', 'P', {K: {aggregation: 'SINGLE', properties: {n: {property: 'weight', aggregation: 'COUNT'}, s: {property: 'weight', aggregation: 'SUM'}}}})")
	require.NoError(t, err)
	values := func(graph, key string) []float64 {
		var out []float64
		for _, relationship := range graphProjections[gdsCatalogKey("gds", graph)].Relationships {
			if value, ok := relationship.Properties[key]; ok && relationship.Source != relationship.Target {
				out = append(out, value)
			}
		}
		return out
	}
	require.Contains(t, values("sum", "weight"), 7.0)
	require.Contains(t, values("count", "n"), 3.0)
	require.Contains(t, values("count", "s"), 7.0)

	// A failed read of the stored nodes or relationships fails the
	// projection.
	failure := errors.New("boom")
	for _, engine := range []storage.Engine{
		&streamErrEngine{MemoryEngine: newTestMemoryEngine(t), nodes: failure},
		&streamErrEngine{MemoryEngine: newTestMemoryEngine(t), edges: failure},
	} {
		_, err := NewStorageExecutor(engine).callGdsGraphProject([]interface{}{"broken", "*", "*"})
		require.ErrorContains(t, err, "boom")
	}

	// GDS's Java type names.
	require.Equal(t, "null", gdsJavaType(nil))
	require.Equal(t, "HashMap", gdsJavaType(map[string]interface{}{}))
	require.Equal(t, "Node", gdsJavaType(&storage.Node{}))
	require.Equal(t, "java.util.Map", gdsJavaClass(map[string]interface{}{}))
}

// TestFastRPBranches covers FastRP's filters and helpers the GDS tests
// don't reach (#1191).
func TestFastRPBranches(t *testing.T) {
	_, run := newGDSGraph(t)
	_, err := run("CREATE (q:Q {name: 'q', age: 3})-[:L]->(:P {name: 'p2', age: 4})")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('two', {P: {properties: {age: {property: 'age', defaultValue: 0.0}}}, Q: {properties: 'age'}}, ['K', 'L'])")
	require.NoError(t, err)
	stream := func(config string) [][]interface{} {
		t.Helper()
		result, err := run("CALL gds.fastRP.stream('two', {embeddingDimension: 4" + config + "})")
		require.NoError(t, err, config)
		return result.Rows
	}
	require.Len(t, stream(""), 6)
	// nodeLabels keeps the Q node only; its relationship to a P node goes.
	require.Len(t, stream(", nodeLabels: ['Q']"), 1)
	// relationshipTypes drops the L relationship.
	require.Len(t, stream(", relationshipTypes: ['K']"), 6)
	// Feature properties on every requested label.
	require.Len(t, stream(", propertyRatio: 0.5, featureProperties: ['age']"), 6)
	for config, message := range map[string]string{
		", normalizationStrength: 'x'":                     "The value of `normalizationStrength` must be of type `Number` but was `String`.",
		", nodeSelfInfluence: 'x'":                         "The value of `nodeSelfInfluence` must be of type `Number` but was `String`.",
		", relationshipTypes: 5":                           "The value of `relationshipTypes` must be of type `List` but was `Long`.",
		", iterationWeights: [null]":                       "Iteration weights must be numbers, but found `<nil>` of type `null`",
		", relationshipWeightProperty: date('2026-01-01')": "The value of `relationshipWeightProperty` must be of type `String` but was `DATE`.",
	} {
		_, err := run("CALL gds.fastRP.stream('two', {embeddingDimension: 4" + config + "})")
		require.ErrorContains(t, err, message, config)
	}
	_, err = run("CALL gds.fastRP.stream('two', {embeddingDimension: {}})")
	require.ErrorContains(t, err, "The value of `embeddingDimension` must be of type `Integer` but was `HashMap`.")

	require.Nil(t, commonKeys(map[string][]string{"P": {"a"}}, nil))
	require.Equal(t, []string{"a"}, commonKeys(map[string][]string{"P": {"a", "b"}, "Q": {"a"}}, []string{"P", "Q"}))
	require.False(t, anyString([]string{"P"}, []string{"Q"}))
	number, ok := fastRPConfigInt(3.0)
	require.True(t, ok)
	require.Equal(t, 3, number)
	_, ok = fastRPConfigInt("x")
	require.False(t, ok)
}

// TestLinkPredictionProjectionTypeFilter: relationshipTypes narrows a named
// projection's relationships (#1192).
func TestLinkPredictionProjectionTypeFilter(t *testing.T) {
	_, run := newGDSGraph(t)
	_, err := run("MATCH (c:P {name: 'c'}), (iso:P {name: 'iso'}) CREATE (c)-[:L]->(iso)")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('kl', 'P', ['K', 'L'])")
	require.NoError(t, err)
	both, err := run("MATCH (b:P {name: 'b'}) CALL gds.linkPrediction.commonNeighbors.stream('kl', {sourceNode: b}) YIELD node2 RETURN count(*)")
	require.NoError(t, err)
	onlyK, err := run("MATCH (b:P {name: 'b'}) CALL gds.linkPrediction.commonNeighbors.stream('kl', {sourceNode: b, relationshipTypes: ['K']}) YIELD node2 RETURN count(*)")
	require.NoError(t, err)
	require.Greater(t, both.Rows[0][0].(int64), onlyK.Rows[0][0].(int64))
}

// streamErrEngine fails streaming its nodes or relationships.
type streamErrEngine struct {
	*storage.MemoryEngine
	nodes, edges error
}

func (e *streamErrEngine) StreamNodes(ctx context.Context, fn func(node *storage.Node) error) error {
	if e.nodes != nil {
		return e.nodes
	}
	return e.MemoryEngine.StreamNodes(ctx, fn)
}

func (e *streamErrEngine) StreamEdges(ctx context.Context, fn func(edge *storage.Edge) error) error {
	if e.edges != nil {
		return e.edges
	}
	return e.MemoryEngine.StreamEdges(ctx, fn)
}
