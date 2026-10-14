package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newAggregationProjectionGraph is GDS's probe graph for the Cypher
// aggregation projection: documents mentioning parties, and a lone
// document.
func newAggregationProjectionGraph(t *testing.T) (*StorageExecutor, func(string) [][]interface{}, func(string) error) {
	t.Helper()
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "agg"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (d1:Doc {name:'d1'}), (d2:Doc {name:'d2'}), (d3:Doc {name:'d3'}), (p1:Party {name:'p1', size: 2}), (p2:Party {name:'p2', size: 2.5}), (lone:Doc {name:'lone'}),
		(d1)-[:M]->(p1), (d2)-[:M]->(p1), (d2)-[:M]->(p2), (d3)-[:M]->(p2)`, nil)
	require.NoError(t, err)
	rows := func(query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	fails := func(query string) error {
		_, err := exec.Execute(ctx, query, nil)
		return err
	}
	return exec, rows, fails
}

// TestGDSAggregationProjection: RETURN gds.graph.project(...) projects the
// rows' graph as GDS 2.13.12 does on the same graph (#1194).
func TestGDSAggregationProjection(t *testing.T) {
	_, rows, fails := newAggregationProjectionGraph(t)

	// Computed relationships with a weight, undirected, and a document with
	// no party (a null target) alone: GDS gives 6 nodes, 8 relationships.
	query := "MATCH (s:Doc) OPTIONAL MATCH (s)-[:M]->(t:Party) WITH s, t, 1.0 / log(2 + COUNT { (t)--() }) AS w " +
		"RETURN gds.graph.project('a1', s, t, {relationshipType: 'SHARES', relationshipProperties: {weight: w}}, {undirectedRelationshipTypes: ['*']}) AS g"
	result := rows(query)[0][0].(map[string]interface{})
	require.Equal(t, "a1", result["graphName"])
	require.Equal(t, int64(6), result["nodeCount"])
	require.Equal(t, int64(8), result["relationshipCount"])
	require.Equal(t, query, result["query"])
	require.GreaterOrEqual(t, result["projectMillis"].(int64), int64(0))
	configuration := result["configuration"].(map[string]interface{})
	require.Equal(t, []interface{}{"*"}, configuration["undirectedRelationshipTypes"])
	require.Equal(t, []interface{}{}, configuration["inverseIndexedRelationshipTypes"])
	require.Equal(t, int64(4), configuration["readConcurrency"])
	require.Equal(t, true, configuration["logProgress"])
	require.Equal(t, query, configuration["query"])
	require.NotEmpty(t, configuration["jobId"])
	listed := rows("CALL gds.graph.list('a1') YIELD nodeCount, relationshipCount, schemaWithOrientation RETURN nodeCount, relationshipCount, schemaWithOrientation")[0]
	require.Equal(t, int64(6), listed[0])
	require.Equal(t, int64(8), listed[1])
	require.Equal(t, map[string]interface{}{
		"graphProperties": map[string]interface{}{},
		"nodes":           map[string]interface{}{"__ALL__": map[string]interface{}{}},
		"relationships": map[string]interface{}{"SHARES": map[string]interface{}{
			"properties": map[string]interface{}{"weight": "Float (DefaultValue(NaN), TRANSIENT, Aggregation.NONE)"}, "direction": "UNDIRECTED",
		}},
	}, listed[2])

	// Labels and a type; the default projection; node ids.
	result = rows("MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('a2', s, t, {sourceNodeLabels: labels(s), targetNodeLabels: labels(t), relationshipType: 'M'})")[0][0].(map[string]interface{})
	require.Equal(t, int64(5), result["nodeCount"])
	require.Equal(t, int64(4), result["relationshipCount"])
	require.Equal(t, []string{"Doc", "Party"}, graphProjections[gdsCatalogKey("agg", "a2")].Labels)
	require.Equal(t, int64(4), rows("MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('a3', s, t)")[0][0].(map[string]interface{})["relationshipCount"])
	require.Equal(t, int64(1), rows("RETURN gds.graph.project('a4', 1)")[0][0].(map[string]interface{})["nodeCount"])
	// Node properties, both sides or one with the other null.
	rows("MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('a5', s, t, {sourceNodeProperties: null, targetNodeProperties: {size: t.size}, sourceNodeLabels: null, targetNodeLabels: 'Party'})")
	schema := rows("CALL gds.graph.list('a5') YIELD schema RETURN schema")[0][0].(map[string]interface{})
	require.Equal(t, map[string]interface{}{"size": "Float (DefaultValue(NaN), PERSISTENT)"}, schema["nodes"].(map[string]interface{})["Party"])
	// The other configuration keys are ignored, as GDS ignores them.
	rows("MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('a6', s, t, {}, {bogus: 1, readConcurrency: 2, jobId: 'j', logProgress: false, inverseIndexedRelationshipTypes: ['__ALL__']})")
	// One graph per group; the first row's name.
	grouped := rows("MATCH (s:Doc)-[:M]->(t:Party) WITH s, t, s.name AS k RETURN k, gds.graph.project('g-' + k, s, t) AS g ORDER BY k")
	require.Len(t, grouped, 3)
	require.Equal(t, int64(2), grouped[1][1].(map[string]interface{})["relationshipCount"])
	// No rows: null.
	require.Equal(t, [][]interface{}{{nil}}, rows("MATCH (s:Doc) WHERE false RETURN gds.graph.project('none', s, s) AS g"))
	// FastRP and link prediction read the projection.
	require.Len(t, rows("CALL gds.fastRP.stream('a1', {embeddingDimension: 4, relationshipWeightProperty: 'weight'})"), 6)
	require.Equal(t, [][]interface{}{{true}}, rows("RETURN gds.graph.exists('a1')"))

	for query, message := range map[string]string{
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('a3', s, t)":                                                          "Graph a3 already exists",
		"MATCH (s:Doc) RETURN gds.graph.project('x', null, s)":                                                                        "The node has to be either a NODE or an INTEGER, but got NO_VALUE",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, 'notanode')":                                                  "The node has to be either a NODE or an INTEGER, but got String",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {sourceNodeLabels: 5})":                                    "The configuration key 'targetNodeLabels' is missing, but 'sourceNodeLabels' is provided.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {targetNodeProperties: {a: 1}})":                           "The configuration key 'sourceNodeProperties' is missing, but 'targetNodeProperties' is provided.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipType: 5})":                                    "The value of `relationshipType` must be `String`, but was `NUMBER`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {}, {undirectedRelationshipTypes: ['NOPE']})":              "Specified undirectedRelationshipTypes `[NOPE]` were not projected in the graph. Projected types are: `['__ALL__']`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {}, {undirectedRelationshipTypes: 'x'})":                   "The value of `undirectedRelationshipTypes` must be of type `List` but was `String`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {sourceNodeLabels: true, targetNodeLabels: false})":        "Using `true` to load all labels is deprecated, use `{ sourceNodeLabels: labels(s) }` instead",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipProperties: {w: 1}}, {readConcurrency: 'x'})": "Unsupported Concurrency input of type String",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {}, {}, {})":                                               "The parameters for `nodesConfig` and `relationshipsConfig` have been merged.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipProperties: {w: 'x'}})":                       "Unsupported conversion to GDS Value from Neo4j Value with type `String`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {bogus: 1})":                                               "Unexpected configuration key: bogus",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x')":                                                                 "gds.graph.project",
	} {
		err := fails(query)
		require.ErrorContains(t, err, message, query)
	}
}

// TestAggregatePathsAfterOneList: with one list of aggregates, the
// traversal path's stDev and percentiles are aggregates (they were taken
// for plain expressions), a qualified function with an aggregate's name
// (apoc.coll.sum) isn't one, and gds.graph.project works in WITH and is
// listed as aggregating (#1194).
func TestAggregatePathsAfterOneList(t *testing.T) {
	_, rows, _ := newAggregationProjectionGraph(t)
	rows("MATCH (d:Doc) SET d.x = CASE d.name WHEN 'd1' THEN 1 WHEN 'd2' THEN 3 ELSE 5 END")
	got := rows("MATCH (d:Doc)-[:M]->(p:Party) RETURN p.name AS party, stDev(d.x) AS sd, percentileDisc(d.x, 0.5) AS median ORDER BY party")
	// p1: d1 (1), d2 (3); p2: d2 (3), d3 (5): as Neo4j gives them.
	require.Equal(t, "p1", got[0][0])
	require.InDelta(t, 1.4142135623730951, got[0][1].(float64), 1e-12)
	require.Equal(t, int64(1), got[0][2])
	require.InDelta(t, 1.4142135623730951, got[1][1].(float64), 1e-12)
	require.Equal(t, int64(3), got[1][2])
	require.Equal(t, [][]interface{}{{6.0}}, rows("RETURN apoc.coll.sum([1, 2, 3]) AS total"))

	got = rows("MATCH (s:Doc)-[:M]->(t:Party) WITH gds.graph.project('w', s, t) AS g RETURN g.graphName, g.nodeCount, g.relationshipCount")
	require.Equal(t, [][]interface{}{{"w", int64(5), int64(4)}}, got)
	listed := rows("SHOW FUNCTIONS YIELD name, aggregating, isBuiltIn, signature WHERE name = 'gds.graph.project' RETURN aggregating, isBuiltIn, signature")
	require.Equal(t, [][]interface{}{{true, false, "gds.graph.project(graphName :: STRING, sourceNode :: ANY, targetNode = null :: ANY, dataConfig = null :: ANY, configuration = null :: ANY, alphaMigrationConfig = null :: ANY) :: MAP"}}, listed)
}

// TestGDSAggregationProjectionBranches covers the refusals and forms the GDS
// probes don't reach.
func TestGDSAggregationProjectionBranches(t *testing.T) {
	_, rows, fails := newAggregationProjectionGraph(t)
	// false labels; integer node properties; nodes only, undirected.
	rows("MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('b1', s, t, {sourceNodeLabels: false, targetNodeLabels: false, sourceNodeProperties: {n: 1}, targetNodeProperties: null})")
	schema := rows("CALL gds.graph.list('b1') YIELD schema RETURN schema")[0][0].(map[string]interface{})
	require.Equal(t, map[string]interface{}{"n": "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"}, schema["nodes"].(map[string]interface{})["__ALL__"])
	for query, message := range map[string]string{
		"MATCH (s:Doc) RETURN gds.graph.project('x', s, null, {}, {undirectedRelationshipTypes: ['NOPE']})":                               "Projected types are: `['__ALL__']`.",
		"WITH 5 AS g MATCH (s:Doc) RETURN gds.graph.project(g, s)":                                                                        "The value of `graphName` must be of type `String` but was `Long`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {sourceNodeProperties: 5, targetNodeProperties: null})":        "Unsupported conversion to GDS Value from Neo4j Value with type `Long`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {sourceNodeProperties: {a: 'x'}, targetNodeProperties: null})": "Unsupported conversion to GDS Value from Neo4j Value with type `String`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, 5)":                                                            "The value of `dataConfig` must be of type `Map` but was `Long`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipProperties: 5})":                                  "Unsupported conversion to GDS Value from Neo4j Value with type `Long`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {sourceNodeLabels: 5, targetNodeLabels: null})":                "The value of `sourceNodeLabels` must be of type `List` but was `Long`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {sourceNodeLabels: [5], targetNodeLabels: null})":              "The value of `sourceNodeLabels` must be of type `List` but was `List`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {}, 5)":                                                        "The value of `configuration` must be of type `Map` but was `Long`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {}, {undirectedRelationshipTypes: [5]})":                       "The value of `undirectedRelationshipTypes` must be of type `List` but was `List`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {}, {jobId: 7})":                                               "Expected JobId or String. Got Long.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {}, {logProgress: 'x'})":                                       "The value of `logProgress` must be of type `Boolean` but was `String`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipType: true})":                                     "but was `BOOLEAN`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipType: [1]})":                                      "but was `LIST`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipType: {}})":                                       "but was `MAP`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipType: s})":                                        "but was `NODE`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', s, t, {relationshipType: date('2026-01-01')})":                       "but was `DATE`.",
		"MATCH (s:Doc)-[:M]->(t:Party) RETURN gds.graph.project('x', date('not a date'), t)":                                              "",
	} {
		err := fails(query)
		require.Error(t, err, query)
		require.ErrorContains(t, err, message, query)
	}
}

// TestGDSAggregationProjectionFirstOccurrence: a node named by several rows
// keeps its first row's labels and properties, and a key another label has
// reads NaN on it, as GDS 2.13.12 gives on the same graph (#1196 review).
func TestGDSAggregationProjectionFirstOccurrence(t *testing.T) {
	_, rows, _ := newAggregationProjectionGraph(t)
	rows("MATCH (n) DETACH DELETE n")
	rows("CREATE (a:A {name:'a', x: 1}), (b:B {name:'b', y: 2}), (c:A {name:'c', x: 3}), (a)-[:R]->(b), (c)-[:R]->(a)")
	// a first comes as a source (label A, x), then as a target (T, z).
	rows("MATCH (s)-[:R]->(t) WITH s, t ORDER BY s.name RETURN gds.graph.project('first', s, t, {sourceNodeLabels: labels(s), targetNodeLabels: ['T'], sourceNodeProperties: s{.x}, targetNodeProperties: {z: 9}})")
	schema := rows("CALL gds.graph.list('first') YIELD schema RETURN schema")[0][0].(map[string]interface{})
	require.Equal(t, map[string]interface{}{
		"A": map[string]interface{}{"x": "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"},
		"T": map[string]interface{}{"z": "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"},
	}, schema["nodes"])
	values := rows("MATCH (n {name: 'a'}) RETURN gds.util.nodeProperty('first', n, 'x'), gds.util.nodeProperty('first', n, 'z')")[0]
	require.Equal(t, 1.0, values[0])
	require.True(t, math.IsNaN(values[1].(float64)))
	projection := graphProjections[gdsCatalogKey("agg", "first")]
	for id, labels := range projection.NodeLabels {
		if projection.NodeProperties[id]["x"] == 1 {
			require.Equal(t, []string{"A"}, labels)
		}
	}
}
