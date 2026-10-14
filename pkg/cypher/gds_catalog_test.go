package cypher

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestGDSCatalogAsGDS: gds.graph.list, drop and exists, the project
// configuration map, gds.graph.exists() and gds.util.nodeProperty() as GDS
// 2.13.12 gives them on the same graph (#1161).
func TestGDSCatalogAsGDS(t *testing.T) {
	exec, run := newGDSGraph(t)
	rows := func(query string) [][]interface{} {
		t.Helper()
		result, err := run(query)
		require.NoError(t, err, query)
		return result.Rows
	}
	rows("CALL gds.graph.project('g', {P: {properties: 'age'}}, {K: {properties: 'weight'}}, {})")

	result, err := run("CALL gds.graph.list('g')")
	require.NoError(t, err)
	require.Equal(t, []string{"degreeDistribution", "graphName", "database", "databaseLocation", "memoryUsage", "sizeInBytes", "nodeCount", "relationshipCount",
		"configuration", "density", "creationTime", "modificationTime", "schema", "schemaWithOrientation"}, result.Columns)
	row := result.Rows[0]
	// GDS: out-degrees 2, 1, 0, 0.
	require.Equal(t, map[string]interface{}{"min": int64(0), "max": int64(2), "mean": 0.75, "p50": int64(0), "p75": int64(1), "p90": int64(2), "p95": int64(2), "p99": int64(2), "p999": int64(2)}, row[0])
	require.Equal(t, "g", row[1])
	require.Equal(t, "gds", row[2])
	require.Equal(t, "local", row[3])
	require.Equal(t, "", row[4])
	require.Equal(t, int64(-1), row[5])
	require.Equal(t, int64(4), row[6])
	require.Equal(t, int64(3), row[7])
	configuration := row[8].(map[string]interface{})
	for key, want := range map[string]interface{}{"readConcurrency": int64(4), "logProgress": true, "sudo": false, "validateRelationships": false,
		"nodeProperties": map[string]interface{}{}, "relationshipProperties": map[string]interface{}{}} {
		require.Equal(t, want, configuration[key], key)
	}
	require.NotEmpty(t, configuration["jobId"])
	require.Contains(t, configuration, "nodeProjection")
	require.Contains(t, configuration, "relationshipProjection")
	require.Equal(t, 0.25, row[9])
	require.IsType(t, time.Time{}, row[10])
	require.Equal(t, map[string]interface{}{
		"graphProperties": map[string]interface{}{},
		"nodes":           map[string]interface{}{"P": map[string]interface{}{"age": "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"}},
		"relationships":   map[string]interface{}{"K": map[string]interface{}{"weight": "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)"}},
	}, row[12])
	require.Equal(t, map[string]interface{}{"properties": map[string]interface{}{"weight": "Float (DefaultValue(NaN), PERSISTENT, Aggregation.NONE)"}, "direction": "DIRECTED"},
		row[13].(map[string]interface{})["relationships"].(map[string]interface{})["K"])
	require.Len(t, rows("CALL gds.graph.list()"), 1)
	require.Empty(t, rows("CALL gds.graph.list('nope')"))

	// The configuration map: its settings, and properties every projection
	// takes.
	rows("CALL gds.graph.project('g2', 'P', {K: {orientation: 'UNDIRECTED', aggregation: 'SUM'}}, {readConcurrency: 2, jobId: 'j', logProgress: false, sudo: false, validateRelationships: false, nodeProperties: ['age'], relationshipProperties: {w: {property: 'weight', defaultValue: 1.5}}})")
	listed := rows("CALL gds.graph.list('g2') YIELD configuration, schema, schemaWithOrientation RETURN configuration, schema, schemaWithOrientation")[0]
	require.Equal(t, int64(2), listed[0].(map[string]interface{})["readConcurrency"])
	require.Equal(t, "j", listed[0].(map[string]interface{})["jobId"])
	require.Equal(t, map[string]interface{}{"w": "Float (DefaultValue(1.5), PERSISTENT, Aggregation.SUM)"}, listed[1].(map[string]interface{})["relationships"].(map[string]interface{})["K"])
	require.Equal(t, "UNDIRECTED", listed[2].(map[string]interface{})["relationships"].(map[string]interface{})["K"].(map[string]interface{})["direction"])
	for query, message := range map[string]string{
		"CALL gds.graph.project('x', 'P', 'K', {bogus: 1})":               "Unexpected configuration key: bogus",
		"CALL gds.graph.project('x', 'P', 'K', {readConcurrency: 0})":     "Value for `readConcurrency` was `0`, but must be within the range [1, 2147483647].",
		"CALL gds.graph.project('x', 'P', 'K', {sudo: 'x'})":              "The value of `sudo` must be of type `Boolean` but was `String`.",
		"CALL gds.graph.project('x', 'P', 'K', {nodeProperties: 5})":      "Cannot construct a property projection out of a java.lang.Long",
		"CALL gds.graph.project('x', 'P', 'K', 5)":                        "configuration",
		"CALL gds.graph.project('x', 'P', 'K', {nodeProperties: 'nope'})": "Node properties not found: 'nope'",
	} {
		_, err := run(query)
		require.ErrorContains(t, err, message, query)
	}

	// exists, as a procedure and a function.
	require.Equal(t, [][]interface{}{{"g", true}}, rows("CALL gds.graph.exists('g')"))
	require.Equal(t, [][]interface{}{{"nope", false}}, rows("CALL gds.graph.exists('nope')"))
	require.Equal(t, [][]interface{}{{true, false}}, rows("RETURN gds.graph.exists('g'), gds.graph.exists('nope')"))

	// nodeProperty: the projected value, NaN for a node without one.
	values := rows("MATCH (a:P {name: 'a'}), (c:P {name: 'c'}) RETURN gds.util.nodeProperty('g', a, 'age'), gds.util.nodeProperty('g', a, 'age', 'P'), gds.util.nodeProperty('g', c, 'age')")[0]
	require.Equal(t, 1.0, values[0])
	require.Equal(t, 1.0, values[1])
	require.True(t, math.IsNaN(values[2].(float64)))
	for query, message := range map[string]string{
		"MATCH (a:P {name: 'a'}) RETURN gds.util.nodeProperty('g', a, 'nope')":     "No node projection with property 'nope' exists.",
		"MATCH (a:P {name: 'a'}) RETURN gds.util.nodeProperty('g', a, 'age', 'Q')": "No node projection with property 'age' exists.",
		"MATCH (a:P {name: 'a'}) RETURN gds.util.nodeProperty('nope', a, 'age')":   "Graph with name `nope` does not exist on database `gds`. It might exist on another database.",
		"MATCH (a:P {name: 'a'}) RETURN gds.util.nodeProperty(5, a, 'age')":        "expects a graph name",
		"MATCH (a:P {name: 'a'}) RETURN gds.util.nodeProperty('g', a, 5)":          "expects a property key",
		"MATCH (a:P {name: 'a'}) RETURN gds.util.nodeProperty('g', a, 'age', 5)":   "expects a node label",
		"RETURN gds.util.nodeProperty('g', true, 'age')":                           "expects a node or node id",
		"RETURN gds.util.nodeProperty('g')":                                        "gds.util.nodeProperty",
		"RETURN gds.graph.exists(5)":                                               "expects a graph name",
		"RETURN gds.graph.exists()":                                                "gds.graph.exists",
	} {
		_, err := run(query)
		require.ErrorContains(t, err, message, query)
	}
	// A node the graph doesn't hold gives null.
	require.Equal(t, [][]interface{}{{nil}}, rows("RETURN gds.util.nodeProperty('g', 'no-such-node', 'age')"))

	// drop: GDS's columns; failIfMissing; a list; another database's catalog.
	result, err = run("CALL gds.graph.drop('g2', true, '')")
	require.NoError(t, err)
	require.Equal(t, gdsDropColumns, result.Columns)
	require.Equal(t, "g2", result.Rows[0][0])
	require.Empty(t, rows("CALL gds.graph.drop('nope', false)"))
	_, err = run("CALL gds.graph.drop('nope')")
	require.ErrorContains(t, err, "Graph with name `nope` does not exist on database `gds`. It might exist on another database.")
	_, err = run("CALL gds.graph.drop(['g', 'nope'])")
	require.ErrorContains(t, err, "Graph with name `nope` does not exist")
	require.Len(t, rows("CALL gds.graph.list()"), 1, "a failed drop drops nothing")
	require.Equal(t, [][]interface{}{{"g"}}, rows("CALL gds.graph.drop(['g', 'nope'], false) YIELD graphName RETURN graphName"))
	for query, message := range map[string]string{
		"CALL gds.graph.drop(5)":                "graphName",
		"CALL gds.graph.drop(['g', 5])":         "graphName",
		"CALL gds.graph.drop('g', 'x')":         "Type mismatch: expected Boolean but was String",
		"CALL gds.graph.drop('g', true, 5)":     "Type mismatch",
		"CALL gds.graph.drop('g', true, '', 5)": "Type mismatch",
		"CALL gds.graph.drop(null)":             "graphName",
		"CALL gds.graph.list(5)":                "Type mismatch",
		"CALL gds.graph.exists(null)":           "graphName",
	} {
		_, err := run(query)
		require.ErrorContains(t, err, message, query)
	}

	// Each database has its own catalog.
	other := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "other"))
	_, err = other.Execute(context.Background(), "CREATE (:P {name: 'x'})", nil)
	require.NoError(t, err)
	_, err = other.Execute(context.Background(), "CALL gds.graph.project('g', 'P', '*')", nil)
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('g', 'P', 'K')")
	require.NoError(t, err, "the same name in another database")
	result, err = other.Execute(context.Background(), "CALL gds.graph.list() YIELD graphName, nodeCount RETURN graphName, nodeCount", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"g", int64(1)}}, result.Rows)
	result, err = run("CALL gds.graph.drop('g', true, 'other') YIELD database RETURN database")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"other"}}, result.Rows)
	_ = exec
}

// TestGDSProjectionReviewFixes: a property's own aggregation merges
// parallel relationships, '*' counts them with COUNT only, indexInverse is
// reported as given, and a jobId must be a STRING, as GDS 2.13.12 does
// (#1193 review).
func TestGDSProjectionReviewFixes(t *testing.T) {
	_, run := newGDSGraph(t)
	rows := func(query string) [][]interface{} {
		t.Helper()
		result, err := run(query)
		require.NoError(t, err, query)
		return result.Rows
	}
	rows("CREATE (x:P {name:'x'})-[:K {weight: 3.0}]->(y:P {name:'y'}), (x)-[:K {weight: 5.0}]->(y)")
	// Five stored K relationships, two of them parallel: GDS gives 4.
	require.Equal(t, int64(5), rows("CALL gds.graph.project('plain', 'P', 'K') YIELD relationshipCount RETURN relationshipCount")[0][0])
	require.Equal(t, int64(4), rows("CALL gds.graph.project('count', 'P', {K: {properties: {n: {property: '*', aggregation: 'COUNT'}}}}) YIELD relationshipCount RETURN relationshipCount")[0][0])
	require.Equal(t, int64(4), rows("CALL gds.graph.project('sum', 'P', {K: {properties: {s: {property: 'weight', aggregation: 'SUM'}}}}) YIELD relationshipCount RETURN relationshipCount")[0][0])
	counted := false
	for _, relationship := range graphProjections[gdsCatalogKey("gds", "count")].Relationships {
		if relationship.Properties["n"] == 2 {
			counted = true
		}
	}
	require.True(t, counted, "the parallel pair counts 2")

	projection := rows("CALL gds.graph.project('inverse', 'P', {K: {indexInverse: true}}) YIELD relationshipProjection RETURN relationshipProjection")[0][0]
	require.Equal(t, true, projection.(map[string]interface{})["K"].(map[string]interface{})["indexInverse"])

	for query, message := range map[string]string{
		"CALL gds.graph.project('x', 'P', {K: {properties: {n: {property: '*'}}}})": "A '*' property key can only be used in combination with count aggregation.",
		"CALL gds.graph.project('x', 'P', 'K', {jobId: 7})":                         "Expected JobId or String. Got Long.",
		"CALL gds.fastRP.stream('plain', {embeddingDimension: 4, jobId: 7})":        "Expected JobId or String. Got Long.",
	} {
		_, err := run(query)
		require.ErrorContains(t, err, message, query)
	}
}

// TestGDSCatalogArgumentBranches: argument errors the procedure signature
// doesn't catch first, a failing argument, and a Float node property.
func TestGDSCatalogArgumentBranches(t *testing.T) {
	exec, run := newGDSGraph(t)
	_, err := exec.callGdsGraphList([]interface{}{int64(5)})
	require.ErrorContains(t, err, "graphName")
	_, err = exec.callGdsGraphDrop(context.Background(), []interface{}{"g", "x"})
	require.ErrorContains(t, err, "failIfMissing")
	_, err = exec.callGdsGraphDrop(context.Background(), []interface{}{"g", true, int64(5)})
	require.ErrorContains(t, err, "dbName")
	_, err = exec.callGdsGraphDrop(context.Background(), []interface{}{"g", true, "", int64(5)})
	require.ErrorContains(t, err, "username")
	// A failing argument fails the function.
	failing := cypherfn.Context{Eval: func(string) (interface{}, error) { return nil, errors.New("boom") }}
	_, err = fnGDSGraphExists(failing, []string{"x"})
	require.ErrorContains(t, err, "boom")
	_, err = fnGDSNodeProperty(failing, []string{"g", "n", "age"})
	require.ErrorContains(t, err, "boom")
	_, err = run("CREATE (:P {name: 'f', score: 1.5}), (:P {name: 'i', score: 2})")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('scored', {P: {properties: 'score'}}, 'K')")
	require.NoError(t, err)
	result, err := run("CALL gds.graph.list('scored') YIELD schema RETURN schema")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"score": "Float (DefaultValue(NaN), PERSISTENT)"}, result.Rows[0][0].(map[string]interface{})["nodes"].(map[string]interface{})["P"])
}

// TestGDSCatalogIsNeverCached: the catalog changes without a graph write,
// so the same gds.graph.list / exists statement sees a drop (#1161).
func TestGDSCatalogIsNeverCached(t *testing.T) {
	_, run := newGDSGraph(t)
	list := func() [][]interface{} {
		t.Helper()
		result, err := run("CALL gds.graph.list() YIELD graphName RETURN graphName")
		require.NoError(t, err)
		return result.Rows
	}
	_, err := run("CALL gds.graph.project('g', 'P', 'K')")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"g"}}, list())
	exists, err := run("RETURN gds.graph.exists('g')")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, exists.Rows)
	_, err = run("CALL gds.graph.drop('g')")
	require.NoError(t, err)
	require.Empty(t, list())
	exists, err = run("RETURN gds.graph.exists('g')")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{false}}, exists.Rows)
	require.False(t, isCacheableReadQuery("CALL gds.graph.list()"))
	require.True(t, isCacheableReadQuery("MATCH (n) RETURN n"))
}

// TestGDSCatalogHelpers covers the schema text, density and degree
// distribution of edge cases, and the parallel-relationship merge.
func TestGDSCatalogHelpers(t *testing.T) {
	half, whole := 0.5, 2.0
	require.Equal(t, "NaN", gdsDefaultText(nil, "NaN"))
	require.Equal(t, "0.5", gdsDefaultText(half, "NaN"))
	require.Equal(t, "2.0", gdsDefaultText(whole, "NaN"))
	schema := gdsGraphSchema([]gdsElementSpec{{name: "P", properties: []gdsPropertySpec{
		{key: "i", property: "i"}, {key: "f", property: "f"}, {key: "d", property: "d", defaultValue: int64(whole)}, {key: "h", property: "h", defaultValue: half}, {key: "l", property: "l"},
	}}}, nil, map[string]string{gdsPropertyTypeKey("P", "f"): "Float", gdsPropertyTypeKey("P", "h"): "Float", gdsPropertyTypeKey("P", "l"): "List of Integer"})
	require.Equal(t, map[string]interface{}{
		"i": "Integer (DefaultValue(-9223372036854775808), PERSISTENT)",
		"f": "Float (DefaultValue(NaN), PERSISTENT)",
		"d": "Integer (DefaultValue(2), PERSISTENT)",
		"h": "Float (DefaultValue(0.5), PERSISTENT)",
		"l": "List of Integer (DefaultValue(null), PERSISTENT)",
	}, schema["nodes"].(map[string]interface{})["P"])

	empty := &GraphProjection{Name: "e"}
	info := gdsGraphInfo(empty)
	require.Equal(t, 0.0, info["density"])
	require.Equal(t, map[string]interface{}{"min": int64(0), "max": int64(0), "mean": 0.0, "p50": int64(0), "p75": int64(0), "p90": int64(0), "p95": int64(0), "p99": int64(0), "p999": int64(0)}, gdsDegreeDistribution(empty))

	// A parallel relationship's values fold into the kept one's: a value the
	// kept relationship lacked is taken, a missing one changes nothing.
	specs := []gdsPropertySpec{{key: "s", aggregation: "SUM"}, {key: "m", aggregation: "MIN"}, {key: "x", aggregation: "MAX"}, {key: "n", aggregation: "COUNT"}, {key: "o", aggregation: "SINGLE"}}
	kept := map[string]float64{"m": 5, "x": 5, "o": 1}
	mergeParallelProperties(kept, map[string]float64{"s": 2, "m": 3, "x": 9, "o": 7}, specs, 2)
	require.Equal(t, map[string]float64{"s": 2, "m": 3, "x": 9, "n": 2, "o": 1}, kept)
	mergeParallelProperties(kept, map[string]float64{"s": 4, "m": 8, "x": 1}, specs, 3)
	require.Equal(t, map[string]float64{"s": 6, "m": 3, "x": 9, "n": 3, "o": 1}, kept)
	mergeParallelProperties(kept, map[string]float64{}, specs, 4)
	require.Equal(t, map[string]float64{"s": 6, "m": 3, "x": 9, "n": 4, "o": 1}, kept)
}

func TestGDSConfigHas(t *testing.T) {
	c, err := newGDSConfig(map[string]interface{}{"Present": nil}, "present", "absent")
	require.NoError(t, err)
	require.True(t, c.has("present"))
	require.False(t, c.has("absent"))
}

// TestGDSProjectionReadsRelationshipsInIDOrder: a parallel relationship's
// SINGLE value is the one with the smallest id, as GDS keeps the first by
// id, whatever order storage streams them in.
func TestGDSProjectionReadsRelationshipsInIDOrder(t *testing.T) {
	for attempt := 0; attempt < 10; attempt++ {
		graphProjections = make(map[string]*GraphProjection)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "order"))
		for _, query := range []string{
			"CREATE (:P {name: 'a'}), (:P {name: 'b'})",
			"MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) CREATE (a)-[:R {w: 1.0}]->(b)",
			"MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) CREATE (a)-[:R {w: 2.0}]->(b)",
			"MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) CREATE (a)-[:R {w: 3.0}]->(b)",
			"CALL gds.graph.project('g', 'P', {R: {properties: {w: {aggregation: 'SINGLE'}}}})",
		} {
			_, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err, query)
		}
		projection, err := exec.graphProjection("g")
		require.NoError(t, err)
		require.Len(t, projection.Relationships, 1)
		result, err := exec.Execute(context.Background(), "MATCH ()-[r:R]->() RETURN id(r) AS id, r.w ORDER BY id LIMIT 1", nil)
		require.NoError(t, err)
		require.Equal(t, result.Rows[0][1], projection.Relationships[0].Properties["w"])
	}
	graphProjections = make(map[string]*GraphProjection)
}
