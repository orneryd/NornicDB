package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newGDSEngineGraph is an executor over its own database, with nodes of
// fixed ids (so node order is known) created directly in storage.
func newGDSEngineGraph(t *testing.T, database string, nodes ...*storage.Node) (*StorageExecutor, func(string) (*ExecuteResult, error)) {
	t.Helper()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), database)
	for _, node := range nodes {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	exec := NewStorageExecutor(engine)
	return exec, func(query string) (*ExecuteResult, error) {
		return exec.Execute(context.Background(), query, nil)
	}
}

func gdsNode(id, label string, properties map[string]interface{}) *storage.Node {
	return &storage.Node{ID: storage.NodeID(id), Labels: []string{label}, Properties: properties}
}

// TestGDSGraphDropNeedsWriteOnItsDatabase: gds.graph.drop's dbName is
// checked against the caller's rights on that database.
func TestGDSGraphDropNeedsWriteOnItsDatabase(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	_, runPrivate := newGDSEngineGraph(t, "private", gdsNode("p1", "Private", map[string]interface{}{"secret": int64(42)}))
	_, err := runPrivate("CALL gds.graph.project('secretGraph', {Private: {properties: 'secret'}}, '*')")
	require.NoError(t, err)
	exec, run := newGDSEngineGraph(t, "allowed", gdsNode("a1", "A", nil))
	_, err = run("CALL gds.graph.project('mine', 'A', '*')")
	require.NoError(t, err)

	ctx := WithDatabasePermissionResolver(context.Background(), "allowed", func(database, permission string) bool {
		return database == "allowed"
	})
	_, err = exec.Execute(ctx, "CALL gds.graph.drop('secretGraph', true, 'private')", nil)
	var denied *PermissionDeniedError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "write", denied.Permission)
	_, err = lookupGraphProjection("private", "secretGraph")
	require.NoError(t, err, "the other database's graph stays")

	result, err := exec.Execute(ctx, "CALL gds.graph.drop('mine', true, 'allowed') YIELD graphName RETURN graphName", nil)
	require.NoError(t, err)
	require.Equal(t, "mine", result.Rows[0][0])
}

// fastRPVectors runs gds.fastRP.stream and returns each node's embedding
// by id.
func fastRPVectors(t *testing.T, run func(string) (*ExecuteResult, error), query string) map[string][]float64 {
	t.Helper()
	result, err := run(query)
	require.NoError(t, err, query)
	vectors := make(map[string][]float64)
	for _, row := range result.Rows {
		var vector []float64
		switch values := row[1].(type) {
		case []float64:
			vector = values
		case []interface{}:
			for _, value := range values {
				number, _ := toFloat64(value)
				vector = append(vector, number)
			}
		}
		vectors[row[0].(string)] = vector
	}
	return vectors
}

func squaredNorm(vector []float64) float64 {
	sum := 0.0
	for _, value := range vector {
		sum += value * value
	}
	return sum
}

// TestFastRPPropagationAsGDS: as GDS's FastRP, each iteration's
// neighbourhood average carries to the next iteration unnormalised (only
// its contribution to the embedding is normalised), and nodeSelfInfluence
// adds the normalised start vector.
func TestFastRPPropagationAsGDS(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	// a→b→d and a→c→e: after one iteration b holds d's vector and c holds
	// e's, -2 times it; a's second iteration averages them to -0.5 times the
	// vector. Normalising b and c first would cancel a out to zero.
	_, run := newGDSEngineGraph(t, "frp",
		gdsNode("a", "N", map[string]interface{}{"f": 1.0}),
		gdsNode("b", "N", map[string]interface{}{"f": 1.0}),
		gdsNode("c", "N", map[string]interface{}{"f": 1.0}),
		gdsNode("d", "N", map[string]interface{}{"f": 1.0}),
		gdsNode("e", "N", map[string]interface{}{"f": -2.0}))
	for _, query := range []string{
		"MATCH (a {f: 1.0}), (b) WHERE id(a) = 'a' AND id(b) IN ['b', 'c'] CREATE (a)-[:R]->(b)",
		"MATCH (b), (d) WHERE id(b) = 'b' AND id(d) = 'd' CREATE (b)-[:R]->(d)",
		"MATCH (c), (e) WHERE id(c) = 'c' AND id(e) = 'e' CREATE (c)-[:R]->(e)",
		"CALL gds.graph.project('frp', {N: {properties: 'f'}}, 'R')",
	} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	vectors := fastRPVectors(t, run, "CALL gds.fastRP.stream('frp', {embeddingDimension: 16, propertyRatio: 1.0, featureProperties: ['f'], iterationWeights: [0.0, 1.0], randomSeed: 42}) YIELD nodeId, embedding RETURN nodeId, embedding")
	require.InDelta(t, 1.0, squaredNorm(vectors["a"]), 1e-9)

	// nodeSelfInfluence adds the start vector normalised: features 2 and
	// 3 give unit vectors, not 4 and 9 times one.
	_, run = newGDSEngineGraph(t, "self",
		gdsNode("x", "S", map[string]interface{}{"f": 2.0}),
		gdsNode("y", "S", map[string]interface{}{"f": 3.0}))
	_, err := run("CALL gds.graph.project('self', {S: {properties: 'f'}}, '*')")
	require.NoError(t, err)
	vectors = fastRPVectors(t, run, "CALL gds.fastRP.stream('self', {embeddingDimension: 16, propertyRatio: 1.0, featureProperties: ['f'], iterationWeights: [], nodeSelfInfluence: 1.0, randomSeed: 42}) YIELD nodeId, embedding RETURN nodeId, embedding")
	require.InDelta(t, 1.0, squaredNorm(vectors["x"]), 1e-9)
	require.InDelta(t, 1.0, squaredNorm(vectors["y"]), 1e-9)
	// A zero start vector is taken as it is (no division by its norm).
	_, run = newGDSEngineGraph(t, "zero", gdsNode("z", "Z", map[string]interface{}{"f": 0.0}))
	_, err = run("CALL gds.graph.project('zero', {Z: {properties: 'f'}}, '*')")
	require.NoError(t, err)
	vectors = fastRPVectors(t, run, "CALL gds.fastRP.stream('zero', {embeddingDimension: 4, propertyRatio: 1.0, featureProperties: ['f'], iterationWeights: [], nodeSelfInfluence: 1.0, randomSeed: 42}) YIELD nodeId, embedding RETURN nodeId, embedding")
	require.Equal(t, []float64{0, 0, 0, 0}, vectors["z"])

	// A NaN weight is a missing weight, as GDS reads it.
	_, run = newGDSEngineGraph(t, "nan", gdsNode("n1", "W", nil), gdsNode("n2", "W", nil))
	for _, query := range []string{
		"MATCH (a), (b) WHERE id(a) = 'n1' AND id(b) = 'n2' CREATE (a)-[:R]->(b)",
		"CALL gds.graph.project('nan', 'W', {R: {properties: {w: {property: 'w', defaultValue: gds.util.NaN()}}}})",
	} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	_, err = run("CALL gds.fastRP.stream('nan', {embeddingDimension: 16, relationshipWeightProperty: 'w'})")
	require.ErrorContains(t, err, "Missing relationship property `w`")
}

// TestGraphProjectListPropertiesAsGDS: numeric list properties project as
// GDS projects them (GDS 2.13.12 on the same data): List of Float / List
// of Integer in the schema, gds.util.nodeProperty gives the list (or []
// where a node has none), and FastRP takes each element as a feature.
func TestGraphProjectListPropertiesAsGDS(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	_, run := newGDSEngineGraph(t, "lists",
		gdsNode("a", "A", map[string]interface{}{"f": []interface{}{1.0, 2.0}, "i": []interface{}{int64(1), int64(2), int64(3)}, "s": 2.0, "b": []interface{}{true}}),
		gdsNode("b", "A", map[string]interface{}{"f": []float64{3.0, 4.0}, "i": []int64{4, 5, 6}, "s": 3.0}),
		gdsNode("c", "A", map[string]interface{}{"s": int64(1), "f": []interface{}{}}))
	for _, query := range []string{
		"MATCH (a), (b) WHERE id(a) = 'a' AND id(b) = 'b' CREATE (a)-[:R]->(b)",
		"CALL gds.graph.project('lists', {A: {properties: ['f', 'i', 's']}}, 'R')",
	} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	result, err := run("CALL gds.graph.list('lists') YIELD schemaWithOrientation RETURN schemaWithOrientation")
	require.NoError(t, err)
	nodes := result.Rows[0][0].(map[string]interface{})["nodes"].(map[string]interface{})["A"]
	require.Equal(t, map[string]interface{}{
		"f": "List of Float (DefaultValue(null), PERSISTENT)",
		"i": "List of Integer (DefaultValue(null), PERSISTENT)",
		"s": "Float (DefaultValue(NaN), PERSISTENT)",
	}, nodes)
	result, err = run("MATCH (n) WHERE id(n) IN ['a', 'c'] RETURN id(n) AS id, gds.util.nodeProperty('lists', n, 'f'), gds.util.nodeProperty('lists', n, 'i'), gds.util.nodeProperty('lists', n, 's') ORDER BY id")
	require.NoError(t, err)
	require.Equal(t, []interface{}{"a", []interface{}{1.0, 2.0}, []interface{}{int64(1), int64(2), int64(3)}, 2.0}, result.Rows[0])
	require.Equal(t, []interface{}{"c", []interface{}{}, []interface{}{}, 1.0}, result.Rows[1])
	// A fractional default makes an Integer property a Float.
	result, err = run("CALL gds.graph.project('half', {A: {properties: {s2: {property: 'missing', defaultValue: 0.5}, s3: {property: 's', defaultValue: 0.5}}}}, 'R') YIELD graphName RETURN graphName")
	require.NoError(t, err)
	result, err = run("CALL gds.graph.list('half') YIELD schemaWithOrientation RETURN schemaWithOrientation")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"s2": "Float (DefaultValue(0.5), PERSISTENT)", "s3": "Float (DefaultValue(0.5), PERSISTENT)"},
		result.Rows[0][0].(map[string]interface{})["nodes"].(map[string]interface{})["A"])

	// A property no node holds as a number or list isn't projected.
	_, err = run("CALL gds.graph.project('bools', {A: {properties: 'b'}}, '*')")
	require.ErrorContains(t, err, "Node properties not found: 'b'")

	// FastRP: each list element is a feature (two nodes, one length).
	_, run = newGDSEngineGraph(t, "feat",
		gdsNode("a", "A", map[string]interface{}{"f": []interface{}{1.0, 2.0}, "i": []interface{}{int64(1), int64(2), int64(3)}, "s": 2.0}),
		gdsNode("b", "A", map[string]interface{}{"f": []interface{}{3.0, 4.0}, "i": []interface{}{int64(4), int64(5), int64(6)}, "s": 3.0}))
	for _, query := range []string{
		"MATCH (a), (b) WHERE id(a) = 'a' AND id(b) = 'b' CREATE (a)-[:R]->(b)",
		"CALL gds.graph.project('feat', {A: {properties: ['f', 'i', 's']}}, 'R')",
	} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	for _, features := range []string{"['f']", "['i', 's']"} {
		vectors := fastRPVectors(t, run, "CALL gds.fastRP.stream('feat', {embeddingDimension: 8, propertyRatio: 0.5, featureProperties: "+features+", randomSeed: 42}) YIELD nodeId, embedding RETURN nodeId, embedding")
		require.Len(t, vectors, 2)
		require.Len(t, vectors["a"], 8)
	}
}

// TestGraphProjectListPropertyRefusalsAsGDS: GDS 2.13.12's refusals for
// list properties: a property's type is its first value's (a scalar and a
// list don't mix, a List of Integer takes no fractions), string lists
// aren't projected, and FastRP needs one length per list property.
func TestGraphProjectListPropertyRefusalsAsGDS(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	for _, tc := range []struct {
		first, second interface{}
		message       string
	}{
		{1.0, []interface{}{1.0, 2.0}, "Cannot safely convert DoubleArray[1.0, 2.0] into a Double."},
		{int64(1), []interface{}{int64(1), int64(2)}, "Cannot safely convert LongArray[1, 2] into a Long."},
		{[]interface{}{1.0, 2.0}, 1.0, "Cannot safely convert 1.0 into a Double Array."},
		{[]interface{}{int64(1), int64(2)}, int64(1), "Cannot safely convert 1 into a Long Array."},
		{[]interface{}{int64(1), int64(2)}, []interface{}{1.5}, "Cannot safely convert DoubleArray[1.5] into a Long Array. Cannot safely convert 1.50 into an long value"},
		{[]interface{}{"a", "b"}, nil, "Loading of values of type StringArray is currently not supported"},
	} {
		_, run := newGDSEngineGraph(t, "refuse", gdsNode("m1", "M", map[string]interface{}{"v": tc.first}), gdsNode("m2", "M", map[string]interface{}{"v": tc.second}))
		_, err := run("CALL gds.graph.project('m', {M: {properties: 'v'}}, '*')")
		require.ErrorContains(t, err, tc.message)
	}
	// A List of Float takes integer lists after it.
	_, run := newGDSEngineGraph(t, "widen", gdsNode("m1", "M", map[string]interface{}{"v": []interface{}{1.5}}), gdsNode("m2", "M", map[string]interface{}{"v": []interface{}{int64(2)}}))
	_, err := run("CALL gds.graph.project('w', {M: {properties: 'v'}}, '*')")
	require.NoError(t, err)

	_, run = newGDSEngineGraph(t, "lengths", gdsNode("l1", "L", map[string]interface{}{"v": []interface{}{1.0}}), gdsNode("l2", "L", map[string]interface{}{"v": []interface{}{1.0, 2.0}}))
	for _, query := range []string{
		"MATCH (a), (b) WHERE id(a) = 'l1' AND id(b) = 'l2' CREATE (a)-[:R]->(b)",
		"CALL gds.graph.project('l', {L: {properties: 'v'}}, 'R')",
	} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	_, err = run("CALL gds.fastRP.stream('l', {embeddingDimension: 8, propertyRatio: 0.5, featureProperties: ['v'], randomSeed: 42})")
	require.ErrorContains(t, err, "The property `v` contains arrays of differing lengths `2` and `1`.")
	require.Equal(t, "DoubleArray[]", gdsJavaValueText([]float64{}, true))
	// Without widening (GDS's Cypher aggregation), an Integer refuses a
	// fraction.
	_, _, _, err = gdsConvertPropertyValue(2.5, "Integer", false)
	require.ErrorContains(t, err, "Cannot safely convert 2.50 into an long value")
	_, kind, _, err := gdsConvertPropertyValue(2.5, "Integer", true)
	require.NoError(t, err)
	require.Equal(t, "Float", kind)
}

// TestGraphProjectValidateRelationshipsAsGDS: validateRelationships: true
// refuses a projected relationship with an end outside the node
// projection, naming that end, as GDS 2.13.12 does; without it the
// relationship is skipped. Relationships of other types don't count.
func TestGraphProjectValidateRelationshipsAsGDS(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	_, run := newGDSEngineGraph(t, "validate", gdsNode("q1", "Q", nil), gdsNode("a1", "A", nil), gdsNode("a2", "A", nil))
	for _, query := range []string{
		"MATCH (q), (a) WHERE id(q) = 'q1' AND id(a) = 'a1' CREATE (q)-[:R]->(a)",
		"MATCH (a), (q) WHERE id(a) = 'a2' AND id(q) = 'q1' CREATE (a)-[:T]->(q)",
	} {
		_, err := run(query)
		require.NoError(t, err, query)
	}
	_, err := run("CALL gds.graph.project('v', 'A', 'R', {validateRelationships: true})")
	require.ErrorContains(t, err, "Failed to load a relationship because its source-node with id q1 is not part of the node query or projection. To ignore the relationship, set the configuration parameter `validateRelationships` to false.")
	_, err = run("CALL gds.graph.project('v', 'A', 'T', {validateRelationships: true})")
	require.ErrorContains(t, err, "its target-node with id q1 is not part")
	result, err := run("CALL gds.graph.project('v', 'A', 'R') YIELD relationshipCount RETURN relationshipCount")
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Rows[0][0])
	result, err = run("CALL gds.graph.project('v2', ['A', 'Q'], 'R', {validateRelationships: true}) YIELD relationshipCount RETURN relationshipCount")
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
}

// TestGraphProjectIntegerKindsAndAliases: integer properties of every Go
// integer type keep their value, and two labels projecting one stored
// property under different keys are separate properties with their own
// types.
func TestGraphProjectIntegerKindsAndAliases(t *testing.T) {
	graphProjections = make(map[gdsProjectionKey]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[gdsProjectionKey]*GraphProjection) })
	values := []interface{}{int8(-8), int16(16), int32(32), uint(1), uint8(8), uint16(160), uint32(320), uint64(640)}
	nodes := make([]*storage.Node, len(values))
	for i, value := range values {
		nodes[i] = gdsNode("n"+string(rune('a'+i)), "I", map[string]interface{}{"v": value, "l": []interface{}{value, int64(1)}})
	}
	_, run := newGDSEngineGraph(t, "ints", nodes...)
	_, err := run("CALL gds.graph.project('ints', {I: {properties: ['v', 'l']}}, '*')")
	require.NoError(t, err)
	result, err := run("MATCH (n:I) RETURN id(n) AS id, gds.util.nodeProperty('ints', n, 'v'), gds.util.nodeProperty('ints', n, 'l') ORDER BY id")
	require.NoError(t, err)
	want := []float64{-8, 16, 32, 1, 8, 160, 320, 640}
	for i, row := range result.Rows {
		require.Equal(t, want[i], row[1], "value %d", i)
		require.Equal(t, []interface{}{int64(want[i]), int64(1)}, row[2], "list %d", i)
	}

	// Every Go integer type converts, whatever storage hands back.
	for value, want := range map[interface{}]int64{int8(-8): -8, int16(16): 16, int32(32): 32, int64(64): 64, int(1): 1, uint(2): 2, uint8(8): 8, uint16(160): 160, uint32(320): 320, uint64(640): 640} {
		require.Equal(t, want, gdsInteger(value), "%T", value)
	}

	// A.foo and B.bar both read x: a scalar on A, a list on B.
	_, run = newGDSEngineGraph(t, "alias",
		gdsNode("a", "A", map[string]interface{}{"x": int64(1)}),
		gdsNode("b", "B", map[string]interface{}{"x": []interface{}{int64(1), int64(2)}}))
	_, err = run("CALL gds.graph.project('alias', {A: {properties: {foo: {property: 'x'}}}, B: {properties: {bar: {property: 'x'}}}}, '*')")
	require.NoError(t, err)
	result, err = run("CALL gds.graph.list('alias') YIELD schemaWithOrientation RETURN schemaWithOrientation")
	require.NoError(t, err)
	nodesSchema := result.Rows[0][0].(map[string]interface{})["nodes"].(map[string]interface{})
	require.Equal(t, map[string]interface{}{"foo": "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"}, nodesSchema["A"])
	require.Equal(t, map[string]interface{}{"bar": "List of Integer (DefaultValue(null), PERSISTENT)"}, nodesSchema["B"])
}
