package cypher

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func gdsFollowUpCatalog(t *testing.T) {
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
}

func TestGDSProjectionFollowUpIntegralDoubleList(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "reviewIntegral",
		gdsNode("a", "N", map[string]interface{}{"v": []int64{1, 2}}),
		gdsNode("b", "N", map[string]interface{}{"v": []float64{3, 4}}))
	_, err := run("CALL gds.graph.project('reviewIntegral', {N: {properties: 'v'}}, '*')")
	require.NoError(t, err, "GDS losslessly converts integral double arrays to the first long-array type")
	result, err := run("MATCH (n:N) WHERE id(n)='b' RETURN gds.util.nodeProperty('reviewIntegral',n,'v')")
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(3), int64(4)}, result.Rows[0][0])
}

func TestGDSProjectionFollowUpFloatListReadType(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "reviewFloat",
		gdsNode("a", "N", map[string]interface{}{"v": []float64{1.5}}),
		gdsNode("b", "N", map[string]interface{}{"v": []int64{2}}))
	_, err := run("CALL gds.graph.project('reviewFloat', {N: {properties: 'v'}}, '*')")
	require.NoError(t, err)
	result, err := run("MATCH (n:N) WHERE id(n) = 'b' RETURN gds.util.nodeProperty('reviewFloat', n, 'v')")
	require.NoError(t, err)
	require.Equal(t, []interface{}{2.0}, result.Rows[0][0])
}

func TestGDSProjectionFollowUpNaNListFeature(t *testing.T) {
	for _, self := range []bool{false, true} {
		t.Run(map[bool]string{false: "iteration", true: "self"}[self], func(t *testing.T) {
			gdsFollowUpCatalog(t)
			_, run := newGDSEngineGraph(t, "reviewNan", gdsNode("a", "N", map[string]interface{}{"v": []float64{math.NaN()}}))
			_, err := run("CALL gds.graph.project('reviewNan', {N: {properties: 'v'}}, '*')")
			require.NoError(t, err)
			configuration := "iterationWeights: [1.0]"
			if self {
				configuration = "iterationWeights: [], nodeSelfInfluence: 1.0"
			}
			result, err := run("CALL gds.fastRP.stream('reviewNan', {embeddingDimension: 16, propertyRatio: 1.0, featureProperties: ['v'], randomSeed: 42, " + configuration + "})")
			if result != nil {
				t.Logf("returned embeddings: %v", result.Rows)
			}
			require.ErrorContains(t, err, "invalid feature property value NaN")
		})
	}
}

func TestGDSProjectionFollowUpEmptyNumericList(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "reviewEmpty", gdsNode("a", "N", map[string]interface{}{"v": []int64{}}))
	_, err := run("CALL gds.graph.project('reviewEmpty', {N: {properties: 'v'}}, '*')")
	require.NoError(t, err, "typed numeric empty storage arrays are a well-defined extension; native Cypher [] is StringArray")
	result, err := run("MATCH (n:N) RETURN gds.util.nodeProperty('reviewEmpty',n,'v')")
	require.NoError(t, err)
	require.Equal(t, []interface{}{}, result.Rows[0][0])
}

func TestGDSProjectionFollowUpCommonKeyAcrossLabels(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "reviewLabels",
		gdsNode("a", "A", map[string]interface{}{"v": int64(1)}),
		gdsNode("b", "B", map[string]interface{}{"v": []int64{2}}))
	result, err := run("CALL gds.graph.project('reviewLabels', {A: {properties: 'v'}, B: {properties: 'v'}}, '*')")
	if result != nil {
		t.Logf("projected: %v", result.Rows)
	}
	require.Error(t, err, "GDS shares a projected key's type across labels")
}

func TestGDSProjectionFollowUpNaNWeightWithoutIterations(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "reviewNanWeights", gdsNode("a", "N", nil), gdsNode("b", "N", nil))
	for _, query := range []string{
		"MATCH (a), (b) WHERE id(a) = 'a' AND id(b) = 'b' CREATE (a)-[:R]->(b)",
		"CALL gds.graph.project('reviewNanWeights', 'N', {R: {properties: {w: {defaultValue: gds.util.NaN()}}}})",
	} {
		_, err := run(query)
		require.NoError(t, err)
	}
	_, err := run("CALL gds.fastRP.stream('reviewNanWeights', {embeddingDimension: 16, iterationWeights: [], nodeSelfInfluence: 1.0, relationshipWeightProperty: 'w', randomSeed: 42})")
	require.NoError(t, err, "GDS does not read relationship weights when there are no iterations")
}

func TestGDSProjectionFollowUpExactIntegerStorage(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "exact", gdsNode("a", "N", map[string]interface{}{"v": int64(9007199254740992)}), gdsNode("b", "N", map[string]interface{}{"v": int64(9007199254740993)}))
	_, err := run("CALL gds.graph.project('exact', {N:{properties:'v'}}, '*')")
	require.NoError(t, err)
	proj, err := lookupGraphProjection("exact", "exact")
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740992), proj.NodeProperties["a"]["v"])
	require.Equal(t, int64(9007199254740993), proj.NodeProperties["b"]["v"])
	result, err := run("MATCH (n:N) WHERE id(n) = 'b' RETURN gds.util.nodeProperty('exact',n,'v')")
	require.NoError(t, err)
	require.Equal(t, float64(9007199254740992), result.Rows[0][0], "GDS util exposes a Double even for a Long property")
}

func TestGDSProjectionFollowUpEmptySelections(t *testing.T) {
	for _, selection := range []string{"nodeLabels: []", "relationshipTypes: []", "nodeLabels: [], relationshipTypes: []"} {
		t.Run(selection, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			exec, run := newGDSEngineGraph(t, "selection", gdsNode("a", "N", nil), gdsNode("b", "N", nil))
			_, err := run("MATCH (a), (b) WHERE id(a)='a' AND id(b)='b' CREATE (a)-[:R]->(b)")
			require.NoError(t, err)
			_, err = run("CALL gds.graph.project('selection','N','R')")
			require.NoError(t, err)
			result, err := run("CALL gds.fastRP.stream('selection', {embeddingDimension:16, randomSeed:42, " + selection + "})")
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			baseline, err := run("CALL gds.fastRP.stream('selection', {embeddingDimension:16, randomSeed:42})")
			require.NoError(t, err)
			require.Equal(t, baseline.Rows, result.Rows)
			proj, err := lookupGraphProjection(storageDatabaseName(exec.storage), "selection")
			require.NoError(t, err)
			graph, err := fastRPSubgraph(proj, FastRPConfig{NodeLabels: []string{}, RelationshipTypes: []string{}})
			require.NoError(t, err)
			require.Len(t, graph.nodes, 2)
			require.Len(t, graph.outgoing[graph.index["a"]], 1)
		})
	}
}

func TestGDSProjectionFollowUpTypedReads(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "typed", gdsNode("a", "N", map[string]interface{}{"v": 1.2}), gdsNode("b", "N", nil))
	_, err := run("CALL gds.graph.project('typed',{N:{properties:'v'}},'*')")
	require.NoError(t, err)
	proj, err := lookupGraphProjection("typed", "typed")
	require.NoError(t, err)
	_, err = proj.nodePropertyValue("a", "v", "Integer")
	require.EqualError(t, err, "Tried to retrieve a value of type LONG value from properties of type DOUBLE")
	value, err := proj.nodePropertyValue("a", "v", "")
	require.NoError(t, err)
	require.Equal(t, 1.2, value)
	value, err = proj.nodePropertyValue("b", "v", "Float")
	require.NoError(t, err)
	require.True(t, math.IsNaN(value.(float64)))
	_, run = newGDSEngineGraph(t, "longs", gdsNode("a", "N", map[string]interface{}{"v": int64(9007199254740993)}), gdsNode("b", "N", nil), gdsNode("c", "N", map[string]interface{}{"v": int64(math.MinInt64)}))
	_, err = run("CALL gds.graph.project('longs',{N:{properties:'v'}},'*')")
	require.NoError(t, err)
	proj, err = lookupGraphProjection("longs", "longs")
	require.NoError(t, err)
	value, err = proj.nodePropertyValue("a", "v", "Integer")
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), value)
	value, err = proj.nodePropertyValue("b", "v", "Integer")
	require.NoError(t, err)
	require.Equal(t, int64(math.MinInt64), value)
	value, err = proj.nodePropertyValue("c", "v", "Float")
	require.NoError(t, err)
	require.True(t, math.IsNaN(value.(float64)))
}

func TestGDSProjectionFollowUpEmptyListExtension(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "emptyCypher")
	_, err := run("CREATE (:N {v:[]})")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('emptyCypher',{N:{properties:'v'}},'*')")
	require.NoError(t, err, "native GDS refuses the StringArray, but NornicDB retains this harmless numeric-list extension")
}

func TestGDSProjectionFollowUpMissingWeightSelection(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "missingWeights", gdsNode("a", "N", nil), gdsNode("b", "N", nil))
	_, err := run("MATCH (a),(b) WHERE id(a)='a' AND id(b)='b' CREATE (a)-[:R]->(b)")
	require.NoError(t, err)
	_, err = run("CALL gds.graph.project('missingWeights','N',{R:{properties:{w:{defaultValue:gds.util.NaN()}}}})")
	require.NoError(t, err)
	proj, err := lookupGraphProjection("missingWeights", "missingWeights")
	require.NoError(t, err)
	// Also retain a missing map entry: shared selection cannot validate weights
	// on behalf of every algorithm that reuses its topology reader.
	delete(proj.Relationships[0].Properties, "w")
	graph, err := fastRPSubgraph(proj, FastRPConfig{RelationshipWeightProperty: "w"})
	require.NoError(t, err)
	require.True(t, math.IsNaN(graph.outgoing[0][0].weight))
	_, err = run("CALL gds.fastRP.stream('missingWeights',{embeddingDimension:16,iterationWeights:[],nodeSelfInfluence:1.0,relationshipWeightProperty:'w'})")
	require.NoError(t, err)
	_, err = run("CALL gds.fastRP.stream('missingWeights',{embeddingDimension:16,iterationWeights:[0.0],relationshipWeightProperty:'w'})")
	require.ErrorContains(t, err, "Missing relationship property `w`")
	requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
}

func TestGDSProjectionFollowUpRelationshipPropertyTypes(t *testing.T) {
	for _, tc := range []struct{ value, message string }{
		{"[1]", "Unsupported type [NUMBER_ARRAY] of value LongArray[1]. Please use a numeric property."},
		{"'x'", "Unsupported type [TEXT] of value String(\"x\"). Please use a numeric property."},
		{"true", "Unsupported type [BOOLEAN] of value Boolean('true'). Please use a numeric property."},
		{"['x']", "Unsupported type [TEXT_ARRAY] of value StringArray[x]. Please use a numeric property."},
		{"[]", "Unsupported type [TEXT_ARRAY] of value StringArray[]. Please use a numeric property."},
	} {
		t.Run(tc.value, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			_, run := newGDSEngineGraph(t, "reltypes")
			_, err := run("CREATE (:N)-[:R {w:" + tc.value + "}]->(:N)")
			require.NoError(t, err)
			_, err = run("CALL gds.graph.project('reltypes','N',{R:{properties:'w'}})")
			require.ErrorContains(t, err, tc.message)
			requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
		})
	}
}

func TestGDSProjectionFollowUpArrayDefaultType(t *testing.T) {
	for _, tc := range []struct {
		value    interface{}
		expected string
	}{{[]int64{1}, "long[]"}, {[]float64{1.5}, "double[]"}} {
		t.Run(tc.expected, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			_, run := newGDSEngineGraph(t, "arraydefault", gdsNode("a", "N", map[string]interface{}{"v": tc.value}), gdsNode("b", "N", nil))
			_, err := run("CALL gds.graph.project('arraydefault',{N:{properties:{v:{defaultValue:0.0}}}},'*')")
			require.ErrorContains(t, err, "Expected type of default value to be `"+tc.expected+"`. But got `Double`.")
			requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
		})
	}
}
