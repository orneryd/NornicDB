package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These values were replayed through GDS 2.13.12's aggregation projection.
// Inspect typed storage as well as the public utility: GDS's scalar utility
// deliberately returns Double and cannot prove exact Long storage above 2^53.
func TestGDSAggregationSharedPropertyValues(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []interface{}
		kind   string
		want   []interface{}
	}{
		{"large_longs", []interface{}{int64(9007199254740993), int64(9007199254740992)}, "Integer", []interface{}{int64(9007199254740993), int64(9007199254740992)}},
		{"integral_double", []interface{}{int64(1), 2.0}, "Integer", []interface{}{int64(1), int64(2)}},
		{"long_lists", []interface{}{[]int64{9007199254740993}, []float64{2}}, "List of Integer", []interface{}{[]int64{9007199254740993}, []int64{2}}},
		{"double_lists", []interface{}{[]float64{1.5}, []int64{2}}, "List of Float", []interface{}{[]float64{1.5}, []float64{2}}},
		{"mixed_list", []interface{}{[]interface{}{int64(1), 2.5}}, "List of Float", []interface{}{[]float64{1, 2.5}}},
		{"empty_list", []interface{}{[]interface{}{}}, "List of Integer", []interface{}{[]int64{}}},
		{"empty_after_double", []interface{}{[]float64{1.5}, []interface{}{}}, "List of Float", []interface{}{[]float64{1.5}, []float64{}}},
		{"null_then_long", []interface{}{nil, int64(2)}, "Integer", []interface{}{nil, int64(2)}},
		{"double_then_long", []interface{}{1.5, int64(2)}, "Float", []interface{}{1.5, 2.0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, rows, _ := newAggregationProjectionGraph(t)
			input := make([]interface{}, len(tc.values))
			for i, value := range tc.values {
				input[i] = map[string]interface{}{"i": int64(i + 1), "x": value}
			}
			_, err := exec.Execute(context.Background(), "UNWIND $values AS row WITH row ORDER BY row.i RETURN gds.graph.project('typed', row.i, null, {sourceNodeLabels: 'N', targetNodeLabels: null, sourceNodeProperties: {x: row.x}, targetNodeProperties: null})", map[string]interface{}{"values": input})
			require.NoError(t, err)
			projection, err := lookupGraphProjection("agg", "typed")
			require.NoError(t, err)
			require.Equal(t, tc.kind, projection.NodePropertyTypes["x"])
			for i, want := range tc.want {
				id := projection.NodeIDs[i]
				require.Equal(t, want, projection.NodeProperties[id]["x"], "node %s", id)
				read := rows("RETURN gds.util.nodeProperty('typed', " + id + ", 'x')")[0][0]
				if want == nil {
					require.True(t, math.IsNaN(read.(float64)))
				} else if list, ok := cypherListValue(want); ok {
					require.Equal(t, list, read)
				}
			}
		})
	}
}

func TestGDSAggregationSharedPropertyRejections(t *testing.T) {
	for _, query := range []string{
		"UNWIND [1, 2] AS i WITH i ORDER BY i RETURN gds.graph.project('bad', i, null, {sourceNodeProperties: {x: CASE i WHEN 1 THEN 1 ELSE 2.5 END}, targetNodeProperties: null})",
		"UNWIND [1, 2] AS i WITH i ORDER BY i RETURN gds.graph.project('bad', i, null, {sourceNodeProperties: {x: CASE i WHEN 1 THEN [1] ELSE [2.5] END}, targetNodeProperties: null})",
		"UNWIND [1, 2] AS i WITH i ORDER BY i RETURN gds.graph.project('bad', i, null, {sourceNodeProperties: {x: CASE i WHEN 1 THEN [] ELSE [1.5] END}, targetNodeProperties: null})",
		"UNWIND [1, 2] AS i WITH i ORDER BY i RETURN gds.graph.project('bad', i, null, {sourceNodeProperties: {x: CASE i WHEN 1 THEN 1 ELSE [2] END}, targetNodeProperties: null})",
		"RETURN gds.graph.project('bad', 1, 2, {sourceNodeLabels: 'A', targetNodeLabels: 'B', sourceNodeProperties: {x: 1}, targetNodeProperties: {x: [2]}})",
		"RETURN gds.graph.project('bad', 1, null, {sourceNodeProperties: {x: null}, targetNodeProperties: null})",
		"RETURN gds.graph.project('bad', 1, 2, {relationshipProperties: {w: 9007199254740993}})",
		"RETURN gds.graph.project('bad', 1, 2, {relationshipProperties: {w: [1, 2]}})",
		"RETURN gds.graph.project('bad', 1, 2, {relationshipProperties: {w: []}})",
		"RETURN gds.graph.project('bad', 1, 2, {relationshipProperties: {w: '1'}})",
	} {
		t.Run(query, func(t *testing.T) {
			_, _, fails := newAggregationProjectionGraph(t)
			require.Error(t, fails(query))
			_, err := lookupGraphProjection("agg", "bad")
			require.Error(t, err, "a failed projection cannot enter the catalog")
		})
	}
}

func TestGDSAggregationSharedRelationshipValues(t *testing.T) {
	_, rows, _ := newAggregationProjectionGraph(t)
	rows("UNWIND [1, 2, 3] AS i WITH i ORDER BY i RETURN gds.graph.project('rels', i, i + 1, {relationshipType: CASE i WHEN 3 THEN 'B' ELSE 'A' END, relationshipProperties: CASE i WHEN 1 THEN {w: 9007199254740992} WHEN 2 THEN {w: 2.5, extra: 'ignored'} ELSE {w: null} END})")
	projection, err := lookupGraphProjection("agg", "rels")
	require.NoError(t, err)
	require.Equal(t, []string{"w"}, projection.RelationshipPropertyKeys["A"])
	require.Equal(t, []string{"w"}, projection.RelationshipPropertyKeys["B"])
	require.Equal(t, map[string]float64{"w": 9007199254740992}, projection.Relationships[0].Properties)
	require.Equal(t, map[string]float64{"w": 2.5}, projection.Relationships[1].Properties)
	require.Empty(t, projection.Relationships[2].Properties)

	// GDS 2.13.12 crashes when a null-first relationship property later has
	// a value. Retain the defined NornicDB extension: null contributes no
	// value, with the same Float storage and first-type keys as other rows.
	rows("UNWIND [1, 2] AS i WITH i ORDER BY i RETURN gds.graph.project('nullfirst', i, i + 1, {relationshipProperties: {w: CASE i WHEN 1 THEN null ELSE 2 END}})")
	projection, err = lookupGraphProjection("agg", "nullfirst")
	require.NoError(t, err)
	require.Empty(t, projection.Relationships[0].Properties)
	require.Equal(t, map[string]float64{"w": 2}, projection.Relationships[1].Properties)
}

func TestGDSAggregationNativeTypedStorageParity(t *testing.T) {
	exec, rows, _ := newAggregationProjectionGraph(t)
	_, err := exec.Execute(context.Background(), "CREATE (:Parity {x: $x, list: $list})", map[string]interface{}{"x": int64(9007199254740993), "list": []int64{9007199254740993, 2}})
	require.NoError(t, err)
	rows("CALL gds.graph.project('native', {Parity: {properties: ['x', 'list']}}, '*')")
	rows("MATCH (n:Parity) RETURN gds.graph.project('aggregation', n, null, {sourceNodeLabels: labels(n), targetNodeLabels: null, sourceNodeProperties: n{.x, .list}, targetNodeProperties: null})")
	native, err := lookupGraphProjection("agg", "native")
	require.NoError(t, err)
	aggregation, err := lookupGraphProjection("agg", "aggregation")
	require.NoError(t, err)
	require.Equal(t, native.NodePropertyTypes, aggregation.NodePropertyTypes)
	require.Equal(t, native.NodeProperties, aggregation.NodeProperties)
}

func TestGDSAggregationTypedReaderAndSelection(t *testing.T) {
	_, rows, _ := newAggregationProjectionGraph(t)
	rows("RETURN gds.graph.project('selected', 1, 2, {sourceNodeLabels: 'A', targetNodeLabels: 'B', sourceNodeProperties: {x: 9007199254740993, d: 1.5}, targetNodeProperties: {x: null, d: 2.5}, relationshipType: 'R', relationshipProperties: {w: 2.5}})")
	projection, err := lookupGraphProjection("agg", "selected")
	require.NoError(t, err)
	value, err := projection.nodePropertyValue("1", "x", "Integer")
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), value)
	value, err = projection.nodePropertyValue("2", "x", "Integer")
	require.NoError(t, err)
	require.Equal(t, int64(math.MinInt64), value)
	_, err = projection.nodePropertyValue("1", "d", "Integer")
	require.ErrorContains(t, err, "properties of type DOUBLE")
	graph, err := selectGDSSubgraph(projection, []string{"A"}, []string{"R"}, "w")
	require.NoError(t, err)
	require.Equal(t, []string{"1"}, graph.nodes)
	require.Empty(t, graph.outgoing[0], "target outside selected label is excluded")
	graph, err = selectGDSSubgraph(projection, nil, nil, "w")
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, graph.nodes)
	require.Equal(t, 2.5, graph.outgoing[0][0].weight)
	_, err = selectGDSSubgraph(projection, []string{"missing"}, nil, "")
	require.Error(t, err)
	_, err = selectGDSSubgraph(projection, nil, []string{"missing"}, "")
	require.Error(t, err)
}

func TestGDSSharedNodeIDPolicy(t *testing.T) {
	for _, tc := range []struct {
		value       interface{}
		want        string
		aggregation bool
		storedID    bool
	}{
		{int64(9007199254740993), "9007199254740993", true, true},
		{&storage.Node{ID: "node-id"}, "node-id", true, true},
		{"node-id", "node-id", false, true},
		{(*storage.Node)(nil), "", false, false},
		{nil, "", false, false},
		{true, "", false, false},
	} {
		id, err := gdsProjectionNode(tc.value)
		if tc.aggregation {
			require.NoError(t, err)
			require.Equal(t, tc.want, id)
		} else {
			require.Error(t, err)
		}
		stored, ok := gdsNodeID(tc.value)
		require.Equal(t, tc.storedID, ok)
		if ok {
			require.Equal(t, tc.want, string(stored))
		}
	}
}

func TestGDSSharedEmptyProjectionTypeValidation(t *testing.T) {
	_, _, fails := newAggregationProjectionGraph(t)
	for _, key := range []string{"undirectedRelationshipTypes", "inverseIndexedRelationshipTypes"} {
		err := fails("RETURN gds.graph.project('emptytypes', 1, null, {}, {" + key + ": ['missing']})")
		require.ErrorContains(t, err, "Projected types are: `['__ALL__']`")
	}
}

func TestGDSAggregationSharedConverterBoundaries(t *testing.T) {
	_, rows, fails := newAggregationProjectionGraph(t)
	require.ErrorContains(t, fails("RETURN gds.graph.project('unsupportedMap', 1, 2, {relationshipProperties: {w: {x: 1}}})"), "Unsupported conversion")
	configuration := rows("RETURN gds.graph.project('emptyOrientation', 1, 2, {}, {undirectedRelationshipTypes: [], inverseIndexedRelationshipTypes: []}).configuration")[0][0].(map[string]interface{})
	require.Equal(t, []interface{}{}, configuration["undirectedRelationshipTypes"])
	require.Equal(t, []interface{}{}, configuration["inverseIndexedRelationshipTypes"])
	// The specialized traversal can still compute COUNT itself, while the
	// catalog-writing aggregate must reach the general projection pipeline.
	require.True(t, traversalComputesAggregate("COUNT(*)"))
	require.False(t, traversalComputesAggregate("GDS.GRAPH.PROJECT('g', 1)"))
}
