package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestGDSImportRoundsWhereGDSConverts: GDS rounds a Long above 2^53 in a
// list that mixes Longs and Doubles, in either order, and a native
// relationship's stored Long; both projections succeed (review/
// gds-develop-r4-mixed-array and -native-long captures). An aggregation
// relationship value keeps the bound, as in GDS.
func TestGDSImportRoundsWhereGDSConverts(t *testing.T) {
	gdsFollowUpCatalog(t)
	e, run := newGDSEngineGraph(t, "importrounding", gdsNode("a", "N", nil), gdsNode("b", "N", nil))
	for _, tc := range []struct {
		list     string
		expected []interface{}
	}{
		{"[9007199254740993,1.5]", []interface{}{9007199254740992.0, 1.5}},
		{"[1.5,9007199254740993]", []interface{}{1.5, 9007199254740992.0}},
	} {
		_, err := run("RETURN gds.graph.project('mixed',0,null,{sourceNodeProperties:{v:" + tc.list + "},targetNodeProperties:null})")
		require.NoError(t, err, tc.list)
		result, err := run("RETURN gds.util.nodeProperty('mixed',0,'v')")
		require.NoError(t, err)
		require.Equal(t, tc.expected, result.Rows[0][0], tc.list)
		_, err = run("CALL gds.graph.drop('mixed')")
		require.NoError(t, err)
	}

	require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"w": int64(9007199254740993)}}))
	result, err := run("CALL gds.graph.project('nativeLong','N',{R:{properties:'w'}},{readConcurrency:1}) YIELD nodeCount,relationshipCount RETURN nodeCount,relationshipCount")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(1)}}, result.Rows)
	projection := graphProjections[gdsCatalogKey(e.databaseName(), "nativeLong")]
	require.Equal(t, 9007199254740992.0, projection.Relationships[0].Properties["w"])

	// TestGDSRelationshipDefaultImportErrors keeps the default's bound.
	_, err = run("UNWIND [1,2] AS t RETURN gds.graph.project('aggregationValue',0,t,{relationshipProperties:{w:9007199254740993}})")
	require.ErrorContains(t, err, "Cannot safely convert 9007199254740993 into an double value")
}

// TestGDSRelationshipDefaultTypeBeforeValue: an empty-list or map
// relationship default fails GDS's default-type check, not a stored-value
// check (review/gds-develop-r4-empty-default and -map-default captures).
func TestGDSRelationshipDefaultTypeBeforeValue(t *testing.T) {
	gdsFollowUpCatalog(t)
	e, run := newGDSEngineGraph(t, "relationshipdefaults", gdsNode("a", "N", nil), gdsNode("b", "N", nil))
	require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R"}))
	for defaultValue, expected := range map[string]string{
		"[]":    "Expected type of default value to be `Double`. But got `Object[]`.",
		"{a:1}": "Expected type of default value to be `Double`. But got `HashMap`.",
	} {
		_, err := run("CALL gds.graph.project('defaults','N',{R:{properties:{w:{defaultValue:" + defaultValue + "}}}})")
		require.ErrorContains(t, err, expected, defaultValue)
	}
	result, err := run("RETURN gds.graph.exists('defaults')")
	require.NoError(t, err)
	require.Equal(t, false, result.Rows[0][0])
}
