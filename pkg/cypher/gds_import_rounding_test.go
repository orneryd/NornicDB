package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The native projection shares the mixed-list and relationship conversion
// policy without requiring the aggregation projection available on develop.
func TestGDSNativeImportRoundsWhereGDSConverts(t *testing.T) {
	gdsFollowUpCatalog(t)
	for _, values := range [][]interface{}{
		{int64(9007199254740993), 1.5},
		{1.5, int64(9007199254740993)},
	} {
		e, run := newGDSEngineGraph(t, "nativerounding",
			gdsNode("a", "N", map[string]interface{}{"v": values}), gdsNode("b", "N", nil))
		require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]interface{}{"w": int64(9007199254740993)}}))
		_, err := run("CALL gds.graph.project('native',{N:{properties:'v'}},{R:{properties:'w'}},{readConcurrency:1})")
		require.NoError(t, err)
		projection, err := lookupGraphProjection("nativerounding", "native")
		require.NoError(t, err)
		want := []float64{9007199254740992.0, 1.5}
		if values[0] == 1.5 {
			want = []float64{1.5, 9007199254740992.0}
		}
		require.Equal(t, want, projection.NodeProperties["a"]["v"])
		require.Equal(t, 9007199254740992.0, projection.Relationships[0].Properties["w"])
		_, err = run("CALL gds.graph.drop('native')")
		require.NoError(t, err)
	}
}

func TestGDSRelationshipDefaultTypeBeforeValue(t *testing.T) {
	gdsFollowUpCatalog(t)
	e, run := newGDSEngineGraph(t, "relationshipdefaults", gdsNode("a", "N", nil), gdsNode("b", "N", nil))
	require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R"}))
	for defaultValue, expected := range map[string]string{
		"[]":    "Expected type of default value to be `Double`. But got `Object[]`.",
		"{a:1}": "Expected type of default value to be `Double`. But got `HashMap`.",
		"'x'":   "Expected type of default value to be `Double`. But got `String`.",
		"true":  "Expected type of default value to be `Double`. But got `Boolean`.",
		"[1]":   "Expected type of default value to be `Double`. But got `long[]`.",
		"[1.5]": "Expected type of default value to be `Double`. But got `double[]`.",
	} {
		_, err := run("CALL gds.graph.project('defaults','N',{R:{properties:{w:{defaultValue:" + defaultValue + "}}}})")
		require.ErrorContains(t, err, expected, defaultValue)
	}
	result, err := run("RETURN gds.graph.exists('defaults')")
	require.NoError(t, err)
	require.Equal(t, false, result.Rows[0][0])
}
