package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGDSRelationshipDefaultValidation(t *testing.T) {
	for _, tc := range []struct{ value, message string }{
		{"[1,2]", "Expected type of default value to be `Double`. But got `long[]`."},
		{"[1.0,2.0]", "Expected type of default value to be `Double`. But got `double[]`."},
		{"'x'", "Expected type of default value to be `Double`. But got `String`."},
		{"9007199254740993", "Cannot safely convert 9007199254740993 into an double value"},
		{"9007199254740994", "Cannot safely convert 9007199254740994 into an double value"},
	} {
		for _, global := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/global=%v", tc.value, global), func(t *testing.T) {
				gdsFollowUpCatalog(t)
				e, run := newGDSEngineGraph(t, "relationshipdefaults", gdsNode("0", "N", nil), gdsNode("1", "N", nil))
				require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: "0", EndNode: "1", Type: "R"}))
				properties := "{w:{defaultValue:" + tc.value + "}}"
				query := "CALL gds.graph.project('defaults','N',{R:{properties:" + properties + "}})"
				if global {
					query = "CALL gds.graph.project('defaults','N','R',{relationshipProperties:" + properties + "})"
				}
				require.NotPanics(t, func() {
					_, err := run(query)
					require.ErrorContains(t, err, tc.message)
				})
				_, err := e.graphProjection("defaults")
				require.Error(t, err, "failed projections must not enter the catalog")
			})
		}
	}
}

func TestGDSRelationshipDefaultValuesAndNodeArrays(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  float64
	}{
		{"1", 1}, {"1.5", 1.5}, {"9007199254740992", 9007199254740992}, {"-9007199254740993", -9007199254740992},
	} {
		t.Run(tc.value, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			e, run := newGDSEngineGraph(t, "validdefaults", gdsNode("0", "N", map[string]interface{}{"v": []int64{1, 2}}), gdsNode("1", "N", nil))
			require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: "0", EndNode: "1", Type: "R"}))
			_, err := run("CALL gds.graph.project('defaults',{N:{properties:{v:{defaultValue:[0,0]}}}},{R:{properties:{w:{defaultValue:" + tc.value + "}}}})")
			require.NoError(t, err)
			p, err := e.graphProjection("defaults")
			require.NoError(t, err)
			require.Equal(t, tc.want, p.Relationships[0].Properties["w"])
			require.Equal(t, []int64{0, 0}, p.NodeProperties["1"]["v"])
		})
	}
}

func TestGDSRelationshipDefaultImportErrors(t *testing.T) {
	// The importer must propagate conversion failures even when a caller
	// supplies parsed specs directly, without procedure-argument validation.
	for _, value := range []interface{}{[]int64{1, 2}, int64(9007199254740993)} {
		gdsFollowUpCatalog(t)
		e, _ := newGDSEngineGraph(t, "defaultimport", gdsNode("0", "N", nil), gdsNode("1", "N", nil))
		require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "r", StartNode: "0", EndNode: "1", Type: "R"}))
		_, err := e.buildGraphProjection(context.Background(), "defaults", []gdsElementSpec{{name: "N", element: "N"}}, []gdsElementSpec{{name: "R", element: "R", properties: []gdsPropertySpec{{key: "w", property: "w", defaultValue: value}}}}, false)
		require.Error(t, err)
	}
}
