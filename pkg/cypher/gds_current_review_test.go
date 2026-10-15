package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGDSNumericConfigurationLargeLongs(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "1", "R", nil}}, "'N'", "'R'")
	for _, field := range []string{"normalizationStrength:9007199254740993", "iterationWeights:[9007199254740993]"} {
		result, err := e.Execute(context.Background(), "CALL gds.fastRP.stats('rev',{embeddingDimension:4,"+field+",randomSeed:42,concurrency:1})", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(3), result.Rows[0][0])
		configuration := result.Rows[0][3].(map[string]interface{})
		if strings.HasPrefix(field, "normalizationStrength") {
			require.Equal(t, float64(9007199254740992), configuration["normalizationStrength"])
		}
	}
	// Configuration readers allow rounding while property imports retain
	// GDS's strict bound. Stored reads are exercised through the typed owner.
	for _, value := range []interface{}{int64(9007199254740993), []int64{9007199254740993}, []interface{}{int64(9007199254740993), 1.5}} {
		_, _, _, err := gdsConvertPropertyValue(value, "Float", false)
		require.Error(t, err)
	}
}

var populatedBucketTypes = []string{"T02070", "T02191", "T03160", "T03281", "T04095", "T04370", "T04491", "T05064", "T05185", "T05460", "T05581", "T06032", "T06153", "T06274", "T06395", "T06670"}

func TestGDSJavaValueTextPopulatedBuckets(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "1", "R", nil}}, "'N'", "'R'")
	entries := make([]string, len(populatedBucketTypes))
	for i, name := range populatedBucketTypes {
		entries[i] = name + ":1"
	}
	_, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('rev',{useComponents:{"+strings.Join(entries, ",")+"}})", nil)
	require.ErrorContains(t, err, "Invalid component spec: cannot parse type HashMap with value {T06274=1, T02191=1, T05064=1, T05460=1, T02070=1, T05185=1, T06032=1, T06395=1, T05581=1, T06153=1, T06670=1, T04095=1, T03160=1, T04491=1, T03281=1, T04370=1}")
}

// Scores captured from GDS 2.13.12 across native and aggregation projections,
// around bucket treeification, with both forward and reverse type insertion.
func TestNodeSimilarityPopulatedRelationshipBuckets(t *testing.T) {
	data, err := os.ReadFile("testdata/gds_populated_buckets.json")
	require.NoError(t, err)
	var fixtures []struct {
		Form    string
		Count   int
		Reverse bool
		Score   float64
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 30)
	for _, fixture := range fixtures {
		t.Run(fmt.Sprintf("%s/%d/reverse=%v", fixture.Form, fixture.Count, fixture.Reverse), func(t *testing.T) {
			types := append([]string(nil), populatedBucketTypes[:fixture.Count]...)
			if fixture.Reverse {
				for i, j := 0, len(types)-1; i < j; i, j = i+1, j-1 {
					types[i], types[j] = types[j], types[i]
				}
			}
			edges := make([]reviewEdge, 0, fixture.Count+2)
			// Preserve storage edge IDs in the reference's original creation order.
			for i, kind := range populatedBucketTypes[:fixture.Count] {
				edges = append(edges, reviewEdge{"0", "2", kind, map[string]interface{}{"w": float64(i + 1)}})
			}
			literals, specs := make([]string, len(types)), make([]string, len(types))
			for i, kind := range types {
				literals[i], specs[i] = "'"+kind+"'", kind+":{properties:'w'}"
			}
			edges = append(edges, reviewEdge{"1", "2", populatedBucketTypes[0], map[string]interface{}{"w": 1.0}}, reviewEdge{"1", "3", populatedBucketTypes[0], map[string]interface{}{"w": 1.0}})
			e := reviewGraph(t, make([]map[string]interface{}, 4), edges, "'N'", "'*'")
			_, err := e.Execute(context.Background(), "CALL gds.graph.drop('rev')", nil)
			require.NoError(t, err)
			query := "CALL gds.graph.project('rev','N',[" + strings.Join(literals, ",") + "],{relationshipProperties:'w',readConcurrency:1})"
			if fixture.Form == "local" {
				query = "CALL gds.graph.project('rev','N',{" + strings.Join(specs, ",") + "},{readConcurrency:1})"
			} else if fixture.Form == "aggregation" {
				direction := "ASC"
				if fixture.Reverse {
					direction = "DESC"
				}
				query = "MATCH (s:N)-[r]->(t:N) WITH s,r,t ORDER BY id(s) ASC, type(r) " + direction + " RETURN gds.graph.project('rev',s,t,{relationshipType:type(r),relationshipProperties:{w:r.w}})"
			}
			_, err = e.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			projection, err := e.graphProjection("rev")
			require.NoError(t, err)
			t.Logf("order=%v", projection.relationshipTypeOrder)
			result, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('rev',{concurrency:1,relationshipWeightProperty:'w'})", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			for _, row := range result.Rows {
				require.Equal(t, fixture.Score, row[2])
			}
		})
	}
}
