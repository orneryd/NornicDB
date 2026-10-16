package cypher

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/louvain"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// GDS 2.13.12 captures include both native and aggregation projection, signed
// cancellation, parallel arcs, fractional weights, seeds and undirected input.
// Aggregation uses the topic consumer's default relationship type and `weight`
// key, except the multi-type case which explicitly tests constituent ordering.
func TestGDSLouvainNumericalReferenceProjections(t *testing.T) {
	var fixtures []struct {
		Name, Form, Orientation string
		N                       int
		Seeds                   []int64
		NativeIDs               [][]int64
		Edges                   [][]interface{}
		Expected                [][]int64
	}
	data, err := os.ReadFile("testdata/gds_louvain_round2_numerics.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 24)
	for _, tc := range fixtures {
		t.Run(tc.Name, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			nodes := make([]*storage.Node, tc.N)
			ids := make([]string, tc.N)
			logical := make(map[string]int, tc.N)
			for i := range ids {
				ids[i] = strconv.Itoa(i)
			}
			if tc.Form == "native" {
				require.Len(t, tc.NativeIDs, tc.N)
				for _, row := range tc.NativeIDs {
					ids[row[0]] = fmt.Sprintf("%020d", row[1])
				}
			}
			for i := range nodes {
				properties := map[string]interface{}{}
				if tc.Seeds != nil {
					properties["seed"] = tc.Seeds[i]
				}
				nodes[i] = gdsNode(ids[i], "N", properties)
				logical[ids[i]] = i
			}
			e, run := newGDSEngineGraph(t, "louvainnumerical", nodes...)
			if tc.Form == "native" {
				var types []string
				for i, edge := range tc.Edges {
					source, target, kind := int(edge[0].(float64)), int(edge[1].(float64)), edge[2].(string)
					types = appendUnique(types, kind)
					require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%04d", i)), StartNode: storage.NodeID(ids[source]), EndNode: storage.NodeID(ids[target]), Type: kind, Properties: map[string]interface{}{"weight": edge[3]}}))
				}
				for i, kind := range types {
					types[i] = kind + ":{orientation:'" + tc.Orientation + "',properties:'weight'}"
				}
				nodeProjection := "'N'"
				if tc.Seeds != nil {
					nodeProjection = "{N:{properties:'seed'}}"
				}
				_, err = run("CALL gds.graph.project('numeric'," + nodeProjection + ",{" + strings.Join(types, ",") + "},{readConcurrency:1})")
			} else {
				rows := make([]interface{}, len(tc.Edges))
				for i, edge := range tc.Edges {
					source, target := int64(edge[0].(float64)), int64(edge[1].(float64))
					row := map[string]interface{}{"s": source, "t": target, "k": edge[2], "w": edge[3]}
					if tc.Seeds != nil {
						row["ss"], row["ts"] = tc.Seeds[source], tc.Seeds[target]
					}
					rows[i] = row
				}
				properties := "{relationshipProperties:{weight:r.w}"
				if strings.HasPrefix(tc.Name, "type_order_") {
					properties += ",relationshipType:r.k"
				}
				if tc.Seeds != nil {
					properties += ",sourceNodeProperties:{seed:r.ss},targetNodeProperties:{seed:r.ts}"
				}
				properties += "}"
				config := ""
				if tc.Orientation == "UNDIRECTED" {
					config = ",{undirectedRelationshipTypes:['*']}"
				}
				_, err = e.Execute(t.Context(), "UNWIND $rows AS r RETURN gds.graph.project('numeric',r.s,r.t,"+properties+config+")", map[string]interface{}{"rows": rows})
			}
			require.NoError(t, err)
			config := map[string]interface{}{"concurrency": int64(1), "relationshipWeightProperty": "weight"}
			if tc.Seeds != nil {
				config["seedProperty"] = "seed"
			} else {
				config["consecutiveIds"] = true
			}
			result, err := e.Execute(t.Context(), "CALL gds.louvain.stream('numeric',$config)", map[string]interface{}{"config": config})
			require.NoError(t, err)
			require.Len(t, result.Rows, tc.N)
			actual, expected := make([]int, tc.N), make([]int, tc.N)
			for _, row := range result.Rows {
				// Stored nodes' ids are strings; aggregation INTEGER ids stay INTEGERs.
				node, ok := logical[fmt.Sprint(row[0])]
				require.True(t, ok)
				actual[node] = int(row[1].(int64))
			}
			for _, row := range tc.Expected {
				expected[row[0]] = int(row[1])
			}
			if tc.Seeds == nil {
				// Native Neo4j can reuse IDs, changing internal community names.
				// Compare partitions by first logical node; seeded IDs stay exact.
				louvain.Renumber(actual)
				louvain.Renumber(expected)
			}
			require.Equal(t, expected, actual)
			_, err = run("CALL gds.graph.drop('numeric')")
			require.NoError(t, err)
		})
	}
}
