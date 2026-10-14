package cypher

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/louvain"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGDSLouvainFilteredIDsAndSignedWeights(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edges  [][3]float64
		config string
		want   [][]interface{}
	}{
		{"filtered consecutive IDs", [][3]float64{{1, 2, 1}}, "concurrency:1,relationshipWeightProperty:'w',consecutiveIds:true,minCommunitySize:2", [][]interface{}{{"1", int64(0), nil}, {"2", int64(0), nil}}},
		{"signed weights with zero total", [][3]float64{{0, 1, 1}, {1, 2, -1}, {2, 0, 0}}, "concurrency:1,relationshipWeightProperty:'w',consecutiveIds:true", [][]interface{}{{"0", int64(0), nil}, {"1", int64(0), nil}, {"2", int64(1), nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			exec, run := newGDSEngineGraph(t, "louvainRound3", gdsNode("0", "N", nil), gdsNode("1", "N", nil), gdsNode("2", "N", nil))
			for i, edge := range tc.edges {
				require.NoError(t, exec.storage.CreateEdge(&storage.Edge{ID: storage.EdgeID(strconv.Itoa(i)), StartNode: storage.NodeID(strconv.Itoa(int(edge[0]))), EndNode: storage.NodeID(strconv.Itoa(int(edge[1]))), Type: "R", Properties: map[string]interface{}{"w": edge[2]}}))
			}
			_, err := run("CALL gds.graph.project('round3','N',{R:{properties:'w'}})")
			require.NoError(t, err)
			result, err := run("CALL gds.louvain.stream('round3',{" + tc.config + "})")
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Rows)
		})
	}
}

func TestGDSLouvainSignedAndFilteredReferenceCases(t *testing.T) {
	testGDSLouvainReferenceCases(t, "testdata/gds_louvain_round3.json", 273)
}

// These 729 GDS 2.13.12 cases include fractional, negative, zero and missing
// weights, orientations, seeds, size filters and intermediate communities.
func TestGDSLouvainWeightedReferenceCases(t *testing.T) {
	testGDSLouvainReferenceCases(t, "testdata/gds_louvain_weighted_round3.json", 729)
}

func testGDSLouvainReferenceCases(t *testing.T, path string, count int) {
	var cases []struct {
		Name         string
		N            int
		Edges        [][]interface{}
		Orientation  string
		Seeds        []*int64
		Config       map[string]interface{}
		Expected     json.RawMessage
		Modularities []interface{}
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &cases))
	require.Len(t, cases, count)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			gdsFollowUpCatalog(t)
			nodes := make([]*storage.Node, tc.N)
			for i := range nodes {
				properties := map[string]interface{}{}
				if tc.Seeds != nil && tc.Seeds[i] != nil {
					properties["seed"] = *tc.Seeds[i]
				}
				nodes[i] = gdsNode(strconv.Itoa(i), "N", properties)
			}
			exec, run := newGDSEngineGraph(t, "louvainReferenceRound3", nodes...)
			for i, edge := range tc.Edges {
				properties := map[string]interface{}{}
				if edge[2] != nil {
					switch weight := edge[2].(type) {
					case string:
						switch weight {
						case "NaN":
							properties["w"] = math.NaN()
						case "Infinity":
							properties["w"] = math.Inf(1)
						case "-Infinity":
							properties["w"] = math.Inf(-1)
						default:
							t.Fatalf("unknown reference weight %q", weight)
						}
					default:
						properties["w"] = weight
					}
				}
				require.NoError(t, exec.storage.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%04d", i)), StartNode: storage.NodeID(strconv.Itoa(int(edge[0].(float64)))), EndNode: storage.NodeID(strconv.Itoa(int(edge[1].(float64)))), Type: "R", Properties: properties}))
			}
			nodeProjection := "'N'"
			if tc.Seeds != nil {
				nodeProjection = "{N:{properties:{seed:{defaultValue:null}}}}"
			}
			_, err := run("CALL gds.graph.project('round3'," + nodeProjection + ",{R:{orientation:'" + tc.Orientation + "',properties:{w:{defaultValue:null}}}})")
			require.NoError(t, err)
			for _, key := range []string{"concurrency", "maxLevels", "maxIterations", "minCommunitySize"} {
				if number, ok := tc.Config[key].(float64); ok {
					tc.Config[key] = int64(number)
				}
			}
			result, err := exec.Execute(t.Context(), "CALL gds.louvain.stream('round3',$config)", map[string]interface{}{"config": tc.Config})
			require.NoError(t, err)
			got := make([][]interface{}, 0, len(result.Rows))
			for _, row := range result.Rows {
				id, err := strconv.ParseInt(row[0].(string), 10, 64)
				require.NoError(t, err)
				got = append(got, []interface{}{id, row[1], row[2]})
			}
			encoded, err := json.Marshal(got)
			require.NoError(t, err)
			require.JSONEq(t, string(tc.Expected), string(encoded))
			if tc.Seeds == nil && tc.Modularities != nil {
				projection, err := exec.graphProjection("round3")
				require.NoError(t, err)
				weight, _ := tc.Config["relationshipWeightProperty"].(string)
				selected, err := selectGDSSubgraph(projection, nil, nil, weight)
				require.NoError(t, err)
				graph := louvain.NewGraph(len(selected.nodes))
				for i, edges := range selected.outgoing {
					for _, edge := range edges {
						graph.Arc(i, edge.target, edge.weight)
					}
				}
				options := louvain.DefaultOptions()
				if v, ok := tc.Config["maxLevels"].(int64); ok {
					options.MaxLevels = int(v)
				}
				if v, ok := tc.Config["maxIterations"].(int64); ok {
					options.MaxIterations = int(v)
				}
				if v, ok := tc.Config["tolerance"].(float64); ok {
					options.Tolerance = v
				}
				actual := louvain.Run(graph, options).Modularities
				require.Len(t, actual, len(tc.Modularities))
				for i, expected := range tc.Modularities {
					switch value := expected.(type) {
					case float64:
						require.InDelta(t, value, actual[i], 1e-12)
					case string:
						switch strings.ToLower(value) {
						case "nan":
							require.True(t, math.IsNaN(actual[i]))
						case "infinity", "inf":
							require.True(t, math.IsInf(actual[i], 1))
						case "-infinity", "-inf":
							require.True(t, math.IsInf(actual[i], -1))
						default:
							t.Fatalf("unknown modularity %q", value)
						}
					default:
						t.Fatalf("unsupported modularity %v", expected)
					}
				}
			}
		})
	}
}
