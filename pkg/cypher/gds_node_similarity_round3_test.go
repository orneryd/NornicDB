package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNodeSimilarityRound3PriorSupplement(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", map[string]interface{}{"w": 1.0}}, {"1", "2", "R", map[string]interface{}{"w": 1.0}}}, "'N'", "{R:{properties:'w'}}")
	for _, config := range []string{"nodeLabels:[]", "relationshipTypes:[]"} {
		rows := reviewRows(t, e, config)
		require.Len(t, rows, 2)
		require.Equal(t, 1.0, rows[0][2])
	}
	for _, value := range []float64{math.Inf(1), math.Inf(-1)} {
		_, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('rev',{similarityCutoff:$cutoff})", map[string]interface{}{"cutoff": value})
		t.Logf("cutoff=%v error=%v", value, err)
		require.Error(t, err)
	}
}

func TestNodeSimilarityRound3SelectedComponentProperty(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	_, err := e.storage.CreateNode(&storage.Node{ID: "3", Labels: []string{"Other"}, Properties: map[string]interface{}{"comp": int64(7)}})
	require.NoError(t, err)
	_, err = e.Execute(context.Background(), "CALL gds.graph.project('selected',{N:{},Other:{properties:'comp'}},'R')", nil)
	require.NoError(t, err)
	for _, procedure := range []string{"gds.nodeSimilarity.stream", "gds.nodeSimilarity.filtered.stream"} {
		t.Run(procedure, func(t *testing.T) {
			result, err := e.Execute(context.Background(), "CALL "+procedure+"('selected',{nodeLabels:['N'],useComponents:'comp'})", nil)
			t.Logf("result=%v error=%v", result, err)
			require.ErrorContains(t, err, "Component property `comp` not found in graph with node properties: [comp]")
		})
	}
}

func TestNodeSimilarityRound3InvalidComponentMessages(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	for _, tc := range []struct{ config, want string }{
		{"useComponents:''", "Invalid component spec: expected a valid node property"},
		{"useComponents:' '", "Invalid component spec: expected a valid node property"},
		{"useComponents:5", "Invalid component spec: cannot parse type Long with value 5"},
	} {
		t.Run(tc.config, func(t *testing.T) {
			_, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('rev',{"+tc.config+"})", nil)
			t.Logf("error=%v", err)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestNodeSimilarityRound3OutputIDTypes(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	for _, procedure := range []string{"gds.nodeSimilarity.stream", "gds.nodeSimilarity.filtered.stream"} {
		t.Run(procedure, func(t *testing.T) {
			bare, err := e.Execute(context.Background(), "CALL "+procedure+"('rev')", nil)
			require.NoError(t, err)
			t.Logf("bare rows=%v node1 type=%T", bare.Rows, bare.Rows[0][0])
			result, err := e.Execute(context.Background(), "CALL "+procedure+"('rev') YIELD node1 WHERE node1 STARTS WITH '0' RETURN node1", nil)
			t.Logf("string predicate result=%v error=%v", result, err)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"0"}}, result.Rows)
		})
	}
}

func TestNodeSimilarityRound3PipelineOutputIDTypes(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	for _, procedure := range []string{"gds.nodeSimilarity.stream", "gds.nodeSimilarity.filtered.stream"} {
		t.Run(procedure, func(t *testing.T) {
			plain, plainErr := e.Execute(context.Background(), "CALL "+procedure+"('rev') YIELD node1 RETURN size(node1) AS n", nil)
			t.Logf("standalone size result=%v error=%v", plain, plainErr)
			result, err := e.Execute(context.Background(), "WITH 1 AS x CALL "+procedure+"('rev') YIELD node1 RETURN size(node1) AS n", nil)
			t.Logf("pipeline size result=%v error=%v", result, err)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}, {int64(1)}}, result.Rows)
		})
	}
}

func TestNodeSimilarityRound3TypeTraversalOrder(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 4), []reviewEdge{
		{"0", "2", "Z", map[string]interface{}{"w": 2.0}},
		{"0", "2", "a", map[string]interface{}{"w": 1.0}},
		{"1", "2", "A", map[string]interface{}{"w": 1.0}},
		{"1", "3", "A", map[string]interface{}{"w": 1.0}},
	}, "'N'", "{A:{properties:'w'},Z:{properties:'w'},a:{properties:'w'}}")
	for _, tc := range []struct {
		metric string
		score  float64
	}{{"JACCARD", 0.5}, {"OVERLAP", 1.0}} {
		t.Run(tc.metric, func(t *testing.T) {
			rows := reviewRows(t, e, "relationshipWeightProperty:'w',similarityMetric:'"+tc.metric+"'")
			require.Len(t, rows, 2)
			require.Equal(t, tc.score, rows[0][2])
		})
	}
}

func TestNodeSimilarityRound3UnweightedTypeOrder(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 4), []reviewEdge{
		{"0", "2", "A", nil}, {"0", "3", "a", nil}, {"0", "2", "Z", nil}, {"0", "3", "Z", nil},
		{"1", "2", "A", nil}, {"1", "3", "A", nil},
	}, "'N'", "['A','Z','a']")
	rows := reviewRows(t, e, "")
	require.Len(t, rows, 2)
	require.Equal(t, 2.0/3.0, rows[0][2])
	// GDS constructs a different constituent map for the aggregation import.
	// The exact same edges yield 0.5 there, rather than the native list's 2/3.
	_, err := e.Execute(context.Background(), "UNWIND [{s:0,t:2,k:'A'},{s:0,t:3,k:'a'},{s:0,t:2,k:'Z'},{s:0,t:3,k:'Z'},{s:1,t:2,k:'A'},{s:1,t:3,k:'A'}] AS r RETURN gds.graph.project('agg',r.s,r.t,{relationshipType:r.k})", nil)
	require.NoError(t, err)
	result, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('agg',{concurrency:1})", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	for _, row := range result.Rows {
		require.Equal(t, 0.5, row[2])
	}
}

func TestNodeSimilarityRound3SelectedFilterLabel(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	_, err := e.storage.CreateNode(&storage.Node{ID: "3", Labels: []string{"Other"}})
	require.NoError(t, err)
	_, err = e.Execute(context.Background(), "CALL gds.graph.project('selectedfilter',['N','Other'],'R')", nil)
	require.NoError(t, err)
	for _, key := range []string{"sourceNodeFilter", "targetNodeFilter"} {
		t.Run(key, func(t *testing.T) {
			result, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.filtered.stream('selectedfilter',{nodeLabels:['N'],"+key+":'Other'})", nil)
			t.Logf("result=%v error=%v", result, err)
			require.ErrorContains(t, err, "The label `Other` does not exist in the graph")
		})
	}
}

func TestNodeSimilarityRound3DifferentialSweep(t *testing.T) {
	type score struct{ S, T, V string }
	var fixtures []struct {
		Name  string
		Kinds []string
		Edges []struct {
			From, To, Kind string
			W              float64
		}
		Expected, NativeExpected              map[string][]score
		NativeTypeOrder, AggregationTypeOrder []string
	}
	data, err := os.ReadFile("testdata/gds_node_similarity_type_order.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 24)
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			edges := make([]reviewEdge, len(fixture.Edges))
			for i, edge := range fixture.Edges {
				edges[i] = reviewEdge{edge.From, edge.To, edge.Kind, map[string]interface{}{"w": edge.W}}
			}
			kinds := make([]string, len(fixture.Kinds))
			for i, k := range fixture.Kinds {
				kinds[i] = k + ":{properties:'w'}"
			}
			e := reviewGraph(t, make([]map[string]interface{}, 9), edges, "'N'", "{"+strings.Join(kinds, ",")+"}")
			input := make([]interface{}, 0, 9+len(edges))
			for i := 0; i < 9; i++ {
				input = append(input, map[string]interface{}{"s": i, "t": nil, "k": "A", "w": 1.0})
			}
			for _, edge := range fixture.Edges {
				input = append(input, map[string]interface{}{"s": edge.From, "t": edge.To, "k": edge.Kind, "w": edge.W})
			}
			_, err = e.Execute(context.Background(), "UNWIND $rows AS r RETURN gds.graph.project('agg',toInteger(r.s),toInteger(r.t),{relationshipType:r.k,relationshipProperties:{w:r.w}})", map[string]interface{}{"rows": input})
			require.NoError(t, err)
			// Native import and aggregation have different map construction orders.
			// Each reference was captured from GDS using the same import as this run.
			for _, graph := range []struct {
				name      string
				expected  map[string][]score
				typeOrder []string
			}{{"rev", fixture.NativeExpected, fixture.NativeTypeOrder}, {"agg", fixture.Expected, fixture.AggregationTypeOrder}} {
				projection, err := e.graphProjection(graph.name)
				require.NoError(t, err)
				require.Equal(t, graph.typeOrder, projection.relationshipTypeOrder)
				require.Len(t, graph.expected, 6)
				for key, expected := range graph.expected {
					t.Run(graph.name+"/"+key, func(t *testing.T) {
						metric, weight, _ := strings.Cut(key, "_")
						config := "similarityMetric:'" + metric + "',similarityCutoff:0,topK:100,concurrency:1"
						if weight == "weighted" {
							config += ",relationshipWeightProperty:'w'"
						}
						result, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('"+graph.name+"',{"+config+"})", nil)
						require.NoError(t, err)
						require.Len(t, result.Rows, len(expected))
						actual := make(map[string]float64)
						for _, row := range result.Rows {
							actual[fmt.Sprint(row[0])+":"+fmt.Sprint(row[1])] = row[2].(float64)
						}
						for _, row := range expected {
							score, err := strconv.ParseFloat(row.V, 64)
							require.NoError(t, err)
							pair := row.S + ":" + row.T
							require.Contains(t, actual, pair)
							require.InDelta(t, score, actual[pair], 1e-12, "pair %s", pair)
						}
					})
				}
			}
		})
	}
}
func TestNodeSimilarityRelationshipMapOrder(t *testing.T) {
	for _, tc := range []struct {
		types  []string
		native int
		want   []string
	}{
		{[]string{"A", "Z", "a"}, 0, []string{"A", "a", "Z"}},
		{[]string{"a", "Z", "A"}, 0, []string{"a", "A", "Z"}},
		{[]string{"A", "Z", "a"}, 3, []string{"a", "A", "Z"}},
		{[]string{"a", "Z", "A"}, 3, []string{"A", "a", "Z"}},
		{[]string{"A", "Z", "a", "AA", "B"}, 5, []string{"a", "A", "B", "Z", "AA"}},
	} {
		require.Equal(t, tc.want, gdsRelationshipTypeOrder(tc.types, tc.native))
	}
}
func TestNodeSimilarityRelationshipMapOrderManyTypes(t *testing.T) {
	types := []string{"A", "Z", "a", "AA", "B", "R", "r", "BB", "Aa", "é", "😀", "RELATION", "KNOWS", "x"}
	require.Equal(t, []string{"A", "a", "B", "RELATION", "KNOWS", "é", "R", "r", "x", "Z", "😀", "AA", "BB", "Aa"}, gdsRelationshipTypeOrder(types, 0))
	require.Equal(t, []string{"a", "A", "B", "KNOWS", "RELATION", "é", "R", "r", "x", "Z", "😀", "Aa", "BB", "AA"}, gdsRelationshipTypeOrder(gdsNativeProjectionMapOrder(types), 28))
}

func TestNodeSimilarityNativeProjectionMapOrder(t *testing.T) {
	types := []string{"A", "Z", "a", "AA", "B", "R", "r", "BB", "Aa", "é", "😀", "RELATION", "KNOWS", "x"}
	require.Equal(t, []string{"AA", "BB", "Aa", "A", "a", "B", "RELATION", "é", "KNOWS", "r", "R", "x", "😀", "Z"}, gdsNativeProjectionMapOrder(types))
	specs := make([]string, len(types))
	for i, name := range types {
		specs[i] = "`" + name + "`:{type:'R',indexInverse:true}"
	}
	exec := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "{"+strings.Join(specs, ",")+"}")
	projection, err := exec.graphProjection("rev")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "A", "B", "KNOWS", "RELATION", "é", "R", "r", "x", "Z", "😀", "Aa", "BB", "AA"}, projection.relationshipTypeOrder)
}
