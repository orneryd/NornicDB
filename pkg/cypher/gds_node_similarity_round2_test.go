package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type reviewEdge struct {
	from, to, kind string
	props          map[string]interface{}
}

func reviewGraph(t *testing.T, props []map[string]interface{}, edges []reviewEdge, nodeProjection, relProjection string) *StorageExecutor {
	t.Helper()
	graphProjections = make(map[string]*GraphProjection)
	t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "review1197")
	for i, p := range props {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprint(i)), Labels: []string{"N"}, Properties: p})
		require.NoError(t, err)
	}
	for i, r := range edges {
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%03d", i)), StartNode: storage.NodeID(r.from), EndNode: storage.NodeID(r.to), Type: r.kind, Properties: r.props}))
	}
	exec := NewStorageExecutor(engine)
	t.Cleanup(func() {
		for _, graph := range databaseProjections(exec.databaseName()) {
			_, err := exec.Execute(context.Background(), "CALL gds.graph.drop($name)", map[string]interface{}{"name": graph.Name})
			require.NoError(t, err)
		}
	})
	_, err := exec.Execute(context.Background(), "CALL gds.graph.project('rev',"+nodeProjection+","+relProjection+")", nil)
	require.NoError(t, err)
	return exec
}

func reviewRows(t *testing.T, e *StorageExecutor, config string) [][]interface{} {
	t.Helper()
	result, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('rev', {"+config+"})", nil)
	require.NoError(t, err)
	t.Logf("config=%s rows=%v", config, result.Rows)
	return result.Rows
}

func TestReviewRound2ParallelAcrossTypes(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 4), []reviewEdge{
		{"0", "2", "Z", map[string]interface{}{"w": 2.0}},
		{"0", "2", "A", map[string]interface{}{"w": 1.0}},
		{"1", "2", "A", map[string]interface{}{"w": 1.0}},
		{"1", "3", "A", map[string]interface{}{"w": 1.0}},
	}, "'N'", "{A:{properties:'w'}, Z:{properties:'w'}}")
	for _, selection := range []string{"", ",relationshipTypes:['A','Z']", ",relationshipTypes:['Z','A']"} {
		t.Run(selection, func(t *testing.T) {
			rows := reviewRows(t, e, "relationshipWeightProperty:'w'"+selection)
			require.Len(t, rows, 2)
			require.Equal(t, 0.5, rows[0][2], "GDS 2.13.12 selected type A's weight")
		})
	}
}

func TestReviewRound2ComponentPrecision(t *testing.T) {
	e := reviewGraph(t, []map[string]interface{}{{"comp": int64(9007199254740992)}, {"comp": int64(9007199254740993)}, {"comp": int64(5)}}, []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "{N:{properties:'comp'}}", "'R'")
	require.Empty(t, reviewRows(t, e, "useComponents:'comp'"), "GDS treats these as different Long component IDs")
}

func TestReviewRound2ComponentDefault(t *testing.T) {
	e := reviewGraph(t, []map[string]interface{}{nil, {"comp": int64(math.MinInt64)}, {"comp": int64(5)}}, []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "{N:{properties:'comp'}}", "'R'")
	require.Len(t, reviewRows(t, e, "useComponents:'comp'"), 2, "GDS's missing Integer property defaults to MinInt64")
}

func TestReviewRound2ComponentType(t *testing.T) {
	e := reviewGraph(t, []map[string]interface{}{{"comp": 1.2}, {"comp": 1.2}, {"comp": 5.5}}, []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "{N:{properties:'comp'}}", "'R'")
	result, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('rev',{useComponents:'comp'})", nil)
	t.Logf("rows=%v error=%v", result, err)
	require.ErrorContains(t, err, "Tried to retrieve a value of type LONG value from properties of type DOUBLE")
}

func TestReviewRound2UnprojectedStringID(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	_, err := e.storage.CreateNode(&storage.Node{ID: "9", Labels: []string{"Outside"}})
	require.NoError(t, err)
	for _, filter := range []string{"n", "id(n)", "[id(n)]"} {
		t.Run(filter, func(t *testing.T) {
			result, err := e.Execute(context.Background(), "MATCH (n:Outside) CALL gds.nodeSimilarity.filtered.stream('rev',{sourceNodeFilter:"+filter+"}) YIELD node1 RETURN node1", nil)
			t.Logf("result=%v error=%v", result, err)
			require.NoError(t, err, "A valid ID outside the projection should admit no nodes")
			require.Empty(t, result.Rows)
		})
	}
}

func TestReviewRound2FilterListMessages(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	for _, tc := range []struct{ filter, want string }{
		{"[true,true]", "Invalid types in list. Expected Longs or Nodes but found [Boolean, Boolean]"},
		{"[1,'x',2.5,'y']", "Invalid types in list. Expected Longs or Nodes but found [String, Double, String]"},
		{"[{}]", "Invalid types in list. Expected Longs or Nodes but found [HashMap]"},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			_, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.filtered.stream('rev',{sourceNodeFilter:"+tc.filter+"})", nil)
			t.Logf("error=%v", err)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestReviewRound2MissingWeights(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 4), []reviewEdge{
		{"0", "2", "R", map[string]interface{}{"w": 1.0}},
		{"0", "3", "R", nil},
		{"1", "2", "R", map[string]interface{}{"w": 1.0}},
	}, "'N'", "{R:{properties:'w'}}")
	for _, metric := range []string{"JACCARD", "OVERLAP", "COSINE"} {
		t.Run(metric, func(t *testing.T) {
			require.Empty(t, reviewRows(t, e, "relationshipWeightProperty:'w',similarityMetric:'"+metric+"',similarityCutoff:0"), "GDS retains missing weight as NaN, so the score is undefined")
		})
	}
}

func TestReviewRound2ConfirmedFixVariants(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 4), []reviewEdge{{"0", "0", "R", nil}, {"0", "2", "R", nil}, {"1", "0", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	require.Len(t, reviewRows(t, e, ""), 2)
	require.Equal(t, 0.5, reviewRows(t, e, "")[0][2])
	require.Empty(t, reviewRows(t, e, "degreeCutoff:2"))
	_, err := readNodeSimilarityConfig(t.Context(), &GraphProjection{}, map[string]interface{}{"similarityCutoff": math.NaN()}, false)
	require.ErrorContains(t, err, "NaN")
	for _, cutoff := range []float64{math.Inf(1), math.Inf(-1), -0.1, 1.1} {
		_, err = readNodeSimilarityConfig(t.Context(), &GraphProjection{}, map[string]interface{}{"similarityCutoff": cutoff}, false)
		require.Error(t, err)
	}
	for _, value := range []interface{}{nil, []interface{}{}, []int64{}} {
		filter, err := readGDSNodeFilter(&GraphProjection{}, value, nil)
		require.NoError(t, err)
		require.True(t, filter.admits(&GraphProjection{}, "0"))
	}
	for _, config := range []map[string]interface{}{{"topK": int64(10), "bottomK": int64(1)}, {"topK": int64(1), "bottomK": int64(10)}, {"topK": int64(10), "bottomK": int64(10)}, {"topN": int64(0), "bottomN": int64(1)}, {"topN": int64(1), "bottomN": int64(0)}} {
		_, err = readNodeSimilarityConfig(t.Context(), &GraphProjection{}, config, false)
		require.NoError(t, err)
	}
}

func TestReviewRound2ParallelAcrossTypeBoundary(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 4), []reviewEdge{
		{"0", "2", "A", map[string]interface{}{"w": 1.0}},
		{"0", "3", "A", map[string]interface{}{"w": 1.0}},
		{"0", "2", "Z", map[string]interface{}{"w": 2.0}},
		{"1", "2", "A", map[string]interface{}{"w": 1.0}},
		{"1", "3", "A", map[string]interface{}{"w": 1.0}},
	}, "'N'", "{A:{properties:'w'},Z:{properties:'w'}}")
	t.Run("unweighted", func(t *testing.T) {
		require.InDelta(t, 2.0/3, reviewRows(t, e, "")[0][2], 1e-12, "GDS retains non-consecutive repetitions between relationship-type graphs")
	})
	t.Run("weighted", func(t *testing.T) { require.Equal(t, 0.5, reviewRows(t, e, "relationshipWeightProperty:'w'")[0][2]) })
	t.Run("degree", func(t *testing.T) {
		require.Empty(t, reviewRows(t, e, "upperDegreeCutoff:2"), "GDS gives node 0 degree 3 in this union graph")
	})
}

func TestReviewRound2InfiniteOverlapScore(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 5), []reviewEdge{
		{"0", "2", "R", map[string]interface{}{"w": 1.0}},
		{"0", "3", "R", map[string]interface{}{"w": -1.0}},
		{"1", "2", "R", map[string]interface{}{"w": 1.0}},
		{"1", "4", "R", map[string]interface{}{"w": -1.0}},
	}, "'N'", "{R:{properties:'w'}}")
	rows := reviewRows(t, e, "relationshipWeightProperty:'w',similarityMetric:'OVERLAP'")
	require.Len(t, rows, 2, "GDS emits the positive infinite score")
	require.True(t, math.IsInf(rows[0][2].(float64), 1))
	require.Empty(t, reviewRows(t, e, "relationshipWeightProperty:'w',similarityMetric:'JACCARD'"))
	cosine := reviewRows(t, e, "relationshipWeightProperty:'w',similarityMetric:'COSINE'")
	require.Len(t, cosine, 2)
	require.InDelta(t, 0.5, cosine[0][2], 1e-12)
}

func TestReviewRound2ZeroInfiniteWeightFixes(t *testing.T) {
	for _, w := range []float64{0, math.Inf(1)} {
		t.Run(fmt.Sprint(w), func(t *testing.T) {
			e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", map[string]interface{}{"w": w}}, {"1", "2", "R", map[string]interface{}{"w": w}}}, "'N'", "{R:{properties:'w'}}")
			for _, metric := range []string{"JACCARD", "OVERLAP", "COSINE"} {
				for _, cutoff := range []string{"", ",similarityCutoff:0"} {
					require.Empty(t, reviewRows(t, e, "relationshipWeightProperty:'w',similarityMetric:'"+metric+"'"+cutoff))
				}
			}
		})
	}
}

func TestReviewRound2NumericStringIDFix(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	for _, config := range []string{"sourceNodeFilter:'0'", "sourceNodeFilter:['0']", "targetNodeFilter:'1'", "targetNodeFilter:['1']", "sourceNodeFilter:['0'],targetNodeFilter:['1']"} {
		result, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.filtered.stream('rev',{"+config+"})", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "0", result.Rows[0][0])
		require.Equal(t, "1", result.Rows[0][1])
	}
}

func TestReviewRound2EmptyWeightProperty(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", map[string]interface{}{"w": 1.0}}, {"1", "2", "R", map[string]interface{}{"w": 2.0}}}, "'N'", "{R:{properties:'w'}}")
	result, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('rev',{relationshipWeightProperty:''})", nil)
	t.Logf("result=%v error=%v", result, err)
	require.Error(t, err, "GDS reports that the empty weight property is not projected")
}

func TestReviewRound2SelfLoopMissingWeight(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "0", "R", nil}, {"0", "2", "R", map[string]interface{}{"w": 1.0}}, {"1", "2", "R", map[string]interface{}{"w": 1.0}}}, "'N'", "{R:{properties:'w'}}")
	require.Len(t, reviewRows(t, e, "relationshipWeightProperty:'w'"), 2, "GDS ignores the self-loop before reading its missing weight")
}

func TestNodeSimilarityInfiniteCutoffMessages(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	for _, tc := range []struct {
		value float64
		text  string
	}{{math.Inf(1), "Infinity"}, {math.Inf(-1), "-Infinity"}} {
		for _, filtered := range []bool{false, true} {
			_, err := e.callGdsNodeSimilarityStream(t.Context(), []interface{}{"rev", map[string]interface{}{"similarityCutoff": tc.value}}, filtered)
			require.ErrorContains(t, err, "Value for `similarityCutoff` was `"+tc.text+"`")
		}
	}
}

// IDs are strings in NornicDB, including user-supplied nonnumeric ids.
// Labels retain precedence; scalar and list filters use the same node reader.
func TestNodeSimilarityStoredStringFilters(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	_, err := e.storage.CreateNode(&storage.Node{ID: "outside-custom-id", Labels: []string{"Outside"}})
	require.NoError(t, err)
	for _, key := range []string{"sourceNodeFilter", "targetNodeFilter"} {
		for _, filter := range []string{"id(n)", "[id(n)]"} {
			result, err := e.Execute(context.Background(), "MATCH (n:Outside) CALL gds.nodeSimilarity.filtered.stream('rev',{"+key+":"+filter+"}) YIELD node1 RETURN node1", nil)
			require.NoError(t, err)
			require.Empty(t, result.Rows)
		}
	}
}

func TestNodeSimilarityTypedComponentErrors(t *testing.T) {
	for _, tc := range []struct {
		value interface{}
		want  string
	}{
		{[]int64{1, 2}, "LONG_ARRAY"}, {[]float64{1.2, 2.5}, "DOUBLE_ARRAY"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			e := reviewGraph(t, []map[string]interface{}{{"comp": tc.value}, {"comp": tc.value}, {"comp": tc.value}}, []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "{N:{properties:'comp'}}", "'R'")
			_, err := e.Execute(context.Background(), "CALL gds.nodeSimilarity.stream('rev',{useComponents:'comp'})", nil)
			require.ErrorContains(t, err, "Tried to retrieve a value of type LONG value from properties of type "+tc.want)
		})
	}
}

func TestGDSInfiniteNumberMessages(t *testing.T) {
	for _, value := range []float64{math.Inf(1), math.Inf(-1)} {
		want := "Infinity"
		if value < 0 {
			want = "-Infinity"
		}
		_, err := fastRPConfiguration(map[string]interface{}{"embeddingDimension": int64(8), "propertyRatio": value})
		require.ErrorContains(t, err, "was `"+want+"`")
		require.Equal(t, want, gdsDefaultText(value, ""))
	}
}

// These GDS 2.13.12 fixtures produce positive infinity in each metric,
// while all-zero and infinite-weight undefined scores remain suppressed.
func TestNodeSimilarityGDSInfiniteScores(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edges  []reviewEdge
		scores map[string]float64
	}{
		{"jaccard", []reviewEdge{
			{"0", "2", "R", map[string]interface{}{"w": 1.0}},
			{"0", "3", "R", map[string]interface{}{"w": -0.5}},
			{"1", "2", "R", map[string]interface{}{"w": 1.0}},
			{"1", "4", "R", map[string]interface{}{"w": -0.5}},
		}, map[string]float64{"JACCARD": math.Inf(1), "OVERLAP": 2, "COSINE": 0.8}},
		{"cosine", []reviewEdge{
			{"0", "2", "R", map[string]interface{}{"w": 1e-200}},
			{"1", "2", "R", map[string]interface{}{"w": 1e-100}},
		}, map[string]float64{"OVERLAP": 1, "COSINE": math.Inf(1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := reviewGraph(t, make([]map[string]interface{}, 5), tc.edges, "'N'", "{R:{properties:'w'}}")
			for metric, want := range tc.scores {
				rows := reviewRows(t, e, "relationshipWeightProperty:'w',similarityMetric:'"+metric+"'")
				require.Len(t, rows, 2)
				for _, row := range rows {
					if math.IsInf(want, 1) {
						require.True(t, math.IsInf(row[2].(float64), 1))
					} else {
						require.InDelta(t, want, row[2], 1e-12)
					}
				}
			}
			if tc.name == "cosine" {
				require.Empty(t, reviewRows(t, e, "relationshipWeightProperty:'w',similarityMetric:'JACCARD'"))
			}
		})
	}
}

func TestNodeSimilarityIntegralFloatComponentRejected(t *testing.T) {
	e := reviewGraph(t, []map[string]interface{}{{"comp": 1.0}, {"comp": 1.0}, {"comp": 5.5}}, []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "{N:{properties:'comp'}}", "'R'")
	for _, filtered := range []bool{false, true} {
		_, err := e.callGdsNodeSimilarityStream(t.Context(), []interface{}{"rev", map[string]interface{}{"useComponents": "comp"}}, filtered)
		require.ErrorContains(t, err, "Tried to retrieve a value of type LONG value from properties of type DOUBLE")
	}
}

func TestNodeSimilarityExplicitIntegerComponentDefault(t *testing.T) {
	e := reviewGraph(t, []map[string]interface{}{nil, {"comp": int64(7)}, {"comp": int64(5)}}, []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "{N:{properties:{comp:{property:'comp',defaultValue:7}}}}", "'R'")
	rows := reviewRows(t, e, "useComponents:'comp'")
	require.Len(t, rows, 2)
	require.Equal(t, 1.0, rows[0][2])
}

// The same node representations enter node-similarity filters, procedure
// configurations and scalar GDS functions. Narrow Go integers must keep the ID.
func TestReviewRound2SharedNodeIDs(t *testing.T) {
	projection := &GraphProjection{nodeIndex: map[string]int{"9": 0}}
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gds-ids")
	for _, id := range []string{"9", "true", "9.5", "9223372036854775807", "18446744073709551615"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"N"}, Properties: map[string]interface{}{"v": int64(7)}})
		require.NoError(t, err)
	}
	e := NewStorageExecutor(store)
	_, err := e.Execute(context.Background(), "CALL gds.graph.project('gds-ids', {N:{properties:'v'}}, '*')", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := e.Execute(context.Background(), "CALL gds.graph.drop('gds-ids')", nil)
		require.NoError(t, err)
	})
	for _, value := range []interface{}{int64(9), int(9), int8(9), int16(9), int32(9), uint8(9), "9", &storage.Node{ID: "9"}} {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			filter, err := readGDSNodeFilter(projection, []interface{}{value}, nil)
			require.NoError(t, err)
			require.Equal(t, map[string]bool{"9": true}, filter.nodes)
			config, err := linkPredictionConfigFromArguments([]interface{}{map[string]interface{}{"sourceNode": value, "targetNodes": []interface{}{value}}})
			require.NoError(t, err)
			require.Equal(t, storage.NodeID("9"), config.SourceNode)
			require.Equal(t, map[storage.NodeID]bool{"9": true}, config.TargetNodes)
			node := gdsNodeByID(store, value)
			require.NotNil(t, node)
			require.Equal(t, storage.NodeID("9"), node.(*storage.Node).ID)
			result, err := e.Execute(context.Background(), "RETURN gds.util.nodeProperty('gds-ids', $node, 'v') AS value", map[string]interface{}{"node": value})
			require.NoError(t, err)
			require.Equal(t, float64(7), result.Rows[0][0])
		})
	}
	for _, tc := range []struct {
		value interface{}
		id    string
	}{
		{int64(math.MaxInt64), "9223372036854775807"},
		{^uint64(0), "18446744073709551615"},
	} {
		filter, err := readGDSNodeFilter(projection, []interface{}{tc.value}, nil)
		require.NoError(t, err)
		require.True(t, filter.nodes[tc.id])
		config, err := linkPredictionConfigFromArguments([]interface{}{map[string]interface{}{"sourceNode": tc.value}})
		require.NoError(t, err)
		require.Equal(t, storage.NodeID(tc.id), config.SourceNode)
		require.Equal(t, storage.NodeID(tc.id), gdsNodeByID(store, tc.value).(*storage.Node).ID)
		result, err := e.Execute(context.Background(), "RETURN gds.util.nodeProperty('gds-ids', $node, 'v') AS value", map[string]interface{}{"node": tc.value})
		require.NoError(t, err)
		require.Equal(t, float64(7), result.Rows[0][0])
	}
	// asNode's pre-existing extension accepts other values naming string IDs.
	require.Equal(t, storage.NodeID("true"), gdsNodeByID(store, true).(*storage.Node).ID)
	require.Equal(t, storage.NodeID("9.5"), gdsNodeByID(store, 9.5).(*storage.Node).ID)
}

// The common projection selector preserves link prediction's relationship
// degree (including parallel edges and one self-loop), undirected neighbours,
// isolated nodes and the extension where an explicit empty type list picks none.
func TestReviewRound2LinkProjectionSelection(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 4), []reviewEdge{
		{"0", "2", "A", nil}, {"0", "2", "A", nil},
		{"1", "2", "A", nil}, {"0", "0", "Z", nil},
	}, "'N'", "{A:{orientation:'UNDIRECTED'}, Z:{orientation:'UNDIRECTED'}}")
	for _, tc := range []struct {
		name             string
		types            []string
		degree0, degree2 int
	}{{"all", nil, 3, 3}, {"A", []string{"A"}, 2, 3}, {"none", []string{}, 0, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			graph, degree, _, err := e.linkPredictionGraph(context.Background(), &linkPredictionConfig{GraphName: "rev", SourceNode: "0", RelationshipTypes: tc.types})
			require.NoError(t, err)
			require.Len(t, graph, 4)
			require.Empty(t, graph["3"])
			require.Equal(t, tc.degree0, degree("0"))
			require.Equal(t, tc.degree2, degree("2"))
			require.NotContains(t, graph["0"], storage.NodeID("0"))
			if tc.degree2 > 0 {
				require.Contains(t, graph["0"], storage.NodeID("2"))
				require.Contains(t, graph["2"], storage.NodeID("0"))
			} else {
				require.Empty(t, graph["0"])
			}
		})
	}
	_, _, _, err := e.linkPredictionGraph(context.Background(), &linkPredictionConfig{GraphName: "rev", SourceNode: "0", RelationshipTypes: []string{"missing"}})
	require.ErrorContains(t, err, "missing")
	result, err := e.Execute(context.Background(), "CALL gds.linkPrediction.commonNeighbors.stream('rev', {sourceNode:'0'})", nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.Rows)
	require.Equal(t, []interface{}{"0", "1", float64(1)}, result.Rows[0])
}

// The shared selector's immutable-index path is equivalent to selecting every
// label explicitly. Degree statistics retain parallel edges and self-loops.
func TestReviewRound2ProjectionReaderEquivalence(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 4), []reviewEdge{
		{"0", "2", "A", map[string]interface{}{"w": 2.0}},
		{"0", "2", "A", map[string]interface{}{"w": 3.0}},
		{"1", "2", "A", map[string]interface{}{"w": 1.0}},
		{"0", "0", "Z", map[string]interface{}{"w": 4.0}},
	}, "'N'", "{A:{orientation:'UNDIRECTED', properties:'w'}, Z:{properties:'w'}}")
	projection, err := e.graphProjection("rev")
	require.NoError(t, err)
	cached, err := selectGDSSubgraph(projection, nil, nil, "w", gdsSubgraphOptions{includeTypes: true})
	require.NoError(t, err)
	selected, err := selectGDSSubgraph(projection, projection.Labels, projection.Types, "w", gdsSubgraphOptions{includeTypes: true})
	require.NoError(t, err)
	require.Equal(t, selected, cached)
	originalIDs := append([]string(nil), projection.NodeIDs...)
	nodeSimilarityNeighbors(cached)
	require.Equal(t, originalIDs, projection.NodeIDs)
	distribution := gdsDegreeDistribution(projection)
	require.Equal(t, int64(0), distribution["min"])
	require.Equal(t, int64(3), distribution["max"])
	require.Equal(t, 1.75, distribution["mean"])
	result, err := e.Execute(context.Background(), "CALL gds.graph.list('rev') YIELD degreeDistribution RETURN degreeDistribution", nil)
	require.NoError(t, err)
	require.Equal(t, distribution, result.Rows[0][0])
}
