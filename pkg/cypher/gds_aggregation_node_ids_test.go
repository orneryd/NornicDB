package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGDSAggregationNodeOrderAndIDs: an aggregation projection keeps its rows'
// first-occurrence node order, which is GDS's node mapping, and returns the
// INTEGER ids its rows supplied. Expected rows are GDS 2.13.12's
// (review/gds-develop-r4-ordered-ids, -id-order and -integer-output captures).
func TestGDSAggregationNodeOrderAndIDs(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "aggregationnodeids")
	rows := func(query string) [][]interface{} {
		t.Helper()
		result, err := run(query)
		require.NoError(t, err, query)
		return result.Rows
	}

	// Community IDs follow the import order, not the ids' text order.
	for _, tc := range []struct {
		project  string
		expected [][]interface{}
	}{
		{"UNWIND [1,2,10] AS n WITH n ORDER BY n RETURN gds.graph.project('ordered',n,null,{},{readConcurrency:1})",
			[][]interface{}{{int64(1), int64(0)}, {int64(2), int64(1)}, {int64(10), int64(2)}}},
		{"UNWIND [2,10,1] AS n RETURN gds.graph.project('ordered',n,null)",
			[][]interface{}{{int64(2), int64(0)}, {int64(10), int64(1)}, {int64(1), int64(2)}}},
	} {
		rows(tc.project)
		require.Equal(t, tc.expected, rows("CALL gds.louvain.stream('ordered',{concurrency:1}) YIELD nodeId,communityId RETURN nodeId,communityId"), tc.project)
		rows("CALL gds.graph.drop('ordered')")
	}

	// An equal-score tie goes to the earlier node in import order: 2, not 10.
	rows("UNWIND [0,2,10] AS s WITH s ORDER BY s RETURN gds.graph.project('tie',s,100,{},{readConcurrency:1})")
	require.Equal(t, [][]interface{}{{int64(0), int64(2), 1.0}},
		rows("CALL gds.nodeSimilarity.filtered.stream('tie',{concurrency:1,sourceNodeFilter:[0],topK:1}) YIELD node1,node2,similarity RETURN node1,node2,similarity"))
	require.Equal(t, [][]interface{}{{int64(0), int64(2), 1.0}, {int64(2), int64(0), 1.0}, {int64(10), int64(0), 1.0}},
		rows("CALL gds.nodeSimilarity.stream('tie',{concurrency:1,topK:1}) YIELD node1,node2,similarity RETURN node1,node2,similarity"))

	// INTEGER ids stay INTEGERs, so arithmetic on them is arithmetic, in a
	// direct call and after a leading clause alike.
	rows("RETURN gds.graph.project('integer',1,2)")
	for _, prefix := range []string{"", "WITH 1 AS x "} {
		require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}},
			rows(prefix+"CALL gds.louvain.stream('integer',{concurrency:1}) YIELD nodeId RETURN nodeId,nodeId + 1 AS nextId"))
	}
	rows("UNWIND [0,1] AS s RETURN gds.graph.project('integerSimilarity',s,2)")
	require.Equal(t, [][]interface{}{{int64(0), int64(1), int64(1)}, {int64(1), int64(0), int64(2)}},
		rows("CALL gds.nodeSimilarity.stream('integerSimilarity',{concurrency:1}) YIELD node1,node2 RETURN node1,node2,node1 + 1 AS nextId"))
	shared := rows("CALL gds.nodeSimilarity.stream('integerSimilarity',{concurrency:1}) YIELD shared RETURN shared")
	require.Equal(t, int64(2), shared[0][0].([]interface{})[0].(map[string]interface{})["nodeId"])
}

// TestGDSAggregationNegativeNodeID: GDS refuses a negative INTEGER node id;
// zero is a valid id. Nothing is registered for the refused graph.
func TestGDSAggregationNegativeNodeID(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "aggregationnegative")
	_, err := run("RETURN gds.graph.project('negative',-1,null)")
	require.ErrorContains(t, err, "GDS expects node ids to be positive. But got a negative id of `-1`.")
	_, err = run("UNWIND [0,-5] AS t RETURN gds.graph.project('negative',0,t)")
	require.ErrorContains(t, err, "But got a negative id of `-5`.")
	result, err := run("RETURN gds.graph.exists('negative')")
	require.NoError(t, err)
	require.Equal(t, false, result.Rows[0][0])
	result, err = run("RETURN gds.graph.project('zero',0,null).nodeCount")
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Rows[0][0])
}
