package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/apoc/community"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGDSLouvainLibraryAllocationBound(t *testing.T) {
	for _, size := range []int{100, 1000, 3000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			nodes := make([]*community.Node, size)
			rels := make([]*community.Relationship, 0, size*4)
			for i := range nodes {
				nodes[i] = &community.Node{ID: int64(i)}
				for step := 1; step <= 4; step++ {
					rels = append(rels, &community.Relationship{ID: int64(len(rels)), StartNode: int64(i), EndNode: int64((i + step) % size)})
				}
			}
			config := community.DefaultLouvainConfig()
			config.MaxIterations = 1
			var result []community.CommunityResult
			allocations := testing.AllocsPerRun(3, func() { result = community.Louvain(nodes, rels, config) })
			require.Len(t, result, size)
			t.Logf("nodes=%d allocations=%f allocations/node=%f", size, allocations, allocations/float64(size))
			// Includes graph construction, hierarchy and output. A candidate
			// allocation per visited node/pass exceeds this linear budget.
			require.LessOrEqual(t, allocations, float64(3*size+200))
		})
	}
}

func TestGDSLouvainRound2TypeSumOrder(t *testing.T) {
	e := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{
		{"0", "2", "A", map[string]interface{}{"w": 1.0e16}},
		{"0", "2", "Z", map[string]interface{}{"w": -1.0e16}},
		{"0", "2", "a", map[string]interface{}{"w": 1.0}},
		{"1", "2", "R", map[string]interface{}{"w": 1.0}},
	}, "'N'", "{A:{properties:'w'},Z:{properties:'w'},a:{properties:'w'},R:{properties:'w'}}")
	input := []interface{}{
		map[string]interface{}{"s": int64(0), "t": int64(2), "k": "A", "w": 1.0e16},
		map[string]interface{}{"s": int64(0), "t": int64(2), "k": "Z", "w": -1.0e16},
		map[string]interface{}{"s": int64(0), "t": int64(2), "k": "a", "w": 1.0},
		map[string]interface{}{"s": int64(1), "t": int64(2), "k": "R", "w": 1.0},
	}
	_, err := e.Execute(t.Context(), "UNWIND $rows AS r RETURN gds.graph.project('r2sumagg',r.s,r.t,{relationshipType:r.k,relationshipProperties:{w:r.w}})", map[string]interface{}{"rows": input})
	require.NoError(t, err)
	for _, name := range []string{"rev", "r2sumagg"} {
		t.Run(name, func(t *testing.T) {
			p, err := e.graphProjection(name)
			require.NoError(t, err)
			result, err := e.Execute(t.Context(), "CALL gds.louvain.stream('"+name+"',{concurrency:1,relationshipWeightProperty:'w',consecutiveIds:true})", nil)
			require.NoError(t, err)
			t.Logf("graph=%s typeOrder=%v relationships=%+v rows=%+v", name, p.relationshipTypeOrder, p.Relationships, result.Rows)
			id := louvainArithmeticID(name)
			expected := [][]interface{}{{id(0), int64(0), nil}, {id(1), int64(1), nil}, {id(2), int64(1), nil}}
			if name != "rev" {
				// Aggregation rows come in first-occurrence order, as GDS gives them.
				expected[1], expected[2] = expected[2], expected[1]
			}
			require.Equal(t, expected, result.Rows)
		})
	}
}

func TestGDSLouvainRound2ParallelSums(t *testing.T) {
	e := reviewGraph(t, []map[string]interface{}{{"seed": int64(100)}, {"seed": int64(10)}, {"seed": int64(10)}, {"seed": int64(10)}}, []reviewEdge{
		{"0", "1", "R", map[string]interface{}{"w": 1.0e16}},
		{"0", "2", "R", map[string]interface{}{"w": 1.0}},
		{"0", "2", "R", map[string]interface{}{"w": 1.0}},
		{"0", "3", "R", map[string]interface{}{"w": -1.0e16}},
		{"1", "1", "R", map[string]interface{}{"w": 1.0}},
	}, "{N:{properties:'seed'}}", "{R:{properties:'w'}}")
	_, err := e.Execute(t.Context(), "UNWIND [{s:0,t:1,w:1.0e16},{s:0,t:2,w:1.0},{s:0,t:2,w:1.0},{s:0,t:3,w:-1.0e16},{s:1,t:1,w:1.0}] AS r RETURN gds.graph.project('r2collapseagg',r.s,r.t,{sourceNodeProperties:{seed:CASE WHEN r.s=0 THEN 100 ELSE 10 END},targetNodeProperties:{seed:10},relationshipType:'R',relationshipProperties:{w:r.w}})", nil)
	require.NoError(t, err)
	for _, name := range []string{"rev", "r2collapseagg"} {
		t.Run(name, func(t *testing.T) {
			result, err := e.Execute(t.Context(), "CALL gds.louvain.stream('"+name+"',{concurrency:1,seedProperty:'seed',relationshipWeightProperty:'w'})", nil)
			require.NoError(t, err)
			t.Logf("graph=%s rows=%+v", name, result.Rows)
			id := louvainArithmeticID(name)
			require.Equal(t, [][]interface{}{{id(0), int64(100), nil}, {id(1), int64(10), nil}, {id(2), int64(10), nil}, {id(3), int64(10), nil}}, result.Rows)
		})
	}
}

// louvainArithmeticID is a node's id in a row: the stored string id of the
// native graph "rev", or the INTEGER an aggregation row supplied.
func louvainArithmeticID(graph string) func(int64) interface{} {
	return func(id int64) interface{} {
		if graph == "rev" {
			return fmt.Sprint(id)
		}
		return id
	}
}
