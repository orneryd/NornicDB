package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeIdentityConsumerMatrix(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit { mode = "explicit" }
		t.Run(mode, func(t *testing.T) {
			exec, _ := newCompositeIdentityExecutor(t, explicit)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "USE comp CALL gds.graph.project('matrix', '*', '*', {nodeProperties:['v']})", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = exec.Execute(ctx, "USE comp CALL gds.graph.drop('matrix')", nil) })
			for _, query := range []string{
				compositeIdentityQueryPrefix+"RETURN gds.util.nodeProperty('matrix',n,'v'),gds.util.nodeProperty('matrix',m,'v')",
				"USE comp RETURN gds.util.nodeProperty('matrix','4:one:same','v'),gds.util.nodeProperty('matrix','4:two:same','v')",
			} {
				t.Run("projection properties/"+query, func(t *testing.T) {
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{float64(7),float64(7)}}, result.Rows)
				})
			}
			for _, proc := range []string{"pageRank", "betweenness", "closeness", "louvain", "labelPropagation", "wcc"} {
				t.Run("whole graph/"+proc, func(t *testing.T) {
					result, err := exec.Execute(ctx, "USE comp CALL apoc.algo."+proc+"() YIELD node RETURN count(DISTINCT node)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
				})
			}
			for _, tc := range []struct { proc string; want float64 }{
				{"adamicAdar",0}, {"commonNeighbors",0}, {"resourceAllocation",0}, {"preferentialAttachment",1}, {"totalNeighbors",2},
			} {
				t.Run("scalar graph/"+tc.proc, func(t *testing.T) {
					result, err := exec.Execute(ctx, compositeIdentityQueryPrefix+"RETURN gds.alpha.linkprediction."+tc.proc+"(n,m)", nil)
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
					require.InDelta(t, tc.want, result.Rows[0][0], 1e-12)
				})
			}
			for _, tc := range []struct { expression string; ids []interface{}; want int }{
				{"IN ['4:two:same']",nil,0}, {"IN $ids",[]interface{}{"4:two:same"},0},
				{"IN ['4:one:same']",nil,1}, {"IN $ids",[]interface{}{"4:one:same","4:two:same","4:one:same"},1},
				{"= '4:two:same'",nil,0}, {"= $id",nil,0}, {"IN ['same']",nil,1}, {"= 'same'",nil,1},
			} {
				t.Run("qualified seek/"+tc.expression, func(t *testing.T) {
					result, err := exec.Execute(ctx, "USE comp.one MATCH (n:P) WHERE elementId(n) "+tc.expression+" RETURN elementId(n)", map[string]interface{}{"ids":tc.ids,"id":"4:two:same"})
					require.NoError(t, err)
					require.Len(t, result.Rows, tc.want)
					if tc.want != 0 { require.Equal(t, "4:one:same", result.Rows[0][0]) }
				})
			}
		})
	}
}

func TestCompositeNativeGDSStreamConsumerMatrix(t *testing.T) {
	for _, explicit := range []bool{false,true} {
		mode := "autocommit"
		if explicit { mode = "explicit" }
		t.Run(mode, func(t *testing.T) {
			exec, manager := newCompositeIdentityExecutor(t, explicit)
			for _, database := range []string{"one","two"} {
				store, err := manager.GetStorage(database)
				require.NoError(t, err)
				_, err = store.CreateNode(&storage.Node{ID:"shared",Labels:[]string{"T"}})
				require.NoError(t, err)
				for _, start := range []storage.NodeID{"same","target"} {
					require.NoError(t, store.CreateEdge(&storage.Edge{ID:storage.EdgeID(string(start)+"-shared"),StartNode:start,EndNode:"shared",Type:"R"}))
				}
			}
			ctx := context.Background()
			_, err := exec.Execute(ctx,"USE comp CALL gds.graph.project('native_matrix','*','*')",nil)
			require.NoError(t,err)
			t.Cleanup(func(){ _,_ = exec.Execute(ctx,"USE comp CALL gds.graph.drop('native_matrix')",nil) })
			for _, proc := range []string{"adamicAdar","commonNeighbors","resourceAllocation","preferentialAttachment","jaccard","predict"} {
				for _, pair := range []struct{source,target,database string}{{"n","z","one"},{"m","y","two"}} {
					t.Run(proc+"/"+pair.database,func(t *testing.T){
						query := compositeIdentityQueryPrefix+"CALL gds.linkPrediction."+proc+".stream('native_matrix',{sourceNode:"+pair.source+",targetNodes:["+pair.target+"]}) YIELD node1,node2 RETURN node1,node2"
						result,err := exec.Execute(ctx,query,nil)
						require.NoError(t,err)
						require.Equal(t,[][]interface{}{{"4:"+pair.database+":same","4:"+pair.database+":target"}},result.Rows)
					})
				}
			}
		})
	}
}
