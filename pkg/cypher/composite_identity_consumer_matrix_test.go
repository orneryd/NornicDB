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
		if explicit {
			mode = "explicit"
		}
		t.Run(mode, func(t *testing.T) {
			exec, _ := newCompositeIdentityExecutor(t, explicit)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "USE comp CALL gds.graph.project('matrix', '*', '*', {nodeProperties:['v']})", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = exec.Execute(ctx, "USE comp CALL gds.graph.drop('matrix')", nil) })
			for _, query := range []string{
				compositeIdentityQueryPrefix + "RETURN gds.util.nodeProperty('matrix',n,'v'),gds.util.nodeProperty('matrix',m,'v')",
				"USE comp RETURN gds.util.nodeProperty('matrix','4:one:same','v'),gds.util.nodeProperty('matrix','4:two:same','v')",
			} {
				t.Run("projection properties/"+query, func(t *testing.T) {
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{float64(7), float64(7)}}, result.Rows)
				})
			}
			for _, proc := range []string{"pageRank", "betweenness", "closeness", "louvain", "labelPropagation", "wcc"} {
				t.Run("whole graph/"+proc, func(t *testing.T) {
					result, err := exec.Execute(ctx, "USE comp CALL apoc.algo."+proc+"() YIELD node RETURN count(DISTINCT node)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
				})
				t.Run("selected native nodes/"+proc, func(t *testing.T) {
					result, err := exec.Execute(ctx, compositeIdentityQueryPrefix+"CALL apoc.algo."+proc+"([n,m,n]) YIELD node RETURN count(DISTINCT node)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
				})
				t.Run("filtered native nodes/"+proc,func(t *testing.T) {
					result,err := exec.Execute(ctx,compositeIdentityQueryPrefix+"CALL apoc.algo."+proc+"([n,z,m,y],['R'],{}) YIELD node RETURN count(DISTINCT node)",nil)
					require.NoError(t,err)
					require.Equal(t,[][]interface{}{{int64(4)}},result.Rows)
				})
			}
			t.Run("community topology", func(t *testing.T) {
				result, err := exec.Execute(ctx, "USE comp CALL apoc.algo.wcc() YIELD node,componentId RETURN count(DISTINCT componentId)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
			})
			for _,tc := range []struct{filter string; components int64}{{"['R']",2},{"[]",4}} {
				result,err := exec.Execute(ctx,compositeIdentityQueryPrefix+"CALL apoc.algo.wcc([n,z,m,y],"+tc.filter+") YIELD node,componentId RETURN count(DISTINCT componentId)",nil)
				require.NoError(t,err)
				require.Equal(t,[][]interface{}{{tc.components}},result.Rows)
			}
			for _, tc := range []struct {
				proc string
				want float64
			}{
				{"adamicAdar", 0}, {"commonNeighbors", 0}, {"resourceAllocation", 0}, {"preferentialAttachment", 1}, {"totalNeighbors", 2},
			} {
				t.Run("scalar graph/"+tc.proc, func(t *testing.T) {
					result, err := exec.Execute(ctx, compositeIdentityQueryPrefix+"RETURN gds.alpha.linkprediction."+tc.proc+"(n,m)", nil)
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
					require.InDelta(t, tc.want, result.Rows[0][0], 1e-12)
				})
			}
			for _, tc := range []struct {
				expression string
				ids        []interface{}
				want       int
			}{
				{"IN ['4:two:same']", nil, 0}, {"IN $ids", []interface{}{"4:two:same"}, 0},
				{"IN ['4:one:same']", nil, 1}, {"IN $ids", []interface{}{"4:one:same", "4:two:same", "4:one:same"}, 1},
				{"= '4:two:same'", nil, 0}, {"= $id", nil, 0}, {"IN ['same']", nil, 1}, {"= 'same'", nil, 1},
			} {
				t.Run("qualified seek/"+tc.expression, func(t *testing.T) {
					result, err := exec.Execute(ctx, "USE comp.one MATCH (n:P) WHERE elementId(n) "+tc.expression+" RETURN elementId(n)", map[string]interface{}{"ids": tc.ids, "id": "4:two:same"})
					require.NoError(t, err)
					require.Len(t, result.Rows, tc.want)
					if tc.want != 0 {
						require.Equal(t, "4:one:same", result.Rows[0][0])
					}
				})
			}
			for _, expression := range []string{
				"elementId(n) IN ['same']", "id(n) IN ['same']", "elementId(n) = 'same'",
				"elementId(n) IN ['4:one:same','4:two:same','4:one:same']",
				"elementId(n) IN ['4:one:same','same']",
			} {
				t.Run("composite seek/"+expression, func(t *testing.T) {
					result, err := exec.Execute(ctx, "USE comp MATCH (n:P) WHERE "+expression+" RETURN count(DISTINCT n)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
				})
			}
		})
	}
}

func TestCompositeNativeGDSStreamConsumerMatrix(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit"
		}
		t.Run(mode, func(t *testing.T) {
			exec, manager := newCompositeIdentityExecutor(t, explicit)
			for _, database := range []string{"one", "two"} {
				store, err := manager.GetStorage(database)
				require.NoError(t, err)
				// Predictions select non-neighbours. Keep this fixture as a
				// three-node chain, with two hops from P to Q through T.
				require.NoError(t, store.DeleteEdge("same-rel"))
				_, err = store.CreateNode(&storage.Node{ID: "shared", Labels: []string{"T"}})
				require.NoError(t, err)
				for _, start := range []storage.NodeID{"same", "target"} {
					require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(string(start) + "-shared"), StartNode: start, EndNode: "shared", Type: "R"}))
				}
			}
			ctx := context.Background()
			_, err := exec.Execute(ctx, "USE comp CALL gds.graph.project('native_matrix','*','*')", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = exec.Execute(ctx, "USE comp CALL gds.graph.drop('native_matrix')", nil) })
			const prefix = "USE comp CALL { USE comp.one MATCH (n:P) MATCH (z:Q) RETURN n,z } CALL { USE comp.two MATCH (m:P) MATCH (y:Q) RETURN m,y } "
			for _, graph := range []string{"projection", "stored"} {
				for _, shape := range []string{"native", "qualified"} {
					for _, proc := range []string{"adamicAdar", "commonNeighbors", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
						for _, pair := range []struct{ source, target, database string }{{"n", "z", "one"}, {"m", "y", "two"}} {
							t.Run(graph+"/"+shape+"/"+proc+"/"+pair.database, func(t *testing.T) {
								source, target := pair.source, pair.target
								if shape == "qualified" {
									source, target = "elementId("+source+")", "elementId("+target+")"
								}
								argument := ""
								if graph == "projection" {
									argument = "'native_matrix',"
								}
								query := prefix + "CALL gds.linkPrediction." + proc + ".stream(" + argument + "{sourceNode:" + source + ",targetNodes:[" + target + "]}) YIELD node1,node2 RETURN node1,node2"
								result, err := exec.Execute(ctx, query, nil)
								require.NoError(t, err)
								require.Equal(t, [][]interface{}{{"4:" + pair.database + ":same", "4:" + pair.database + ":target"}}, result.Rows)
							})
						}
					}
				}
			}
		})
	}
}

func TestCompositeGraphTraversalConsumerMatrix(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit"
		}
		t.Run(mode, func(t *testing.T) {
			exec, _ := newCompositeIdentityExecutor(t, explicit)
			ctx := context.Background()
			for _, procedure := range []string{"dijkstra", "aStar", "allSimplePaths"} {
				arguments := "n,m,'R','w'"
				if procedure == "allSimplePaths" {
					arguments = "n,m,'R',3"
				}
				result, err := exec.Execute(ctx, compositeIdentityQueryPrefix+"CALL apoc.algo."+procedure+"("+arguments+") YIELD path RETURN count(*)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "different sources with the same local ID are not a zero-hop path")
			}
			for _, procedure := range []string{"expandConfig", "spanningTree"} {
				for _, start := range []string{"[n,m]", "[elementId(n),elementId(m)]"} {
					query := compositeIdentityQueryPrefix + "CALL apoc.path." + procedure + "(" + start + ",{relationshipFilter:'R>',minLevel:1,maxLevel:1}) YIELD path RETURN count(DISTINCT path)"
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
				}
			}
			for _, tc := range []struct {
				query string
				want  int64
			}{
				{"CALL apoc.path.expand([n,m],'R>','',1,1) YIELD path RETURN count(DISTINCT path)", 2},
				{"CALL apoc.path.subgraphNodes([n,m],{relationshipFilter:'R>',maxLevel:1}) YIELD node RETURN count(DISTINCT node)", 4},
				{"CALL apoc.path.subgraphAll([n,m],{relationshipFilter:'R>',maxLevel:1}) YIELD nodes UNWIND nodes AS node RETURN count(DISTINCT node)", 4},
				{"CALL apoc.path.subgraphAll([n,m],{relationshipFilter:'R>',maxLevel:1}) YIELD relationships UNWIND relationships AS edge RETURN count(DISTINCT edge)", 2},
			} {
				result, err := exec.Execute(ctx, compositeIdentityQueryPrefix+tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
			}
			for _, query := range []string{
				"UNWIND [n,m] AS root CALL apoc.neighbors.tohop(root,'R>',1) YIELD node RETURN elementId(root),elementId(node) ORDER BY elementId(root)",
				"UNWIND [n,m] AS root CALL apoc.neighbors.byhop(root,'R>',1) YIELD nodes UNWIND nodes AS node RETURN elementId(root),elementId(node) ORDER BY elementId(root)",
			} {
				result, err := exec.Execute(ctx, compositeIdentityQueryPrefix+query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"4:one:same", "4:one:target"}, {"4:two:same", "4:two:target"}}, result.Rows)
			}
		})
	}
}

func TestCompositeEntityLookupConsumers(t *testing.T) {
	for _,explicit := range []bool{false,true} {
		t.Run(map[bool]string{false:"autocommit",true:"explicit"}[explicit],func(t *testing.T) {
			ctx := context.Background()
			for _,procedure := range []string{"kp.resolve","nornicdb.knowledgepolicy.resolve"} {
				exec,_ := newCompositeIdentityExecutor(t,explicit)
				_,err := exec.Execute(ctx,"USE comp.one CALL "+procedure+"('4:one:same')",nil)
				require.NoError(t,err)
				_,err = exec.Execute(ctx,"USE comp.one CALL "+procedure+"('4:two:same')",nil)
				require.Error(t,err,"a foreign entity cannot resolve against this database")
			}
			// Use a fresh executor after the expected error, since an explicit
			// transaction is correctly poisoned by its first failed statement.
			exec,manager := newCompositeIdentityExecutor(t,explicit)
			for _,id := range []string{"4:two:same","4:one:same"} {
				result,err := exec.Execute(ctx,"USE comp.one MATCH (n:P) WHERE elementId(n) = $id CREATE (n)-[:MATRIX_LINK]->(:Created) RETURN elementId(n)",map[string]interface{}{"id":id})
				require.NoError(t,err)
				if id == "4:two:same" { require.Empty(t,result.Rows) } else { require.Equal(t,[][]interface{}{{"4:one:same"}},result.Rows) }
			}
			if explicit { _,err := exec.Execute(ctx,"COMMIT",nil); require.NoError(t,err) }
			store,err := manager.GetStorage("one")
			require.NoError(t,err)
			nodes,err := store.GetNodesByLabel("Created")
			require.NoError(t,err)
			require.Len(t,nodes,1)
		})
	}
}
