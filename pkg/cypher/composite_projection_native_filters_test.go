package cypher

import (
 "context"
 "fmt"
 "testing"

 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestCompositeProjectionNativeSimilarityFilters(t *testing.T) {
 for _, composite := range []bool{false,true} {
  for _, explicit := range []bool{false,true} {
   t.Run(fmt.Sprintf("composite=%v/explicit=%v",composite,explicit),func(t *testing.T) {
    base, manager := newCompositeIdentityExecutor(t,false)
    one,err := manager.GetStorage("one"); require.NoError(t,err)
    _,err = one.CreateNode(&storage.Node{ID:"peer",Labels:[]string{"B"}}); require.NoError(t,err)
    require.NoError(t,one.CreateEdge(&storage.Edge{ID:"peer-rel",StartNode:"peer",EndNode:"target",Type:"R"}))
    database := "one"; if composite { database = "comp" }
    ctx := context.Background()
    _,err = base.Execute(ctx,"USE "+database+" CALL gds.graph.project('native_filters','*','*')",nil); require.NoError(t,err)
    t.Cleanup(func(){ _,_ = base.Execute(ctx,"USE "+database+" CALL gds.graph.drop('native_filters')",nil) })
    store,err := manager.GetStorage(database); require.NoError(t,err)
    exec := NewStorageExecutor(store)
    exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager:manager})
    if explicit {
     _,err = exec.Execute(ctx,"BEGIN",nil); require.NoError(t,err)
     t.Cleanup(func(){ _,_ = exec.Execute(ctx,"ROLLBACK",nil) })
    }
    prefix := "USE one MATCH (n:P) MATCH (k:B) "
    source,target := "same","peer"
    if composite {
     prefix = "USE comp CALL { USE comp.one MATCH (n:P) MATCH (k:B) RETURN n,k } "
     source,target = "4:one:same","4:one:peer"
    }
    for _, filter := range []string{"{sourceNodeFilter:n,targetNodeFilter:k}","{sourceNodeFilter:[n],targetNodeFilter:[k]}","{sourceNodeFilter:elementId(n),targetNodeFilter:elementId(k)}"} {
     result,err := exec.Execute(ctx,prefix+"CALL gds.nodeSimilarity.filtered.stream('native_filters',"+filter+") YIELD node1,node2,similarity RETURN node1,node2,similarity",nil)
     require.NoError(t,err,filter)
     require.Equal(t,[][]interface{}{{source,target,float64(1)}},result.Rows,filter)
    }
   })
  }
 }
}
func TestCompositeAggregationProjectionIdentityOwner(t *testing.T) {
 for _, explicit := range []bool{false,true} {
  t.Run(fmt.Sprint(explicit),func(t *testing.T) {
   exec,_ := newCompositeIdentityExecutor(t,explicit)
   ctx := context.Background()
   result,err := exec.Execute(ctx,compositeIdentityQueryPrefix+"UNWIND [n,m,n] AS x RETURN gds.graph.project('aggregate_native',x,null,{sourceNodeProperties:{v:x.v},targetNodeProperties:{v:7}}) AS graph",nil)
   require.NoError(t,err)
   require.EqualValues(t,2,result.Rows[0][0].(map[string]interface{})["nodeCount"])
   t.Cleanup(func(){ _,_ = exec.Execute(ctx,"USE comp CALL gds.graph.drop('aggregate_native')",nil) })
   result,err = exec.Execute(ctx,"USE comp CALL gds.fastRP.stream('aggregate_native',{embeddingDimension:4}) YIELD nodeId RETURN count(DISTINCT nodeId),count(DISTINCT elementId(gds.util.asNode(nodeId)))",nil)
   require.NoError(t,err)
   require.Equal(t,[][]interface{}{{int64(2),int64(2)}},result.Rows)
   result,err = exec.Execute(ctx,compositeIdentityQueryPrefix+"RETURN gds.util.nodeProperty('aggregate_native',n,'v'),gds.util.nodeProperty('aggregate_native',m,'v')",nil)
   require.NoError(t,err)
   require.Equal(t,[][]interface{}{{float64(7),float64(7)}},result.Rows)
  })
 }
}
