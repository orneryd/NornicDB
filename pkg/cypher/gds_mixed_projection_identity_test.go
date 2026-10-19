package cypher

import (
 "context"
 "fmt"
 "strings"
 "testing"

 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestGDSMixedProjectionIdentityAcrossStreams(t *testing.T) {
 gdsFollowUpCatalog(t)
 exec,run:=newGDSEngineGraph(t,"mixed_projection_ids")
 _,err:=exec.storage.CreateNode(&storage.Node{ID:"1",Labels:[]string{"MixedIdentity"}}); require.NoError(t,err)
 for _,explicit:=range []bool{false,true} {
  for _,input:=range []struct{name,rows string}{
   {"native_first_source","[[n,3],[1,3],[2,3]]"},
   {"integer_first_source","[[1,3],[n,3],[2,3]]"},
   {"native_first_target","[[3,n],[3,1],[3,2]]"},
   {"integer_first_target","[[3,1],[3,n],[3,2]]"},
   {"integer_only","[[1,3],[2,3]]"},
  } {
   t.Run(fmt.Sprintf("%s/explicit=%v",input.name,explicit),func(t *testing.T){
    must:=func(query string)*ExecuteResult{t.Helper(); result,err:=run(query); require.NoError(t,err,query); return result}
    if explicit {must("BEGIN"); defer exec.Execute(context.Background(),"ROLLBACK",nil)}
    graph:="mixed_"+input.name
    must(fmt.Sprintf("MATCH (n:MixedIdentity) UNWIND %s AS e RETURN gds.graph.project('%s',e[0],e[1],{}, {undirectedRelationshipTypes:['*']})",input.rows,graph))
    defer exec.Execute(context.Background(),"CALL gds.graph.drop('"+graph+"')",nil)
    calls:=map[string]string{
     "gds.fastRP.stream":"{embeddingDimension:4}",
     "gds.louvain.stream":"{}",
     "gds.nodeSimilarity.stream":"{}",
     "gds.nodeSimilarity.filtered.stream":"{sourceNodeFilter:[1]}",
    }
    for _,name:=range []string{"adamicAdar","commonNeighbors","resourceAllocation","preferentialAttachment","jaccard","predict"} {calls["gds.linkPrediction."+name+".stream"]="{sourceNode:1,targetNodes:[2]}"}
    ensureBuiltInProceduresRegistered()
    for _,spec:=range ListRegisteredProcedures() {
     if strings.HasPrefix(spec.Name,"gds.")&&strings.HasSuffix(spec.Name,".stream") {require.Contains(t,calls,spec.Name,"every registered stream must share the projection output contract")}
    }
    for name,options:=range calls {
     t.Run(name,func(t *testing.T){
      result:=must("CALL "+name+"('"+graph+"',"+options+")")
      require.NotEmpty(t,result.Rows)
      for _,row:=range result.Rows {
       for index,column:=range result.Columns {
        switch column {
        case "nodeId","node1","node2": require.IsType(t,int64(0),row[index],"%s %s=%v",name,column,row[index])
        case "shared":
         if entries,ok:=row[index].([]interface{});ok {for _,entry:=range entries {require.IsType(t,int64(0),entry.(map[string]interface{})["nodeId"])}}
        }
       }
      }
     })
    }
   })
  }
 }
}