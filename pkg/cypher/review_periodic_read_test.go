package cypher

import (
 "context"
 "fmt"
 "testing"

 "github.com/stretchr/testify/require"
)

func TestReviewR73PeriodicReadAfterBatches(t *testing.T) {
 for _,explicit:=range []bool{false,true} {for _,dynamic:=range []bool{false,true} {
  t.Run(fmt.Sprintf("explicit=%v/dynamic=%v",explicit,dynamic),func(t *testing.T){
   e,store:=newProcedureAdmissionExecutor(t,false)
   ctx:=context.Background()
   if explicit {_,err:=e.Execute(ctx,"BEGIN",nil);require.NoError(t,err)}
   query:="CALL apoc.periodic.iterate('UNWIND [1,2,3] AS x RETURN x','CREATE (:ReviewR73PeriodicRead)',{batchSize:1}) YIELD total WITH total MATCH (n:ReviewR73PeriodicRead) RETURN count(n) AS found"
   params:=map[string]interface{}{}
   if dynamic {params["body"]=query;query="CALL apoc.cypher.run($body,{}) YIELD value RETURN value.found AS found"}
   result,err:=e.Execute(ctx,query,params);require.NoError(t,err)
   t.Logf("row after periodic batches=%v",result.Rows)
   if explicit {_,err=e.Execute(ctx,"COMMIT",nil);require.NoError(t,err)}
   nodes,err:=store.GetNodesByLabel("ReviewR73PeriodicRead");require.NoError(t,err);require.Len(t,nodes,3)
   require.Equal(t,[][]interface{}{{int64(3)}},result.Rows,"later clauses must see the completed batches")
  })
 }}
}
