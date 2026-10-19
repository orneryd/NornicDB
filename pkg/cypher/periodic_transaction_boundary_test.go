package cypher

import (
 "context"
 "errors"
 "fmt"
 "testing"

 "github.com/stretchr/testify/require"
)

// Observe committed storage from inside the next public procedure action.
// Reading the caller's pending wrapper would conceal an outer-transaction bug.
func TestPeriodicProceduresPreserveBatchBoundaries(t *testing.T) {
 for _,name:=range []string{"apoc.periodic.iterate","apoc.periodic.rock_n_roll","apoc.periodic.commit"} {
  for _,composite:=range []bool{false,true} {
   for _,explicit:=range []bool{false,true} {
    for _,failure:=range []bool{false,true} {
     t.Run(fmt.Sprintf("%s/fabric=%v/explicit=%v/failure=%v",name,composite,explicit,failure),func(t *testing.T){
      exec,committed:=newProcedureAdmissionExecutor(t,composite)
      cleanupAdmissionProcedure(t,"u5batch.step")
      calls,visible:=0,-1
      require.NoError(t,RegisterUserProcedure(ProcedureSpec{Name:"u5batch.step",Mode:ProcedureModeWrite,Returns:[]ProcedureColumn{{Name:"v",Type:"INTEGER"}}},func(ctx context.Context,caller *StorageExecutor,_ string,_ []interface{})(*ExecuteResult,error){
       calls++
       if calls==1 { return caller.Execute(ctx,"CREATE (:U5BatchBoundary) RETURN 1 AS v",nil) }
       nodes,err:=committed.GetNodesByLabel("U5BatchBoundary"); if err!=nil { return nil,err }; visible=len(nodes)
       if failure { return nil,errors.New("u5 deliberate later batch failure") }
       return &ExecuteResult{Columns:[]string{"v"},Rows:[][]interface{}{{int64(0)}},Stats:&QueryStats{}},nil
      }))
      body:="CALL u5batch.step() YIELD v RETURN v LIMIT 1"
      call:="CALL "+name+"('UNWIND [1,2] AS input RETURN input',$body,{batchSize:1}) YIELD total RETURN total"
      if name=="apoc.periodic.commit" { call="CALL "+name+"($body,{}) YIELD executions RETURN executions" }
      if composite { call="CALL { USE admission_comp.one "+call+" } RETURN *" }
      if explicit { _,err:=exec.Execute(context.Background(),"BEGIN",nil); require.NoError(t,err) }
      result,err:=exec.Execute(context.Background(),call,map[string]interface{}{"body":body})
      require.Equal(t,1,visible,"completed batch must be committed before next action; result=%v error=%v",result,err)
      require.NoError(t,err)
      if explicit { _,err=exec.Execute(context.Background(),"ROLLBACK",nil); require.NoError(t,err) }
      nodes,err:=committed.GetNodesByLabel("U5BatchBoundary"); require.NoError(t,err); require.Len(t,nodes,1,"outer rollback must not undo completed periodic batches")
     })
    }
   }
  }
 }
}
