package cypher

import (
 "context"
 "strings"
 "sync/atomic"
 "testing"
 runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
 "github.com/orneryd/nornicdb/pkg/multidb"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

// The coordinator has v1 of a procedure; its remote constituent has v2,
// adding an optional options argument while keeping the same result schema.
func TestReview1640RemoteProcedureSignatureProbe(t *testing.T) {
 replaceProcedureRegistryForTest(t,NewProcedureRegistry())
 require.NoError(t,globalProcedureRegistry.RegisterUser(ProcedureSpec{
  Name:"custom.enrich", Mode:ProcedureModeRead, MinArgs:1, MaxArgs:1,
  Params:[]ProcedureParam{{Name:"name",Type:"STRING"}},
  Returns:[]ProcedureColumn{{Name:"value",Type:"INTEGER"}},
 },func(context.Context,*StorageExecutor,string,[]interface{})(*ExecuteResult,error){
  t.Error("the coordinator must not execute a remote procedure")
  return &ExecuteResult{Columns:[]string{"value"}},nil
 }))
 var calls atomic.Int64
 cfg := multidb.DefaultConfig()
 cfg.RemoteEngineFactory=func(ref multidb.ConstituentRef,_ string)(storage.Engine,error){
  return storage.NewStatementRemoteEngine(nil,func(_ context.Context,query string,_ map[string]interface{},_ storage.RemoteAccessMode)([]string,[][]interface{},error){
   if strings.Contains(query,"custom.enrich") {
    calls.Add(1)
    return []string{"value"},[][]interface{}{{int64(42)}},nil
   }
   t.Errorf("unexpected remote statement: %s",query)
   return nil,nil,nil
  }),nil
 }
 base:=runtimeFixtureOwner.Track(t,storage.NewMemoryEngine(),storage.ErrStorageClosed)
 mgr,err:=multidb.NewDatabaseManager(t.Context(),base,cfg); require.NoError(t,err)
 t.Cleanup(func(){require.NoError(t,mgr.Close())})
 require.NoError(t,mgr.CreateCompositeDatabase(t.Context(),"comp",[]multidb.ConstituentRef{
  {Alias:"remote",DatabaseName:"remote_one",Type:"remote",AccessMode:"read",URI:"http://remote.example"},
 }))
 adapter:=&testDatabaseManagerAdapter{manager:mgr}
 exec:=newCompositeTestExecutor(t,mgr,mgr.DefaultDatabaseName(),adapter)
 for _,args:=range []string{"'alice'","'alice', {}"} {
  t.Run(args,func(t *testing.T){
   result,err:=exec.Execute(t.Context(),"USE comp.remote CALL custom.enrich("+args+") YIELD value RETURN value",nil)
   require.NoError(t,err)
   require.Equal(t,[]string{"value"},result.Columns)
   require.Equal(t,[][]interface{}{{int64(42)}},result.Rows)
  })
 }
 require.Equal(t,int64(2),calls.Load(),"both signatures belong to the remote participant")
}
