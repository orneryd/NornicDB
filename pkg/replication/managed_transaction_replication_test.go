package replication

import (
 "context"
 "testing"
 "time"
 "github.com/orneryd/nornicdb/pkg/cypher"
 "github.com/orneryd/nornicdb/pkg/multidb"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)
type xhighRejectWrites struct { Replicator; reject bool; calls int }
func (r *xhighRejectWrites) Apply(cmd *Command, timeout time.Duration)error{
 r.calls++;if r.reject{return ErrNotLeader};return r.Replicator.Apply(cmd,timeout)
}
func TestXHighCypherTransactionsPreserveReplication(t *testing.T){
 for _,route:=range []string{"implicit","explicit"}{t.Run(route,func(t *testing.T){
  base:=storage.NewMemoryEngine();defer base.Close()
  adapter,err:=NewStorageAdapterWithWAL(base,t.TempDir());require.NoError(t,err);defer adapter.Close()
  local:=NewStandaloneReplicator(DefaultConfig(),adapter);require.NoError(t,local.Start(context.Background()));defer local.Shutdown()
  router:=&xhighRejectWrites{Replicator:local}
  manager,err:=multidb.NewDatabaseManager(NewReplicatedEngine(base,router,time.Second),nil);require.NoError(t,err)
  require.NoError(t,manager.CreateDatabase("cypher_repl"));view,err:=manager.GetStorage("cypher_repl");require.NoError(t,err)
  executor:=cypher.NewStorageExecutor(view);ctx:=context.Background()
  router.reject=true;router.calls=0
  var commitErr error
  if route=="implicit"{
   _,commitErr=executor.Execute(ctx,"UNWIND [1,2] AS k CREATE (:XHighReplica {k:k}) RETURN count(*) AS n",nil)
  }else{
   _,err=executor.Execute(ctx,"BEGIN",nil);require.NoError(t,err)
   _,err=executor.Execute(ctx,"CREATE (:XHighReplica {k:1})",nil);require.NoError(t,err)
   _,commitErr=executor.Execute(ctx,"COMMIT",nil)
  }
  count,err:=view.NodeCount();require.NoError(t,err)
  t.Logf("route=%s Apply_calls=%d commit=%v stored_nodes=%d",route,router.calls,commitErr,count)
  require.ErrorIs(t,commitErr,ErrNotLeader,"public transaction commit must reach replication admission")
  require.Zero(t,count,"a replica refusing writes cannot commit locally")
 })}
}
