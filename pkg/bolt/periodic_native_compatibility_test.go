package bolt

import (
 "context"
 "fmt"
 "testing"
 "time"

 "github.com/neo4j/neo4j-go-driver/v5/neo4j"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestPeriodicNativeDriverCompatibility(t *testing.T) {
 for _,procedure:=range []struct{name,query string}{
  {"iterate","CALL apoc.periodic.iterate('UNWIND [1,2,3] AS x RETURN x','CREATE (:U5PeriodicCompatMarker)',{batchSize:1,parallel:false})"},
  {"commit","CALL apoc.periodic.commit('MATCH (n:U5PeriodicCompatSeed) WITH n LIMIT 1 DELETE n RETURN count(n)',{})"},
 } {
  for _,finish:=range []string{"autocommit","COMMIT","ROLLBACK","executeWrite"} {
   t.Run(procedure.name+"/"+finish,func(t *testing.T){
    ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second); defer cancel()
    base:=storage.NewMemoryEngine(); t.Cleanup(func(){require.NoError(t,base.Close())})
    store:=storage.NewNamespacedEngine(base,"test")
    manager:=&mockDBManager{stores:map[string]storage.Engine{"test":store},defaultDB:"test"}
    server:=NewWithDatabaseManager(&Config{Port:0,MaxConnections:8,ReadBufferSize:8192,WriteBufferSize:8192},&mockExecutor{},manager)
    port:=startBoltTestServer(t,server)
    driver,err:=neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d",port),neo4j.NoAuth()); require.NoError(t,err); defer driver.Close(ctx)
    session:=driver.NewSession(ctx,neo4j.SessionConfig{DatabaseName:"test",AccessMode:neo4j.AccessModeWrite}); defer session.Close(ctx)
    seed,err:=session.Run(ctx,"UNWIND [1,2,3] AS x CREATE (:U5PeriodicCompatSeed)",nil); require.NoError(t,err); _,err=seed.Consume(ctx); require.NoError(t,err)
    count:=func()string{markers,e:=store.GetNodesByLabel("U5PeriodicCompatMarker"); require.NoError(t,e); seeds,e:=store.GetNodesByLabel("U5PeriodicCompatSeed"); require.NoError(t,e); return fmt.Sprintf("markers=%d seeds=%d",len(markers),len(seeds))}
    run:=func(tx neo4j.ManagedTransaction)(interface{},error){r,e:=tx.Run(ctx,procedure.query,nil); if e==nil {_,e=r.Consume(ctx)}; require.NoError(t,e); require.Equal(t,"markers=0 seeds=3",count(),"native explicit periodic calls retain shipped folding"); return nil,e}
    if finish=="executeWrite" {_,err=session.ExecuteWrite(ctx,run)} else if finish=="autocommit" {var r neo4j.ResultWithContext; r,err=session.Run(ctx,procedure.query,nil); if err==nil {_,err=r.Consume(ctx)}; t.Logf("PROBE native-driver %s %s accepted=%v error=%v",procedure.name,finish,err==nil,err)} else {
     tx,e:=session.BeginTransaction(ctx); require.NoError(t,e)
     r,callErr:=tx.Run(ctx,procedure.query,nil); if callErr==nil {_,callErr=r.Consume(ctx)}
     require.NoError(t,callErr); require.Equal(t,"markers=0 seeds=3",count(),"native explicit periodic calls retain shipped folding")
     if finish=="COMMIT" {err=tx.Commit(ctx)} else {err=tx.Rollback(ctx)}
    }
    require.NoError(t,err); want:="markers=0 seeds=3"; if finish!="ROLLBACK" {if procedure.name=="iterate" {want="markers=3 seeds=3"} else {want="markers=0 seeds=0"}}; require.Equal(t,want,count())
   })
  }
 }
}