package cypher

import (
 "fmt"
 "testing"
 "time"

 fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
 nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
 "github.com/orneryd/nornicdb/pkg/multidb"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/assert"
 "github.com/stretchr/testify/require"
)

func reviewR2Manager(t *testing.T) *multidb.DatabaseManager {
 t.Helper()
 engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
  InMemory: true, EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: 300*time.Millisecond}},
 }))
 require.NoError(t, err)
 base := fixtureowner.Own(t, fixtureowner.Track(t, &storage.MemoryEngine{BadgerEngine:engine}, storage.ErrStorageClosed))
 mgr, err := multidb.NewDatabaseManager(t.Context(), base, nil)
 require.NoError(t, err)
 t.Cleanup(func(){ require.NoError(t, mgr.Close()) })
 return mgr
}

func TestReviewR2CompositeIdleEnd(t *testing.T) {
 for _, reason := range []string{"timeout", "terminate"} { t.Run(reason, func(t *testing.T){
  mgr:=reviewR2Manager(t)
  require.NoError(t,mgr.CreateDatabase(t.Context(),multidb.DatabaseCreateRequest{Name:"r2leaf"}))
  require.NoError(t,mgr.CreateCompositeDatabase(t.Context(),"r2root",[]multidb.ConstituentRef{{Alias:"l",DatabaseName:"r2leaf",Type:"local",AccessMode:"read_write"}}))
  leaf,err:=mgr.GetStorage("r2leaf"); require.NoError(t,err)
  _,err=NewStorageExecutor(leaf).Execute(t.Context(),"CREATE CONSTRAINT r2_u FOR (n:R2Hold) REQUIRE n.k IS UNIQUE",nil); require.NoError(t,err)
  root,err:=mgr.GetStorage("r2root");require.NoError(t,err)
  holder:=NewStorageExecutor(root);holder.SetDatabaseManager(&testDatabaseManagerAdapter{manager:mgr})
  _,err=holder.Execute(t.Context(),"BEGIN",nil);require.NoError(t,err)
  t.Cleanup(func(){_,_=holder.Execute(t.Context(),"ROLLBACK",nil)})
  _,err=holder.Execute(t.Context(),"CALL { USE r2root.l MERGE (n:R2Hold {k: 1}) RETURN n.k AS k } RETURN k",nil);require.NoError(t,err)
  code:=nornicerrors.TransactionTimedOut
  if reason=="timeout" {time.Sleep(700*time.Millisecond)} else {
   code=transactionTerminatedCode
   _,err=NewStorageExecutor(leaf).Execute(t.Context(),fmt.Sprintf("TERMINATE TRANSACTIONS '%s'",holder.txContext.running.id()),nil);require.NoError(t,err)
  }
  _,err=NewStorageExecutor(leaf).Execute(t.Context(),"MERGE (:R2Hold {k:1})",nil)
  assert.NoError(t,err,"the idle composite holder must have released its participant's key")
  _,err=holder.Execute(t.Context(),"COMMIT",nil)
  got,_:=nornicerrors.Neo4jStatus(err)
  assert.Equal(t,code,got,"COMMIT after %s: %v",reason,err)
 }) }
}

func TestReviewR2AdministrationIdleOwner(t *testing.T) {
 mgr:=reviewR2Manager(t)
 store,err:=mgr.GetStorage("nornic");require.NoError(t,err)
 holder:=NewStorageExecutor(store);holder.SetDatabaseManager(&testDatabaseManagerAdapter{manager:mgr})
 _,err=holder.Execute(t.Context(),"BEGIN",nil);require.NoError(t,err)
 t.Cleanup(func(){_,_=holder.Execute(t.Context(),"ROLLBACK",nil)})
 _,err=holder.Execute(t.Context(),"SHOW DATABASES",nil);require.NoError(t,err)
 t.Logf("registration after administration routing: %v",holder.txContext.running)
 time.Sleep(700*time.Millisecond)
 _,err=holder.Execute(t.Context(),"COMMIT",nil)
 got,_:=nornicerrors.Neo4jStatus(err)
 require.Equal(t,nornicerrors.TransactionTimedOut,got,"administration transaction COMMIT past timeout: %v",err)
}

func TestReviewR2FailedHandleSurvivesStatements(t *testing.T) {
 for _,reason:=range []string{"timeout","terminate"} {t.Run(reason,func(t *testing.T){
  store:=idleEndStore(t,300*time.Millisecond)
  holder:=idleEndHolder(t,t.Context(),store,1)
  code:=nornicerrors.TransactionTimedOut
  if reason=="timeout" {time.Sleep(700*time.Millisecond)} else {
   code=transactionTerminatedCode
   _,err:=NewStorageExecutor(store).Execute(t.Context(),fmt.Sprintf("TERMINATE TRANSACTIONS '%s'",holder.txContext.running.id()),nil)
   require.NoError(t,err)
  }
  _,err:=holder.Execute(t.Context(),"RETURN 1",nil)
  got,_:=nornicerrors.Neo4jStatus(err);require.Equal(t,code,got)
  assert.True(t,holder.HasActiveTransaction(),"a failed statement must not detach the client's failed handle")
  _,err=holder.Execute(t.Context(),"CREATE (:U {k:99})",nil)
  got,_=nornicerrors.Neo4jStatus(err)
  assert.Equal(t,code,got,"a later write must remain refused: %v",err)
  assert.Equal(t,int64(0),countU(t,store,99),"the later write auto-committed")
  _,err=holder.Execute(t.Context(),"COMMIT",nil)
  got,_=nornicerrors.Neo4jStatus(err);assert.Equal(t,code,got,"COMMIT: %v",err)
 })}
}
