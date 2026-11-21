package storage

import (
 "context"
 "errors"
 "path/filepath"
 "strings"
 "testing"
 "time"

 "github.com/stretchr/testify/require"
)

func ei4r7Engine(t *testing.T, dir string, async bool) *BadgerEngine {
 return newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir:dir, wal:true, synchronous:!async, manualDrain:true, records:WALRecordsLarge})
}

func ei4r7Commit(t *testing.T, b *BadgerEngine, mode EntityCommitMode, name string, stage func(*BadgerTransaction) error) error {
 t.Helper()
 ctx:=context.Background()
 tx,err:=BeginEntityTransaction(ctx,b,mode); if err!=nil{return err}
 defer tx.Rollback()
 if err=tx.SetNamespace("test");err!=nil{return err}
 if err=stage(tx);err!=nil{return err}
 p,err:=tx.PrepareEntityCommit(ctx,EntityCommitRequest{LogicalID:name});if err!=nil{return err}
 r,err:=NewWALEngine(b,b.bufferedCommitWAL).SubmitEntityCommit(ctx,p,nil);if err!=nil{p.Abort();return err}
 out:=r.WaitOutcome(ctx)
 t.Logf("%s state=%v error=%v",name,out.State,out.Err)
 return out.Err
}

func TestEI4R7SnapshotAdmissionExpansion(t *testing.T) {
 for _,kind:=range []string{"long-edge-endpoints","narrow-node-list","narrow-edge-list"}{
  t.Run(kind,func(t *testing.T){
   dir:=t.TempDir();b:=ei4r7Engine(t,dir,false);ctx:=context.Background()
   id1,id2:=NodeID("test:a"),NodeID("test:b")
   if kind=="long-edge-endpoints"{id1=NodeID("test:"+strings.Repeat("a",20000));id2=NodeID("test:"+strings.Repeat("b",20000))}
   for _,id:=range []NodeID{id1,id2}{_,err:=b.CreateNode(ctx,&Node{ID:id});require.NoError(t,err)}
   var value any=strings.Repeat("x",(16<<20)-(16<<10))
   if kind!="long-edge-endpoints"{value=make([]int32,2_000_000)}
   err:=ei4r7Commit(t,b,CommitModeExplicit,"expand",func(tx *BadgerTransaction)error{
    if kind=="narrow-node-list"{_,err:=tx.CreateNode(ctx,&Node{ID:"test:large",Properties:map[string]any{"data":value}});return err}
    return tx.CreateEdge(ctx,&Edge{ID:"test:edge",StartNode:id1,EndNode:id2,Type:"R",Properties:map[string]any{"data":value}})
   })
   t.Logf("commit error=%v",err)
   require.NoError(t,err)
   if kind=="narrow-node-list"{n,e:=b.GetNode(ctx,"test:large");require.NoError(t,e);t.Logf("stored value type=%T",n.Properties["data"])}
   err=SaveStreamingSnapshot(ctx,b,filepath.Join(dir,"snapshot.nds"),SnapshotOptions{})
   t.Logf("snapshot error=%v",err)
   require.NoError(t,err,"an acknowledged entity must fit the snapshot")
  })
 }
}


func ei4r7NativeCause(err error)error{for{var accepted *AcceptedCommitError;if !errors.As(err,&accepted)||accepted.Err==nil{return err};err=accepted.Err}}

func TestEI4R7NormalizedBodyCrashAdmission(t *testing.T){
 dir:=t.TempDir();b:=ei4r7Engine(t,dir,false);ctx:=context.Background()
 tx,err:=BeginEntityTransaction(ctx,b,CommitModeExplicit);require.NoError(t,err);require.NoError(t,tx.SetNamespace("test"))
 _,err=tx.CreateNode(ctx,&Node{ID:"test:normalized",Properties:map[string]any{"data":make([]int32,2_000_000)}});require.NoError(t,err)
 p,err:=tx.PrepareEntityCommit(ctx,EntityCommitRequest{LogicalID:"normalized-crash"});require.NoError(t,err)
 payload,err:=EncodeEntityTransaction(EntityTransactionRequest{Prepared:p});require.NoError(t,err);p.Abort()
 t.Logf("accepted prepared payload=%d bytes",len(payload))
 require.NoError(t,b.bufferedCommitWAL.AppendWithDatabase(OpEntityCommit,walEntityCommitData{Payload:payload},"test"))
 crashClose(t,b,b.bufferedCommitWAL)
 b=ei4r7Engine(t,dir,false);n,err:=RecoverBufferedCommitsAtOpen(b,b.bufferedCommitWAL)
 t.Logf("recovery count=%d error=%v native cause=%v",n,err,ei4r7NativeCause(err))
 require.NoError(t,err,"a journaled admitted image must be recoverable")
}
