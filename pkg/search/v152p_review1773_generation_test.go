package search

import (
 "context"
 "fmt"
 "log/slog"
 "runtime"
 "strings"
 "sync"
 "sync/atomic"
 "testing"
 "time"
 fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestReview1773LatePaymentCannotCompleteNextGeneration(t *testing.T) {
 servingTestEnv(t)
 t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION","none")
 svc := NewService(newNamespacedEngine(t), 3, nil)
 t.Cleanup(func(){ _ = svc.Close() })
 entered := []chan struct{}{make(chan struct{}), make(chan struct{})}
 release := []chan struct{}{make(chan struct{}), make(chan struct{})}
 var releaseOnce [2]sync.Once
 open := func(i int){ releaseOnce[i].Do(func(){close(release[i])}) }
 var calls atomic.Int32
 var builderID atomic.Uint64
 hnswCatchUpHook = func(ctx context.Context){
  i := int(calls.Add(1))-1
  if i >= 2 { return }
  if i==0 {builderID.Store(fixtureowner.CurrentGoroutineID())}
  close(entered[i])
  select { case <-release[i]: case <-ctx.Done(): }
 }
 put := func(id string, vec []float32) error {
  return svc.IndexNode(&storage.Node{ID:storage.NodeID(id),ChunkEmbeddings:[][]float32{vec}})
 }
 var beforeOnce sync.Once
 hnswBeforeInstallHook=func(ctx context.Context){
  beforeOnce.Do(func(){require.NoError(t,put("worker",[]float32{0,1,0}))})
 }
 t.Cleanup(func(){open(0);open(1);hnswCatchUpHook=nil;hnswBeforeInstallHook=nil})
 wait:=func(ch <-chan struct{},what string){t.Helper();select{case <-ch:case <-time.After(5*time.Second):t.Fatalf("timeout: %s",what)}}
 require.NoError(t,put("seed",[]float32{0,0,1}))
 _,first,err:=svc.startHNSWBuild(3)
 require.NoError(t,err)
 wait(entered[0],"first catch-up")
 svc.hnswCatchUp.mu.Lock()
 old:=svc.hnswCatchUp.target
 svc.hnswCatchUp.mu.Unlock()
 require.NotNil(t,old)
 workerInside,payerInside:=make(chan struct{}),make(chan struct{})
 workerRelease,payerRelease:=make(chan struct{}),make(chan struct{})
 var workerGate,payerGate,workerOpen,payerOpen sync.Once
 openWorker:=func(){workerOpen.Do(func(){close(workerRelease)})}
 openPayer:=func(){payerOpen.Do(func(){close(payerRelease)})}
 t.Cleanup(func(){openWorker();openPayer()})
 lookup:=svc.getVectorLookup()
 old.SetVectorLookup(func(id string)([]float32,bool){
  if fixtureowner.CurrentGoroutineID()==builderID.Load(){
   workerGate.Do(func(){close(workerInside);<-workerRelease})
  }else{
   payerGate.Do(func(){close(payerInside);<-payerRelease})
  }
  return lookup(id)
 })
 open(0)
 wait(workerInside,"builder inside graph insertion")
 oldDone:=make(chan error,1)
 go func(){oldDone<-put("x",[]float32{1,0,0})}()
 waitFlight:=func(){
  require.Eventually(t,func()bool{
   svc.hnswCatchUp.mu.Lock();defer svc.hnswCatchUp.mu.Unlock()
   _,ok:=svc.hnswCatchUp.inFlight[nodeMainVectorKey("x")]
   return ok
  },5*time.Second,time.Millisecond)
 }
 waitFlight()
 cleared:=make(chan error,1)
 go func(){cleared<-svc.ClearVectorIndex()}()
 wait(first.ctx.Done(),"clear cancels construction")
 openWorker()
 wait(payerInside,"old payment inside insertion")
 select{case err:=<-cleared:require.NoError(t,err);case <-time.After(5*time.Second):t.Fatal("clear joined an old payment")}
 require.NoError(t,put("seed",[]float32{0,0,1}))
 _,second,err:=svc.startHNSWBuild(3)
 require.NoError(t,err)
 wait(entered[1],"next catch-up")
 svc.hnswCatchUp.mu.Lock()
 next:=svc.hnswCatchUp.target
 svc.hnswCatchUp.mu.Unlock()
 require.NotNil(t,next)
 next.mu.Lock()
 nextLocked:=true
 defer func(){if nextLocked{next.mu.Unlock()}}()
 nextDone:=make(chan error,1)
 go func(){nextDone<-put("x",[]float32{0,1,0})}()
 waitFlight()
 openPayer()
 select{case err:=<-oldDone:require.NoError(t,err);case <-time.After(5*time.Second):t.Fatal("old payment did not return")}
 svc.hnswCatchUp.mu.Lock()
 flight:=len(svc.hnswCatchUp.inFlight)
 svc.hnswCatchUp.mu.Unlock()
 t.Logf("next insert parked before graph lock; tracked inserts after old completion=%d",flight)
 if flight!=1 {t.Errorf("old completion removed next generation's in-flight marker")}
 open(1)
 require.Eventually(t,func()bool{
  svc.hnswCatchUp.mu.Lock();defer svc.hnswCatchUp.mu.Unlock()
  return svc.hnswCatchUp.target==nil
 },5*time.Second,time.Millisecond)
 locked:=svc.indexMu.TryLock()
 if locked {svc.indexMu.Unlock()}
 t.Logf("new publication gate detached with new payment still parked; indexMu available=%t",locked)
 if !locked {t.Errorf("publication owns global indexMu while an unaccounted writer insertion is still parked")}
 next.mu.Unlock();nextLocked=false
 require.NoError(t,<-nextDone)
 wait(second.built.Done(),"next build completion")
}

// Same-key ordering after a late completion: execute the exact pop/apply/
// complete phases used by step in a fixed order, so the stale value is visible.
func TestReview1773LateCompletionAllowsStalePublishedVector(t *testing.T) {
 var c hnswCatchUp
 discarded:=NewHNSWIndex(2,DefaultHNSWConfig())
 c.begin(discarded,map[string][]float32{"x":{1,0}},nil)
 c.mu.Lock();oldID,_,ok:=c.popLocked();c.mu.Unlock()
 require.True(t,ok)
 c.end()
 graph:=NewHNSWIndex(2,DefaultHNSWConfig())
 c.begin(graph,map[string][]float32{"x":{1,0}},nil)
 c.mu.Lock();id,a,ok:=c.popLocked();c.mu.Unlock()
 require.True(t,ok)
 require.True(t,c.liveUpdate("x",[]float32{0,1}))
 // Completion of the discarded graph's x, from step(), after begin().
 c.mu.Lock();c.completeLocked(oldID,nil);c.mu.Unlock()
 // This should be impossible while the new graph's older x remains in flight.
 c.mu.Lock();newID,b,taken:=c.popLocked();c.mu.Unlock()
 require.True(t,taken,"late completion did not make the newer x eligible")
 require.NoError(t,applyCatchUpChange(graph,newID,b))
 c.mu.Lock();c.completeLocked(newID,nil);c.mu.Unlock()
 require.NoError(t,applyCatchUpChange(graph,id,a))
 c.mu.Lock();c.completeLocked(id,nil);c.mu.Unlock()
 require.True(t,c.quiesce())
 published,err:=c.finishLocked()
 require.NoError(t,err)
 require.True(t,published)
 got:=catchUpStoredVector(t,graph,"x")
 t.Logf("published x=%v, latest recorded x=[0 1]",got)
 require.Equal(t,[]float32{0,1},got,"published graph contains an older vector")
}


func TestReview1773TransitionDoesNotReplayRetiredServingGraph(t *testing.T) {
 servingTestEnv(t)
 t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N","1000")
 svc:=NewService(newNamespacedEngine(t),3,nil)
 t.Cleanup(func(){_ = svc.Close()})
 put:=func(i int,vec []float32){require.NoError(t,svc.IndexNode(&storage.Node{ID:storage.NodeID(fmt.Sprintf("v%d",i)),ChunkEmbeddings:[][]float32{vec}}))}
 for i:=0;i<12;i++{put(i,[]float32{1,float32(i),1})}
 old,err:=svc.getOrCreateHNSWIndex(context.Background(),3)
 require.NoError(t,err)
 require.NoError(t,svc.RemoveNode("v0"))
 _,err=svc.getOrCreateVectorPipeline(context.Background())
 require.NoError(t,err)
 require.Equal(t,strategyModeBruteCPU,svc.currentPipelineStrategy())
 svc.strategyTransitionMu.Lock()
 svc.strategyTransitionInProgress=true
 svc.strategyTransitionMu.Unlock()
 for i:=1;i<12;i++{put(i,[]float32{-1,float32(i),1})}
 old.mu.RLock();before:=len(old.nodeLevel);old.mu.RUnlock()
 staged,release:=make(chan struct{}),make(chan struct{})
 var once,openOnce sync.Once
 open:=func(){openOnce.Do(func(){close(release)})}
 hnswCatchUpHook=func(ctx context.Context){once.Do(func(){close(staged);select{case <-release:case <-ctx.Done():}})}
 t.Cleanup(func(){open();hnswCatchUpHook=nil})
 published:=make(chan *HNSWIndex,1)
 svc.SetLogger(slog.New(fixtureowner.RawLogHandler(func(_ context.Context,r slog.Record)error{
  if strings.Contains(r.Message,"HNSW index rebuilt:"){
   svc.hnswMu.RLock();g:=svc.servingHNSW();svc.hnswMu.RUnlock();published<-g
  }
  return nil
 })))
 maintenanceDone:=make(chan error,1)
 maintenanceID:=make(chan uint64,1)
 go func(){maintenanceID<-fixtureowner.CurrentGoroutineID();maintenanceDone<-svc.maybeRebuildHNSW(context.Background(),0,1,time.Nanosecond)}()
 maintID:=<-maintenanceID
 select{case <-staged:case <-time.After(5*time.Second):t.Fatal("maintenance catch-up did not start")}
 waitStack:=func(id uint64,contains string,excludes string){
  t.Helper()
  require.Eventually(t,func()bool{
   buf:=make([]byte,1<<20);n:=runtime.Stack(buf,true)
   for _,stack:=range strings.Split(string(buf[:n]),"\n\n"){
    if strings.HasPrefix(stack,fmt.Sprintf("goroutine %d ",id)){
     return strings.Contains(stack,contains)&&(excludes==""||!strings.Contains(stack,excludes))
    }
   }
   return false
  },5*time.Second,time.Millisecond)
 }
 svc.hnswMu.Lock();hLocked:=true
 defer func(){if hLocked{svc.hnswMu.Unlock()}}()
 transitionDone:=make(chan struct{})
 transitionID:=make(chan uint64,1)
 go func(){transitionID<-fixtureowner.CurrentGoroutineID();svc.runStrategyTransition(strategyModeHNSW);close(transitionDone)}()
 transID:=<-transitionID
 waitStack(transID,".startHNSWBuild(","")
 svc.pipelineMu.Lock();pLocked:=true
 defer func(){if pLocked{svc.pipelineMu.Unlock()}}()
 open()
 waitStack(maintID,".lockForHNSWPublication(","")
 waitStack(maintID,"sync.(*RWMutex).Lock(","")
 svc.hnswMu.Unlock();hLocked=false
 waitStack(transID,"sync.(*RWMutex).Lock(",".startHNSWBuild(")
 svc.pipelineMu.Unlock();pLocked=false
 require.NoError(t,<-maintenanceDone)
 <-transitionDone
 rebuilt:=<-published
 svc.hnswMu.RLock();final:=svc.servingHNSW();svc.hnswMu.RUnlock()
 old.mu.RLock();after:=len(old.nodeLevel);old.mu.RUnlock()
 t.Logf("maintenance replaced old=%p with new=%p; transition serves=%p; old graph slots before/after=%d/%d; discarded rebuilt size=%d",old,rebuilt,final,before,after,rebuilt.Size())
 require.Same(t,rebuilt,final,"transition replayed and reinstalled its old serving graph after maintenance published its replacement")
 require.Equal(t,before,after,"transition replay reinserted the entire logged write population under global locks")
}
