package storage

// Research only (R12 CDC lines, maintainer-reproducible): a store in use.
// Steady single-node commits with change capture FULL and the engine's own
// retention job (started by SetChangeCapture, checks every minute).
// Phase A: default retention (48 h). Phase B: retention shortened to
// R12_RET (time compression standing in for 48 h of operation); nothing
// else changed. Every minute: events written, change bounds, the time of
// one bounds read, deleted change-event versions still in the LSM, LSM shape.
import (
 "context"
 "fmt"
 "os"
 "strings"
 "sync/atomic"
 "testing"
 "time"
 "github.com/dgraph-io/badger/v4"
)

func r12EnvDur(name string, def time.Duration) time.Duration {if d,e:=time.ParseDuration(os.Getenv(name));e==nil&&d>0{return d};return def}

func TestZZR12CDCInUse(t *testing.T){
 phaseA:=r12EnvDur("R12_PHASE_A",8*time.Minute);phaseB:=r12EnvDur("R12_PHASE_B",36*time.Minute);idle:=r12EnvDur("R12_IDLE",3*time.Minute);ret:=r12EnvDur("R12_RET",3*time.Minute)
 rate:=500
 var b *BadgerEngine;var err error
 if os.Getenv("R12_INUSE_HP")=="1"{b,err=NewBadgerEngineWithOptions(BadgerOptions{DataDir:t.TempDir(),HighPerformance:true})}else{b,err=NewBadgerEngine(t.TempDir())}
 if err!=nil{t.Fatal(err)};defer b.Close()
 fmt.Printf("R12 INUSE options high_performance=%v (the product passes HighPerformance unless storage mode is low)\n",os.Getenv("R12_INUSE_HP")=="1")
 const ns="inuse"
 b.SetChangeCapture(ns,ChangeCaptureFull)
 var written atomic.Int64;stop:=make(chan struct{});done:=make(chan struct{})
 go func(){defer close(done);tick:=time.NewTicker(time.Second/time.Duration(rate/10));defer tick.Stop();i:=0
  for{select{case <-stop:return;case <-tick.C:}
   for k:=0;k<10;k++{n:=&Node{ID:NodeID(fmt.Sprintf("%s:n%08d",ns,i)),Labels:[]string{"Item"},Properties:map[string]any{"id":fmt.Sprint(i),"body":strings.Repeat("x",200)}};if _,e:=b.CreateNode(context.Background(),n);e!=nil{t.Errorf("CreateNode: %v",e);return};i++;written.Add(1)}}}()
 start:=time.Now()
 sample:=func(phase string){
  at:=time.Now();first,last,e:=b.ChangeBounds(ns);if e!=nil{t.Fatal(e)};boundsSec:=time.Since(at).Seconds()
  var deleted,live int64;prefix:=changeEventPrefix(ns);cat:=time.Now()
  _=b.db.View(func(txn *badger.Txn)error{opts:=badgerIteratorOptions();opts.Prefix=prefix;opts.AllVersions=true;it:=txn.NewIterator(opts);defer it.Close();for it.Seek(prefix);it.ValidForPrefix(prefix);it.Next(){if it.Item().IsDeletedOrExpired(){deleted++}else{live++}};return nil})
  lv:=[]string{};for _,l:=range b.db.DB.Levels(){if l.NumTables>0{lv=append(lv,fmt.Sprintf("L%d:%dt/%dMB",l.Level,l.NumTables,l.Size>>20))}}
  fmt.Printf("R12 INUSE phase=%s minute=%.1f written=%d first=%d last=%d bounds_seconds=%.6f deleted_versions=%d live_versions=%d count_seconds=%.2f levels=%s\n",phase,time.Since(start).Minutes(),written.Load(),first,last,boundsSec,deleted,live,time.Since(cat).Seconds(),strings.Join(lv," "))
 }
 run:=func(phase string,d time.Duration){end:=time.Now().Add(d);for time.Now().Before(end){time.Sleep(time.Minute);sample(phase)}}
 fmt.Printf("R12 INUSE config rate=%d/s body=200B retentionA=default(48h) retentionB=%s phaseA=%s phaseB=%s idle=%s\n",rate,ret,phaseA,phaseB,idle)
 run("A-default-48h",phaseA)
 b.SetChangeRetention(ret)
 run("B-retention-"+ret.String(),phaseB)
 close(stop);<-done
 run("idle-no-writes",idle)
 fmt.Printf("R12 INUSE END written=%d wall_minutes=%.1f\n",written.Load(),time.Since(start).Minutes())
}
