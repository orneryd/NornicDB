package storage

import (
 "bytes"
 "context"
 "fmt"
 "reflect"
 "runtime/pprof"
 "strings"
 "testing"
 "time"
)

func r12Call(t *testing.T, target any,method string,args ...any) []reflect.Value {
 t.Helper();fn:=reflect.ValueOf(target).MethodByName(method);if !fn.IsValid(){t.Fatalf("missing %T.%s",target,method)}
 values:=[]reflect.Value{};if fn.Type().NumIn()==len(args)+1{values=append(values,reflect.ValueOf(context.Background()))};for _,a:=range args{values=append(values,reflect.ValueOf(a))};out:=fn.Call(values)
 if len(out)>0{if err,ok:=out[len(out)-1].Interface().(error);ok&&err!=nil{t.Fatalf("%s: %v",method,err)}};return out
}
func r12StorageDump(name string){var b bytes.Buffer;pprof.Lookup("goroutine").WriteTo(&b,2);fmt.Printf("R12 STORAGE DUMP %s\n",name);for _,s:=range strings.Split(b.String(),"\n\n"){if strings.Contains(s,"r12StorageMeasure")||strings.Contains(s,"TestZZR12Storage")||strings.Contains(s,"AppendWithDatabaseReturningSeq"){fmt.Println(s)}}}
func r12StorageMeasure(name string,run func()) {done:=make(chan struct{});go func(){select{case <-time.After(200*time.Millisecond):r12StorageDump(name);case <-done:}}();at:=time.Now();run();close(done);fmt.Printf("R12 STORAGE RESULT %s seconds=%.6f\n",name,time.Since(at).Seconds())}

func TestZZR12Storage(t *testing.T){
 root:=t.TempDir(); b,err:=NewBadgerEngine(root);if err!=nil{t.Fatal(err)};defer b.Close()
 b.SetChangeCapture("probe",ChangeCaptureFull)
 for i:=0;i<2000;i++{n:=&Node{ID:NodeID(fmt.Sprintf("probe:n%06d",i)),Labels:[]string{"PDGraphNode"},Properties:map[string]any{"id":fmt.Sprint(i),"text":strings.Repeat("body ",200)}};r12Call(t,b,"CreateNode",n)}
 fmt.Println("R12 STORAGE seeded=2000 body=1000B cdc=full no-worker no-snapshot")
 for i:=0;i<3;i++{r12StorageMeasure("cdc-bounds",func(){first,last,e:=b.ChangeBounds("probe");if e!=nil{t.Fatal(e)};fmt.Printf("R12 bounds first=%d last=%d\n",first,last)})}
 r12StorageMeasure("cdc-page-1000",func(){events,e:=b.ChangeEvents("probe",0,0,1000);if e!=nil{t.Fatal(e)};fmt.Printf("R12 events=%d\n",len(events))})
 r12StorageMeasure("begin-warm",func(){r12Call(t,b,"BeginTransaction")[0].Interface().(*BadgerTransaction).Rollback()})
 r12StorageMeasure("label-scan",func(){r12Call(t,b,"GetNodesByLabel","PDGraphNode")})
 wal,e:=NewWAL(t.TempDir(),nil);if e!=nil{t.Fatal(e)};defer wal.Close();owner:=NewWALEngine(b,wal)
 wal.mu.Lock();go func(){time.Sleep(time.Second);wal.mu.Unlock()}()
 r12StorageMeasure("wal-mutex-held-append-begin",func(){_,e:=wal.AppendWithDatabaseReturningSeq(OpTxBegin,WALTxData{TxID:"probe-begin"},"probe");if e!=nil{t.Fatal(e)}})
 owner.mutationMu.Lock()
 r12StorageMeasure("mutation-exclusive-cdc-bounds",func(){_,_,e:=b.ChangeBounds("probe");if e!=nil{t.Fatal(e)}})
 r12StorageMeasure("mutation-exclusive-begin-snapshot",func(){r12Call(t,b,"BeginTransaction")[0].Interface().(*BadgerTransaction).Rollback()})
 go func(){time.Sleep(time.Second);owner.mutationMu.Unlock()}()
 r12StorageMeasure("mutation-exclusive-direct-create",func(){r12Call(t,owner,"CreateNode",&Node{ID:"probe:after",Labels:[]string{"Probe"},Properties:map[string]any{"id":"after"}})})
}
