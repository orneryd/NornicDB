package cypher

import (
 "context"
 "crypto/sha256"
 "encoding/json"
 "fmt"
 "os"
 "os/exec"
 "reflect"
 "runtime"
 "runtime/pprof"
 "strings"
 "testing"
 "time"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/orneryd/nornicdb/pkg/search"
)

// The releases differ only in the storage context argument. Adapt that API
// without replacing the public executor, storage, transactions, or indexes.
func bmCall(t *testing.T,s any,name string,args ...any) []reflect.Value {
 t.Helper();m:=reflect.ValueOf(s).MethodByName(name);vs:=[]reflect.Value{}
 if m.Type().NumIn()==len(args)+1 {vs=append(vs,reflect.ValueOf(context.Background()))}
 for _,v:=range args {vs=append(vs,reflect.ValueOf(v))};out:=m.Call(vs)
 if len(out)>0 {v:=out[len(out)-1];if v.Type().Implements(reflect.TypeOf((*error)(nil)).Elem())&&!v.IsNil(){t.Fatalf("%s: %v",name,v.Interface())}}
 return out
}
func bmExec(t *testing.T,e *StorageExecutor,q string,p map[string]any)*ExecuteResult {t.Helper();r,err:=e.Execute(context.Background(),q,p);if err!=nil{t.Fatal(err)};return r}

func TestBM25RegSynthetic(t *testing.T) {
 b,err:=storage.NewBadgerEngine(t.TempDir());if err!=nil{t.Fatal(err)};defer b.Close()
 s:=storage.NewNamespacedEngine(b,"personal_documents")
 e:=NewStorageExecutor(s);auto:=os.Getenv("BR_AUTO")=="1";if !auto{e.cache=nil;e.queryCacheMaxEntries=0}
 // Preserve real source/vector shape: 1,771 sources holding 143,818
 // 2048-dimensional chunks. Disk Badger supports their large node values.
 nodes:=make([]*storage.Node,0,1000)
 flush:=func(){if len(nodes)>0{bmCall(t,s,"BulkCreateNodes",nodes);nodes=nil}}
 textBytes:=0
 for i:=0;i<172000;i++ {
  id:=fmt.Sprintf("n%06d",i);labels:=[]string{"PDGraphNode"};p:=map[string]any{"id":id,"current":true}
  switch {
  case i<1771:
   labels=[]string{"PDSearchSource"};p["document_id"]=fmt.Sprintf("n%06d",164951+i%1801);p["version_id"]=fmt.Sprintf("n%06d",166752+i%1801)
   words:="routine correspondence meeting attachment reference schedule services account document "
   if i%40==0 {words="contract invoice payment due research handoff "+words}
   p["text"]=strings.Repeat(words,600+i%1000);textBytes+=len(p["text"].(string))
  case i>=143000&&i<164951:
   labels=[]string{"PDIdentityCase"};p["status"]="pending";if i%17==0{p["status"]="failed"};if i>=164748{p["status"]="decided"}
   p["planned_at"]=fmt.Sprintf("2026-10-%02dT12:%02d:00Z",1+i%28,i%60);p["case_key"]=id;p["case_kind"]="resolve_mention";p["subject_ids"]=[]string{"synthetic"};p["member_of"]=[]string{"library"};p["generation"]=int64(1);p["attempt"]=int64(0);p["cause"]="synthetic rehearsal"
  case i>=164951&&i<166752: labels=[]string{"PDDocument","PDGraphNode"};p["current_version_id"]=fmt.Sprintf("n%06d",166752+i-164951)
  case i>=166752&&i<168553: labels=[]string{"PDVersion"};p["document_id"]=fmt.Sprintf("n%06d",164951+i-166752);p["current_source_id"]=fmt.Sprintf("n%06d",i-166752)
  case i>=168553&&i<168557: labels=[]string{"PDCollection"};p["archived"]=false
  case i>=168557&&i<171200: labels=[]string{"PDSummary","PDSearchable"};p["owner_kind"]="document";p["owner_id"]=fmt.Sprintf("n%06d",164951+i%1801);p["source_ids"]=[]string{fmt.Sprintf("n%06d",i%1801)}
  }
  n:=&storage.Node{ID:storage.NodeID(id),Labels:labels,Properties:p}
  // Deterministic stored chunk vectors: 81 each, plus one on 367 nodes.
  if i<1771 {count:=81;if i<367{count++};n.ChunkEmbeddings=make([][]float32,count);for k:=range n.ChunkEmbeddings{v:=make([]float32,2048);for j:=range v{v[j]=float32((i*31+k*13+j*17)%101+1)/101};n.ChunkEmbeddings[k]=v}}
  nodes=append(nodes,n);if len(nodes)==100{flush()}
 };flush()
 for i:=0;i<290000;i++ {
  a:=fmt.Sprintf("n%06d",164951+i%1801);z:=fmt.Sprintf("n%06d",i%143000);kind:="PD_RELATION"
  if i<1801{kind="MEMBER_OF";z=fmt.Sprintf("n%06d",168553+i%4)}else if i<3602{kind="HAS_VERSION";z=fmt.Sprintf("n%06d",166752+i%1801)}else if i<5373{kind="HAS_SOURCE";z=fmt.Sprintf("n%06d",i%1771)}
  bmCall(t,s,"CreateEdge",&storage.Edge{ID:storage.EdgeID(fmt.Sprintf("r%06d",i)),StartNode:storage.NodeID(a),EndNode:storage.NodeID(z),Type:kind,Properties:map[string]any{"id":fmt.Sprintf("r%06d",i)}})
 }
 t.Logf("BR fixture nodes=172000 edges=290000 sources=1771 cases=21951 pending=21748 vectors=143818 dims=2048 textBytes=%d",textBytes)
 for _,l:=range []string{"PDSearchSource","PDDocument","PDVersion","PDCollection","PDIdentityCase","PDGraphNode"}{bmExec(t,e,fmt.Sprintf("CREATE CONSTRAINT br_%s IF NOT EXISTS FOR (n:%s) REQUIRE n.id IS UNIQUE",l,l),nil)}
 for _,p:=range []string{"status","queue","held_by","wait_reason","claim"}{bmExec(t,e,fmt.Sprintf("CREATE INDEX br_case_%s FOR (n:PDIdentityCase) ON (n.%s)",p,p),nil)}
 bmExec(t,e,"CREATE FULLTEXT INDEX pd_source_text IF NOT EXISTS FOR (n:PDSearchSource) ON EACH [n.text]",nil)
 svc:=search.NewService(s,2048,nil);e.SetSearchService(svc);defer svc.Close()
 raw,err:=os.ReadFile("bm25reg_queries.json");if err!=nil{t.Fatal(err)};qs:=map[string]string{};if err=json.Unmarshal(raw,&qs);err!=nil{t.Fatal(err)}
 v:=make([]float64,2048);for j:=range v{v[j]=float64(float32((j*17)%101+1)/101)}
 for _,name:=range []string{"BM25_SEARCH","PENDING","VECTOR_SEARCH"} {
  if only:=os.Getenv("BR_ONLY");only!=""&&only!=name{continue}
  p:=map[string]any{"limit":int64(25),"query":"contract","query_input":v,"status":nil,"wait_reason":nil}
  q:=qs[name];want:=25;if name=="PENDING"{want=21748}
  if name=="VECTOR_SEARCH" {svc.SetLazyWarming(true,search.WarmFunc(func(){if err:=svc.BuildIndexes(context.Background());err!=nil{panic(err)}}))}
  var expected string
  run:=func(round int,logged bool) {
   if !auto{bmExec(t,e,"BEGIN",nil)};var a,z runtime.MemStats;runtime.ReadMemStats(&a);start:=time.Now();r:=bmExec(t,e,q,p);elapsed:=time.Since(start);runtime.ReadMemStats(&z);if !auto{bmExec(t,e,"ROLLBACK",nil)}
   if len(r.Rows)!=want{t.Fatalf("%s rows=%d want=%d",name,len(r.Rows),want)}
   if !logged{return}
   // Hash observable values while removing internal mutable node metadata.
   rows:=make([][]any,len(r.Rows));for i,row:=range r.Rows{rows[i]=append([]any(nil),row...);for j,x:=range rows[i]{if n,ok:=x.(*storage.Node);ok{rows[i][j]=[]any{n.ID,n.Labels,n.Properties}}}}
   buf,err:=json.Marshal(rows);if err!=nil{t.Fatal(err)};hash:=fmt.Sprintf("%x",sha256.Sum256(buf));if expected==""{expected=hash}else if expected!=hash{t.Fatalf("%s results changed",name)}
   if logged{t.Logf("BR name=%s round=%d ms=%.3f alloc=%d rows=%d hash=%s",name,round,float64(elapsed.Nanoseconds())/1e6,z.TotalAlloc-a.TotalAlloc,len(r.Rows),hash)}
  }
  for i:=0;i<10;i++{run(i,true)}
  if auto{h,m,n,_,_:=e.cache.Stats();t.Logf("BR cache name=%s hits=%d misses=%d entries=%d",name,h,m,n)}
  // Independent profile interval, excluded from latency samples.
  path:=t.TempDir()+"/cpu.pprof";f,err:=os.Create(path);if err!=nil{t.Fatal(err)};if err=pprof.StartCPUProfile(f);err!=nil{t.Fatal(err)};start:=time.Now();n:=0;for time.Since(start)<3*time.Second{run(-1,false);n++};pprof.StopCPUProfile();f.Close()
  t.Logf("BR profile name=%s iterations=%d elapsed=%s",name,n,time.Since(start))
  for _,flags:=range [][]string{{"-top","-nodecount=22"},{"-top","-cum","-nodecount=22"}}{args:=append([]string{"tool","pprof"},flags...);args=append(args,os.Args[0],path);out,err:=exec.Command("go",args...).CombinedOutput();t.Logf("BR pprof %s %v err=%v\n%s",name,flags,err,out)}
 }
}
