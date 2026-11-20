package server

// Research only (R12 effective-link EVALUATE): the exact statement with the
// documents-shaped parameter of the effective_documents phase, at growing
// numbers of found links, to measure whether per-call time grows linearly.
// Synthetic graph: one document owning P points and P targets; edges
// document->point and point->target, so 2P links are found.
import (
 "encoding/base64"
 "fmt"
 "os"
 "os/exec"
 "path/filepath"
 "runtime/pprof"
 "strconv"
 "strings"
 "testing"
 "time"
)

func r12ScaleRun(l *slowbLab, name, q string, p map[string]any, dumps []time.Duration) (map[string]any, float64) {
 done:=make(chan struct{})
 go func(){start:=time.Now();for _,d:=range dumps{select{case <-time.After(d-time.Since(start)):slowbDump(fmt.Sprintf("R12 %s at %s",name,d));case <-done:return}}}()
 out,code,d:=l.post("/db/nornic/tx/commit",map[string]any{"statements":[]any{map[string]any{"statement":q,"parameters":p}}});close(done)
 raw:=fmt.Sprint(out);if len(raw)>300{raw=raw[:300]}
 fmt.Printf("R12 RESULT name=%s seconds=%.6f code=%d out=%s\n",name,d.Seconds(),code,raw)
 if errs,_:=out["errors"].([]any);len(errs)>0{fmt.Printf("R12 ERROR name=%s errors=%v\n",name,errs)}
 return out,d.Seconds()
}

func TestZZR12EvalScale(t *testing.T){
 t.Setenv("NORNICDB_HTTP_TX_TIMEOUT","50m")
 l:=r12Lab(t)
 raw,err:=os.ReadFile("testdata/r12-effective.cypher");if err!=nil{t.Fatal(err)};query:=string(raw)
 at:=strings.Index(query,"  CALL (effl_found_links) {");if at<0{t.Fatal("selection boundary missing")}
 selection:=query[:at]+"RETURN size(effl_found_links) AS found\n}\nRETURN found"
 spec:=os.Getenv("R12_SCALE");if spec==""{spec="1000,4000,16000,32000"}
 stop:=1500.0;if v,e:=strconv.ParseFloat(os.Getenv("R12_SCALE_STOP"),64);e==nil{stop=v}
 for _,label:=range []string{"PDGraphNode","PDDocument","PDPoint","PDContext","PDVersion","PDCorrection","PDOcrReport","PDDefinition"}{l.cypher("CREATE INDEX es_"+label+" FOR (n:"+label+") ON (n.id)",nil)}
 l.cypher(`CREATE INDEX es_document FOR (n:PDGraphNode) ON (n.document_id)`,nil)
 root:="doc-root"
 l.cypher(`CREATE (d:PDGraphNode:PDDocument {id:$id,current_version_id:'v1',document_id:$id}) CREATE (v:PDVersion {id:'v1',document_id:$id})`,map[string]any{"id":root})
 l.cypher(`MATCH (a:PDGraphNode {id:$root}) CREATE (c:PDCorrection {id:'p11-exclude',operation:'exclude_link',targets:[$root,'t0000000'],scope:['all'],status:'active',link_kind:'mentions'}) CREATE (c)-[:CORRECTS]->(a)`,map[string]any{"root":root})
 params:=map[string]any{"links":[]any{},"node_ids":[]any{},"documents":[]any{root},"correction_ids":[]any{},"selected":[]any{},"gone_ids":[]any{}}
 have:=0;var lastProfile string;var lastP int
 for _,s:=range strings.Split(spec,","){
  p,_:=strconv.Atoi(strings.TrimSpace(s))
  seedAt:=time.Now()
  for b:=have;b<p;b+=500{rows:=[]any{};for i:=b;i<b+500&&i<p;i++{rows=append(rows,map[string]any{"p":fmt.Sprintf("p%07d",i),"t":fmt.Sprintf("t%07d",i)})}
   l.cypher(`UNWIND $rows AS r CREATE (:PDGraphNode:PDPoint {id:r.p,document_id:'doc-root',updated_at:1,revision:1,origin:'automatic',search_text:'point body'}) CREATE (:PDGraphNode:PDContext {id:r.t,document_id:'doc-root',updated_at:1,revision:1,origin:'automatic',search_text:'target body'})`,map[string]any{"rows":rows})
   l.cypher(`UNWIND $rows AS r MATCH (d:PDGraphNode {id:'doc-root'}) MATCH (a:PDGraphNode {id:r.p}) MATCH (b:PDGraphNode {id:r.t}) CREATE (d)-[:PD_RELATION {id:'rp-'+r.p,kind:'mentions',origin:'automatic',document_id:'doc-root',updated_at:1,revision:1,eff_usable:true}]->(a) CREATE (a)-[:PD_RELATION {id:'pt-'+r.p,kind:'mentions',origin:'automatic',document_id:'doc-root',updated_at:1,revision:1,eff_usable:true}]->(b)`,map[string]any{"rows":rows})}
  have=p
  fmt.Printf("R12 SCALE STAGE points=%d targets=%d links=%d seed_seconds=%.3f\n",p,p,2*p,time.Since(seedAt).Seconds())
  r12ScaleRun(l,fmt.Sprintf("scale-selection-p%d",p),selection,params,nil)
  // invalidate any cached result: change an unused property of the root first
  l.cypher(`MATCH (d:PDDocument {id:'doc-root'}) SET d.r12_touch=$v`,map[string]any{"v":time.Now().UnixNano()})
  file:=filepath.Join(t.TempDir(),fmt.Sprintf("scale-p%d.pprof",p));f,e:=os.Create(file);if e!=nil{t.Fatal(e)};if e=pprof.StartCPUProfile(f);e!=nil{t.Fatal(e)}
  _,secs:=r12ScaleRun(l,fmt.Sprintf("scale-full-p%d-links%d",p,2*p),query,params,[]time.Duration{10*time.Second,60*time.Second,300*time.Second})
  pprof.StopCPUProfile();f.Close();lastProfile=file;lastP=p
  fmt.Printf("R12 SCALE PERLINK p=%d links=%d seconds=%.3f ms_per_link=%.4f\n",p,2*p,secs,secs*1000/float64(2*p))
  if secs>stop{fmt.Printf("R12 SCALE STOP after p=%d seconds=%.1f > %.0f\n",p,secs,stop);break}
 }
 if lastProfile!=""{
  for _,mode:=range [][]string{{"-top","-nodecount=45"},{"-top","-cum","-nodecount=70"},{"-peek","keywordIndexFrom|callBodyFromSyntax|queryBodyFromSyntax|GetEdge$|pipelineApplyOptionalMatch$|validatePipelinePercentileExpressions"}}{
   args:=append([]string{"tool","pprof"},mode...);args=append(args,lastProfile);out,e:=exec.Command("go",args...).CombinedOutput();fmt.Printf("R12 SCALE PPROF p=%d mode=%v error=%v\n%s\n",lastP,mode,e,out)}
  b,_:=os.ReadFile(lastProfile);fmt.Printf("R12 SCALE PROFILE p=%d base64=%s\n",lastP,base64.StdEncoding.EncodeToString(b))
 }
 fmt.Printf("R12 SCALE END wall=%s\n",time.Now().UTC().Format(time.RFC3339Nano))
}
