package server

// Research only (Q-r8j): is there an avoidable far-end node load when a
// variable-length-free OPTIONAL MATCH filters on relationship properties only?
// Neutral graph, plain Cypher:
//   CREATE INDEX hub_key FOR (n:Hub) ON (n.key)
//   20 (:Hub {key}); hubs h00-h09 have 7,188 (h)-[:REL {kind:'other', id}]->(:Item {id})
//   relationships, hubs h10-h19 have 400; every hub also one (h)-[:REL {kind:'same'}]->(:Item).
// Forms A-F as proposed in Q-r8j, plus G (every relationship, no far node, no
// filter) and H (A's pattern and filter, returning count(r)).
import (
 "fmt"
 "os"
 "os/exec"
 "path/filepath"
 "runtime/pprof"
 "strings"
 "testing"
 "time"
)

func TestZZR12Endpoint(t *testing.T){
 t.Setenv("NORNICDB_HTTP_TX_TIMEOUT","50m")
 l:=r12Lab(t);at:=time.Now()
 l.cypher(`CREATE INDEX hub_key FOR (n:Hub) ON (n.key)`,nil)
 for h:=0;h<20;h++{k:=fmt.Sprintf("h%02d",h);deg:=400;if h<10{deg=7188}
  l.cypher(`CREATE (:Hub {key:$k})`,map[string]any{"k":k})
  for b:=0;b<deg;b+=1000{e:=b+1000;if e>deg{e=deg};l.cypher(`MATCH (h:Hub {key:$k}) UNWIND range($from,$to-1) AS i CREATE (h)-[:REL {kind:'other', id:$k+'-'+toString(i)}]->(:Item {id:$k+'-'+toString(i)})`,map[string]any{"k":k,"from":b,"to":e})}
  l.cypher(`MATCH (h:Hub {key:$k}) CREATE (h)-[:REL {kind:'same', id:$k+'-same'}]->(:Item {id:$k+'-same'})`,map[string]any{"k":k})}
 fmt.Printf("R12 EP SEED hubs=20 seconds=%.1f\n",time.Since(at).Seconds())
 pre:="UNWIND $keys AS k MATCH (h:Hub {key: k}) "
 forms:=[]struct{name,q string}{
  {"A-coalesce-label-return-o",pre+"OPTIONAL MATCH (h)-[r:REL]-(o:Item) WHERE coalesce(r.kind, 'same') = 'same' RETURN count(o)"},
  {"B-plain-where",pre+"OPTIONAL MATCH (h)-[r:REL]-(o:Item) WHERE r.kind = 'same' RETURN count(o)"},
  {"C-inline-property",pre+"OPTIONAL MATCH (h)-[r:REL {kind: 'same'}]-(o:Item) RETURN count(o)"},
  {"D-no-label",pre+"OPTIONAL MATCH (h)-[r:REL]-(o) WHERE coalesce(r.kind, 'same') = 'same' RETURN count(o)"},
  {"E-no-far-node",pre+"OPTIONAL MATCH (h)-[r:REL]-() WHERE coalesce(r.kind, 'same') = 'same' RETURN count(r)"},
  {"F-every-far-node",pre+"MATCH (h)-[r:REL]-(o:Item) RETURN count(o)"},
  {"G-every-rel-no-far-node",pre+"MATCH (h)-[r:REL]-() RETURN count(r)"},
  {"H-label-filter-return-r",pre+"OPTIONAL MATCH (h)-[r:REL]-(o:Item) WHERE coalesce(r.kind, 'same') = 'same' RETURN count(r)"},
 }
 keys:=func(from,n int)[]any{o:=[]any{};for i:=from;i<from+n;i++{o=append(o,fmt.Sprintf("h%02d",i))};return o}
 sets:=[]struct{name string;k []any}{{"deg400-1",keys(10,1)},{"deg400-5",keys(10,5)},{"deg400-10",keys(10,10)},{"deg7188-1",keys(0,1)},{"deg7188-5",keys(0,5)},{"deg7188-10",keys(0,10)}}
 fns:=[]string{"loadTraversalEndpointNode","relationshipEndpointReader","getQueryNode","GetQueryNode","getNodeByID","findPaths","undirectedIncidentEdges","ReadAdjacentEdges","readAdjacentEdges","readIndexedEdgeInTxn","traversalEdges","readAdjacency","evaluateRowExpressionWithContext","pipelineApplyOptionalMatch","mallocgc","gcBgMarkWorker"}
 for _,f:=range forms{
  for _,s:=range sets{for rep:=0;rep<2;rep++{
   l.cypher(`MATCH (h:Hub {key:'h19'}) SET h.touch=$v`,map[string]any{"v":time.Now().UnixNano()})
   var pf *os.File;var file string
   if s.name=="deg7188-10"&&rep==1{file=filepath.Join(t.TempDir(),f.name+".pprof");pf,_=os.Create(file);pprof.StartCPUProfile(pf)}
   out,secs:=r12ScaleRun(l,fmt.Sprintf("ep-%s-%s-rep%d",f.name,s.name,rep),f.q,map[string]any{"keys":s.k},nil)
   if pf!=nil{pprof.StopCPUProfile();pf.Close()
    o,_:=exec.Command("go","tool","pprof","-top","-cum","-nodecount=400",file).CombinedOutput();lines:=strings.Split(string(o),"\n")
    for _,ln:=range lines{if strings.HasPrefix(strings.TrimSpace(ln),"Duration:"){fmt.Printf("R12 EP PROFILE form=%s %s\n",f.name,strings.TrimSpace(ln))}}
    for _,fn:=range fns{for _,ln:=range lines{fs:=strings.Fields(ln);if len(fs)>=6{nm:=fs[len(fs)-1];if strings.HasSuffix(nm,"."+fn)||strings.Contains(nm,"."+fn+"["){fmt.Printf("R12 EP SHARE form=%s fn=%s cum=%s cumpct=%s full=%s\n",f.name,fn,fs[3],fs[4],nm)}}}}}
   raw:=fmt.Sprint(out);if i:=strings.Index(raw,"row:[");i>=0&&len(raw)>i+20{raw=raw[i:i+20]}
   fmt.Printf("R12 EP ROW form=%s set=%s rep=%d seconds=%.4f result=%s\n",f.name,s.name,rep,secs,raw)}}
  pq:="PROFILE "+f.q
  out,_,_:=l.post("/db/nornic/tx/commit",map[string]any{"statements":[]any{map[string]any{"statement":pq,"parameters":map[string]any{"keys":keys(10,1)}}}});raw:=fmt.Sprint(out);if i:=strings.Index(raw,"plan:");i>=0{raw=raw[i:]};if len(raw)>1500{raw=raw[:1500]}
  fmt.Printf("R12 EP PLAN form=%s deg400-1 %s\n",f.name,raw)
 }
 fmt.Printf("R12 EP END\n")
}
