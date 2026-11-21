package cypher

import (
 "context"
 "encoding/json"
 "fmt"
 "os"
 "reflect"
 "runtime"
 "testing"
 "time"
 "github.com/orneryd/nornicdb/pkg/storage"
)

// Reflection only adapts the context argument added between the two releases;
// query text and fixture population are identical.
func pdvCall(t *testing.T, target any, method string, args ...any) []reflect.Value {
 t.Helper(); m:=reflect.ValueOf(target).MethodByName(method); if !m.IsValid(){t.Fatalf("missing %s",method)}
 values:=[]reflect.Value{}; if m.Type().NumIn()==len(args)+1 { values=append(values,reflect.ValueOf(context.Background())) }
 for _,v:=range args {values=append(values,reflect.ValueOf(v))}; out:=m.Call(values)
 if len(out)>0 {last:=out[len(out)-1]; if last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !last.IsNil(){t.Fatalf("%s: %v",method,last.Interface())}}
 return out
}
func pdvExec(t *testing.T,e *StorageExecutor,q string,p map[string]any)*ExecuteResult {t.Helper(); r,err:=e.Execute(context.Background(),q,p); if err!=nil {t.Fatalf("query %s: %v",q,err)}; return r}
func pdvQueries(t *testing.T)map[string]string { b,err:=os.ReadFile("pdverify_v1_queries.json");if err!=nil{t.Fatal(err)};var q map[string]string;if err=json.Unmarshal(b,&q);err!=nil{t.Fatal(err)};return q }
func pdvStore(t *testing.T)(*storage.BadgerEngine,storage.Engine,*StorageExecutor){t.Helper();b,err:=storage.NewBadgerEngineInMemory();if err!=nil{t.Fatal(err)};t.Cleanup(func(){b.Close()});s:=storage.NewNamespacedEngine(b,"personal_documents");return b,s,NewStorageExecutor(s)}
func pdvNode(t *testing.T,s storage.Engine,id string,labels []string,p map[string]any){pdvCall(t,s,"CreateNode",&storage.Node{ID:storage.NodeID(id),Labels:labels,Properties:p})}
func pdvEdge(t *testing.T,s storage.Engine,id,a,b,kind string,p map[string]any){pdvCall(t,s,"CreateEdge",&storage.Edge{ID:storage.EdgeID(id),StartNode:storage.NodeID(a),EndNode:storage.NodeID(b),Type:kind,Properties:p})}
func pdvMeasure(t *testing.T,e *StorageExecutor,name,q string,p map[string]any,tx bool){t.Helper();if tx{pdvExec(t,e,"BEGIN",nil)};runtime.GC();var a,b runtime.MemStats;runtime.ReadMemStats(&a);start:=time.Now();r,err:=e.Execute(context.Background(),q,p);elapsed:=time.Since(start);runtime.ReadMemStats(&b);rows:=0;if r!=nil{rows=len(r.Rows)};t.Logf("OBS %s explicit=%v elapsed=%s alloc=%d rows=%d err=%v",name,tx,elapsed,b.TotalAlloc-a.TotalAlloc,rows,err);if r!=nil&&rows<8{v,_:=json.Marshal(r.Rows);t.Logf("OUTPUT %s %s",name,v)};if tx{pdvExec(t,e,"ROLLBACK",nil)}}

func TestPDVerifyV1Foreach(t *testing.T){
 for _,tx:=range []bool{false,true}{
  _,_,e:=pdvStore(t);pdvExec(t,e,"CREATE (:PDAlias {thing_id: 'probe', survivor_id: 'before'})",nil)
  pdvMeasure(t,e,"empty-foreach","MATCH (a:PDAlias {thing_id: $id}) DETACH DELETE a WITH a FOREACH (x IN [] | SET a.survivor_id = 'after') RETURN 1 AS seen",map[string]any{"id":"probe"},tx)
 }
}

func TestPDVerifyV1CallMemory(t *testing.T){
 _,s,e:=pdvStore(t);ids:=[]any{}
 for i:=0;i<40;i++{id:=fmt.Sprintf("source%d",i);ids=append(ids,id);chunks:=make([][]float32,128);for j:=range chunks{chunks[j]=make([]float32,2048);chunks[j][0]=1};pdvCall(t,s,"CreateNode",&storage.Node{ID:storage.NodeID(id),Labels:[]string{"PDSearchSource"},Properties:map[string]any{"id":id,"search_hidden":false,"current":true},ChunkEmbeddings:chunks})}
 q:="CALL () { RETURN $eff_view AS eff_view } UNWIND $ids AS id MATCH (s:PDSearchSource {id: id}) RETURN s.id, s.has_embedding"
 p:=map[string]any{"eff_view":map[string]any{},"ids":ids}
 for round:=0;round<2;round++{pdvMeasure(t,e,"inline-CALL",q,p,false);pdvMeasure(t,e,"inline-CALL",q,p,true)}
}

func TestPDVerifyV1Scale(t *testing.T){
 _,s,e:=pdvStore(t)
 // 5,280 documents; 351 high-degree search candidates; 172k nodes / 290k links.
 for i:=0;i<172000;i++{id:=fmt.Sprintf("n%d",i);labels:=[]string{"PDGraphNode"};if i<5280{labels=append(labels,"PDDocument")};if i==5280{labels=[]string{"PDCollection"}};pdvNode(t,s,id,labels,map[string]any{"id":id,"archived":false})}
 for i:=0;i<290000;i++{a:=fmt.Sprintf("n%d",i%351);b:="n5280";kind:="MEMBER_OF";p:=map[string]any{};if i>=351{b=fmt.Sprintf("n%d",5281+i%(172000-5281));kind="PD_RELATION";p["id"]=fmt.Sprintf("r%d",i)};pdvEdge(t,s,fmt.Sprintf("r%d",i),a,b,kind,p)}
  for _,q:=range []string{"CREATE CONSTRAINT pd_document_id IF NOT EXISTS FOR (d:PDDocument) REQUIRE d.id IS UNIQUE","CREATE CONSTRAINT pd_graph_node_id IF NOT EXISTS FOR (d:PDGraphNode) REQUIRE d.id IS UNIQUE","CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE"}{pdvExec(t,e,q,nil)}
 ids:=[]any{};for i:=0;i<351;i++{ids=append(ids,fmt.Sprintf("n%d",i))};p:=map[string]any{"ids":ids,"id":"r289999"}
 forward:="UNWIND $ids AS id MATCH (elig_d:PDDocument {id: id}) OPTIONAL MATCH (elig_d)-[:MEMBER_OF]->(elig_k:PDCollection) WITH elig_d, collect(CASE WHEN elig_k IS NOT NULL THEN [elig_k.archived] END) AS elig_held WITH elig_d, [elig_f IN elig_held | elig_f[0]] AS elig_flags WITH elig_d, any(elig_f IN elig_flags WHERE elig_f = true) AND all(elig_f IN elig_flags WHERE coalesce(elig_f, false)) AS elig_closed RETURN elig_d.id, elig_closed"
 inverse:="MATCH (elig_k:PDCollection)<-[:MEMBER_OF]-(elig_d:PDDocument) WHERE elig_d.id IN $ids RETURN elig_d.id, elig_k.archived"
 unique:="MATCH (a)-[r:PD_RELATION {id: $id}]->(b) RETURN r.id, a.id, b.id"
 for round:=0;round<2;round++{pdvMeasure(t,e,"typed-forward",forward,p,true);pdvMeasure(t,e,"typed-inverse",inverse,p,true);pdvMeasure(t,e,"unique-property",unique,p,true)}
 for _,prefix:=range []string{"EXPLAIN ","PROFILE "}{r:=pdvExec(t,e,prefix+unique,p);b,_:=json.Marshal(r.Metadata);t.Logf("PLAN %s %s",prefix,b)}
 // Exact grouped reach used by register/holdings, with representative substitutions.
 grouped:=`UNWIND $items AS rr_item
WITH rr_item.node AS rr_node, apoc.map.fromPairs(collect([rr_item.key, rr_item])) AS rr_wanted
MATCH (rr_start:PDGraphNode {id: rr_node})
MATCH (rr_start)-[r:PD_RELATION]->(rr_end)
WITH r, rr_wanted[coalesce(r.id, type(r) + '>' + endNode(r).id)] AS item
WHERE item IS NOT NULL
RETURN r.id`
 pdvMeasure(t,e,"reach-grouped",grouped,map[string]any{"items":[]any{map[string]any{"node":fmt.Sprintf("n%d",289999%351),"key":"r289999"}}},true)
}

func TestPDVerifyV1Related(t *testing.T){pdvRelated(t,[]string{"related._party_groups:0","related.RELATED","related.SAME_PARTY"})}
func pdvRelated(t *testing.T,keys []string){
 _,s,e:=pdvStore(t)
 for i:=0;i<172000;i++{id:=fmt.Sprintf("n%d",i);labels:=[]string{"PDGraphNode"};p:=map[string]any{"id":id};if i<5280{labels=append(labels,"PDDocument")};if i>=5280&&i<5580{labels=append(labels,"PDIdentity");p["status"]="confirmed";p["survivor"]=id;p["name_key"]=fmt.Sprintf("party%d",(i-5280)/2);p["kind"]="person";p["collection_id"]="collection"};if i>=5580&&i<5880{labels=[]string{"PDHeld"};p["thing_id"]=fmt.Sprintf("n%d",5280+i-5580);p["holders"]=int64(18)};if i==5880{labels=[]string{"PDFreshness"};p["id"]="holdings";p["documents"]=int64(5280)};pdvNode(t,s,id,labels,p)}
 for i:=0;i<290000;i++{a:=fmt.Sprintf("n%d",i%5280);b:=fmt.Sprintf("n%d",5881+i%(172000-5881));kind:="PD_RELATION";props:=map[string]any{"id":fmt.Sprintf("e%d",i)};if i<5280{kind="PD_HAS";b=fmt.Sprintf("n%d",5580+i%300);props=map[string]any{"identity":true}};pdvEdge(t,s,fmt.Sprintf("e%d",i),a,b,kind,props)}
  for _,q:=range []string{"CREATE CONSTRAINT pd_document_id IF NOT EXISTS FOR (d:PDDocument) REQUIRE d.id IS UNIQUE","CREATE CONSTRAINT pd_held_thing IF NOT EXISTS FOR (h:PDHeld) REQUIRE h.thing_id IS UNIQUE","CREATE CONSTRAINT pd_freshness_id IF NOT EXISTS FOR (f:PDFreshness) REQUIRE f.id IS UNIQUE","CREATE INDEX pd_party_survivor FOR (n:PDIdentity) ON (n.survivor)","CREATE INDEX pd_party_anchor FOR (n:PDIdentityAnchor) ON (n.party)","CREATE INDEX pd_graph_node_id FOR (n:PDGraphNode) ON (n.id)"}{pdvExec(t,e,q,nil)}
 qs:=pdvQueries(t);p:=map[string]any{"limit":int64(20),"per_item":int64(10),"start_id":"n0","collection_ids":nil}
 for _,key:=range keys{pdvMeasure(t,e,key,qs[key],p,true)}
}
