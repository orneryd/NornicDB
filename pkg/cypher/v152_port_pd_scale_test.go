package cypher

import (
 "context"
 "encoding/json"
 "fmt"
 "github.com/orneryd/nornicdb/pkg/storage"
 "reflect"
 "runtime"
 "testing"
 "time"
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
func pdvExec(t *testing.T,e *StorageExecutor,q string,p map[string]any)*ExecuteResult {t.Helper(); r,err:=e.Execute(WithTransactionControl(context.Background()),q,p); if err!=nil {t.Fatalf("query %s: %v",q,err)}; return r}
func pdvStore(t *testing.T)(*storage.BadgerEngine,storage.Engine,*StorageExecutor){t.Helper();b,err:=storage.NewBadgerEngineInMemory();if err!=nil{t.Fatal(err)};t.Cleanup(func(){b.Close()});s:=storage.NewNamespacedEngine(b,"personal_documents");return b,s,NewStorageExecutor(s)}
func pdvNode(t *testing.T,s storage.Engine,id string,labels []string,p map[string]any){pdvCall(t,s,"CreateNode",&storage.Node{ID:storage.NodeID(id),Labels:labels,Properties:p})}
func pdvEdge(t *testing.T,s storage.Engine,id,a,b,kind string,p map[string]any){pdvCall(t,s,"CreateEdge",&storage.Edge{ID:storage.EdgeID(id),StartNode:storage.NodeID(a),EndNode:storage.NodeID(b),Type:kind,Properties:p})}
func pdvMeasure(t *testing.T,e *StorageExecutor,name,q string,p map[string]any,tx bool){t.Helper();if tx{pdvExec(t,e,"BEGIN",nil)};runtime.GC();var a,b runtime.MemStats;runtime.ReadMemStats(&a);start:=time.Now();r,err:=e.Execute(WithTransactionControl(context.Background()),q,p);elapsed:=time.Since(start);runtime.ReadMemStats(&b);rows:=0;if r!=nil{rows=len(r.Rows)};t.Logf("OBS %s explicit=%v elapsed=%s alloc=%d rows=%d err=%v",name,tx,elapsed,b.TotalAlloc-a.TotalAlloc,rows,err);if r!=nil&&rows<8{v,_:=json.Marshal(r.Rows);t.Logf("OUTPUT %s %s",name,v)};if tx{pdvExec(t,e,"ROLLBACK",nil)}}

func TestV152PortPDScale(t *testing.T){
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



func TestV152PortPDKeyed(t *testing.T){
 _,s,e:=pdvStore(t)
 for i:=0;i<172000;i++{id:=fmt.Sprintf("n%d",i);labels:=[]string{"PDGraphNode"};p:=map[string]any{"id":id};if i<5280{labels=append(labels,"PDDocument")};if i==5280{labels=[]string{"PDCollection"}};if i==6000{labels=[]string{"PDOriginal"};p["content_hash"]="synthetic-hash"};if i==6001{labels=[]string{"PDVersion"};p["document_id"]="n0";p["created_at"]=int64(1)};pdvNode(t,s,id,labels,p)}
 for i:=0;i<290000;i++{a:=fmt.Sprintf("n%d",i%351);b:="n5280";kind:="MEMBER_OF";p:=map[string]any{};if i<5280{a=fmt.Sprintf("n%d",i)}else{b=fmt.Sprintf("n%d",5281+i%(172000-5281));kind="PD_RELATION";p["id"]=fmt.Sprintf("r%d",i)};pdvEdge(t,s,fmt.Sprintf("r%d",i),a,b,kind,p)}
 pdvEdge(t,s,"has-original","n6001","n6000","HAS_ORIGINAL",nil)
 for _,label:=range []string{"PDDocument","PDCollection","PDGraphNode","PDVersion"}{pdvExec(t,e,fmt.Sprintf("CREATE CONSTRAINT ck_%s IF NOT EXISTS FOR (d:%s) REQUIRE d.id IS UNIQUE",label,label),nil)}
 pdvExec(t,e,"CREATE CONSTRAINT ck_hash IF NOT EXISTS FOR (d:PDOriginal) REQUIRE d.content_hash IS UNIQUE",nil)
 pdvExec(t,e,"CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE",nil)
 ids:=[]any{};for i:=0;i<351;i++{ids=append(ids,fmt.Sprintf("n%d",i))}
 b:=[]byte("{\"COLLECTION_DOCUMENTS\": \"\\nUNWIND $ids AS each\\nWITH DISTINCT each\\nMATCH (d:PDDocument {id: each})-[:MEMBER_OF]->(:PDCollection {id: $collection_id})\\nRETURN d.id AS id\\n\", \"CONTENT_DOCUMENT\": \"\\nMATCH (o:PDOriginal {content_hash: $sha256})<-[:HAS_ORIGINAL]-(v:PDVersion)\\nMATCH (d:PDDocument {id: v.document_id})-[:MEMBER_OF]->(:PDCollection {id: $collection_id})\\nRETURN d.id AS id, v.created_at AS at\\nORDER BY at, id\\nLIMIT 1\\n\"}");var err error;var qs map[string]string;if err=json.Unmarshal(b,&qs);err!=nil{t.Fatal(err)}
 p:=map[string]any{"ids":ids,"collection_id":"n5280","sha256":"synthetic-hash"}
 crMeasure(t,e,"pd-COLLECTION_DOCUMENTS",qs["COLLECTION_DOCUMENTS"],p,351)
 crMeasure(t,e,"pd-CONTENT_DOCUMENT",qs["CONTENT_DOCUMENT"],p,1)
}


func TestV152PortStoredLink(t *testing.T){
 raw:=[]byte("{\"statement\": \"\\nMATCH (:PDGraphNode {id: $from_id})-[r:PD_RELATION {id: $id}]->(:PDGraphNode {id: $to_id})\\nRETURN r.kind AS kind, r.statement AS statement, r.source_id AS source_id,\\n       r.cite_start AS cite_start, r.cite_end AS cite_end, r.citation_claim AS citation_claim\\n\", \"params\": {\"id\": \"synthetic-stored-link\", \"from_id\": \"synthetic-from\", \"to_id\": \"synthetic-to\"}}");var err error
 var input struct{Statement string `json:"statement"`;Params map[string]any `json:"params"`};if err=json.Unmarshal(raw,&input);err!=nil{t.Fatal(err)}
 for _,degree:=range []int{1000,16000}{t.Run(fmt.Sprintf("incoming%d",degree),func(t *testing.T){
  _,s,e:=pdvStore(t)
  for _,id:=range []string{"synthetic-from","synthetic-to","synthetic-spare-a","synthetic-spare-b"}{pdvNode(t,s,id,[]string{"PDGraphNode"},map[string]any{"id":id})}
  wanted:=map[string]any{"id":"synthetic-stored-link","kind":"supports","statement":"synthetic stored claim","source_id":"synthetic-source","cite_start":[]any{int64(5)},"cite_end":[]any{int64(15)},"citation_claim":"synthetic-citation-claim"}
  pdvEdge(t,s,"edge-wanted","synthetic-from","synthetic-to","PD_RELATION",wanted)
  for i:=0;i<degree-1;i++{id:=fmt.Sprintf("synthetic-noise-%d",i);pdvNode(t,s,id,[]string{"PDGraphNode"},map[string]any{"id":id});pdvEdge(t,s,fmt.Sprintf("noise-edge-%d",i),id,"synthetic-to","PD_RELATION",map[string]any{"id":fmt.Sprintf("noise-link-%d",i),"kind":"supports","statement":"synthetic noise"})}
  for i,id:=range []string{"synthetic-spare-a","synthetic-spare-b"}{pdvEdge(t,s,fmt.Sprintf("spare-edge-%d",i),"synthetic-from",id,"PD_RELATION",map[string]any{"id":fmt.Sprintf("spare-link-%d",i),"kind":"supports"})}
  for _,q:=range []string{"CREATE CONSTRAINT pd_graph_node_id IF NOT EXISTS FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE","CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE"}{pdvExec(t,e,q,nil)}
  expected,_:=json.Marshal([][]any{{"supports","synthetic stored claim","synthetic-source",[]any{int64(5)},[]any{int64(15)},"synthetic-citation-claim"}})
  for round:=1;round<=3;round++{
   pdvExec(t,e,"BEGIN",nil)
   start:=time.Now();r,err:=e.Execute(WithTransactionControl(context.Background()),input.Statement,input.Params);elapsed:=time.Since(start)
   if err!=nil{t.Fatal(err)};actual,_:=json.Marshal(r.Rows);if string(actual)!=string(expected){t.Fatalf("stored claim changed: got %s want %s",actual,expected)}
   t.Logf("STORED_LINK degree=%d round=%d us=%.3f rows=%d output=%s",degree,round,float64(elapsed.Nanoseconds())/1000,len(r.Rows),actual)
   pdvExec(t,e,"ROLLBACK",nil)
  }
 })}
}

func crMeasure(t *testing.T,e *StorageExecutor,name,q string,p map[string]any,want int) {
 t.Helper()
 for round:=0;round<3;round++ {
  pdvExec(t,e,"BEGIN",nil);start:=time.Now();r,err:=e.Execute(WithTransactionControl(context.Background()),q,p);elapsed:=time.Since(start)
  if err!=nil {t.Fatal(err)};if len(r.Rows)!=want {t.Fatalf("%s rows=%d want %d",name,len(r.Rows),want)}
  t.Logf("%s round=%d elapsed=%s rows=%d",name,round,elapsed,len(r.Rows));pdvExec(t,e,"ROLLBACK",nil)
 }
}
