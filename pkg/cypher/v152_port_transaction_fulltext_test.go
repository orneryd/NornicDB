package cypher

import (
 "context"
 "crypto/sha256"
 "encoding/json"
 "fmt"
 "reflect"
 "runtime"
 "strings"
 "testing"
 "time"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/orneryd/nornicdb/pkg/search"
)

// The releases differ only in the storage context argument. Adapt that API
// without replacing the public executor, storage, transactions, or indexes.
func brCall(t *testing.T,s any,name string,args ...any) []reflect.Value {
 t.Helper();m:=reflect.ValueOf(s).MethodByName(name);vs:=[]reflect.Value{}
 if m.Type().NumIn()==len(args)+1 {vs=append(vs,reflect.ValueOf(context.Background()))}
 for _,v:=range args {vs=append(vs,reflect.ValueOf(v))};out:=m.Call(vs)
 if len(out)>0 {v:=out[len(out)-1];if v.Type().Implements(reflect.TypeOf((*error)(nil)).Elem())&&!v.IsNil(){t.Fatalf("%s: %v",name,v.Interface())}}
 return out
}
func brExec(t *testing.T,e *StorageExecutor,q string,p map[string]any)*ExecuteResult {t.Helper();r,err:=e.Execute(WithTransactionControl(context.Background()),q,p);if err!=nil{t.Fatal(err)};return r}

func TestV152PortFullTextTransactionCost(t *testing.T) {
 b,err:=storage.NewBadgerEngine(t.TempDir());if err!=nil{t.Fatal(err)};defer b.Close()
 s:=storage.NewNamespacedEngine(b,"personal_documents")
 e:=NewStorageExecutor(s, QueryCachePolicy{MaxEntries: 0})
 // Preserve real source/vector shape: 1,771 sources holding 143,818
 // 2048-dimensional chunks. Disk Badger supports their large node values.
 nodes:=make([]*storage.Node,0,1000)
 flush:=func(){if len(nodes)>0{brCall(t,s,"BulkCreateNodes",nodes);nodes=nil}}
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
  brCall(t,s,"CreateEdge",&storage.Edge{ID:storage.EdgeID(fmt.Sprintf("r%06d",i)),StartNode:storage.NodeID(a),EndNode:storage.NodeID(z),Type:kind,Properties:map[string]any{"id":fmt.Sprintf("r%06d",i)}})
 }
 t.Logf("BR fixture nodes=172000 edges=290000 sources=1771 cases=21951 pending=21748 vectors=143818 dims=2048 textBytes=%d",textBytes)
 for _,l:=range []string{"PDSearchSource","PDDocument","PDVersion","PDCollection","PDIdentityCase","PDGraphNode"}{brExec(t,e,fmt.Sprintf("CREATE CONSTRAINT br_%s IF NOT EXISTS FOR (n:%s) REQUIRE n.id IS UNIQUE",l,l),nil)}
 for _,p:=range []string{"status","queue","held_by","wait_reason","claim"}{brExec(t,e,fmt.Sprintf("CREATE INDEX br_case_%s FOR (n:PDIdentityCase) ON (n.%s)",p,p),nil)}
 brExec(t,e,"CREATE FULLTEXT INDEX pd_source_text IF NOT EXISTS FOR (n:PDSearchSource) ON EACH [n.text]",nil)
 svc:=search.NewService(s,2048,nil);e.SetSearchService(svc);defer svc.Close()
 p := map[string]any{"query": "contract", "limit": int64(25)}
 const statement = `
CALL () {
  UNWIND [1] AS once
  WITH COLLECT { MATCH (closed:PDCollection {archived: true})<-[:MEMBER_OF]-(member:PDDocument)
                 RETURN member.id AS id } AS in_archived,
       COLLECT { MATCH (open:PDCollection) WHERE NOT coalesce(open.archived, false)
                 RETURN open.id AS id } AS open_collections
  WITH in_archived, open_collections,
       COLLECT { UNWIND CASE WHEN size(in_archived) = 0 THEN [] ELSE [1] END AS archived_any
                 MATCH (open:PDCollection)<-[:MEMBER_OF]-(member:PDDocument)
                 WHERE NOT coalesce(open.archived, false)
                 RETURN member.id AS id } AS open_members
  WITH open_collections, apoc.coll.subtract(in_archived, open_members) AS closed_only
  WITH open_collections, closed_only,
       COLLECT { UNWIND closed_only AS archived_document
                 MATCH (hidden:PDSearchSource {document_id: archived_document})
                 RETURN hidden.id AS id } AS hidden_sources
  CALL (open_collections, closed_only) {
    UNWIND CASE WHEN false THEN [1] ELSE [] END AS with_summaries
    WITH open_collections, closed_only,
         apoc.map.fromPairs(COLLECT { MATCH (document:PDDocument)
                                      RETURN [document.id, document.current_version_id] AS pair })
           AS current_versions,
         apoc.map.fromPairs(COLLECT { MATCH (version:PDVersion)
                                      RETURN [version.id, [version.document_id, version.current_source_id]] AS pair })
           AS versions
    UNWIND [summary IN COLLECT { MATCH (hidden:PDSummary)
                                 WHERE coalesce(hidden.current, false)
                                 RETURN hidden {.id, .owner_kind, .owner_id, .source_ids} AS summary }
      WHERE NOT coalesce((summary.owner_kind = 'collection' AND summary.owner_id IN open_collections)
                         OR (summary.owner_kind = 'document'
                             AND versions[current_versions[summary.owner_id]][0] = summary.owner_id
                             AND versions[current_versions[summary.owner_id]][1] IN summary.source_ids
                             AND NOT summary.owner_id IN closed_only), false)
      | summary.id] AS hidden_summary
    RETURN collect(hidden_summary) AS hidden_summaries
  }
  RETURN hidden_sources + hidden_summaries AS hidden_ids
}
CALL db.index.fulltext.queryNodes('pd_source_text', $query) YIELD node, score
WHERE (node:PDSearchSource OR false OR coalesce(node.current, true)) AND (NOT node:PDSummary OR coalesce(node.current, false)) AND NOT node.id IN hidden_ids
RETURN node, score ORDER BY score DESC, node.id LIMIT $limit
`
 var expected [32]byte
 for attempt := 1; attempt <= 10; attempt++ {
  brExec(t, e, "BEGIN", nil)
  var before, after runtime.MemStats
  runtime.ReadMemStats(&before)
  start := time.Now()
  result := brExec(t, e, statement, p)
  elapsed := time.Since(start)
  runtime.ReadMemStats(&after)
  brExec(t, e, "ROLLBACK", nil)
  if len(result.Rows) != 25 { t.Fatalf("fulltext rows=%d; want 25", len(result.Rows)) }
  rows := make([][]any, len(result.Rows))
  for i, row := range result.Rows {
   rows[i] = append([]any(nil), row...)
   for j, v := range row {
    if node, ok := v.(*storage.Node); ok { rows[i][j] = []any{node.ID, node.Labels, node.Properties} }
   }
  }
  materialized, err := json.Marshal(rows)
  if err != nil { t.Fatal(err) }
  digest := sha256.Sum256(materialized)
  if attempt == 1 { expected = digest }
  if digest != expected { t.Fatalf("call %d returned different rows", attempt) }
  t.Logf("PORT_FULLTEXT call=%d rows=%d bytes=%d elapsed=%s result=%x", attempt, len(rows), after.TotalAlloc-before.TotalAlloc, elapsed, digest)
 }
}
