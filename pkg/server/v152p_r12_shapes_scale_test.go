package server

// Research only (R12 scan/correlated lines): the same shapes as
// TestZZR12Shapes, run on one store grown in steps, to measure how each
// shape's time grows with the population and with document/context degree.
// 100 documents and 100 contexts; every point links to one of each, so each
// document's and context's degree is points/100.
import (
 "fmt"
 "os"
 "path/filepath"
 "strconv"
 "strings"
 "testing"
 "time"
)

func TestZZR12ShapesScale(t *testing.T){
 t.Setenv("NORNICDB_HTTP_TX_TIMEOUT","50m")
 l:=r12Lab(t)
 spec:=os.Getenv("R12_SHAPE_SIZES");if spec==""{spec="10000,40000,160000"}
 for _,label:=range []string{"PDGraphNode","PDDocument","PDPoint","PDContext","PDReading","PDVersion","PDCorrection","PDSearchSource"}{l.cypher("CREATE INDEX ss_"+label+" FOR (n:"+label+") ON (n.id)",nil)}
 rows:=[]any{};for i:=0;i<100;i++{rows=append(rows,map[string]any{"id":fmt.Sprintf("d%04d",i),"ctx":fmt.Sprintf("c%04d",i),"reading":fmt.Sprintf("r%04d",i)})}
 l.cypher(`UNWIND $rows AS r CREATE (d:PDGraphNode:PDDocument {id:r.id,document_id:r.id,read_reading_id:r.reading,current_version_id:r.id,family_key:r.id}) CREATE (c:PDGraphNode:PDContext {id:r.ctx,updated_at:1,kind:'topic',name:r.ctx,search_text:'context body',collection_ids:['coll']}) CREATE (v:PDVersion {id:r.id,document_id:r.id}) CREATE (read:PDReading {id:r.reading,document_id:r.id,context_ids:[r.ctx],context_versions:['old']})`,map[string]any{"rows":rows})
 kinds:=map[string]any{"kinds":[]any{"mentions","in_context"},"after":nil,"limit":100}
 ctxIDs:=map[string]any{"ids":[]any{"c0000","c0001","c0002","c0003","c0004","absent"}}
 docIDs:=map[string]any{"ids":[]any{"d0000","d0001","d0002","d0003","d0004","absent"}}
 assets:=map[string]string{};for _,a:=range []string{"holdings","family"}{b,err:=os.ReadFile(filepath.Join("testdata","r12-"+a+".cypher"));if err!=nil{t.Fatal(err)};assets[a]=string(b)}
 qs:=[]struct{name,q string;p map[string]any}{
 {"orphan-count",`MATCH (n:PDGraphNode) OPTIONAL MATCH (ue_owner:PDDocument {id:n.document_id}) WITH n,ue_owner WHERE NOT n:PDDocument AND ue_owner IS NULL RETURN count(DISTINCT n.id) AS count`,nil},
 {"projection-count",`MATCH (n:PDGraphNode) WHERE n.assessed_projection IS NOT NULL AND NOT n:PDContext RETURN count(DISTINCT n.id) AS count`,nil},
 {"relation-count",`MATCH (:PDGraphNode)-[n:PD_RELATION]->(:PDGraphNode) WHERE n.kind IN $kinds AND n.updated_at IS NOT NULL AND n.correction_id IS NULL RETURN count(DISTINCT n.id) AS count`,kinds},
 {"projection-page",`MATCH (n:PDGraphNode) WHERE n.assessed_projection IS NOT NULL AND NOT n:PDContext AND ($after IS NULL OR n.id > $after) RETURN DISTINCT n.id AS id ORDER BY id LIMIT $limit`,kinds},
 {"relation-page",`MATCH (:PDGraphNode)-[n:PD_RELATION]->(:PDGraphNode) WHERE n.kind IN $kinds AND n.updated_at IS NOT NULL AND n.correction_id IS NULL AND ($after IS NULL OR n.id > $after) RETURN DISTINCT n.id AS id ORDER BY id LIMIT $limit`,kinds},
 {"context-prefix",`UNWIND $ids AS context_id MATCH (c:PDGraphNode:PDContext {id:context_id}) WHERE c.updated_at IS NOT NULL WITH c,apoc.convert.toJson({kind:c.kind,label:coalesce(c.label,c.name),text:c.search_text,offered:apoc.coll.sort(coalesce(c.collection_ids,[]))}) AS projection CALL (c) { OPTIONAL MATCH (c)<-[link:PD_RELATION {kind:'in_context'}]-(point:PDPoint:PDGraphNode) WHERE coalesce(link.eff_usable,false) RETURN collect(point.id) AS points } RETURN c.id,size(points)`,ctxIDs},
 {"reading-prefix",`UNWIND $ids AS document_id MATCH (d:PDDocument {id:document_id}) WHERE d.read_reading_id IS NOT NULL AND NOT (d.due_at IS NOT NULL) MATCH (r:PDReading {id:d.read_reading_id}) WITH collect({document_id:d.id,reading:r}) AS readings CALL { WITH readings UNWIND readings AS item WITH item.reading AS r UNWIND coalesce(r.context_ids,[]) AS context_id OPTIONAL MATCH (c:PDGraphNode:PDContext {id:context_id}) RETURN collect(c.id) AS contexts } RETURN contexts`,docIDs},
 {"holdings",assets["holdings"],map[string]any{"ids":[]any{"d0000","d0001","d0002","d0003","d0004"}}},
 {"family",assets["family"],map[string]any{"keys":[]any{"d0000","d0001","d0002","d0003","d0004"}}},
 }
 have:=0
 for _,s:=range strings.Split(spec,","){
  n,_:=strconv.Atoi(strings.TrimSpace(s));at:=time.Now()
  for b:=have;b<n;b+=500{rows:=[]any{};for i:=b;i<b+500&&i<n;i++{rows=append(rows,map[string]any{"id":fmt.Sprintf("p%07d",i),"doc":fmt.Sprintf("d%04d",i%100),"ctx":fmt.Sprintf("c%04d",i%100),"body":strings.Repeat("research body ",80)})}
   l.cypher(`UNWIND $rows AS r CREATE (n:PDGraphNode:PDPoint {id:r.id,document_id:r.doc,search_text:r.body,assessed_projection:'old',updated_at:1})`,map[string]any{"rows":rows})
   l.cypher(`UNWIND $rows AS r MATCH (p:PDPoint {id:r.id}) MATCH (d:PDDocument {id:r.doc}) MATCH (c:PDContext {id:r.ctx}) CREATE (d)-[:PD_RELATION {id:r.id+'d',kind:'mentions',updated_at:1,eff_usable:true}]->(p) CREATE (p)-[:PD_RELATION {id:r.id+'c',kind:'in_context',updated_at:1,eff_usable:true}]->(c)`,map[string]any{"rows":rows})}
  have=n
  fmt.Printf("R12 SHAPESCALE STAGE points=%d nodes=%d edges=%d degree=%d seed_seconds=%.3f\n",n,n+400,2*n,n/100,time.Since(at).Seconds())
  for _,q:=range qs{
   l.cypher(`MATCH (d:PDDocument {id:'d0099'}) SET d.r12_touch=$v`,map[string]any{"v":time.Now().UnixNano()})
   _,secs:=r12ScaleRun(l,fmt.Sprintf("shape-%s-n%d",q.name,n),q.q,q.p,[]time.Duration{3*time.Second})
   fmt.Printf("R12 SHAPESCALE ROW shape=%s points=%d seconds=%.6f\n",q.name,n,secs)
  }
 }
 fmt.Printf("R12 SHAPESCALE END wall=%s\n",time.Now().UTC().Format(time.RFC3339Nano))
}
