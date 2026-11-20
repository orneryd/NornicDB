package server

// Research-only untracked probe. Shared slowb helpers are the preserved prior probe.
import (
 "context"
 "encoding/base64"
 "encoding/binary"
 "fmt"
 "os"
 "path/filepath"
 "runtime"
 "strings"
 "testing"
 "time"
 "github.com/orneryd/nornicdb/pkg/auth"
 "github.com/orneryd/nornicdb/pkg/nornicdb"
 "github.com/orneryd/nornicdb/pkg/storage"
)

func r12Lab(t *testing.T) *slowbLab {
 t.Helper()
 cfg:=nornicdb.DefaultConfig()
 cfg.Memory.DecayEnabled=false; cfg.Memory.AutoLinksEnabled=false; cfg.Memory.EmbeddingEnabled=false
 cfg.Database.WALAutoCompactionEnabled=false
 t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY","test-remote-credential-key-32b!")
 root:=t.TempDir(); db,err:=nornicdb.Open(root,cfg); if err!=nil {t.Fatal(err)}
 t.Cleanup(func(){db.Close()})
 a,err:=auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled:true,JWTSecret:[]byte("test-secret-key-for-testing-only-32b")},storage.NewNamespacedEngine(db.GetBaseStorageForManager(),"system")); if err!=nil{t.Fatal(err)}
 if err=slowbCreateUser(a,"admin");err!=nil{t.Fatal(err)}
 scfg:=DefaultConfig();scfg.Port=0;scfg.EmbeddingEnabled=false
 srv,err:=New(db,a,scfg);if err!=nil{t.Fatal(err)}
 if os.Getenv("R12_CDC")=="on" {base:=db.GetBaseStorageForManager();if w,ok:=base.(*storage.WALEngine);ok{base=w.GetInnerEngine()};base.(*storage.BadgerEngine).SetChangeCapture("nornic",storage.ChangeCaptureFull)}
 t.Cleanup(func(){ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel();srv.Stop(ctx)})
 tok,err:=slowbToken(a);if err!=nil{t.Fatal(err)}
 return &slowbLab{t:t,db:db,srv:srv,router:srv.buildRouter(),authz:"Bearer "+tok,start:time.Now(),root:root}
}

func r12Run(l *slowbLab, name,q string,p map[string]any) map[string]any {
 start:=time.Now();done:=make(chan struct{});go func(){select{case <-time.After(1200*time.Millisecond):slowbDump("R12 "+name);case <-done:}}()
 out,code,d:=l.post("/db/nornic/tx/commit",map[string]any{"statements":[]any{map[string]any{"statement":q,"parameters":p}}});close(done)
 raw:=fmt.Sprint(out);if len(raw)>600{raw=raw[:600]}
 fmt.Printf("R12 RESULT name=%s seconds=%.6f code=%d start=%s out=%s\n",name,d.Seconds(),code,start.UTC().Format(time.RFC3339Nano),raw)
 return out
}

func TestZZR12Shapes(t *testing.T){
 l:=r12Lab(t); n:=slowbEnvInt("R12_N",2000); batch:=200
 fmt.Printf("R12 CONFIG n=%d GOMAXPROCS=%d no-worker no-compaction\n",n,runtime.GOMAXPROCS(0))
 if os.Getenv("R12_INDEX")=="on"{for _,label:=range []string{"PDGraphNode","PDDocument","PDPoint","PDContext","PDReading","PDVersion","PDCorrection","PDSearchSource"}{l.cypher("CREATE INDEX r12_"+label+" FOR (n:"+label+") ON (n.id)",nil)}}
 for s:=0;s<n;s+=batch{rows:=[]any{};for i:=s;i<s+batch&&i<n;i++{rows=append(rows,map[string]any{"id":fmt.Sprintf("p%06d",i),"doc":fmt.Sprintf("d%04d",i%100),"body":strings.Repeat("research body ",80),"ctx":fmt.Sprintf("c%04d",i%100)})};l.cypher(`UNWIND $rows AS r CREATE (n:PDGraphNode:PDPoint {id:r.id,document_id:r.doc,search_text:r.body,assessed_projection:'old',updated_at:1})`,map[string]any{"rows":rows})}
 rows:=[]any{};for i:=0;i<100;i++{rows=append(rows,map[string]any{"id":fmt.Sprintf("d%04d",i),"ctx":fmt.Sprintf("c%04d",i),"reading":fmt.Sprintf("r%04d",i)})}
 l.cypher(`UNWIND $rows AS r CREATE (d:PDGraphNode:PDDocument {id:r.id,read_reading_id:r.reading,current_version_id:r.id,family_key:r.id}) CREATE (c:PDGraphNode:PDContext {id:r.ctx,updated_at:1,kind:'topic',name:r.ctx,search_text:'context body',collection_ids:['coll']}) CREATE (v:PDVersion {id:r.id,document_id:r.id}) CREATE (read:PDReading {id:r.reading,document_id:r.id,context_ids:[r.ctx],context_versions:['old']})`,map[string]any{"rows":rows})
 for s:=0;s<n;s+=batch{rows:=[]any{};for i:=s;i<s+batch&&i<n;i++{rows=append(rows,map[string]any{"id":fmt.Sprintf("p%06d",i),"doc":fmt.Sprintf("d%04d",i%100),"ctx":fmt.Sprintf("c%04d",i%100)})};l.cypher(`UNWIND $rows AS r MATCH (p:PDPoint {id:r.id}) MATCH (d:PDDocument {id:r.doc}) MATCH (c:PDContext {id:r.ctx}) CREATE (d)-[:PD_RELATION {id:r.id+'d',kind:'mentions',updated_at:1,eff_usable:true}]->(p) CREATE (p)-[:PD_RELATION {id:r.id+'c',kind:'in_context',updated_at:1,eff_usable:true}]->(c)`,map[string]any{"rows":rows})}
 fmt.Printf("R12 SEED seconds=%.3f\n",time.Since(l.start).Seconds())
 if os.Getenv("R12_VECTORS")=="on"{base:=l.db.GetBaseStorageForManager();if w,ok:=base.(*storage.WALEngine);ok{base=w.GetInnerEngine()};docs,err:=slowbNodesByLabel(storage.NewNamespacedEngine(base,"nornic"),"PDGraphNode");if err!=nil{t.Fatal(err)};for _,d:=range docs{e:=storage.CopyNode(d);e.ID=storage.NodeID("nornic:"+string(d.ID));e.ChunkEmbeddings=[][]float32{make([]float32,2048)};e.ChunkEmbeddings[0][0]=1;e.EmbedMeta=map[string]any{"has_embedding":true,"chunk_count":1,"embedding_dimensions":2048};if err:=slowbSidecar(base,e);err!=nil{t.Fatal(err)}};fmt.Printf("R12 VECTORS n=%d dims=2048 seconds=%.3f\n",len(docs),time.Since(l.start).Seconds())}
 kinds:=map[string]any{"kinds":[]any{"mentions","in_context"},"after":nil,"limit":100}
 qs:=[]struct{name,q string;p map[string]any}{
 {"cdc-current",`CALL db.cdc.current() YIELD id RETURN id`,nil},
 {"cdc-earliest-query",`CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id, [], 100) YIELD id AS change RETURN count(change)`,nil},
 {"orphan-count",`MATCH (n:PDGraphNode) OPTIONAL MATCH (ue_owner:PDDocument {id:n.document_id}) WITH n,ue_owner WHERE NOT n:PDDocument AND ue_owner IS NULL RETURN count(DISTINCT n.id) AS count`,nil},
 {"projection-count",`MATCH (n:PDGraphNode) WHERE n.assessed_projection IS NOT NULL AND NOT n:PDContext RETURN count(DISTINCT n.id) AS count`,nil},
 {"relation-count",`MATCH (:PDGraphNode)-[n:PD_RELATION]->(:PDGraphNode) WHERE n.kind IN $kinds AND n.updated_at IS NOT NULL AND n.correction_id IS NULL RETURN count(DISTINCT n.id) AS count`,kinds},
 {"projection-page",`MATCH (n:PDGraphNode) WHERE n.assessed_projection IS NOT NULL AND NOT n:PDContext AND ($after IS NULL OR n.id > $after) RETURN DISTINCT n.id AS id ORDER BY id LIMIT $limit`,kinds},
 {"relation-page",`MATCH (:PDGraphNode)-[n:PD_RELATION]->(:PDGraphNode) WHERE n.kind IN $kinds AND n.updated_at IS NOT NULL AND n.correction_id IS NULL AND ($after IS NULL OR n.id > $after) RETURN DISTINCT n.id AS id ORDER BY id LIMIT $limit`,kinds},
 {"context-prefix",`UNWIND $ids AS context_id MATCH (c:PDGraphNode:PDContext {id:context_id}) WHERE c.updated_at IS NOT NULL WITH c,apoc.convert.toJson({kind:c.kind,label:coalesce(c.label,c.name),text:c.search_text,offered:apoc.coll.sort(coalesce(c.collection_ids,[]))}) AS projection CALL (c) { OPTIONAL MATCH (c)<-[link:PD_RELATION {kind:'in_context'}]-(point:PDPoint:PDGraphNode) WHERE coalesce(link.eff_usable,false) RETURN collect(point.id) AS points } RETURN c.id,projection,points`,map[string]any{"ids":[]any{"c0000","c0001","c0002","c0003","c0004","absent"}}},
 {"reading-prefix",`UNWIND $ids AS document_id MATCH (d:PDDocument {id:document_id}) WHERE d.read_reading_id IS NOT NULL AND NOT (d.due_at IS NOT NULL) MATCH (r:PDReading {id:d.read_reading_id}) WITH collect({document_id:d.id,reading:r}) AS readings CALL { WITH readings UNWIND readings AS item WITH item.reading AS r UNWIND coalesce(r.context_ids,[]) AS context_id OPTIONAL MATCH (c:PDGraphNode:PDContext {id:context_id}) RETURN collect(c.id) AS contexts } RETURN contexts`,map[string]any{"ids":[]any{"d0000","d0001","d0002","d0003","d0004","absent"}}},
 }
 for _,q:=range qs{r12Run(l,q.name,q.q,q.p)}
 if os.Getenv("R12_CDC")=="on"{r12CDC(l)}
 for _,asset:=range []struct{name string;p map[string]any}{
 {"holdings",map[string]any{"ids":[]any{"d0000","d0001","d0002","d0003","d0004"}}},
 {"effective",map[string]any{"links":[]any{map[string]any{"from":"d0000","link":"p000000d"}},"node_ids":[]any{"p000000"},"documents":[]any{"d0000"},"correction_ids":[]any{},"selected":[]any{"d0000"},"gone_ids":[]any{}}},
 {"family",map[string]any{"keys":[]any{"d0000","d0001","d0002","d0003","d0004"}}},
 }{b,err:=os.ReadFile(filepath.Join("testdata","r12-"+asset.name+".cypher"));if err!=nil{t.Fatal(err)};r12Run(l,asset.name,string(b),asset.p)}
}

func r12CDC(l *slowbLab){
 out:=r12Run(l,"cdc-current-enabled",`CALL db.cdc.current() YIELD id RETURN id`,nil)
 results,_:=out["results"].([]any);if len(results)==0{l.t.Fatal("no CDC current result")};data,_:=results[0].(map[string]any)["data"].([]any);if len(data)==0{l.t.Fatal("no CDC current row")};row,_:=data[0].(map[string]any)["row"].([]any);cursor,_:=row[0].(string)
 if cursor==""{l.t.Fatal("missing CDC cursor")}
 raw,err:=base64.RawURLEncoding.DecodeString(cursor);if err!=nil||len(raw)!=21{l.t.Fatalf("bad cursor %s %v",cursor,err)};binary.BigEndian.PutUint64(raw[9:17],0);binary.BigEndian.PutUint32(raw[17:21],1);expired:=base64.RawURLEncoding.EncodeToString(raw)
 for _,position:=range []struct{name,id string}{{"current",cursor},{"expired",expired}}{
  p:=map[string]any{"cursor":position.id,"selectors":[]any{},"limit":1000}
  r12Run(l,"cdc-ids-"+position.name,`CALL db.cdc.query($cursor,$selectors,$limit) YIELD id WITH collect(id) AS ids RETURN size(ids) AS read,last(ids) AS cursor`,p)
  r12Run(l,"cdc-details-"+position.name,`CALL db.cdc.query($cursor,$selectors,$limit) YIELD id,txId,metadata,event WITH id,txId AS tx,toString(metadata.txCommitTime) AS at,event.eventType AS entity,event.elementId AS event_element,event.start.elementId AS from_element,event.end.elementId AS to_element,event.operation AS operation,event.type AS type,coalesce(event.labels,[]) AS labels,coalesce(event.state.before.labels,[]) AS removed,coalesce(event.state.after.labels,[]) AS added RETURN id,tx,at,entity,event_element,from_element,to_element,operation,type,labels,removed,added`,p)
 }
}
