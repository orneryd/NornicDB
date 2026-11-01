// Author: Codex R17 implementation lane. Standalone public API measurement.
package main
import("context";"crypto/sha256";"encoding/hex";"encoding/json";"embed";"fmt";"os";"runtime";"time";"github.com/orneryd/nornicdb/pkg/nornicdb")
//go:embed statements.json
var inputs embed.FS
func must(e error){if e!=nil{panic(e)}}
func emit(v any){b,e:=json.Marshal(v);must(e);fmt.Println("R17_PUBLIC "+string(b))}
func main(){
 validation:=len(os.Args)==2&&os.Args[1]=="validate"
 dir,e:=os.MkdirTemp("","r17-public-");must(e);defer os.RemoveAll(dir)
 cfg:=nornicdb.DefaultConfig();cfg.Database.AsyncWritesEnabled=false;cfg.Database.StrictDurability=true;cfg.Database.WALSyncMode="immediate"
 db,e:=nornicdb.Open(dir,cfg);must(e);defer db.Close();ctx:=context.Background()
 run:=func(q string,p map[string]interface{})*nornicdb.CypherResult{r,e:=db.ExecuteCypher(ctx,q,p);must(e);return r}
 ids:=[]interface{}{};corrections:=[]interface{}{}
 for i:=0;i<100;i++{id:=fmt.Sprintf("d%d",i);rid:=fmt.Sprintf("r%d",i);gid:=fmt.Sprintf("g%d",i);ids=append(ids,id,gid);run("CREATE (d:PDDocument {id:$id,current_version_id:'v1',read_reading_id:$rid,due_at:'2026-01-01T00:00:00Z',read_knowledge_at:'2026-01-02T00:00:00Z'}) CREATE (r:PDReading {id:$rid,identity_ids:[$gid],identity_revisions:[1],subject_ids:[],subject_versions:[],used_ids:[$gid]}) CREATE (g:PDGraphNode:PDIdentity {id:$gid,revision:1})",map[string]interface{}{"id":id,"rid":rid,"gid":gid})}
 run("CREATE (c:PDCorrection {id:'c0',status:'active',revision:1,scope:['v1']})",nil);corrections=append(corrections,"c0")
 params:=map[string]interface{}{"full":true,"ids":ids,"correction_ids":corrections,"mentions":[]interface{}{},"edges":[]interface{}{},"creatable":map[string]interface{}{},"document_id":"d0","source_id":"s0","version_id":"v1","separator":"|"}
 emit(map[string]interface{}{"kind":"fixture","documents":100,"readings":100,"identities":100,"corrections":1,"edges":0,"parameters":params,"go":runtime.Version(),"cpus":runtime.NumCPU(),"gomaxprocs":runtime.GOMAXPROCS(0),"strictDurability":true,"asyncWrites":false,"wal":"immediate"})
 check:=run("MATCH (d:PDDocument) RETURN count(d) AS n",nil);mustCount:=fmt.Sprint(check.Rows);if mustCount!="[[100]]"{panic(mustCount)}
 if validation{r:=run("RETURN 1 AS n",nil);if fmt.Sprint(r.Rows)!="[[1]]"{panic("validation result")};emit(map[string]interface{}{"kind":"validation","passed":true});return}
 b,e:=inputs.ReadFile("statements.json");must(e);var texts map[string]string;must(json.Unmarshal(b,&texts))
 for _,id:=range []string{"42f785d4d9dbeb87","dd6661ce13671934","588c584d45dab5f8"}{q:=texts[id];sum:=sha256.Sum256([]byte(q));for sample:=0;sample<3;sample++{start:=time.Now();r,e:=db.ExecuteCypher(ctx,q,params);ns:=time.Since(start).Nanoseconds();record:=map[string]interface{}{"kind":"statement","originalId":id,"sha256":hex.EncodeToString(sum[:]),"bytes":len(q),"sample":sample,"nanoseconds":ns};if e!=nil{record["error"]=e.Error();emit(record);panic(e)};record["rows"]=len(r.Rows);record["columns"]=r.Columns;emit(record)}}
}
