package cypher

import (
 "context"
 "fmt"
 "reflect"
 "runtime"
 "strings"
 "testing"
 "time"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

// Same probe source on both commits. Reflection bridges only the storage API's
// added context argument; all queries and data are identical.
func pfCall(o any,m string,args ...any) []reflect.Value {
 f:=reflect.ValueOf(o).MethodByName(m); if !f.IsValid(){panic("missing method "+m)}
 a:=[]reflect.Value{}; if f.Type().NumIn()>0 && f.Type().In(0).String()=="context.Context" {a=append(a,reflect.ValueOf(context.Background()))}
 for _,v:=range args {a=append(a,reflect.ValueOf(v))};return f.Call(a)
}
func pfOK(t testing.TB,o any,m string,args ...any) []reflect.Value {t.Helper();v:=pfCall(o,m,args...);if len(v)>0 {last:=v[len(v)-1];if last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem())&&!last.IsNil(){t.Fatal(last.Interface())}};return v}
func pfEngine(t testing.TB)(*storage.BadgerEngine,*storage.NamespacedEngine,*StorageExecutor){t.Helper();b,e:=storage.NewBadgerEngine(t.TempDir());require.NoError(t,e);t.Cleanup(func(){_=b.Close()});ns:=storage.NewNamespacedEngine(b,"nornic");ex:=NewStorageExecutor(ns);ex.cache=nil;return b,ns,ex}
func pfQ(t testing.TB,e *StorageExecutor,q string,p map[string]any)*ExecuteResult {t.Helper();r,err:=e.Execute(context.Background(),q,p);require.NoError(t,err,q);return r}
func pfObserve(t *testing.T,e *StorageExecutor,name,q string,p map[string]any){t.Helper();pfQ(t,e,q,p);runtime.GC();var a,b runtime.MemStats;runtime.ReadMemStats(&a);s:=time.Now();r:=pfQ(t,e,q,p);elapsed:=time.Since(s);runtime.ReadMemStats(&b);t.Logf("PF %s rows=%v ms=%.3f bytes=%d allocs=%d",name,r.Rows,float64(elapsed.Microseconds())/1000,b.TotalAlloc-a.TotalAlloc,b.Mallocs-a.Mallocs)}

func TestPFFactsAndSnapshot(t *testing.T){
 _,ns,e:=pfEngine(t)
 q:="CREATE (:Audit {amount: 1.0_0})";t.Logf("PF facts=%+v",QueryPermissionRequirements(q));r,err:=e.Execute(context.Background(),q,nil);t.Logf("PF grouped_write rows=%v err=%v",r,err)
 pfQ(t,e,"CREATE (:P {id:'a'}),(:P {id:'b'})",nil)
 tx:=NewStorageExecutor(ns);tx.cache=nil
 pfQ(t,tx,"BEGIN",nil);pfQ(t,tx,"MATCH (n:P) RETURN count(n)",nil)
 pfQ(t,e,"MATCH (a:P {id:'a'}),(b:P {id:'b'}) CREATE (a)-[:LINK]->(b)",nil)
 t.Logf("PF snapshot-count=%v snapshot-rows=%v",pfQ(t,tx,"MATCH ()-[r]->() RETURN count(r)",nil).Rows,pfQ(t,tx,"MATCH ()-[r]->() RETURN r",nil).Rows)
 t.Logf("PF merge-endpoints=%v",pfQ(t,tx,"MATCH (a:P {id:'a'}),(b:P {id:'b'}) SET a += {} WITH a,b MERGE (a)-[r:LINK]->(b) RETURN elementId(r), startNode(r).id, endNode(r).id",nil).Rows)
 pfQ(t,tx,"ROLLBACK",nil)
}

func TestPFQueryCosts(t *testing.T){
 _,ns,e:=pfEngine(t)
 for i:=0;i<4000;i++ {id:=fmt.Sprintf("n%05d",i);pfOK(t,ns,"CreateNode",&storage.Node{ID:storage.NodeID(id),Labels:[]string{"Page"},Properties:map[string]any{"id":id,"status":"common","k":int64(i%2)}})}
 pfQ(t,e,"CREATE INDEX page_id IF NOT EXISTS FOR (n:Page) ON (n.id)",nil)
 pfQ(t,e,"CREATE INDEX page_status IF NOT EXISTS FOR (n:Page) ON (n.status)",nil)
 pfQ(t,e,"CREATE INDEX page_k IF NOT EXISTS FOR (n:Page) ON (n.k)",nil)
 for _,c:=range []struct{n,q string}{{"page_alias","MATCH (n:Page) WHERE n.id > 'n00000' RETURN n.id AS id ORDER BY id LIMIT 3"},{"page_qualified","MATCH (n:Page) WHERE n.id > 'n00000' RETURN n.id AS id ORDER BY n.id LIMIT 3"},{"page_distinct","MATCH (n:Page) WHERE n.id > 'n00000' RETURN DISTINCT n.id AS id ORDER BY id LIMIT 3"},{"or_and","MATCH (n:Page) WHERE (n.k=999 OR n.id='n00002') AND n.status='common' RETURN count(n)"}} {pfObserve(t,e,c.n,c.q,nil)}
 // Correlated aggregate imports a large value whose contents the query never uses.
 for _,n:=range []int{1,10000}{large:=make([]any,n);for i:=range large{large[i]=strings.Repeat("a",128)};pfObserve(t,e,fmt.Sprintf("import_size_%d",n),"WITH $big AS big UNWIND range(1,30) AS x CALL { WITH big, x UNWIND range(1,2) AS y RETURN count(y) AS c } RETURN sum(c)",map[string]any{"big":large})}
}

func TestPFHubAndChain(t *testing.T){
 for _,n:=range []int{32,128} {t.Run(fmt.Sprint(n),func(t *testing.T){_,ns,e:=pfEngine(t)
 pfOK(t,ns,"CreateNode",&storage.Node{ID:"h",Labels:[]string{"Hub"},Properties:map[string]any{"id":"h"}})
 for i:=0;i<n;i++ {id:=storage.NodeID(fmt.Sprintf("l%d",i));pfOK(t,ns,"CreateNode",&storage.Node{ID:id,Labels:[]string{"Leaf"},Properties:map[string]any{"id":string(id),"wanted":i==0}});pfOK(t,ns,"CreateEdge",&storage.Edge{ID:storage.EdgeID(fmt.Sprintf("e%d",i)),StartNode:id,EndNode:"h",Type:"LINK"});pfOK(t,ns,"CreateEdge",&storage.Edge{ID:storage.EdgeID(fmt.Sprintf("z%d",i)),StartNode:"h",EndNode:id,Type:"OTHER"})}
 pfObserve(t,e,"typed_optional","MATCH (d:Leaf)-[:LINK]->(h:Hub) OPTIONAL MATCH (h)-[r:OTHER]->(x) RETURN count(r)",nil)
 pfObserve(t,e,"chain_late","MATCH (a:Leaf)-[:LINK]->(h:Hub)<-[:LINK]-(b:Leaf) WHERE a.wanted=true RETURN count(b)",nil)
 pfObserve(t,e,"chain_split","MATCH (a:Leaf) WHERE a.wanted=true WITH a MATCH (a)-[:LINK]->(h:Hub)<-[:LINK]-(b:Leaf) RETURN count(b)",nil)
 })}
}

func TestPFEvaluatorCopy(t *testing.T){_,_,e:=pfEngine(t);for _,n:=range []int{0,256}{v:=make([][]float32,n);for i:=range v{v[i]=make([]float32,256)};node:=&storage.Node{ID:"nornic:x",Labels:[]string{"N"},Properties:map[string]any{"id":"x"},ChunkEmbeddings:v};nodes:=map[string]*storage.Node{"n":node};for _,expr:=range []string{"n","[n][0]","coalesce(n,n)","n.id"}{f:=func(){_ = e.evaluateExpressionWithContextFull(context.Background(),expr,nodes,nil,nil,nil,nil,0)};f();runtime.GC();var a,b runtime.MemStats;runtime.ReadMemStats(&a);for i:=0;i<30;i++{f()};runtime.ReadMemStats(&b);t.Logf("PF eval chunks=%d expr=%s bytes/op=%d allocs/op=%d",n,expr,(b.TotalAlloc-a.TotalAlloc)/30,(b.Mallocs-a.Mallocs)/30)}}}


func TestPFMiddlePredicate(t *testing.T){for _,n:=range []int{64,256}{t.Run(fmt.Sprint(n),func(t *testing.T){_,ns,e:=pfEngine(t);pfOK(t,ns,"CreateNode",&storage.Node{ID:"hub",Labels:[]string{"N"},Properties:map[string]any{"document_id":"other"}});for i:=0;i<n;i++{d:=storage.NodeID(fmt.Sprintf("d%d",i));o:=storage.NodeID(fmt.Sprintf("o%d",i));pfOK(t,ns,"CreateNode",&storage.Node{ID:d,Labels:[]string{"Doc"},Properties:map[string]any{"id":string(d)}});pfOK(t,ns,"CreateNode",&storage.Node{ID:o,Labels:[]string{"N"},Properties:map[string]any{"document_id":string(d)}});for j,pair:=range [][2]storage.NodeID{{d,o},{d,"hub"},{o,"hub"}}{pfOK(t,ns,"CreateEdge",&storage.Edge{ID:storage.EdgeID(fmt.Sprintf("e%d-%d",i,j)),StartNode:pair[0],EndNode:pair[1],Type:"LINK"})}};pfObserve(t,e,"middle_late","MATCH (d:Doc)-[l:LINK]-(o:N)-[r:LINK]-(s) WHERE o.document_id=d.id RETURN count(s)",nil);pfObserve(t,e,"middle_split","MATCH (d:Doc)-[l:LINK]-(o:N) WHERE o.document_id=d.id WITH d,l,o MATCH (o)-[r:LINK]-(s) WHERE l <> r RETURN count(s)",nil)})}}
func TestPFVolatileSpacing(t *testing.T){_,ns,e:=pfEngine(t);for i:=0;i<2000;i++{a:=storage.NodeID(fmt.Sprintf("a%d",i));m:=storage.NodeID(fmt.Sprintf("m%d",i));z:=storage.NodeID(fmt.Sprintf("z%d",i));for _,id:=range []storage.NodeID{a,m,z}{pfOK(t,ns,"CreateNode",&storage.Node{ID:id,Labels:[]string{string(id[0])},Properties:map[string]any{"keep":true}})};pfOK(t,ns,"CreateEdge",&storage.Edge{ID:storage.EdgeID(fmt.Sprintf("r%d",i)),StartNode:a,EndNode:m,Type:"R"});pfOK(t,ns,"CreateEdge",&storage.Edge{ID:storage.EdgeID(fmt.Sprintf("s%d",i)),StartNode:m,EndNode:z,Type:"R"})};for _,f:=range []string{"rand()","rand ()","rand /* comment */ ()"}{q:="MATCH (a:a)-[:R]->(m:m)-[:R]->(z:z) WHERE a.keep = ("+f+" < 0.5) RETURN count(z)";for rep:=0;rep<3;rep++{t.Logf("PF volatile form=%s rows=%v",f,pfQ(t,e,q,nil).Rows)}}}

func TestPFCorrelatedUnused(t *testing.T){_,_,e:=pfEngine(t);pfQ(t,e,"UNWIND range(1,64) AS i CREATE (:EstimatorCase {id: i})",nil);ids:=make([]any,64);for i:=range ids{ids[i]=int64(i+1)};for _,n:=range []int{8,20000}{large:=make([]any,n);for i:=range large{large[i]=map[string]any{"id":fmt.Sprint(i),"text":"unused import","depth":i}};for rep:=0;rep<3;rep++{pfObserve(t,e,fmt.Sprintf("unused_import_%d_rep%d",n,rep),"UNWIND $ids AS id MATCH (c:EstimatorCase {id: id}) CALL { WITH c RETURN count(c) AS hits } RETURN c.id AS id, hits ORDER BY id",map[string]any{"ids":ids,"unused":large})}}}


