package cypher

import (
 "context"
 "fmt"
 "sort"
 "testing"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func ei4R5Exec(t testing.TB, setup ...string) *StorageExecutor {
 t.Helper()
 e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5"), QueryCachePolicy{MaxEntries:0, TTL:0})
 for _, q := range setup { _,err:=e.Execute(context.Background(),q,nil); require.NoError(t,err,q) }
 return e
}
func ei4R5Rows(t *testing.T,e *StorageExecutor,q string,p map[string]interface{}) []string {
 t.Helper(); r,err:=e.Execute(context.Background(),q,p); require.NoError(t,err,q)
 out:=make([]string,0,len(r.Rows));for _,row:=range r.Rows {out=append(out,fmt.Sprint(row...))};sort.Strings(out);return out
}
func TestEI4R5StaleIndexRoutes(t *testing.T) {
 for _,route:=range []string{"match","optional","traversal","end","call","unwind"} {t.Run(route,func(t *testing.T){
 e:=ei4R5Exec(t,"CREATE INDEX r5_s_a FOR (n:R5SA) ON (n.k)","CREATE INDEX r5_s_b FOR (n:R5SB) ON (n.k)","CREATE (a:R5SA:R5SB {id:'a',k:'hit'})-[:R5SR]->(b:R5SE {id:'b'})")
 s:=e.storage.GetSchema();ids,err:=s.PropertyIndexLookup(context.Background(),"R5SB","k","hit");require.NoError(t,err);require.Len(t,ids,1);require.NoError(t,s.PropertyIndexDelete("R5SB","k",ids[0],"hit"))
 q:=map[string]string{
 "match":"MATCH (n:R5SA:R5SB) WHERE n.k='hit' RETURN n.id",
 "optional":"OPTIONAL MATCH (n:R5SA:R5SB) WHERE n.k='hit' RETURN n.id",
 "traversal":"MATCH (n:R5SA:R5SB)-[:R5SR]->(b) WHERE n.k='hit' RETURN n.id",
 "end":"MATCH (b:R5SE)<-[:R5SR]-(n:R5SA:R5SB {k:'hit'}) RETURN n.id",
 "call":"CALL { MATCH (n:R5SA:R5SB) WHERE n.k='hit' RETURN n.id AS id } RETURN id",
 "unwind":"UNWIND ['hit'] AS k MATCH (n:R5SA:R5SB) WHERE n.k=k RETURN n.id",
 }[route]
 got:=ei4R5Rows(t,e,q,nil);t.Logf("q=%s actual=%v Neo4j=[a]",q,got);require.Equal(t,[]string{"a"},got)
 })}
}

func TestEI4R5BatchMatrix(t *testing.T) {
 for _,write:=range []string{"CREATE","MERGE"} {for _,listed:=range []int{0,1,2,3}{t.Run(fmt.Sprintf("%s/listed%d",write,listed),func(t *testing.T){
 e:=ei4R5Exec(t,"CREATE INDEX r5_batch_k FOR (n:R5Batch) ON (n.k)","CREATE (:R5Batch {k:'dup',id:'a'}),(:R5Batch {k:'dup',id:'b'}),(:R5Batch {k:'dup',id:'c'}),(:R5Batch {k:'target',id:'t'})")
 s:=e.storage.GetSchema();ids,err:=s.PropertyIndexLookup(context.Background(),"R5Batch","k","dup");require.NoError(t,err);for _,id:=range ids[listed:] {require.NoError(t,s.PropertyIndexDelete("R5Batch","k",id,"dup"))}
 q:="UNWIND $rows AS row MATCH (a:R5Batch {k:row.a}) MATCH (b:R5Batch {k:row.b}) "+write+" (a)-[:R5BatchR]->(b) RETURN a.id"
 got:=ei4R5Rows(t,e,q,map[string]interface{}{"rows":[]interface{}{map[string]interface{}{"a":"dup","b":"target"},map[string]interface{}{"a":"absent","b":"target"}}})
 t.Logf("listed=%d actual=%v Neo4j=[a b c]",listed,got);require.Equal(t,[]string{"a","b","c"},got)
 })}}
}


func TestEI4R5NativeBatch(t *testing.T){
 for _,write:=range []string{"create","merge"}{for _,listed:=range []int{0,1,2,3}{t.Run(fmt.Sprintf("%s/%d",write,listed),func(t *testing.T){
 e:=ei4R5Exec(t,"CREATE INDEX r5nb_k FOR (n:R5NB) ON (n.k)","CREATE (:R5NB {k:'dup',id:'a'}),(:R5NB {k:'dup',id:'b'}),(:R5NB {k:'dup',id:'c'}),(:R5NB {k:'target',id:'t'})")
 s:=e.storage.GetSchema();ids,err:=s.PropertyIndexLookup(context.Background(),"R5NB","k","dup");require.NoError(t,err);for _,id:=range ids[listed:]{require.NoError(t,s.PropertyIndexDelete("R5NB","k",id,"dup"))}
 p:=map[string]interface{}{"rows":[]interface{}{map[string]interface{}{"a":"dup","b":"target","uuid":"edge","embedding":[]float64{1,0,0,0}},map[string]interface{}{"a":"absent","b":"target","uuid":"missing","embedding":[]float64{1,0,0,0}}}}
 q:="UNWIND $rows AS row MATCH (a:R5NB {k:row.a}) MATCH (b:R5NB {k:row.b}) CREATE (a)-[:R5NBR]->(b) RETURN count(*)"
 if write=="merge"{q=`UNWIND $rows AS row MATCH (a:R5NB {k:row.a}) MATCH (b:R5NB {k:row.b}) MERGE (a)-[rel:R5NBR {uuid:row.uuid}]->(b) SET rel=row WITH rel,row CALL db.create.setRelationshipVectorProperty(rel,"embedding",row.embedding) RETURN row.uuid`}
 got:=ei4R5Rows(t,e,q,p);t.Logf("write=%s listed=%d result=%v trace=%+v",write,listed,got,e.LastHotPathTrace())
 edges:=ei4R5Rows(t,e,"MATCH (:R5NB)-[r:R5NBR]->(:R5NB) RETURN count(r)",nil);t.Logf("relationships=%v Neo4j=[3]",edges);require.Equal(t,[]string{"3"},edges)
 })}}
}

func TestEI4R5MergeIndexStates(t *testing.T){
 for _,first:=range []bool{false,true}{for _,missing:=range []bool{false,true}{
 e:=ei4R5Exec(t,"CREATE INDEX r5m_late FOR (n:R5MLate) ON (n.id)","CREATE (:R5MFirst:R5MLate {id:'target',broad:'hot'})","UNWIND range(1,100) AS i CREATE (:R5MFirst {id:toString(i),broad:'hot'})")
 if first{_,err:=e.Execute(context.Background(),"CREATE INDEX r5m_first FOR (n:R5MFirst) ON (n.id)",nil);require.NoError(t,err)}
 label:="R5MLate";if first{label="R5MFirst"}
 if missing{s:=e.storage.GetSchema();ids,err:=s.PropertyIndexLookup(context.Background(),label,"id","target");require.NoError(t,err);require.Len(t,ids,1);require.NoError(t,s.PropertyIndexDelete(label,"id",ids[0],"target"))}
 got:=ei4R5Rows(t,e,"MERGE (n:R5MFirst:R5MLate {id:'target',broad:'hot'}) RETURN n.id",nil);n:=ei4R5Rows(t,e,"MATCH (n:R5MFirst:R5MLate) RETURN count(n)",nil)
 t.Logf("firstIndex=%v missingEntry=%v rows=%v nodes=%v Neo4j count=[1]",first,missing,got,n);require.Equal(t,[]string{"1"},n)
 }}
}
