package cypher

import (
 "context"
 "testing"
 "time"
 "github.com/orneryd/nornicdb/pkg/search"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func solR2CacheExecutor(t *testing.T) *StorageExecutor {
 t.Helper()
 store:=storage.NewNamespacedEngine(newTestMemoryEngine(t),"solr2")
 for _,id:=range []storage.NodeID{"a","b"} {
  _,err:=store.CreateNode(&storage.Node{ID:id,Properties:map[string]any{"content":"library transcript", "kind":string(id)}});require.NoError(t,err)
 }
 svc:=search.NewService(store);t.Cleanup(func(){require.NoError(t,svc.Close())})
 require.NoError(t,svc.BuildIndexes(context.Background()))
 exec:=NewStorageExecutorWithQueryCachePolicy(store,64,time.Hour);exec.SetSearchService(svc)
 return exec
}

func TestSolReviewR2CacheParameterInputs(t *testing.T) {
 exec:=solR2CacheExecutor(t)
 query:="CALL db.rretrieve($request)"
 for _,tc:=range []struct{request map[string]any; kind string; count int}{
  {map[string]any{"query":"transcript","limit":int64(1)},"",1},
  {map[string]any{"query":"transcript","limit":int64(2)},"",2},
  {map[string]any{"query":"missing","limit":int64(2)},"",0},
  {map[string]any{"query":"transcript","where":"node.kind = $kind"},"a",1},
  {map[string]any{"query":"transcript","where":"node.kind = $kind"},"absent",0},
 } {
  params:=map[string]any{"request":tc.request,"kind":tc.kind}
  result,err:=exec.Execute(context.Background(),query,params);require.NoError(t,err);require.Len(t,result.Rows,tc.count)
 }
 for _,query:=range []string{"RETURN rand() AS value","CREATE (n:SolCacheWrite) RETURN n"} {
  first,err:=exec.Execute(context.Background(),query,nil);require.NoError(t,err)
  second,err:=exec.Execute(context.Background(),query,nil);require.NoError(t,err)
  require.NotSame(t,first,second,"non-deterministic and write statements must execute")
 }
}

func TestSolReviewR2CacheParameterizedTimePredicate(t *testing.T) {
 exec:=solR2CacheExecutor(t)
 query:="CALL db.rretrieve($request)"
 deadline:=time.Now().Add(time.Second)
 params:=map[string]any{"request":map[string]any{"query":"transcript","where":"timestamp() < $deadline"},"deadline":deadline.UnixMilli()}
 first,err:=exec.Execute(context.Background(),query,params);require.NoError(t,err);require.Len(t,first.Rows,2)
 time.Sleep(time.Until(deadline.Add(100*time.Millisecond)))
 second,err:=exec.Execute(context.Background(),query,params);require.NoError(t,err)
 t.Logf("same cached result=%t; before=%d after=%d",first==second,len(first.Rows),len(second.Rows))
 require.Empty(t,second.Rows,"the parameter-supplied time predicate must be evaluated again after its deadline")
}
