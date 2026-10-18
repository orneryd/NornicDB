package cypher

import (
 "context"
 "testing"
 "time"
 "github.com/orneryd/nornicdb/pkg/resultstream"
 "github.com/orneryd/nornicdb/pkg/search"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestSearchStreamParameterAwareCache(t *testing.T) {
 store:=storage.NewNamespacedEngine(newTestMemoryEngine(t),"bench")
 for _,id:=range []storage.NodeID{"a","b"} {
  _,err:=store.CreateNode(&storage.Node{ID:id,Properties:map[string]any{"content":"library transcript"}});require.NoError(t,err)
 }
 svc:=search.NewService(store);t.Cleanup(func(){require.NoError(t,svc.Close())})
 require.NoError(t,svc.BuildIndexes(context.Background()))
 registry,err:=resultstream.NewRegistry(resultstream.Config{MaxStreams:1});require.NoError(t,err);t.Cleanup(registry.Close)
 svc.SetContinuationRegistry(registry)
 exec:=NewStorageExecutorWithQueryCachePolicy(store,64,time.Hour);exec.SetSearchService(svc)
 run:=func(ctx context.Context,request map[string]any)(*ExecuteResult,error){return exec.Execute(ctx,"CALL db.rretrieve($request)",map[string]any{"request":request})}
 ctx:=WithAuthenticatedPrincipal(context.Background(),"alice")
 plain:=map[string]any{"query":"transcript","limit":int64(2)}
 first,err:=run(ctx,plain);require.NoError(t,err);require.Len(t,first.Rows,2)
 again,err:=run(ctx,plain);require.NoError(t,err);require.Same(t,first,again,"one-shot request should hit result cache")
 empty,err:=run(ctx,map[string]any{"query":"nonexistent","limit":int64(2)});require.NoError(t,err);require.Empty(t,empty.Rows)
 start:=map[string]any{"query":"transcript","limit":int64(2),"n":int64(1)}
 started,err:=run(ctx,start);require.NoError(t,err)
 qid:=started.Rows[0][0].(map[string]any)["qid"].(string);require.NotEmpty(t,qid)
 _,err=run(WithAuthenticatedPrincipal(context.Background(),"bob"),map[string]any{"qid":qid,"n":int64(1)});require.Error(t,err)
 pull:=map[string]any{"qid":qid,"n":int64(1)}
 last,err:=run(ctx,pull);require.NoError(t,err);require.False(t,last.Rows[0][0].(map[string]any)["has_more"].(bool))
 _,err=run(ctx,pull);require.Error(t,err,"terminal PULL must reach registry")
 for i:=0;i<2;i++ {_,err=run(ctx,map[string]any{"qid":qid,"discard":true});require.NoError(t,err)}
 next,err:=run(ctx,start);require.NoError(t,err)
 nextQID:=next.Rows[0][0].(map[string]any)["qid"].(string);require.NotEqual(t,qid,nextQID,"START must allocate a new stream")
}
