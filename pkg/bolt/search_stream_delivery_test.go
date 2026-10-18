package bolt

import (
 "context"
 "io"
 "testing"
 "github.com/orneryd/nornicdb/pkg/cypher"
 "github.com/orneryd/nornicdb/pkg/resultstream"
 "github.com/orneryd/nornicdb/pkg/search"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestSearchStreamDeliveryBoltFailedPullAfterRunSuccess(t *testing.T) {
 base:=storage.NewMemoryEngine();t.Cleanup(func(){require.NoError(t,base.Close())})
 store:=storage.NewNamespacedEngine(base,"nornic")
 for _,id:=range []storage.NodeID{"a","b"} {_,err:=store.CreateNode(&storage.Node{ID:id});require.NoError(t,err)}
 svc:=search.NewService(store);t.Cleanup(func(){require.NoError(t,svc.Close())})
 registry,err:=resultstream.NewRegistry(resultstream.Config{MaxStreams:1});require.NoError(t,err);t.Cleanup(registry.Close)
 svc.SetContinuationRegistry(registry)
 exec:=cypher.NewStorageExecutor(store);exec.SetSearchService(svc)
 conn:=&errorConn{}
 session:=newTestSession(conn,&cypherQueryExecutor{executor:exec})
 session.connCtx=resultstream.WithDelivery(context.Background(),&session.continuationDelivery)
 require.NoError(t,session.handleRun(buildRunMessageData("CALL db.retrieve({mode: 'id', n: 1})",nil,nil)))
 require.NotEmpty(t,conn.writeData,"RUN SUCCESS was sent before PULL")
 conn.writeErr=io.ErrClosedPipe
 require.ErrorIs(t,session.handlePull(encodePackStreamMap(map[string]any{"n":int64(-1)})),io.ErrClosedPipe)
 session.continuationDelivery.Fail() // handleConnection's deferred disconnect cleanup
 _,err=svc.SearchTextContinuation(context.Background(),"",search.DefaultSearchOptions(),search.SearchContinuationRequest{Owner:"replacement",Mode:search.SearchContinuationID,N:1},nil,nil,nil,search.ChunkedSearchErrorPolicy{})
 require.NoError(t,err,"failed page PULL after successful RUN must release the only slot")
}

