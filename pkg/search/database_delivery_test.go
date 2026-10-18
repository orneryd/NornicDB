package search

import (
 "context"
 "testing"
 "github.com/orneryd/nornicdb/pkg/resultstream"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)
func TestSearchStreamDeliveryDatabaseDiscardDuringPreparation(t *testing.T) {
 base:=storage.NewMemoryEngine();t.Cleanup(func(){require.NoError(t,base.Close())})
 store:=storage.NewNamespacedEngine(base,"one")
 for _,id:=range []storage.NodeID{"a","b","c"} { _,err:=store.CreateNode(&storage.Node{ID:id});require.NoError(t,err) }
 svc:=NewService(store);t.Cleanup(func(){require.NoError(t,svc.Close())})
 registry,err:=resultstream.NewRegistry(resultstream.Config{MaxStreams:1});require.NoError(t,err);t.Cleanup(registry.Close)
 svc.SetContinuationRegistry(registry)
 entered,resume:=make(chan struct{}),make(chan struct{})
 done:=make(chan error,1)
 go func(){
  _,err:=svc.SearchTextContinuation(context.Background(),"transcript",DefaultSearchOptions(),
   SearchContinuationRequest{Owner:"alice",Database:"one",Mode:SearchContinuationRanked,N:1},nil,nil,
   func(context.Context,string,[]float32,*SearchOptions)(*SearchResponse,error){
    close(entered);<-resume
    return &SearchResponse{Results:[]SearchResult{{ID:"a"},{ID:"b"},{ID:"c"}},SearchMethod:"fulltext"},nil
   },ChunkedSearchErrorPolicy{})
  done<-err
 }()
 <-entered
 registry.DiscardDatabase("one")
 close(resume)
 require.ErrorIs(t,<-done,resultstream.ErrInvalidated,"a prepared old service cannot publish after database reset/drop")
 // The old service must stay invalid even if a later request begins after reset.
 _,err=svc.SearchTextContinuation(context.Background(),"",DefaultSearchOptions(),SearchContinuationRequest{Owner:"alice",Database:"one",Mode:SearchContinuationID,N:1},nil,nil,nil,ChunkedSearchErrorPolicy{})
 require.ErrorIs(t,err,resultstream.ErrInvalidated)
 fresh:=NewService(store);t.Cleanup(func(){require.NoError(t,fresh.Close())});fresh.SetContinuationRegistry(registry)
 page,err:=fresh.SearchTextContinuation(context.Background(),"",DefaultSearchOptions(),SearchContinuationRequest{Owner:"alice",Database:"one",Mode:SearchContinuationID,N:1},nil,nil,nil,ChunkedSearchErrorPolicy{})
 require.NoError(t,err);require.NotEmpty(t,page.QID,"replacement service owns the available slot")
}

