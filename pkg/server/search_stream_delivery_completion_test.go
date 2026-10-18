package server

import (
 "context"
 "net/http"
 "net/http/httptest"
 "testing"
 "time"
 "sync/atomic"

 "github.com/orneryd/nornicdb/pkg/resultstream"
 "github.com/stretchr/testify/require"
)
type deliveryProbeStream struct{ closed atomic.Int32 }
func(s *deliveryProbeStream) Pull(context.Context,uint64,int)(*resultstream.Page,error) {
 return &resultstream.Page{Rows:[][]any{{"one"}},Next:1,HasMore:true},nil
}
func(s *deliveryProbeStream) Close()error{s.closed.Add(1);return nil}

func TestSearchStreamDeliveryHTTPCompletion(t *testing.T) {
 for _,end:=range []string{"timeout_late_publish","error_response","panic","success"} {
  t.Run(end,func(t *testing.T) {
   registry,err:=resultstream.NewRegistry(resultstream.Config{MaxStreams:1});require.NoError(t,err);t.Cleanup(registry.Close)
   scope:=resultstream.Scope{Owner:"alice",Database:"one"}
   stream:=&deliveryProbeStream{}
   resume,finished:=make(chan struct{}),make(chan struct{})
   handler:=http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
    defer close(finished)
    if end=="timeout_late_publish"{<-resume}
    ctx:=resultstream.WithDelivery(context.Background(),resultstream.DeliveryFromContext(r.Context()))
    _,err:=registry.Start(ctx,scope,stream,1)
    if err!=nil { panic(err) }
    switch end {
    case "error_response": w.WriteHeader(http.StatusInternalServerError)
    case "panic": panic("response aborted")
    default: _,_=w.Write([]byte("page"))
    }
   })
   var next http.Handler=handler
   if end=="timeout_late_publish" { next=http.TimeoutHandler(next,time.Millisecond,"busy") }
   writer:=httptest.NewRecorder()
   func(){
    defer func(){if end=="panic"{require.NotNil(t,recover())}}()
    continuationDeliveryMiddleware(next).ServeHTTP(writer,httptest.NewRequest(http.MethodPost,"/db/one/tx/commit",nil))
   }()
   if end=="timeout_late_publish" { require.Equal(t,http.StatusServiceUnavailable,writer.Code);close(resume) }
   <-finished
   if end=="success" {
    require.Zero(t,stream.closed.Load(),"durable page survives completed response")
   } else {
    require.EqualValues(t,1,stream.closed.Load())
    _,err=registry.Start(context.Background(),scope,&deliveryProbeStream{},1)
    require.NoError(t,err,"failed response makes its only slot reusable")
   }
  })
 }
}

