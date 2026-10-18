package server

import (
 "context"
 "io"
 "net/http"

 "github.com/orneryd/nornicdb/pkg/resultstream"
)

type searchDeliveryWriter struct {
 http.ResponseWriter
 delivery *resultstream.Delivery
}
func (w searchDeliveryWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w searchDeliveryWriter) Write(data []byte)(int,error) {
 n,err:=w.ResponseWriter.Write(data)
 if err==nil&&n!=len(data) { err=io.ErrShortWrite }
 if err!=nil { w.delivery.Fail() }
 return n,err
}
func (w searchDeliveryWriter) WriteHeader(status int) {
 if status>=http.StatusBadRequest { w.delivery.Fail() }
 w.ResponseWriter.WriteHeader(status)
}
func (w searchDeliveryWriter) Flush() {
 if err:=http.NewResponseController(w.ResponseWriter).Flush();err!=nil { w.delivery.Fail() }
}

// All HTTP producers share the final response boundary, outside any timeout
// buffers. This includes Cypher transactions, GraphQL and tool adapters as well
// as the direct search route.
func continuationDeliveryMiddleware(next http.Handler) http.Handler {
 return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
  delivery:=&resultstream.Delivery{}
  failed:=make(chan struct{})
  stop:=context.AfterFunc(r.Context(),func(){ defer close(failed);delivery.Fail() })
  stopped:=false
  defer func(){
   if !stopped&&!stop() { <-failed }
   delivery.Fail()
  }()
  r=r.WithContext(resultstream.WithDelivery(r.Context(),delivery))
  next.ServeHTTP(searchDeliveryWriter{w,delivery},r)
  stopped=stop()
  if !stopped { <-failed;return }
  if r.Context().Err()==nil { delivery.Delivered() }
 })
}
