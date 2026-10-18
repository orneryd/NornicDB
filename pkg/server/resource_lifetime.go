package server

import (
 "context"
 "net/http"
 "github.com/orneryd/nornicdb/pkg/lifecycle"
)

func(s *Server) operationGroup()*lifecycle.Group{
 s.lifetimeOnce.Do(func(){if s.lifetime==nil{owner:=lifecycle.New();if s.db!=nil&&s.db.Lifecycle()!=nil{owner=s.db.Lifecycle()};s.lifetime=owner.NewGroup()}})
 return s.lifetime
}
func(s *Server) isStopped()bool{return s.operationGroup().Closing()}
func(s *Server) lifetimeMiddleware(next http.Handler)http.Handler{
 return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  var operation lifecycle.Scope
  if err:=operation.EnterGroup(r.Context(),s.operationGroup());err!=nil{s.writeJSON(w,http.StatusServiceUnavailable,map[string]any{"error":err.Error(),"http_code":http.StatusServiceUnavailable,"retryable":true});return}
  defer operation.Finish()
  next.ServeHTTP(w,r.WithContext(&operation))
 })
}
// Stop is a component transition on the DB owner. Every caller joins the same
// initializer/request drain, including callers that arrive during another Stop.
func(s *Server) Stop(ctx context.Context)error{return s.operationGroup().Stop(ctx,func()error{return s.stop(ctx)})}
