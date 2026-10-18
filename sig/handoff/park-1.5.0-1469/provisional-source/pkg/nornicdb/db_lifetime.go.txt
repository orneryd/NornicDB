package nornicdb

import (
 "context"
 "reflect"
 "github.com/orneryd/nornicdb/pkg/embed"
 "github.com/orneryd/nornicdb/pkg/lifecycle"
)

func sameEmbedder(a,b embed.Embedder) bool {a=nativeEmbedder(a);b=nativeEmbedder(b);return a!=nil&&reflect.TypeOf(a).Comparable()&&a==b}
func nativeEmbedder(provider embed.Embedder) embed.Embedder {
 for provider!=nil {
  if managed,ok:=provider.(interface{unmanaged() embed.Embedder});ok {provider=managed.unmanaged();continue}
  alias,ok:=provider.(interface{Unwrap() embed.Embedder});if !ok {break};provider=alias.Unwrap()
 }
 return provider
}
func(db *DB) ownProvider(provider embed.Embedder)(*lifecycle.Resource,bool) {
 native:=nativeEmbedder(provider)
 if native==nil {return nil,false}
 var identity any
 if reflect.TypeOf(native).Comparable(){identity=native}
 return db.lifetime.Own(identity,func()error{
  if closer,ok:=native.(interface{Close()error});ok{return closer.Close()};return nil
 })
}
// OwnEmbedder transfers a constructed model to the DB before checking health.
// Every alias and publication shares its generation's drain and release result.
func(db *DB) OwnEmbedder(provider embed.Embedder)(func()error,bool) {
 resource,accepted:=db.ownProvider(provider)
 if resource==nil{return func()error{return nil},false}
 return resource.Release,accepted
}
// Lifecycle returns the DB's single resource/activity owner. Components attach
// their scopes and task groups here rather than implementing another shutdown.
func(db *DB) Lifecycle()*lifecycle.Owner{return db.lifetime}
// BeginOperation admits a public adapter's complete DB operation, including
// storage work after embedding. The caller must defer scope.Finish().
func(db *DB) BeginOperation(ctx context.Context)(*lifecycle.Scope,error){
 scope:=new(lifecycle.Scope);if err:=scope.Enter(ctx,db.lifetime);err!=nil{return nil,err};return scope,nil
}
func(db *DB) isClosed()bool{return db.lifetime!=nil&&db.lifetime.Closing()}
func(db *DB) backgroundScope()(*lifecycle.Scope,error){
 s:=new(lifecycle.Scope);if err:=s.EnterTask(db.background.Context(),db.lifetime);err!=nil{return nil,err};return s,nil
}
