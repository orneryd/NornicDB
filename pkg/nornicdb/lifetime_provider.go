package nornicdb

import (
 "context"
 "github.com/orneryd/nornicdb/pkg/cypher"
 "github.com/orneryd/nornicdb/pkg/embed"
 "github.com/orneryd/nornicdb/pkg/lifecycle"
)

type managedProvider struct {source embed.Embedder;resource *lifecycle.Resource}
func(m *managedProvider) unmanaged()embed.Embedder{return m.source}
func(m *managedProvider) Close()error{return m.resource.Release()}
func providerCall[T any](m *managedProvider,ctx context.Context,call func(context.Context)(T,error))(T,error){
 var zero T;var scope lifecycle.Scope
 if err:=m.resource.Enter(ctx,&scope);err!=nil{return zero,err};defer scope.Finish()
 value,err:=call(&scope)
 if shutdown:=scope.Check();shutdown!=nil{return zero,shutdown}
 return value,lifecycle.Error(&scope,err)
}
func providerRead[T any](m *managedProvider,read func()T)T{
 var zero T;var scope lifecycle.Scope
 if err:=m.resource.Enter(context.Background(),&scope);err!=nil{return zero};defer scope.Finish();return read()
}
func(m *managedProvider) Embed(ctx context.Context,text string)([]float32,error){return providerCall(m,ctx,func(ctx context.Context)([]float32,error){return m.source.Embed(ctx,text)})}
func(m *managedProvider) EmbedBatch(ctx context.Context,texts []string)([][]float32,error){return providerCall(m,ctx,func(ctx context.Context)([][]float32,error){return m.source.EmbedBatch(ctx,texts)})}
func(m *managedProvider) ChunkText(text string,size,overlap int)([]string,error){return providerCall(m,context.Background(),func(context.Context)([]string,error){return m.source.ChunkText(text,size,overlap)})}
func(m *managedProvider) Model()string{return providerRead(m,m.source.Model)}
func(m *managedProvider) Dimensions()int{return providerRead(m,m.source.Dimensions)}
func(m *managedProvider) Backend()string{return providerRead(m,m.source.Backend)}
func(m *managedProvider) UsesDocumentProperties()bool{return providerRead(m,func()bool{p,ok:=m.source.(embed.DocumentPropertyChunkEmbedder);return ok&&p.UsesDocumentProperties()})}

type managedTyped struct {managed *managedProvider}
func(p managedTyped) EmbedWithInputType(ctx context.Context,text,kind string)([]float32,error){return providerCall(p.managed,ctx,func(ctx context.Context)([]float32,error){return p.managed.source.(embed.TypedEmbedder).EmbedWithInputType(ctx,text,kind)})}
func(p managedTyped) EmbedBatchWithInputType(ctx context.Context,texts []string,kind string)([][]float32,error){return providerCall(p.managed,ctx,func(ctx context.Context)([][]float32,error){return p.managed.source.(embed.TypedEmbedder).EmbedBatchWithInputType(ctx,texts,kind)})}
type managedDocumentChunks struct {managed *managedProvider}
func(p managedDocumentChunks) EmbedDocumentChunks(ctx context.Context,text string,size,overlap int)(*embed.DocumentChunkResult,error){return providerCall(p.managed,ctx,func(ctx context.Context)(*embed.DocumentChunkResult,error){return p.managed.source.(embed.DocumentChunkEmbedder).EmbedDocumentChunks(ctx,text,size,overlap)})}
type managedPropertyChunks struct {managed *managedProvider}
func(p managedPropertyChunks) EmbedDocumentPropertyChunks(ctx context.Context,text string,props map[string]any,size,overlap int)(*embed.DocumentChunkResult,error){return providerCall(p.managed,ctx,func(ctx context.Context)(*embed.DocumentChunkResult,error){return p.managed.source.(embed.DocumentPropertyChunkEmbedder).EmbedDocumentPropertyChunks(ctx,text,props,size,overlap)})}
type managedDocumentBatch struct {managed *managedProvider}
func(p managedDocumentBatch) EmbedDocumentBatchChunks(ctx context.Context,texts []string,size,overlap int)([]*embed.DocumentChunkResult,error){return providerCall(p.managed,ctx,func(ctx context.Context)([]*embed.DocumentChunkResult,error){return p.managed.source.(embed.DocumentBatchChunkEmbedder).EmbedDocumentBatchChunks(ctx,texts,size,overlap)})}
type managedPropertyBatch struct {managed *managedProvider}
func(p managedPropertyBatch) EmbedDocumentPropertyBatchChunks(ctx context.Context,texts []string,props []map[string]any,size,overlap int)([]*embed.DocumentChunkResult,error){return providerCall(p.managed,ctx,func(ctx context.Context)([]*embed.DocumentChunkResult,error){return p.managed.source.(embed.DocumentPropertyBatchChunkEmbedder).EmbedDocumentPropertyBatchChunks(ctx,texts,props,size,overlap)})}
type managedEmbeddingSpace struct {managed *managedProvider}
func(p managedEmbeddingSpace) EmbeddingSpace()string{return providerRead(p.managed,p.managed.source.(embed.EmbeddingSpaceProvider).EmbeddingSpace)}

func(db *DB) ownQueryProvider(provider cypher.QueryEmbedder)cypher.QueryEmbedder{
 if provider==nil{return nil}
 if _,ok:=provider.(interface{unmanaged()embed.Embedder});ok{return provider}
 if full,ok:=provider.(embed.Embedder);ok{
  r,accepted:=db.ownProvider(full);if !accepted{return nil};return newManagedProvider(full,r)
 }
 r,accepted:=db.lifetime.Own(provider,func()error{if c,ok:=provider.(interface{Close()error});ok{return c.Close()};return nil})
 if !accepted{return nil}
 return &managedQueryProvider{source:provider,resource:r}
}
type managedQueryProvider struct {source cypher.QueryEmbedder;resource *lifecycle.Resource}
func(m *managedQueryProvider) Embed(ctx context.Context,text string)([]float32,error){
 var scope lifecycle.Scope;if err:=m.resource.Enter(ctx,&scope);err!=nil{return nil,err};defer scope.Finish()
 v,err:=m.source.Embed(&scope,text);if ended:=scope.Check();ended!=nil{return nil,ended};return v,lifecycle.Error(&scope,err)
}
func(m *managedQueryProvider) ChunkText(text string,size,overlap int)([]string,error){
 var scope lifecycle.Scope;if err:=m.resource.Enter(context.Background(),&scope);err!=nil{return nil,err};defer scope.Finish();return m.source.ChunkText(text,size,overlap)
}
