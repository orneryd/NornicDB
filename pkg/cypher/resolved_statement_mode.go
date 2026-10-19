package cypher

import (
 "context"
 "sync/atomic"
)

type resolvedStatementModeKey struct{}
type resolvedStatementMode struct{ write atomic.Bool }
const resolvedStatementModeMetadata = "resolved_query_type"

// ResolvedStatementQueryType is the execution owner's mode, when available.
// Protocol adapters share this reader; no transport reclassifies argument text.
func ResolvedStatementQueryType(metadata map[string]interface{}) (string,bool) {
 mode,ok := metadata[resolvedStatementModeMetadata].(string)
 switch mode { case "r","w","rw","s": return mode,ok }
 return "",false
}

func resolvedStatementModeResult(result *ExecuteResult, write bool) *ExecuteResult {
 if result == nil { return nil }
 copy := *result
 copy.Metadata=make(map[string]interface{},len(result.Metadata)+1)
 for key,value := range result.Metadata { copy.Metadata[key]=value }
 mode:="r"; if write { mode="w" }
 copy.Metadata[resolvedStatementModeMetadata]=mode
 return &copy
}

// Only CALL results need a resolved procedure mode beyond the ordinary query
// summary. Conditional procedures share an observation with evaluated nested
// execution; fixed procedure contracts need no extra context allocation.
func (e *StorageExecutor) observeResolvedStatementMode(ctx context.Context,info *QueryInfo,statement string) (context.Context,func(*ExecuteResult)*ExecuteResult) {
 if !info.HasCall { return ctx,nil }
 state,_ := ctx.Value(resolvedStatementModeKey{}).(*resolvedStatementMode)
 if state == nil && callsDynamicStatementProcedure(statement) {
  state=&resolvedStatementMode{}
  ctx=context.WithValue(ctx,resolvedStatementModeKey{},state)
 }
 fixedWrite:=statementWritesGraph(info,statement)
 return ctx,func(result *ExecuteResult)*ExecuteResult {
  write:=fixedWrite || state!=nil && state.write.Load()
  return resolvedStatementModeResult(result,write)
 }
}