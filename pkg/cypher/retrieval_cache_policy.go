package cypher

import (
 "context"
 "strings"
)

// Resolve direct typed parameter values before lookup, without evaluating an
// expression twice or reading row bindings that do not exist at this boundary.
// The result cache keeps its existing parameter, database/scope and revision
// keys; this is eligibility for that cache, not another retrieval cache.
func cacheableBoundRetrievalRequest(ctx context.Context, argument string) (bool,bool) {
 argument=strings.TrimSpace(argument)
 if !strings.HasPrefix(argument,"$") { return false,false }
 params:=getParamsFromContext(ctx)
 var value any
 resolved:=false
 scanParameterReferences(argument,func(dollar,start,end int) {
  if dollar==0&&end==len(argument) { value,resolved=params[argument[start:end]] }
 })
 if !resolved { value,resolved=resolveSetMergeSourceFromParams(params,argument[1:]) }
 if !resolved { return false,false }
 req,err:=ragProcedureRequest("db.rretrieve",[]any{value})
 if err!=nil { return false,true }
 return !readRetrievalContinuation(req).requested(),true
}

