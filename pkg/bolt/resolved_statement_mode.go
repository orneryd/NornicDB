package bolt

import "github.com/orneryd/nornicdb/pkg/cypher"

func (stream *resultStream) resolvedQueryType() string {
 if stream.result!=nil {
  if mode,ok:=cypher.ResolvedStatementQueryType(stream.result.Metadata); ok { return mode }
 }
 if stream.result!=nil { return boltSummaryQueryType(stream.isWrite,stream.result.Stats) }
 if stream.isWrite { return "w" }
 return "r"
}

func (stream *resultStream) resolvedWrite() bool {
 switch stream.resolvedQueryType() { case "w","rw","s": return true }
 return false
}