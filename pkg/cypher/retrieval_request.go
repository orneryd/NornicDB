package cypher

import (
 "strings"
 "github.com/orneryd/nornicdb/pkg/search"
)

// Both execution and result-cache admission interpret the evaluated request
// here. Presence of an integer n/ranked limit has meaning; an empty qid or a
// false discard alone does not turn a one-shot search into a cursor operation.
type retrievalContinuation struct {
 qid string
 discard bool
 n int
 nPresent bool
 mode search.SearchContinuationMode
 groupBy string
 rankedLimit int
 rankedLimitPresent bool
}
func readRetrievalContinuation(req map[string]any) retrievalContinuation {
 n,nPresent:=toInt(req["n"])
 rankedLimit,rankedLimitPresent:=toInt(firstPresent(req,"rankedLimit","ranked_limit"))
 discard,_:=toBool(req["discard"])
 return retrievalContinuation{
  qid:stringOr(req["qid"],""),discard:discard,n:n,nPresent:nPresent,
  mode:search.SearchContinuationMode(lowerASCII(strings.TrimSpace(stringOr(req["mode"],"")))),
  groupBy:stringOr(firstPresent(req,"groupBy","group_by"),""),
  rankedLimit:rankedLimit,rankedLimitPresent:rankedLimitPresent,
 }
}
func (r retrievalContinuation) requested() bool {
 return r.qid!=""||r.discard||r.nPresent||r.mode!=""||r.groupBy!=""||r.rankedLimitPresent
}

