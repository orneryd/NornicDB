package cypher

import (
	"github.com/orneryd/nornicdb/pkg/search"
	"strings"
)

// Both execution and result-cache admission interpret the evaluated request
// here. Presence of an integer n/ranked limit has meaning; an empty qid or a
// false discard alone does not turn a one-shot search into a cursor operation.
type retrievalContinuation struct {
	qid                string
	discard            bool
	n                  int
	nPresent           bool
	mode               search.SearchContinuationMode
	groupBy            string
	rankedLimit        int
	rankedLimitPresent bool
}

func readRetrievalContinuation(req map[string]any) retrievalContinuation {
	n, nPresent := toInt(req["n"])
	rankedLimit, rankedLimitPresent := toInt(firstPresent(req, "rankedLimit", "ranked_limit"))
	discard, _ := toBool(req["discard"])
	return retrievalContinuation{
		qid: stringOr(req["qid"], ""), discard: discard, n: n, nPresent: nPresent,
		mode:        search.SearchContinuationMode(lowerASCII(strings.TrimSpace(stringOr(req["mode"], "")))),
		groupBy:     stringOr(firstPresent(req, "groupBy", "group_by"), ""),
		rankedLimit: rankedLimit, rankedLimitPresent: rankedLimitPresent,
	}
}
func (r retrievalContinuation) requested() bool {
	return r.qid != "" || r.discard || r.nPresent || r.mode != "" || r.groupBy != "" || r.rankedLimitPresent
}

// retrievalResultSchemas is the shared row/page header contract for retrieval.
func retrievalResultSchemas(graph bool) [][]string {
	columns := []string{"node", "score", "rrf_score", "vector_rank", "bm25_rank", "search_method", "fallback_triggered", "fallback_reason", "query_chunks_searched", "query_chunks_total", "matches"}
	if graph {
		columns = append([]string{"kind", "node", "relationship", "start", "end"}, columns[1:]...)
	}
	return [][]string{columns, {"page"}}
}
