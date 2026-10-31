package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: procedure headers are read from their registration.

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

// retrievalResultSchemas splits a retrieval procedure's declared columns
// into its two closed headers: the result rows, and the page alone.
func retrievalResultSchemas(returns []ProcedureColumn) [][]string {
	var rows, page []string
	for _, column := range returns {
		if column.Name == "page" {
			page = append(page, column.Name)
		} else {
			rows = append(rows, column.Name)
		}
	}
	return [][]string{rows, page}
}
