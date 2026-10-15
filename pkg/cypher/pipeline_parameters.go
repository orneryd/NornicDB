package cypher

import "strings"

// bindPipelineParameters leaves expression clauses' parameters as typed row
// bindings. Formatting a large list/map as a literal makes projection and
// clause readers repeatedly scan its contents. Pattern/procedure owners that
// still consume literals retain their substitution. A CALL body is bound by
// the same owner when its branch runs; its transaction modifiers bind here.
// The original clause slice remains available for result column names.
func (e *StorageExecutor) bindPipelineParameters(clauses []pipelineClause, params map[string]interface{}) []pipelineClause {
	if len(params) == 0 {
		return clauses
	}
	var bound []pipelineClause
	for index, clause := range clauses {
		text := clause.text
		switch clause.kind {
		case pipelineClauseWith, pipelineClauseUnwind, pipelineClauseReturn, pipelineClauseLet, pipelineClauseFilter:
			continue
		case pipelineClauseCallSubquery:
			body, _, _, _ := e.parseCallSubquery(text)
			if body != "" {
				start := strings.Index(text, body)
				if start >= 0 {
					text = e.substituteParamsExcept(text[:start], params, isLongStringParam) + body + e.substituteParamsExcept(text[start+len(body):], params, isLongStringParam)
					break
				}
			}
			fallthrough
		default:
			text = e.substituteParamsExcept(text, params, isLongStringParam)
		}
		if text == clause.text {
			continue
		}
		if bound == nil {
			bound = append([]pipelineClause(nil), clauses...)
		}
		bound[index].text = text
	}
	if bound != nil {
		return bound
	}
	return clauses
}
