package cypher

import "strings"

// unwindProjectionPrecedesMutation identifies an initial UNWIND ... WITH
// horizon. A trailing WITH after MATCH/CREATE belongs to specialized bulk
// mutation executors and must not be diverted from their fast paths.
func unwindProjectionPrecedesMutation(query string) bool {
	with := findKeywordIndexInContext(query, "WITH")
	if with < 0 {
		return false
	}
	for _, keyword := range []string{"MATCH", "OPTIONAL MATCH", "CREATE", "MERGE", "SET", "DELETE"} {
		if position := findKeywordIndexInContext(query, keyword); position > 0 && position < with {
			return false
		}
	}
	return true
}

// unwindNeedsRowPipeline identifies expressions that guarded bulk plans do
// not implement. They use the canonical row evaluator instead of textual
// per-row substitution.
func unwindNeedsRowPipeline(query string) bool {
	clauses, ok := canExecuteAsPipeline(query)
	if !ok || len(clauses) == 0 || clauses[0].kind != pipelineClauseUnwind {
		return false
	}
	variable := ""
	if asIndex := findKeywordIndexInContext(clauses[0].text, "AS"); asIndex >= 0 {
		fields := strings.Fields(strings.TrimSpace(clauses[0].text[asIndex+len("AS"):]))
		if len(fields) > 0 {
			variable = fields[0]
		}
	}
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseSet, pipelineClauseCreate:
			if strings.Contains(clause.text, " + ") ||
				(variable != "" && (strings.Contains(clause.text, variable+"[") ||
					strings.Contains(clause.text, "["+variable+".") ||
					strings.Contains(clause.text, "["+variable+"]"))) {
				return true
			}
		}
	}
	return false
}

// hasMultipleUnwindClauses keeps every chained UNWIND query on the general
// row pipeline, which applies expansion horizons iteratively at any arity.
func hasMultipleUnwindClauses(query string) bool {
	clauses, ok := canExecuteAsPipeline(query)
	if !ok {
		return false
	}
	count := 0
	for _, clause := range clauses {
		if clause.kind == pipelineClauseUnwind {
			count++
		}
	}
	return count > 1
}
