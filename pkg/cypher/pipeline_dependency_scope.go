package cypher

import "strings"

// pipelineIndependent proves that a statement shortcut can omit imports.
// Name occurrences alone are insufficient: projections consume scope through
// RETURN * and WITH *. Expand them with the same typeScope projection owner
// used by semanticScopeWalk and CALL return typing. This is deliberately a
// proof for the read/projection clauses the shortcuts implement, rather than
// another scope interpreter. Unknown clauses and nested subqueries (whose
// bodies can consume imports implicitly too) keep the general row pipeline.
func (imports typeScope) pipelineIndependent(clauses []pipelineClause) bool {
	if len(imports) == 0 {
		return true
	}
	if len(clauses) == 0 {
		return false
	}
	for _, clause := range clauses {
		if mayContainSubqueryExpression(clause.text) {
			return false
		}
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch:
		case pipelineClauseReturn, pipelineClauseWith:
			projection := clause.text
			if clause.kind == pipelineClauseReturn {
				projection = "WITH " + strings.TrimSpace(projection[len("RETURN"):])
			}
			consumed := false
			imports.forEachProjectedBinding(projection, func(_ string, _ string, inherited bool) {
				consumed = consumed || inherited
			})
			if consumed {
				return false
			}
		default:
			return false
		}
		for name := range imports {
			if isIdentifierReferenced(clause.text, name) {
				return false
			}
		}
	}
	return true
}
