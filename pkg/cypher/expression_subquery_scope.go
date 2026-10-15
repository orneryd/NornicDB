package cypher

import "strings"

// expressionSubqueryClauseBindingError protects an expression subquery's
// outer variables from declarations in its clauses. Passing a variable
// through WITH or RETURN, including redundant grouping, keeps its binding;
// assigning a new value shadows it. CALL retains its imported-name rebinding extension.
func expressionSubqueryClauseBindingError(clause pipelineClause, enclosing subqueryScope) error {
	if !enclosing.outer || len(enclosing.visible) == 0 {
		return nil
	}
	check := func(expression, alias string) error {
		name := normalizeProjectionColumnName(alias)
		if simpleSemanticIdentifier(unwrapOuterParens(expression)) != name {
			return enclosing.redeclarationError(name)
		}
		return nil
	}
	switch clause.kind {
	case pipelineClauseUnwind:
		return check("", unwindBindingName(clause.text))
	case pipelineClauseWith, pipelineClauseReturn:
		keyword := "WITH"
		if clause.kind == pipelineClauseReturn {
			keyword = "RETURN"
		}
		projection, _ := projectionSemanticBodyAndTail(clause.text, keyword)
		projection, _ = cutDistinct(strings.TrimSpace(projection))
		for _, raw := range splitTopLevelComma(projection) {
			expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
			if err := check(expression, alias); err != nil {
				return err
			}
		}
	}
	return nil
}
