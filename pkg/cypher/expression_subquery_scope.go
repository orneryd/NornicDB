package cypher

import "context"

import "strings"

// expressionSubqueryClauseBindingError protects an expression subquery's
// outer variables from declarations in its clauses. Passing a variable
// through WITH or RETURN, including redundant grouping, keeps its binding;
// assigning a new value to that name shadows it. CALL bodies permit
// redeclaring imports (enclosing.outer is false).
func expressionSubqueryClauseBindingError(ctx context.Context, clause pipelineClause, enclosing subqueryScope) error {
	if !enclosing.outer {
		return nil
	}
	check := func(expression, alias string) error {
		name := alias
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
			itemSource := readProjectionItemSource(ctx, strings.TrimSpace(raw), projectionAdmitted)
			expression, alias := itemSource.expression, itemSource.alias
			if err := check(expression, alias); err != nil {
				return err
			}
		}
	}
	return nil
}
