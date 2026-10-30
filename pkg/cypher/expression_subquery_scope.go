// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import "context"

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
		name := normalizeProjectionColumnName(alias)
		if simpleSemanticIdentifier(unwrapOuterParens(expression)) != name {
			return enclosing.redeclarationError(name)
		}
		return nil
	}
	switch clause.kind {
	case pipelineClauseUnwind:
		return check("", unwindBindingName(clause))
	case pipelineClauseWith, pipelineClauseReturn:
		for _, itemSource := range projectionItemsFromSyntax(clause.syntax) {
			expression, alias := itemSource.expression, itemSource.alias
			if err := check(expression, alias); err != nil {
				return err
			}
		}
	}
	return nil
}
