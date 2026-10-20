package cypher

// expressionSubqueryClauseBindingError protects an expression subquery's
// outer variables from declarations in its clauses. Passing a variable
// through WITH or RETURN, including redundant grouping, keeps its binding;
// assigning a new value to that name shadows it. CALL bodies permit
// redeclaring imports (enclosing.outer is false).
func expressionSubqueryClauseBindingError(clause pipelineClause, enclosing subqueryScope) error {
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
		return check("", clause.iterationAlias)
	case pipelineClauseWith, pipelineClauseReturn:
		for _, item := range admittedProjection(clause).Items {
			if err := check(item.Expression, item.Column); err != nil {
				return err
			}
		}
	}
	return nil
}
