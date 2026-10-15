package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// expressionSubqueryClauseBindingError protects an expression subquery's
// outer variables from declarations in its clauses. Passing a variable
// through WITH or RETURN keeps its binding; assigning a new value to that
// name shadows it. CALL bodies permit redeclaring imports (enclosing.outer is false).
func expressionSubqueryClauseBindingError(clause pipelineClause, enclosing subqueryScope) error {
	if !enclosing.outer {
		return nil
	}
	check := func(expression, alias string) error {
		name := normalizeProjectionColumnName(alias)
		if simpleSemanticIdentifier(expression) != name {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
				localization.CypherCoreVariableDeclaredInOuterScope(name))
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
