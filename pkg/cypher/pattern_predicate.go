package cypher

import (
	"context"
)

// patternPredicatePlan owns a pattern predicate's scope and evaluation. Its
// graph-independent syntax is compiled once; execution always uses the current
// executor and row. Complex patterns use the EXISTS owner with the whole row.
// A simple bound hop uses that owner's existing expansion, without reparsing
// the pattern or constructing an EXISTS expression for each row.
type patternPredicatePlan struct {
	variables []string
	graph *graphExpressionPlan
}

func planPatternPredicate(pattern string) *patternPredicatePlan {
	graph := planGraphExpression(graphPatternPredicate, pattern, "")
	return &patternPredicatePlan{variables: graph.variables, graph: graph}
}

// validatePatternPredicateVariables is shared by static scope validation and
// the runtime owner. An absent name is a SyntaxError, never an existential
// local; anonymous endpoints and explicit EXISTS bodies can bind locally.
func (e *StorageExecutor) validatePatternPredicateVariables(variables []string, bound func(string) bool) error {
	for _, variable := range variables {
		if bound(variable) {
			continue
		}
		if _, externallyBound := e.fabricRecordBindings[variable]; !externallyBound {
			return createUndefinedVariableError(variable)
		}
	}
	return nil
}

func (plan *patternPredicatePlan) evaluate(e *StorageExecutor, ctx context.Context, scope compiledRowScope) (bool, error) {
	value, _, err := plan.graph.evaluate(e, ctx, scope)
	matched, _ := value.(bool)
	return matched, err
}
