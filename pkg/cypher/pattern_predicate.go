package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// patternPredicatePlan owns a pattern predicate's scope and evaluation. Its
// graph-independent syntax is compiled once; execution always uses the current
// executor and row. Complex patterns use the EXISTS owner with the whole row.
// A simple bound hop uses that owner's existing expansion, without reparsing
// the pattern or constructing an EXISTS expression for each row.
type patternPredicatePlan struct {
	pattern   string
	variables []string
	hop       *TraversalMatch
}

var patternPredicatePlans = newBoundedCache[string, *patternPredicatePlan](1024, boundedCacheStringShardIndex)

func planPatternPredicate(pattern string) *patternPredicatePlan {
	pattern = strings.TrimSpace(pattern)
	if plan, cached := patternPredicatePlans.get(pattern); cached {
		return plan
	}
	plan := &patternPredicatePlan{
		pattern:   pattern,
		variables: append(extractNodeVariables(pattern), extractRelationshipVariables(pattern)...),
	}
	// With no maps, parameters or inline predicates the traversal parser reads
	// syntax alone. A zero executor ensures the cached plan owns no database or
	// statement values. Maps (including row-dependent ones) stay with EXISTS.
	if !strings.ContainsAny(pattern, "{$") && !patternHasInlineWhere(pattern) {
		if shape := parseBoundChainShape(pattern); shape.ok && shape.where == "" {
			var parser StorageExecutor
			if hop := parser.parseTraversalPattern(context.Background(), pattern); hop != nil && !hop.IsChained && !hop.Relationship.VariableLength && hop.Relationship.MinHops == 1 && hop.Relationship.MaxHops == 1 {
				plan.hop = hop
			}
		}
	}
	patternPredicatePlans.put(pattern, plan)
	return plan
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
	if err := e.validatePatternPredicateVariables(plan.variables, func(name string) bool {
		_, bound := scope.lookup(name)
		return bound
	}); err != nil {
		return false, err
	}
	values := scope.materialize()
	if hop := plan.hop; hop != nil && chainVariablesBindEntities(hop, values) {
		if seed, _ := values[hop.StartNode.variable].(*storage.Node); seed != nil {
			if count, expanded := e.boundExpandCount(withoutLocalSubqueryScope(ctx), hop, seed, "", values, true); expanded {
				return count > 0, getExpressionFailure(ctx)
			}
		}
	}
	value, _, err := e.rowSubqueryValue(ctx, "EXISTS", plan.pattern, values)
	matched, _ := value.(bool)
	return matched, err
}
