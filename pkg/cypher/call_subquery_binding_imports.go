package cypher

import (
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// callBranchBindingPlan is the one invocation/import decision for a CALL
// branch. The semantic walk binds operands with it; native execution binds
// values with it; Fabric retains the walk's resulting logical frames.
// Syntax roles and first declarations remain owned by the native grammar.
type callBranchBindingPlan struct {
	inputs, globals []string
}

func planInvocationBindings[T any](query *fabric.QueryStructure, branch string, outer map[string]T, scoped bool, imports []string, expression bool, branchIndex ...int) (callBranchBindingPlan, error) {
	if scoped && imports != nil {
		return callBranchBindingPlan{inputs: imports, globals: imports}, nil
	}
	if query == nil {
		query = logicalStatementSyntax(branch)
	}
	semantic := query.SemanticStructure()
	var projection *fabric.QueryProjection
	if !scoped && !expression {
		if semantic.Starts("WITH") {
			clause := semantic.Clauses[0]
			candidate := clause.Projection
			importing := candidate != nil && candidate.Star
			for name, declares := range clause.BindingRoles() {
				_, bound := outer[name]
				importing = importing || bound && !declares
			}
			if importing {
				if len(semantic.Clauses) < 2 {
					return callBranchBindingPlan{}, localizedError(localization.CypherSubqueriesWithQueryClauseRequired(), nil)
				}
				// Later UNION branches retain NornicDB's projection extension.
				// The first branch's legacy import is a simple-reference list.
				if len(branchIndex) == 0 || branchIndex[0] == 0 {
					invalid := clause.Predicate != "" || candidate.Modifiers != ""
					for _, item := range candidate.Items {
						invalid = invalid || item.HasAlias
					}
					if invalid {
						return callBranchBindingPlan{}, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport",
							"Importing WITH must contain only simple references to outside variables")
					}
				}
				projection = candidate
			}
		}
	}
	roles := query.BindingRoles()
	var plan callBranchBindingPlan
	for name := range outer {
		if strings.HasPrefix(name, "$") {
			continue
		}
		declares, mentioned := roles[name]
		if expression && !mentioned || !expression && !scoped && (declares || !(projection != nil && projection.Star) && !mentioned) {
			continue
		}
		plan.inputs = append(plan.inputs, name)
		projected := false
		if projection != nil {
			projected = projection.Star
			for _, item := range projection.Items {
				projected = projected || item.Column == name
			}
		}
		if scoped || !projected {
			plan.globals = append(plan.globals, name)
		}
	}
	slices.Sort(plan.inputs)
	slices.Sort(plan.globals)
	return plan, nil
}

// Both binding domains use the same selection, including masking an enclosing
// frame when the CALL imports nothing. Parameters are bound separately.
func callBranchBindingValues[T any](plan callBranchBindingPlan, outer map[string]T) (map[string]T, map[string]T, string) {
	input := make(map[string]T, len(plan.inputs))
	var globals map[string]T
	if len(plan.globals) > 0 {
		globals = make(map[string]T, len(plan.globals))
	}
	for _, name := range plan.inputs {
		value, exists := outer[name]
		if !exists {
			return nil, nil, name
		}
		input[name] = value
	}
	for _, name := range plan.globals {
		globals[name] = input[name]
	}
	return input, globals, ""
}
