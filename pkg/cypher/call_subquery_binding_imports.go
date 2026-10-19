package cypher

import (
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// callBranchBindingPlan is the one invocation/import decision for a CALL
// branch. The semantic walk binds operands with it; native execution binds
// values with it; Fabric retains the walk's resulting logical frames.
// Syntax roles and first declarations remain owned by the native grammar.
type callBranchBindingPlan struct {
	inputs, globals []string
}

func planCallBranchBindings[T any](branch string, outer map[string]T, scoped bool, imports []string) (callBranchBindingPlan, error) {
	if scoped && imports != nil {
		return callBranchBindingPlan{inputs: imports, globals: imports}, nil
	}
	var projection *fabric.QueryProjection
	if !scoped {
		local := strings.TrimSpace(fabric.SemanticQuery(branch))
		_, importing, err := branchLeadingWithImport(local, func(name string) bool { _, exists := outer[name]; return exists }, false)
		if err != nil {
			return callBranchBindingPlan{}, err
		}
		if importing {
			if clause := fabric.ReadQueryStructure(local).First("WITH"); clause != nil {
				projection = clause.Projection
			}
		}
	}
	roles := fabric.BranchBindings(branch)
	var plan callBranchBindingPlan
	for name := range outer {
		if strings.HasPrefix(name, "$") {
			continue
		}
		declares, mentioned := roles[name]
		if !scoped && (declares || !(projection != nil && projection.Star) && !mentioned) {
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
