package cypher

import "context"

type pipelineImportsKey struct{}

// withPipelineImports starts one CALL body's import scope. An empty scope
// masks the enclosing CALL's imports, including for legacy importing WITH.
func withPipelineImports(ctx context.Context, imports pipelineRow) context.Context {
	if len(imports) == 0 && ctx.Value(pipelineImportsKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, pipelineImportsKey{}, imports)
}

func pipelineImports(ctx context.Context) pipelineRow {
	imports, _ := ctx.Value(pipelineImportsKey{}).(pipelineRow)
	return imports
}

// carryPipelineRowImports keeps explicit CALL imports available alongside a
// WITH projection. Callers select DISTINCT/grouping keys from the projection
// aliases alone. A projected or rebound name keeps its current value, as in
// the existing imported-name rebinding extension.
func carryPipelineRowImports(projected, input, imports pipelineRow) {
	carrySubqueryImports(projected, input, imports)
}

// carrySubqueryImports applies the same retention rule to runtime values and
// static operands: the projection wins, then the current input, then the
// original import. This preserves imported-name rebinding across WITH.
func carrySubqueryImports[T any](projected, input, imports map[string]T) {
	for name, imported := range imports {
		if _, exists := projected[name]; exists {
			continue
		}
		if current, exists := input[name]; exists {
			projected[name] = current
		} else {
			projected[name] = imported
		}
	}
}

// carryPipelineImports restores CALL imports after an aggregate projection has
// completed, so they remain available to following clauses without becoming
// grouping or DISTINCT keys for the projection that just ran.
func carryPipelineImports(rows []pipelineRow, scope map[string]struct{}, imports pipelineRow) {
	for _, row := range rows {
		carryPipelineRowImports(row, row, imports)
	}
	if scope != nil {
		for name := range imports {
			scope[name] = struct{}{}
		}
	}
}
