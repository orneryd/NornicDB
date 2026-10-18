package cypher

import "context"

// withPipelineImports starts one CALL body's import scope. An empty scope
// masks the enclosing CALL's imports, including for legacy importing WITH.
func withPipelineImports(ctx context.Context, imports pipelineRow) context.Context {
	return withGraphScopeImports(ctx, imports)
}

func pipelineImports(ctx context.Context) pipelineRow {
	return graphScopeImports(ctx, nil)
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
