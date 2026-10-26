package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

type boundStatementProgramKey struct{}

func withBoundStatementProgram(ctx context.Context, program *boundStatementProgram) context.Context {
	return context.WithValue(ctx, boundStatementProgramKey{}, program)
}

// bindPipelineProgram admits a pipeline through the owner before any operator
// executes. Public execution carries the already bound program. A direct
// pipeline entry supplies its declared incoming scope to the same compiler.
func (e *StorageExecutor) bindPipelineProgram(ctx context.Context, clauses []pipelineClause, incoming map[string]struct{}) (context.Context, error) {
	if len(clauses) == 0 {
		return ctx, nil
	}
	if !clauses[0].syntax.Valid() {
		return ctx, unresolvedStatementError()
	}
	source := clauses[0].syntax.Source()
	if program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); ok && program != nil && program.syntax.Source() == source {
		return ctx, nil
	}
	var retained []string
	for name := range pipelineImports(ctx) {
		retained = append(retained, name)
	}
	names := make([]string, 0, len(incoming))
	for name := range incoming {
		if len(name) == 0 || name[0] != '$' {
			names = append(names, name)
		}
	}
	program := e.projectedStatementScope(ctx, source, resultmetadata.Known(names), resultmetadata.Known(retained), cypherVersionFromContext(ctx) == "25")
	if !program.complete {
		if program.err != nil {
			return ctx, program.err
		}
		return ctx, unresolvedStatementError()
	}
	return withBoundStatementProgram(ctx, program.bound), nil
}

func executionClauseEffects(ctx context.Context, clause pipelineClause) (pipelineClauseEffects, error) {
	if clause.syntax.Valid() {
		if program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); ok && program != nil && program.syntax.Source() == clause.syntax.Source() {
			effects, found := program.effects[clause.syntax.ID()]
			if !found {
				return pipelineClauseEffects{}, unresolvedStatementError()
			}
			return effects, ctx.Err()
		}
		// Effect-only callers need no resolved scope. They observe this retained
		// syntax node; row execution has a separate mandatory binding admission.
		var effects pipelineClauseEffects
		clause.syntax.Walk(pipelineEffectVisitor(clause.grammarView, &effects))
		return effects, ctx.Err()
	}
	return pipelineClauseEffectsFor(ctx, clause.text)
}
