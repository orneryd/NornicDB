// Author of retained syntax cache identity: Codex R17 implementation agent.
package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"

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
	source := clauses[0].syntax.Source()
	if program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); ok && program != nil && program.syntax.Source() == source && program.syntax.Origin() == clauses[0].syntax.Origin() {
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
	program := e.projectionFactsForSyntax(ctx, clauses[0].syntax.Statement(), resultmetadata.Known(names), resultmetadata.Known(retained), cypherVersionFromContext(ctx) == "25")
	if !program.complete {
		if program.err != nil {
			return ctx, program.err
		}
		return ctx, unresolvedStatementError()
	}
	return withBoundStatementProgram(ctx, program.bound), nil
}

func executionClauseEffects(ctx context.Context, clause pipelineClause) (pipelineClauseEffects, error) {
	if program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); ok && program != nil && program.syntax.Source() == clause.syntax.Source() && program.syntax.Origin() == clause.syntax.Origin() {
		effects, found := program.effects[clause.syntax.ID()]
		if !found {
			return pipelineClauseEffects{}, unresolvedStatementError()
		}
		return effects, ctx.Err()
	}
	// Effect-only callers use the same observer on their admitted occurrence.
	// They do not need a resolved scope or another parse of its source text.
	var effects pipelineClauseEffects
	clause.syntax.Walk(pipelineEffectVisitor(&effects))
	return effects, ctx.Err()
}

// retainedClauseColumns observes the bound compiler's output facts for this
// occurrence. No source-based header inference or second schema owner is used.
func (e *StorageExecutor) retainedClauseColumns(ctx context.Context, clause pipelineClause, incoming map[string]struct{}) ([]string, error) {
	bound, err := e.bindPipelineProgram(ctx, []pipelineClause{clause}, incoming)
	if err != nil {
		return nil, err
	}
	return retainedNodeColumns(bound, clause.syntax)
}

func retainedNodeColumns(ctx context.Context, node cypherantlr.SyntaxNode) ([]string, error) {
	program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if program == nil || !node.Valid() || program.syntax.Source() != node.Source() || program.syntax.Origin() != node.Origin() {
		return nil, unresolvedStatementError()
	}
	facts, ok := program.columns[node.ID()]
	if !ok {
		return nil, unresolvedStatementError()
	}
	var columns []string
	var failure error
	facts.Visit(func(names []string) { columns = names }, func() { failure = unresolvedStatementError() })
	return columns, failure
}
