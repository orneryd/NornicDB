package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

type boundStatementProgramKey struct{}

// A scalar body continuation keeps the operation supplied by its caller. The
// constructor still decides all bindings; this source window prevents the
// operation from leaking into an independent nested statement or CALL.
type scalarCompilationKey struct{}
type scalarCompilationInput struct{ source, kind string }

func withScalarCompilation(ctx context.Context, source, kind string) context.Context {
	return context.WithValue(ctx, scalarCompilationKey{}, scalarCompilationInput{source, kind})
}
func scalarCompilationKind(ctx context.Context, source string) string {
	input, _ := ctx.Value(scalarCompilationKey{}).(scalarCompilationInput)
	if input.source == source {
		return input.kind
	}
	return ""
}

func withBoundStatementProgram(ctx context.Context, program *boundStatementProgram) context.Context {
	return context.WithValue(ctx, boundStatementProgramKey{}, program)
}

// bindPipelineProgram admits a pipeline through the owner before any operator
// executes. Public execution carries the already bound program. A direct
// pipeline entry supplies its declared incoming scope to the same compiler.
func (e *StorageExecutor) bindPipelineProgram(ctx context.Context, clauses []pipelineClause, incoming map[string]struct{}) (context.Context, []pipelineClause, error) {
	if len(clauses) == 0 {
		return ctx, clauses, nil
	}
	if !clauses[0].syntax.Valid() {
		return ctx, nil, unresolvedStatementError()
	}
	source := clauses[0].syntax.Source()
	authored := statementAdmissionSource(ctx, source)
	selectClauses := func(program *boundStatementProgram) ([]pipelineClause, error) {
		if source == authored {
			return clauses, nil
		}
		// FINISH was consumed by the release framing owner. Execute the
		// constructor's retained operators, preserving authored syntax IDs and
		// spans. Do not compile a new statement with a manufactured terminator.
		if !endsInFinish(ctx, source) || len(program.root.branches) != 1 {
			return nil, unresolvedStatementError()
		}
		declared := program.root.branches[0].shape.clauses
		if len(declared) != len(clauses) {
			return nil, unresolvedStatementError()
		}
		selected := make([]pipelineClause, len(declared))
		for i, clause := range declared {
			if clause.kind != clauses[i].kind {
				return nil, unresolvedStatementError()
			}
			selected[i] = clause.pipelineClause
		}
		return selected, nil
	}
	if program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); ok && program != nil && program.syntax.Source() == authored {
		selected, err := selectClauses(program)
		return ctx, selected, err
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
	program := e.projectedStatementScope(ctx, source, resultmetadata.Known(names), resultmetadata.Known(retained), cypherVersionFromContext(ctx) == "25", semanticBodyFromContext(ctx))
	if !program.complete {
		if program.err != nil {
			return ctx, nil, program.err
		}
		return ctx, nil, unresolvedStatementError()
	}
	selected, err := selectClauses(program.bound)
	return withBoundStatementProgram(ctx, program.bound), selected, err
}

func executionClauseScope(ctx context.Context, clause pipelineClause, after bool) (map[string]struct{}, error) {
	program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if !ok || program == nil || !clause.syntax.Valid() || program.syntax.Source() != clause.syntax.Source() {
		return nil, unresolvedStatementError()
	}
	step, found := program.scopes[clause.syntax.ID()]
	if !found {
		return nil, unresolvedStatementError()
	}
	view := step.Before
	if after {
		view = step.After
	}
	var scope map[string]struct{}
	view.Visit(func(names []string) {
		scope = make(map[string]struct{}, len(names))
		for _, name := range names {
			scope[name] = struct{}{}
		}
	}, func() {})
	if scope == nil {
		return nil, unresolvedStatementError()
	}
	return scope, nil
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
		clause.syntax.Walk(pipelineEffectVisitor(&effects))
		return effects, ctx.Err()
	}
	return pipelineClauseEffectsFor(ctx, clause.text)
}

func executionCallFacts(ctx context.Context, clause pipelineClause) *resultmetadata.Call {
	program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if program == nil || !clause.syntax.Valid() || program.syntax.Source() != clause.syntax.Source() {
		return nil
	}
	return program.calls[clause.syntax.ID()]
}

// A terminal procedure still has declared columns when its input is empty.
// This adapter reads the constructor's projection, never executed row keys.
func executionProcedureColumns(ctx context.Context, clause pipelineClause) ([]string, error) {
	program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if program == nil || !clause.syntax.Valid() || program.syntax.Source() != clause.syntax.Source() {
		return nil, unresolvedStatementError()
	}
	step, found := program.scopes[clause.syntax.ID()]
	if !found || step.Projection == nil {
		return nil, unresolvedStatementError()
	}
	var columns []string
	var failure error
	step.Projection.Names.Visit(func(names []string) { columns = append([]string{}, names...) }, func() { failure = unresolvedStatementError() })
	return columns, failure
}

// executionBranchFrame transports the original admitted entry into a local
// branch adapter. Rebinding source text consumes that frame, never row keys.
func executionBranchFrame(ctx context.Context, start int) *semanticBodyScope {
	program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if program == nil {
		return nil
	}
	return program.frames[start]
}
