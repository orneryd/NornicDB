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
	program := e.projectedStatementScope(ctx, source, resultmetadata.Known(names), resultmetadata.Known(retained), cypherVersionFromContext(ctx) == "25", semanticBodyFromContext(ctx))
	if !program.complete {
		if program.err != nil {
			return ctx, program.err
		}
		return ctx, unresolvedStatementError()
	}
	return withBoundStatementProgram(ctx, program.bound), nil
}

func executionReturnProjection(ctx context.Context, clause pipelineClause) (*returnProjectionPlan, error) {
	program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if !ok || program == nil || !clause.syntax.Valid() || program.syntax.Source() != clause.syntax.Source() {
		return nil, unresolvedStatementError()
	}
	plan := program.projections[clause.syntax.ID()]
	if plan == nil || !plan.valid {
		return nil, unresolvedStatementError()
	}
	return plan, nil
}

// Each fused operator binds its own immutable clause snapshot, never the
// context left by an earlier WITH or the keys of an imported runtime row.
func executionBodyScope(ctx context.Context, clause pipelineClause) *semanticBodyScope {
	if clause.bindings != nil {
		return clause.bindings
	}
	if program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); ok && program != nil && clause.syntax.Valid() && program.syntax.Source() == clause.syntax.Source() {
		return program.bodies[clause.syntax.ID()]
	}
	return nil
}

// executionClauseScope consumes the admitted transition. Readable inputs are
// distinct from wildcard membership: WITH can drop an import from the latter
// while DELETE or a later nested CALL can still read its carried binding.
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

func executionLetFrames(ctx context.Context, clause pipelineClause) ([]*semanticBodyScope, error) {
	program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if !ok || program == nil || !clause.syntax.Valid() || program.syntax.Source() != clause.syntax.Source() {
		return nil, unresolvedStatementError()
	}
	var frames []*semanticBodyScope
	for index := 0; index < clause.syntax.ChildCount(); index++ {
		item := clause.syntax.Child(index)
		if item.Rule() != cypherantlr.CypherParserRULE_letItem {
			continue
		}
		start, _ := item.Span()
		frame := program.frames[start]
		if frame == nil {
			return nil, unresolvedStatementError()
		}
		frames = append(frames, frame)
	}
	return frames, nil
}

func bindPipelineRowWith(ctx context.Context, plan pipelineRowWith) pipelineRowWith {
	plan = plan.withImports(pipelineImports(ctx))
	if body := semanticBodyFromContext(ctx); body != nil && plan.star {
		plan.columns = pipelineScopeColumns(bindingNames(body.projection(body.entry)))
		if plan.columns == nil {
			plan.columns = []string{}
		}
	}
	return plan
}

func pipelineProjectionColumns(ctx context.Context, rows []pipelineRow) []string {
	if body := semanticBodyFromContext(ctx); body != nil {
		return pipelineScopeColumns(bindingNames(body.projection(body.entry)))
	}
	return pipelineWildcardColumns(rows)
}

// CALL headers are the nested declaration of the admitted statement, including
// zero-row calls. Reading them must not compile another CALL fragment program.
func executionCallColumns(ctx context.Context, clause pipelineClause) ([]string, error) {
	program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if !ok || program == nil || !clause.syntax.Valid() || program.syntax.Source() != clause.syntax.Source() {
		return nil, unresolvedStatementError()
	}
	call := program.calls[clause.syntax.ID()]
	if call == nil {
		return nil, unresolvedStatementError()
	}
	var columns []string
	var err error
	call.Body.Visit(func(names []string) { columns = append([]string(nil), names...) }, func() { err = unresolvedStatementError() })
	return columns, err
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
