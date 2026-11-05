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
	if program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); ok && boundStatementAddresses(program, clauses[0].syntax) {
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
	program := e.projectedStatementScope(ctx, clauses[0].syntax.Source(), resultmetadata.Known(names), resultmetadata.Known(retained), cypherVersionFromContext(ctx) == "25")
	if !program.complete {
		if program.err != nil {
			return ctx, program.err
		}
		return ctx, unresolvedStatementError()
	}
	return withBoundStatementProgram(ctx, program.bound), nil
}

// boundStatementAddresses reports whether a bound program's node-ID maps —
// columns, effects, projections and binding uses — address this clause. That
// holds for the program's own retained product. It also holds for another read
// of the same original source: compilation is cached by source, scope and
// dialect, so a compiled program can outlive the parse it was built from, and
// identical source yields identical node identities. Two products of different
// source never share an address, so a clause from a text split of some other
// window is refused rather than read through the wrong node.
func boundStatementAddresses(program *boundStatementProgram, node cypherantlr.SyntaxNode) bool {
	if program == nil || !program.syntax.Valid() || !node.Valid() {
		return false
	}
	if program.syntax.Identity() == node.Identity() {
		return true
	}
	return program.syntax.Source() == node.Source()
}

// boundClauseProgram is the statement program the executing clause belongs to.
func boundClauseProgram(ctx context.Context, clause pipelineClause) (*boundStatementProgram, error) {
	program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if !boundStatementAddresses(program, clause.syntax) {
		return nil, unresolvedStatementError()
	}
	return program, nil
}

// retainedClauseColumns is a clause's declared result columns from the bound
// statement program that compiled it. It never recompiles the clause or its
// parent on its own: the program already holds this clause's columns map.
func (e *StorageExecutor) retainedClauseColumns(ctx context.Context, clause pipelineClause, incoming map[string]struct{}) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !clause.syntax.Valid() {
		return nil, unresolvedStatementError()
	}
	bound, err := e.bindPipelineProgram(ctx, []pipelineClause{clause}, incoming)
	if err != nil {
		return nil, err
	}
	program, err := boundClauseProgram(bound, clause)
	if err != nil {
		return nil, err
	}
	columns, found := program.columns[clause.syntax.ID()]
	if !found {
		return nil, unresolvedStatementError()
	}
	var names []string
	var failure error
	columns.Visit(func(known []string) { names = known }, func() { failure = unresolvedStatementError() })
	if failure != nil {
		return nil, failure
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func executionClauseEffects(ctx context.Context, clause pipelineClause) (pipelineClauseEffects, error) {
	if clause.syntax.Valid() {
		if program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); ok && boundStatementAddresses(program, clause.syntax) {
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
