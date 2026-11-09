package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"

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

// regionalStatementColumns is the columns owner's compile-only regional read:
// the columns the clause that ends region declares in the admitted statement
// program of query. Query is a compile-only candidate view of a conditional
// arm, so it compiles under the same incoming scope and dialect as the
// statement it stands in, and the owner's cache (accepted text plus scope)
// serves every repeat; no row value is an input. An incomplete program, an
// unknown header and a region that owns no clause are errors, never an empty
// successful header.
func (e *StorageExecutor) regionalStatementColumns(ctx context.Context, query string, region cyphertext.Span) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cypher25 := cypherVersionFromContext(ctx) == "25" || cypherGrammarVersion(query) == "25"
	program := e.statementProjectionProgram(ctx, query, e.resultIncomingScope(), cypher25, nil)
	if !program.complete {
		if program.err != nil {
			return nil, program.err
		}
		return nil, unresolvedStatementError()
	}
	columns, found := regionalClauseColumns(program.bound, region)
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

// regionalClauseColumns reads the declared columns of the clause that ends
// region in program's bound product. Clause identities and byte spans are the
// product's own, so the read never recompiles a fragment and never infers a
// header from a value.
func regionalClauseColumns(program *boundStatementProgram, region cyphertext.Span) (resultmetadata.Columns, bool) {
	if program == nil || !program.syntax.Valid() {
		return resultmetadata.Columns{}, false
	}
	var selected cypherantlr.SyntaxNode
	start, end := 0, -1
	program.syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if _, declared := program.columns[node.ID()]; !declared {
			return nil
		}
		from, to := node.Span()
		if from < region.Start || to > region.End {
			return nil
		}
		if selected.Valid() && (to < end || to == end && from <= start) {
			return nil
		}
		selected, start, end = node, from, to
		return nil
	})
	if !selected.Valid() {
		return resultmetadata.Columns{}, false
	}
	columns, found := program.columns[selected.ID()]
	return columns, found
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
