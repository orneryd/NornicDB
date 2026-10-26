package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// The statement binder observes its retained CST through the same body scope
// value as typed validation. This adapter performs no type walk or execution.
func bindingTypes(names map[string]struct{}) typeScope {
	types := make(typeScope, len(names))
	for name := range names {
		types[name] = staticOperand{}
	}
	return types
}

func bindingNames(types typeScope) map[string]struct{} {
	names := make(map[string]struct{}, len(types))
	for name := range types {
		names[name] = struct{}{}
	}
	return names
}

// Body constructors are immutable inputs; each branch clones them before
// applying transitions. Rewriting consumes the same lexical/tail/LET frame
// that statement binding constructs instead of guessing from keyword text.
func (compilation *boundCompilation) scopeFrame(start int, frame *semanticBodyScope) {
	if compilation.product.frames == nil {
		compilation.product.frames = make(map[int]*semanticBodyScope)
	}
	compilation.product.frames[start] = frame
}

func (compilation *boundCompilation) enterBodyScope(ctx context.Context, branch *boundBranch, index int) error {
	if branch.body.bindings == nil {
		return nil
	}
	branch.bindings = branch.body.bindings.clone(branch.body.bindings.entry)
	branch.dependencies = branch.body.dependencies.nested()
	clauses := make([]pipelineClause, len(branch.shape.clauses))
	for index, clause := range branch.shape.clauses {
		clauses[index] = clause.pipelineClause
	}
	entry, err := branch.bindings.branchEntry(ctx, clauses, index, branch.bindings.entry)
	if err != nil {
		return err
	}
	branch.bindings.replaceCurrent(entry)
	branch.scope = bindingNames(branch.bindings.visible(entry))
	return nil
}

func (compilation *boundCompilation) beginBodyClause(ctx context.Context, clause *boundClause) {
	body := clause.branch.bindings
	if body == nil {
		return
	}
	proof := clause.branch.dependencies
	source := clause.source
	switch source.kind {
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge:
		clause.fresh = make(map[string]bool)
		syntaxPatternDefinitions(source.syntax, func(_ int, symbol cypherantlr.SyntaxNode) {
			name := syntaxSymbolName(symbol)
			_, weak := body.implicit[name]
			clause.fresh[name] = weak
			body.pattern(name, matchBindingUnknown, body.entry, proof, nil)
		})
	}
	body.readClause(ctx, source.pipelineClause, body.entry, proof)

	clause.branch.scope = bindingNames(body.visible(body.entry))
}

func (compilation *boundCompilation) finishBodyClause(ctx context.Context, clause *boundClause) {
	body := clause.branch.bindings
	if body == nil {
		return
	}
	proof := clause.branch.dependencies
	declare := func(name string) {
		body.declare(name, body.entry, proof, nil, false)
		body.bindCurrent(name, staticOperand{})
	}
	switch clause.source.kind {
	case pipelineClauseWith, pipelineClauseReturn:
		if clause.plan != nil {
			for _, item := range clause.plan.projections {
				if !item.direct || item.variable != item.alias {
					body.declare(item.alias, body.entry, proof, nil, false)
				}
			}
			after := bindingTypes(clause.localProjection)
			body.project(ctx, body.entry, after, clause.source.text)
			body.replaceCurrent(after)
		}
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge:
		syntaxPatternDefinitions(clause.source.syntax, func(_ int, symbol cypherantlr.SyntaxNode) {
			body.bindCurrent(syntaxSymbolName(symbol), staticOperand{})
		})
	case pipelineClauseUnwind:
		declare(syntaxSymbolName(clause.source.syntax.ChildRule(cypherantlr.CypherParserRULE_symbol)))
	case pipelineClauseCallSubquery:
		for _, branch := range clause.body.branches {
			for name := range branch.bindings.headerReads {
				body.reference(name, body.entry)
			}
		}
		for name := range projectedNameMap(clause.outputs) {
			body.export(name, body.entry, proof, nil, clause.beforeIDs[name] != 0 && clause.afterIDs[name] == clause.beforeIDs[name])
			body.bindCurrent(name, staticOperand{})
		}
	case pipelineClauseCall:
		for name := range projectedNameMap(clause.outputs) {
			declare(name)
		}
	}
	if clause.source.kind == pipelineClauseReturn {
		clause.after = projectedNameMap(clause.outputs)
	} else {
		clause.after = bindingNames(body.visible(body.entry))
	}
}
