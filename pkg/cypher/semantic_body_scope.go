package cypher

import (
	"context"
	"maps"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// semanticBodyScope is the binding value of a CALL/scalar body. entry records
// ordinary body bindings; carried inputs remain readable after WITH. retained
// is the scoped CALL readability fact, separate from current wildcard columns.
// implicit tags #907 fallback imports: readable, but never declarations that
// can conflict with a name introduced by this body. Every declaration producer
// uses declare; consumers must not reconstruct provenance from row keys.
type semanticBodyScope struct {
	entry       typeScope
	carried     typeScope
	retained    typeScope
	original    typeScope
	implicit    typeScope
	caller      typeScope
	headerReads typeScope
	scalar      bool
	bare        bool
	entered     bool
	projected   *semanticBodyScope
	iteration   *semanticBodyScope
}

// A CALL wildcard requires a current body binding. Availability of an unread
// scoped import does not itself supply a wildcard column.
func returnStarScopeError(wildcard bool, columns int) error {
	if !wildcard || columns != 0 {
		return nil
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "NoVariablesInScope", localization.CypherMatchingReturnStarNoVariables())
}

func (body *semanticBodyScope) clone(entry typeScope) *semanticBodyScope {
	if body == nil {
		return nil
	}
	return &semanticBodyScope{entry: maps.Clone(entry), carried: maps.Clone(body.carried), retained: maps.Clone(body.retained), original: maps.Clone(body.original), implicit: maps.Clone(body.implicit), caller: maps.Clone(body.caller), headerReads: maps.Clone(body.headerReads), scalar: body.scalar, bare: body.bare, entered: body.entered}
}

// reference records an explicit input read in this body's ordinary scope.
// Readability alone does not contribute columns. Nested scalar reads belong to
// the scalar; #907 weak inputs keep their fallback/declaration contract.
func (body *semanticBodyScope) reference(name string, local typeScope) {
	if body == nil || body.scalar || body.bare {
		return
	}
	if _, current := local[name]; current {
		return
	}
	if operand, imported := body.carried[name]; imported {
		local[name] = operand
	}
}

// A child reports its importing-WITH reads without mutating its caller's
// value. The caller's own walk applies them to its mutable construction scope;
// runtime scope readers can therefore consume cached clause values unchanged.
func (body *semanticBodyScope) referenceCallHeaders(model *semanticQueryScope, local typeScope) {
	for _, branch := range model.branches {
		for name := range branch.bindings.headerReads {
			body.reference(name, local)
		}
	}
}

// clauseScope includes this clause's pattern bindings for its expressions,
// without making them incoming bindings to the pattern validator itself.
func (body *semanticBodyScope) clauseScope(local typeScope, patterns matchSemanticScope, source pipelineClause) *semanticBodyScope {
	clause := body.clone(local)
	if clause != nil {
		for name, kind := range patterns {
			clause.entry[name] = operandOfKind(kind)
		}
		if source.kind == pipelineClauseForeach {
			if update := queryBodyFromSyntax(source.syntax).foreach; update != nil {
				clause.iteration = clause.localChild(update.variable)
			}
		}
	}
	return clause
}

// A lexical update body adds its iterator without changing the enclosing CALL
// projection or import provenance. Both canonical binding and execution use it.
func (body *semanticBodyScope) localChild(name string) *semanticBodyScope {
	child := body.clone(body.entry)
	if child != nil {
		child.declare(name, child.entry, nil, nil, false)
		child.entry[name] = staticOperand{}
	}
	return child
}

// projection is the current clause membership. Carried scoped imports remain
// readable across WITH but enter this membership only through a direct read.
func (body *semanticBodyScope) projection(local typeScope) typeScope {
	if body == nil {
		return local
	}
	return local
}

// A legacy importing WITH sees the caller's actual projection facts, including
// scoped retained imports, without importing scalar-only readable ancestors.
func (body *semanticBodyScope) legacyCurrent(local typeScope) typeScope {
	return body.projection(local)
}

func (body *semanticBodyScope) visible(local typeScope) typeScope {
	if body == nil {
		return local
	}
	return scopeVisibleTypes(local, body.carried)
}

func (body *semanticBodyScope) strong(local typeScope) typeScope {
	visible := body.visible(local)
	if body == nil || len(body.implicit) == 0 {
		return visible
	}
	strong := maps.Clone(visible)
	for name := range body.implicit {
		delete(strong, name)
	}
	return strong
}

// A scalar expression treats its enclosing values as constants. A CALL is a
// query body: a persistent import remains readable, but is not an implicit
// grouping key in an aggregate projection.
func (body *semanticBodyScope) aggregateConstants() typeScope {
	if body != nil && body.scalar {
		return body.visible(nil)
	}
	return nil
}

// ORDER BY can read a retained scoped import after DISTINCT without making
// that import an implicit grouping key in the aggregate projection itself.
func (body *semanticBodyScope) orderConstants() typeScope {
	if body == nil {
		return nil
	}
	return scopeVisibleTypes(body.aggregateConstants(), body.retained)
}

// declare is the single transition from weak input to a body binding. A prior
// read still requires the outer input at runtime, but cannot make it strong.
func (body *semanticBodyScope) declare(name string, local typeScope, proof *semanticDependencyScope, result *semanticQueryScope, replaceInput bool) {
	if body != nil {
		if _, weak := body.implicit[name]; weak {
			delete(body.implicit, name)
			delete(body.carried, name)
			delete(body.retained, name)
			delete(body.original, name)
			if replaceInput {
				delete(local, name)
			}
			if result != nil && (proof == nil || !proof.reads[name]) {
				result.locals[name] = true
			}
		}
	}
	if proof != nil {
		delete(proof.active, name)
	}
}

// scalarChild preserves strong/weak identity across expression boundaries.
// Its ancestors are readable but do not enter the scalar's RETURN * scope.
func (body *semanticBodyScope) scalarChild(visible typeScope) *semanticBodyScope {
	child := &semanticBodyScope{entry: typeScope{}, carried: maps.Clone(visible), original: maps.Clone(visible), scalar: true}
	if body != nil {
		child.implicit = make(typeScope)
		for name, operand := range body.implicit {
			if _, exists := visible[name]; exists {
				child.implicit[name] = operand
			}
		}
	}
	return child
}

func (body *semanticBodyScope) pattern(name string, kind matchBindingKind, local typeScope, proof *semanticDependencyScope, result *semanticQueryScope) {
	if body != nil {
		if _, weak := body.implicit[name]; weak {
			body.declare(name, local, proof, result, true)
			return
		}
	}
	proof.read(name)
}

func (body *semanticBodyScope) runtimeCurrent(names []string) typeScope {
	if body != nil {
		return maps.Clone(body.entry)
	}
	current := semanticRuntimeInput(names)
	if body != nil && body.scalar {
		for name := range body.carried {
			if _, local := body.entry[name]; !local {
				delete(current, name)
			}
		}
	}
	return current
}

func withSemanticProjectedBody(ctx context.Context) context.Context {
	if body := semanticBodyFromContext(ctx); body != nil && body.projected != nil {
		return withSemanticBodyClause(ctx, body.projected)
	}
	return ctx
}

func (body *semanticBodyScope) project(ctx context.Context, before, after typeScope, clause string) {
	if body == nil {
		return
	}
	visible := body.visible(before)
	carried := maps.Clone(after)
	keyword := "WITH"
	if startsWithKeywordFold(strings.TrimSpace(clause), "RETURN") {
		keyword = "RETURN"
	}
	projection, _ := cutDistinct(projectionSemanticBody(clause, keyword))
	var empty typeScope
	if len(body.original) > 0 && pipelineProjectionHasOnlyAggregates(returnProjectionPlanFor(ctx, "RETURN "+projection).projections) {
		empty = maps.Clone(carried)
		carrySubqueryImports(empty, nil, body.original)
	}
	carrySubqueryImports(carried, visible, body.original)
	for name, operand := range empty {
		carried[name] = joinedOperand(carried[name], operand)
	}
	for name := range body.carried {
		if operand, exists := carried[name]; exists {
			body.carried[name] = operand
			if _, retained := body.retained[name]; retained {
				body.retained[name] = operand
			}
		}
	}
}

func (body *semanticBodyScope) projectionTail(ctx context.Context, merged, projected typeScope, clause string) *semanticBodyScope {
	if body == nil {
		return nil
	}
	tail := body.clone(merged)
	projected.forEachProjectedBinding(ctx, clause, func(name, expression, text string, inherited bool) {
		if !inherited && projectedSourceVariable(expression) != name {
			tail.declare(name, projected, nil, nil, false)
		}
	})
	tail.project(ctx, merged, projected, clause)
	body.projected = tail
	return tail
}

// callBodyScope is the shared import-form constructor for typed validation and
// the canonical statement binder. Neither consumer invents another scope from
// projected columns or runtime row values.
func callBodyScope(names []string, scoped bool, local typeScope, parent *semanticBodyScope, scalarParent bool) (typeScope, *semanticBodyScope, error) {
	outer := parent.visible(local)
	header := outer
	if scalarParent {
		header = local
	}
	imports := make(typeScope)
	if scoped {
		for _, name := range names {
			operand, exists := outer[name]
			if !exists {
				return nil, nil, createUndefinedVariableError(name)
			}
			imports[name] = operand
		}
		if names == nil {
			maps.Copy(imports, header)
		}
	} else {
		maps.Copy(imports, outer)
	}
	initial := maps.Clone(imports)
	if scoped {
		initial = typeScope{}
	}
	body := &semanticBodyScope{entry: initial, carried: maps.Clone(imports), original: maps.Clone(imports), caller: maps.Clone(parent.legacyCurrent(local)), bare: !scoped}
	if !scoped {
		body.implicit = maps.Clone(imports)
	} else {
		body.retained = maps.Clone(imports)
	}
	return imports, body, nil
}

// branchEntry is the per-import-form transition shared by every UNION branch.
// A leading importing WITH creates ordinary locals; only unselected caller
// locals retain #907 weak fallback. Scalar-only ancestors require a name.
func (body *semanticBodyScope) branchEntry(ctx context.Context, clauses []pipelineClause, branchIndex int, initial typeScope) (typeScope, error) {
	if body == nil || body.entered {
		return initial, nil
	}
	// Clause snapshots already passed this transition. Recompiling a fragment
	// must not reinterpret a later UNION branch as a new first branch.
	body.entered = true
	if !body.bare || len(clauses) == 0 || clauses[0].kind != pipelineClauseWith {
		return initial, nil
	}
	imports, importing := callWithImports(ctx, clauses[0].text, body.caller, initial)
	if !importing {
		return initial, nil
	}
	body.headerReads = maps.Clone(imports)
	projection, tail := projectionSemanticBodyAndTail(clauses[0].text, "WITH")
	if branchIndex == 0 && (strings.TrimSpace(tail) != "" || topLevelKeywordIndex(projection, "AS") >= 0) {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherSubqueriesWithImportExpressionInvalid(projection))
	}
	if len(clauses) == 1 {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherSubqueriesWithQueryClauseRequired())
	}
	for name := range body.carried {
		_, local := body.caller[name]
		_, selected := imports[name]
		if !local || selected {
			delete(body.carried, name)
			delete(body.implicit, name)
		}
	}
	return imports, nil
}

type semanticBodyScopeKey struct{}
type semanticBodyInputKey struct{}

func semanticBodyFromContext(ctx context.Context) *semanticBodyScope {
	body, _ := ctx.Value(semanticBodyScopeKey{}).(*semanticBodyScope)
	return body
}

// Runtime consumes the immutable clause scope produced by the owner. Values
// stay request-local; neither the scope nor any cache stores a row value.
func withSemanticBodyClause(ctx context.Context, body *semanticBodyScope) context.Context {
	if body == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, semanticBodyScopeKey{}, body)
	input, supplied := ctx.Value(semanticBodyInputKey{}).(pipelineRow)
	if !supplied {
		return ctx
	}
	carried := make(pipelineRow, len(body.carried))
	for name := range body.carried {
		if value, exists := input[name]; exists {
			carried[name] = value
		}
	}
	return withPipelineImports(ctx, carried)
}

// These transitions are the only writers of the body's current membership.
func (body *semanticBodyScope) bindCurrent(name string, operand staticOperand) {
	body.entry[name] = operand
}
func (body *semanticBodyScope) replaceCurrent(current typeScope) { body.entry = maps.Clone(current) }

// A nested export that preserves binding identity is a read/pass-through,
// rather than a declaration shadowing the caller's same-name weak candidate.
func (body *semanticBodyScope) export(name string, local typeScope, proof *semanticDependencyScope, result *semanticQueryScope, unchanged bool) {
	if !unchanged {
		body.declare(name, local, proof, result, false)
	}
}
