package cypher

import (
	"context"
	"fmt"
	"maps"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
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
	if body != nil && body.scalar {
		// Scalar bodies carry their enclosing inputs through ordinary WITH.
		// Their wildcard expands that existing scope, regardless of row count.
		return body.visible(local)
	}
	return local
}

// A legacy importing WITH sees the caller's actual projection facts, including
// scoped retained imports, without importing scalar-only readable ancestors.
func (body *semanticBodyScope) legacyCurrent(local typeScope) typeScope {
	return local
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

// Projection predicates retain the existing weak bare-CALL readability
// extension. Ordering at an aggregating/DISTINCT horizon admits only scalar
// constants and explicit retained imports, as on release. A body declaration
// removes weak provenance before either view is selected.
func (body *semanticBodyScope) projectionConstants(ordering bool) typeScope {
	if body == nil {
		return nil
	}
	constants := scopeVisibleTypes(body.aggregateConstants(), body.retained)
	if !ordering {
		constants = scopeVisibleTypes(constants, body.implicit)
	}
	return constants
}

func (body *semanticBodyScope) orderConstants() typeScope {
	return body.projectionConstants(true)
}

// declare is the single transition from weak input to a body binding. A prior
// read still requires the outer input at runtime, but cannot make it strong.
// An unscoped CALL declaration is a new local; if WITH drops that local, its
// same-name weak input must not become readable again.
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

		}
	}
	if proof != nil {
		delete(proof.active, name)
	}
}

// scalarChild preserves strong/weak identity across expression boundaries.
// Its carried ancestors retain their existing scalar wildcard membership.
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

func withSemanticProjectedBody(ctx context.Context) context.Context {
	if body := semanticBodyFromContext(ctx); body != nil && body.projected != nil {
		return withSemanticBodyClause(ctx, body.projected)
	}
	return ctx
}

func (body *semanticBodyScope) project(before, after typeScope, plan *returnProjectionPlan) {
	if body == nil {
		return
	}
	visible := body.visible(before)
	carried := maps.Clone(after)
	var empty typeScope
	if len(body.original) > 0 && pipelineProjectionHasOnlyAggregates(plan.projections) {
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

// callBodyScope constructs imports only inside canonical binding. Type
// enrichment consumes the frozen body; it never enters this transition.
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
		// NornicDB extension, diverging from Neo4j: a bare CALL may read
		// outer variables without an import clause. These inputs are weak;
		// a new local declaration shadows one and ordinary WITH can drop it.
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
func (body *semanticBodyScope) branchEntry(ctx context.Context, clauses []pipelineClause, branchIndex int, initial typeScope, plan *returnProjectionPlan) (typeScope, error) {
	if body == nil || body.entered {
		return initial, nil
	}
	// Clause snapshots already passed this transition. Recompiling a fragment
	// must not reinterpret a later UNION branch as a new first branch.
	body.entered = true
	if !body.bare || len(clauses) == 0 || clauses[0].kind != pipelineClauseWith {
		return initial, nil
	}
	imports, importing := callWithImports(ctx, plan, body.caller, initial)
	if !importing {
		return initial, nil
	}
	body.headerReads = maps.Clone(imports)
	projection, tail := projectionSemanticBodyAndTail(clauses[0].text, "WITH")
	if branchIndex == 0 && (strings.TrimSpace(tail) != "" || topLevelKeywordIndex(projection, "AS") >= 0) {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", "Importing WITH must contain only simple references to outside variables")
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

// admitExports runs only in canonical bound identity construction.
// A strong caller binding may only be exported unchanged by every UNION branch.
func (body *semanticBodyScope) admitExports(local typeScope, columns []string, unchanged func(string) bool) error {
	strong := body.strong(local)
	for _, name := range columns {
		if _, bound := strong[name]; bound && !unchanged(name) {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherCoreVariableDeclaredInOuterScope(name))
		}
	}
	return nil
}

// admitDeclaration is the constructor's declaration legality transition.
// Type checking may reject an operand's kind, never admit a new name itself.
func (body *semanticBodyScope) admitDeclaration(name string, form string) error {
	if body == nil {
		return nil
	}
	_, strong := body.strong(body.entry)[name]
	_, ancestor := body.strong(nil)[name]
	if body.scalar && ancestor {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherCoreVariableDeclaredInOuterScope(name))
	}
	if form == "FOR" && body.readable(name, body.entry) || form == "LET" && strong {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", "variable "+name+" is already declared")
	}
	if form == "YIELD" && strong {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherCoreProcedureOutputShadowsVariable(name))
	}
	return nil
}

// Pattern declarations use the same strong/weak admission input as every
// other declaration form. Entity-kind compatibility is a later operand rule.
func (body *semanticBodyScope) admitPatternDeclarations(ctx context.Context, clause pipelineClause) error {
	strong := body.strong(body.entry)
	pattern := strings.TrimSpace(clause.text)
	for _, keyword := range []string{"OPTIONAL MATCH", "MATCH"} {
		if startsWithKeywordFold(pattern, keyword) {
			pattern = strings.TrimSpace(pattern[len(keyword):])
			break
		}
	}
	pattern = matchPatternStructure(pattern)
	if mergePatternUsesParameterPredicate(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidParameterUse",
			"MATCH pattern predicates must use a property map, not a parameter map",
		)
	}
	if invalidRelationshipPattern(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidRelationshipPattern",
			"invalid variable-length relationship pattern",
		)
	}

	nodes, relationships := quantifiedGroupVariables(pattern)
	for _, names := range []map[string]struct{}{nodes, relationships} {
		for name := range names {
			if _, bound := strong[name]; bound {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherMatchingQuantifiedPathVariableBound(name))
			}
		}
	}
	for _, part := range splitTopLevelComma(pattern) {
		path := extractPathAssignmentVariable(strings.TrimSpace(part))
		if path == "" {
			continue
		}
		if _, bound := strong[path]; bound {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", fmt.Sprintf("path variable %s is already bound", path))
		}
		entity := part
		if equals := strings.Index(part, "="); equals >= 0 {
			entity = part[equals+1:]
		}
		for _, name := range append(extractNodeVariables(entity), extractRelationshipVariables(entity)...) {
			if normalizeProjectionColumnName(name) == path {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", fmt.Sprintf("path variable %s is already bound", path))
			}
		}
	}
	// Local declarations inside a quantified group cannot also name an
	// element outside it. Check retained declaration handles here, before the
	// physical MATCH parser can open and independently admit the pattern.
	written := clause.syntax.Text()
	groups := quantifiedGroups(written)
	if len(groups) != 0 {
		start, _ := clause.syntax.Span()
		outside := map[string]bool{}
		syntaxPatternDefinitions(clause.syntax, func(_ int, symbol cypherantlr.SyntaxNode) {
			position, _ := symbol.Span()
			for _, group := range groups {
				if position-start > group.open && position-start < group.close {
					return
				}
			}
			outside[syntaxSymbolName(symbol)] = true
		})
		for _, group := range groups {
			for _, name := range append(group.nodes, group.relationships...) {
				if outside[name] {
					return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathVariableOutside(name))
				}
			}
		}
	}
	return nil
}
