package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"maps"
	"strings"
)

// typeScope is the one static type of every variable a clause sees (#1055).
// The match validator's binding kinds (matchSemanticScope) and the static
// checks' value and member types (staticTypeScope) are views of it, so a
// projected expression is typed once, by projectedOperand, for both.
type typeScope map[string]staticOperand

// isEntityTypeName reports a static type the binding kinds hold themselves.
func isEntityTypeName(typeName string) bool {
	switch typeName {
	case "Node", "Relationship", "List<Node>", "List<Relationship>", "Path":
		return true
	}
	return false
}

// kinds is the binding-kind view of types.
func (types typeScope) kinds() matchSemanticScope {
	kinds := make(matchSemanticScope, len(types))
	for name, operand := range types {
		kinds[name] = gateProven(operand).bindingKind()
	}
	return kinds
}

// static is the static checks' view of types: the binding kinds, the types of
// variables that aren't entities, and the member types of maps.
func (types typeScope) static(cypher25 bool) staticTypeScope {
	scope := staticTypeScope{kinds: types.kinds(), complete: true, cypher25: cypher25}
	for name, operand := range types {
		// The views hold only what a verified rule proved.
		operand = gateProven(operand)
		// Every value operand travels whole (its alternatives and its
		// constraints), so a later clause reads exactly what the projection
		// derived; the type-name view below is derived from it.
		if operand.typeName() != "" && !isEntityTypeName(operand.typeName()) || operand.nonEntity {
			if scope.derived == nil {
				scope.derived = make(map[string]staticOperand)
			}
			scope.derived[name] = operand
		}
		if typeName := operand.typeName(); typeName != "" && !isEntityTypeName(typeName) {
			if scope.values == nil {
				scope.values = make(map[string]string)
			}
			scope.values[name] = typeName
		}
		if operand.members != nil {
			if scope.members == nil {
				scope.members = make(map[string]map[string]staticOperand)
			}
			scope.members[name] = operand.members
		}
	}
	return scope
}

// typeScopeOf is the typeScope a staticTypeScope's views describe.
func typeScopeOf(scope staticTypeScope) typeScope {
	types := make(typeScope, len(scope.kinds)+len(scope.values))
	for name, kind := range scope.kinds {
		types[name] = operandOfKind(kind)
	}
	for name, typeName := range scope.values {
		if isEntityTypeName(types[name].typeName()) {
			continue
		}
		types[name] = typedOperand(typeName).proved()
	}
	for name, members := range scope.members {
		operand := types[name]
		operand.members = members
		types[name] = operand
	}
	for name, operand := range scope.derived {
		types[name] = operand
	}
	return types
}

// adoptKinds records in types the binding kinds a pattern clause (MATCH,
// CREATE, MERGE) bound or changed in kinds.
func (types typeScope) adoptKinds(kinds matchSemanticScope) {
	for name, kind := range kinds {
		if current, bound := types[name]; bound && current.bindingKind() == kind {
			continue
		}
		types[name] = operandOfKind(kind)
	}
}

// projectTypeScope is the typeScope a WITH clause projects from types: each
// item's operand by its alias, every variable for WITH *.
func projectTypeScope(ctx context.Context, types typeScope, cypher25 bool, clause braceView) typeScope {
	return projectTypeScopeIn(ctx, types, types.static(cypher25), clause)
}

// projectTypeScopeIn is projectTypeScope for a caller that holds types'
// static view (input) already.
func projectTypeScopeIn(ctx context.Context, types typeScope, input staticTypeScope, clause braceView) typeScope {
	body, _ := projectionSemanticSourceAndTail(clause, "WITH")
	input.complete = false
	projected := make(typeScope)
	for _, raw := range body.commaViews() {
		if raw.text() == "*" {
			for name, operand := range types {
				projected[name] = operand
			}
			continue
		}
		expressionView, alias, valid := projectionItemView(ctx, raw)
		if !valid {
			continue
		}
		expression := expressionView.text()
		if expression == "*" {
			for name, operand := range types {
				projected[name] = operand
			}
			continue
		}
		name := alias
		if name == "" {
			name = simpleSemanticIdentifier(expression)
		}
		if name == "" {
			continue
		}
		operand := projectedOperand(types, input, expressionView)
		if simpleSemanticIdentifier(name) == "" {
			// Unaliased, a column is named by its text (WITH {a: 1}); only a
			// name binds a variable whose members are known.
			operand.members = nil
		}
		projected[normalizeProjectionColumnName(name)] = operand
	}
	return projected
}

// projectedOperand is the static type of a projected expression, as Neo4j
// types it before the statement runs: a bare variable's own; null a Null
// (open to any kind); coalesce of variables of one kind and a list of nodes or of
// relationships that kind; collect of nodes or relationships their list, a
// count an Integer, any other aggregate a value; a node's or relationship's
// property a value of unknown type; a map member unknown in Cypher 5; and
// any other expression the type the static operator check gives it (a
// literal, arithmetic, a function's result, CASE, a comparison, a map
// projection, a comprehension, a subscript or slice of a typed list).
func projectedOperand(types typeScope, input staticTypeScope, view braceView) staticOperand {
	view = view.trim()
	expression := view.text()
	if strings.EqualFold(expression, "null") {
		// Null, as written in place (+z of WITH null AS z is a number); it
		// fits every position and binds as any kind (bindingKind).
		return provenOperand("Null")
	}
	if source := simpleSemanticIdentifier(expression); source != "" && !isLiteralKeyword(source) {
		return types[source]
	}
	if kind, ok := entityListLiteralKind(expression, input.kinds); ok {
		return operandOfKind(kind)
	}
	// Every other expression has the one static type its scope derives.
	return input.operandOf(view)
}

// unwoundOperand is the static type of the variable UNWIND binds from list:
// a list's element type (a node of a List<Node>, an Integer of a
// List<Integer>), a known non-list value itself (UNWIND 5), unknown for a
// list of unknown elements.
func unwoundOperand(types typeScope, input staticTypeScope, clause braceView) staticOperand {
	expression, _, _ := parsePipelineIterationSource(clause)
	return listElementOperand(types, input, expression)
}

// listElementOperand is the static type of an element of list (UNWIND's
// variable, a comprehension's or quantifier's local; localBindingKind).
func listElementOperand(types typeScope, input staticTypeScope, list braceView) staticOperand {
	operand := projectedOperand(types, input, list)
	if !operand.union && operand.known() && !operand.list() {
		return operand // UNWIND 5 binds 5
	}
	if unwound := operand.unwoundOperand(); unwound.typeName() != "" {
		return unwound
	}
	// The deriver typed the list; its elements are what it derived, or
	// unknown.
	return staticOperand{}
}

// projectMatchSemanticScope is the binding-kind view of the variables a WITH
// clause projects (projectTypeScope).
func projectMatchSemanticScope(ctx context.Context, input matchSemanticScope, clause braceView) matchSemanticScope {
	scope := staticTypeScope{kinds: input}
	return projectTypeScopeIn(ctx, typeScopeOf(scope), scope, clause).kinds()
}

// projectStaticValueTypes is the value-type view of the variables a WITH
// clause projects (projectTypeScope).
func projectStaticValueTypes(ctx context.Context, scope staticTypeScope, clause braceView) map[string]string {
	return projectTypeScopeIn(ctx, typeScopeOf(scope), scope, clause).static(scope.cypher25).values
}

// projectStaticValueMembers is, in a Cypher 25 statement, the member-type
// view of the variables a WITH clause projects (projectTypeScope).
func projectStaticValueMembers(ctx context.Context, scope staticTypeScope, clause braceView) map[string]map[string]staticOperand {
	if !scope.cypher25 {
		return nil
	}
	return projectTypeScopeIn(ctx, typeScopeOf(scope), scope, clause).static(true).members
}

// callSubqueryImports is the scope a CALL subquery's body starts from: CALL
// (a, b) { … } imports a and b, CALL (*) { … } and an old-style CALL { … }
// every outer variable (the latter's leading WITH picks them). ok is false
// for a CALL without a body; scoped is set for CALL (…) { }. A variable a
// scope clause names that outer doesn't bind is Neo4j's compile-time
// "Variable `x` not defined", reported here, where every statement is
// checked before it plans or runs, on every route alike.
func callSubqueryImports(ctx context.Context, callView braceView, outer typeScope) (typeScope, bool, bool, error) {
	callText := callView.text()
	scope := readCallSubqueryScope(ctx, callView)
	if scope.invalid {
		return nil, false, false, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", "CALL imports must contain only variable names")
	}
	if scope.bodyOffset == 0 || scope.bodyOffset >= len(callText) || !cyphertext.IsOpenBrace(callText[scope.bodyOffset]) {
		return nil, false, false, nil
	}
	initial := make(typeScope)
	if scope.scoped && scope.names != nil {
		for _, name := range scope.names {
			operand, bound := outer[name]
			if !bound {
				return nil, false, false, createUndefinedVariableError(name)
			}
			initial[name] = operand
		}
		return initial, true, true, nil
	}
	for name, operand := range outer {
		initial[name] = operand
	}
	return initial, scope.scoped, true, nil
}

// callSubqueryReturnTypes walks a CALL subquery's body, each UNION branch
// as a query of its own, with the one clause walk (semanticScopeWalk) from
// the variables it imports (callSubqueryImports), and returns the static
// type of each column a one-branch body returns. Its expression subqueries
// see the body's scope, not the enclosing query's. A CALL (a, b) body sees
// its imports through the whole body, after a WITH in it too, and may
// declare them again (subqueryScope). A body whose walk stops early, or that
// doesn't end in RETURN, gives no types; a scoped body's walk error is the
// statement's.
func (e *StorageExecutor) callSubqueryReturnTypes(ctx context.Context, callText braceView, branches []braceView, outer typeScope, cypher25 bool) (typeScope, braceView, error) {
	initial, scoped, ok, err := callSubqueryImports(ctx, callText, outer)
	if err != nil || !ok {
		return nil, braceView{}, err
	}
	var returned typeScope
	firstReturn := braceView{}
	typedBranches := 0
	for branchIndex, branch := range branches {
		split := cachedPipelineClauses(withBraceView(ctx, branch), branch.text(), true)
		if branchIndex == 0 {
			firstReturn = split.firstReturn
		}
		clauses, ok := split.clauses, split.ok
		if !ok || len(clauses) == 0 {
			continue
		}
		last := clauses[len(clauses)-1]
		retained := initial
		importing := false
		if !scoped {
			// Unlisted outer names remain implicit imports under the documented
			// gh648 extension. A leading importing WITH explicitly projects its
			// listed names (including *), so those names follow ordinary WITH
			// scope instead. A branch's own declarations never become imports.
			retained = maps.Clone(initial)
			_, importing, _ = branchLeadingWithImport(withBraceView(ctx, branch), branch, func(name string) bool { _, exists := initial[name]; return exists }, false)
			if importing {
				for name := range projectTypeScope(ctx, initial, cypher25, clauses[0].input(ctx)) {
					delete(retained, name)
				}
			}
			for name := range retained {
				if branchDeclaresVariable(withBraceView(ctx, branch), branch.text(), name) {
					delete(retained, name)
				}
			}
		}
		final, complete, err := e.semanticScopeWalk(ctx, clauses, maps.Clone(initial), cypher25, subqueryScope{visible: retained})
		if err != nil {
			if !scoped {
				if detail, classified := err.(interface{ BoltErrorDetail() string }); importing && classified && detail.BoltErrorDetail() == "UndefinedVariable" {
					return nil, firstReturn, err
				}
				// An old-style CALL { … } keeps NornicDB's implicit imports
				// (gh648: an outer name it doesn't import is visible, or
				// declared anew), so its walk only types its columns.
				return nil, firstReturn, nil
			}
			return nil, firstReturn, err
		}
		if !complete || last.kind != pipelineClauseReturn {
			return nil, firstReturn, nil
		}

		branchTypes := projectTypeScope(ctx, final, cypher25, last.input(ctx).clauseBody("RETURN").wrap("WITH ", ""))
		if typedBranches == 0 {
			returned = branchTypes
		} else {
			// A UNION column is a value of any branch's type.
			for name, operand := range returned {
				returned[name] = joinedOperand(operand, branchTypes[name])
			}
		}
		typedBranches++
	}
	return returned, firstReturn, nil
}
