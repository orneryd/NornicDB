package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"context"
	"slices"
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

// projectTypeScope is the typeScope a WITH clause projects from types: each
// item's operand by its alias, every variable for WITH *.
func projectTypeScope(ctx context.Context, types typeScope, cypher25 bool, clause string) typeScope {
	return projectTypeScopeIn(ctx, types, types.static(cypher25), clause)
}

// projectTypeScopeIn is projectTypeScope for a caller that holds types'
// static view (input) already.
func projectTypeScopeIn(ctx context.Context, types typeScope, input staticTypeScope, clause string) typeScope {
	return projectLocalTypeScope(ctx, types, types, input, clause)
}

func projectLocalTypeScope(ctx context.Context, types, named typeScope, input staticTypeScope, clause string) typeScope {
	if input.projected != nil {
		return typeScopeOf(*input.projected)
	}
	values, _, err := (&StorageExecutor{}).enrichBoundTypes(ctx, clause, types, input.bindings, "", input.cypher25)
	if err != nil || values == nil {
		return nil
	}
	statement := values.program.root.program.statement
	return values.selectValues(statement.After)
}

// forEachProjectedBinding consumes the constructor-held importing-WITH plan.
// It never parses a clause string or builds a second projection schedule.
func (types typeScope) forEachProjectedBinding(plan *returnProjectionPlan, visit func(name, expression, text string, inherited bool)) (wildcard bool) {
	if plan == nil || !plan.valid {
		return false
	}
	wildcard = plan.star
	if wildcard {
		names := make([]string, 0, len(types))
		for name := range types {
			names = append(names, name)
		}
		slices.Sort(names)
		plan = plan.withStarExpanded(names)
	}
	for _, item := range plan.projections {
		inherited := wildcard && item.definitionStart == -1 && item.direct
		text := item.expr + " AS " + cyphertext.PrintIdentifier(item.alias, cyphertext.IsIdentifier, cyphertext.AcceptedSource)
		visit(item.alias, item.expr, text, inherited)
	}
	return wildcard
}

// projectedSourceVariable distinguishes a variable's identity from a literal
// with the same spelling. Quoted true/null remain variables; their unquoted
// spellings are values. Projection types and import provenance share this fact.
func projectedSourceVariable(expression string) string {
	expression = unwrapOuterParens(strings.TrimSpace(StripComments(expression)))
	name := simpleSemanticIdentifier(expression)
	if name != "" && (isBacktickQuotedName(expression) || !isLiteralKeyword(name)) {
		return name
	}
	return ""
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
func projectedOperand(ctx context.Context, types typeScope, input staticTypeScope, expression string) staticOperand {
	expression = strings.TrimSpace(expression)
	if strings.EqualFold(expression, "null") {
		// Null, as written in place (+z of WITH null AS z is a number); it
		// fits every position and binds as any kind (bindingKind).
		return provenOperand("Null")
	}
	if source := projectedSourceVariable(expression); source != "" {
		return types[source]
	}
	if kind, ok := entityListLiteralKind(expression, input.kinds); ok {
		return operandOfKind(kind)
	}
	// Every other expression has the one static type its scope derives.
	return input.operandOf(ctx, expression)
}

// unwoundOperand is the static type of the variable UNWIND binds from list:
// a list's element type (a node of a List<Node>, an Integer of a
// List<Integer>), a known non-list value itself (UNWIND 5), unknown for a
// list of unknown elements.
func unwoundOperand(ctx context.Context, types typeScope, input staticTypeScope, clause string) staticOperand {
	return listElementOperand(ctx, types, input, unwindSourceExpression(clause))
}

// listElementOperand is the static type of an element of list (UNWIND's
// variable, a comprehension's or quantifier's local; localBindingKind).
func listElementOperand(ctx context.Context, types typeScope, input staticTypeScope, list string) staticOperand {
	operand := projectedOperand(ctx, types, input, list)
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

// projectStaticValueTypes is the value-type view of the variables a WITH
// clause projects (projectTypeScope).
func projectStaticValueTypes(ctx context.Context, scope staticTypeScope, clause string) map[string]string {
	return projectTypeScopeIn(ctx, typeScopeOf(scope), scope, clause).static(scope.cypher25).values
}

// projectStaticValueMembers is, in a Cypher 25 statement, the member-type
// view of the variables a WITH clause projects (projectTypeScope).
func projectStaticValueMembers(ctx context.Context, scope staticTypeScope, clause string) map[string]map[string]staticOperand {
	if !scope.cypher25 {
		return nil
	}
	return projectTypeScopeIn(ctx, typeScopeOf(scope), scope, clause).static(true).members
}
