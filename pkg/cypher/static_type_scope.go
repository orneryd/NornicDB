package cypher

import (
	"slices"
	"strings"
)

// typeScope is the one static type of every variable a clause sees (#1055).
// The match validator's binding kinds (matchSemanticScope) and the static
// checks' value and member types (staticTypeScope) are views of it, so a
// projected expression is typed once, by projectedOperand, for both.
type typeScope map[string]staticOperand

// bindingKind is the match validator's view of an operand: the entity it is
// (a node, a relationship, a list of either, a path), a value that is none of
// them, or unknown. A type of several choices ("Map, Node or Relationship")
// is a value: never read as an entity.
func (operand staticOperand) bindingKind() matchBindingKind {
	switch operand.kind {
	case "Node":
		return matchBindingNode
	case "Relationship":
		return matchBindingRelationship
	case "List<Node>":
		return matchBindingNodeList
	case "List<Relationship>":
		return matchBindingRelationshipList
	case "Path":
		return matchBindingPath
	case "", "Null":
		if operand.nonEntity || operand.nonBoolean {
			return matchBindingValue
		}
		return matchBindingUnknown
	}
	// A type of several choices that may be a node or relationship ("Map,
	// Node or Relationship") is checked when the row runs.
	for _, choice := range staticTypeChoices(operand.typeName()) {
		if choice == "Node" || choice == "Relationship" {
			return matchBindingUnknown
		}
	}
	return matchBindingValue
}

// listOfType is the type of a list of typeName's values: List<X> for one
// type, each choice's list for several ("List<A>, List<B> or List<C>").
func listOfType(typeName string) string {
	choices := staticTypeChoices(typeName)
	for index, choice := range choices {
		choices[index] = "List<" + choice + ">"
	}
	return joinTypeNames(choices)
}

// elementOfType is the type of an element of a list of typeName ("" when
// typeName isn't a list of known elements): X of List<X>, each choice's
// element for several list types.
func elementOfType(typeName string) string {
	choices := staticTypeChoices(typeName)
	if len(choices) == 0 {
		return ""
	}
	elements := make([]string, 0, len(choices))
	for _, choice := range choices {
		if !strings.HasPrefix(choice, "List<") || choice == "List<T>" {
			return ""
		}
		elements = append(elements, strings.TrimSuffix(strings.TrimPrefix(choice, "List<"), ">"))
	}
	return joinTypeNames(elements)
}

// typedOperand is the operand of a static type name. A property value's type
// (staticPropertyValueType) names every type a property can hold: the
// function and pattern checks reject it where none fits (labels(v),
// MATCH (v)); the operator checks, which take one type, read it as unknown.
func typedOperand(typeName string) staticOperand {
	if typeName == staticPropertyValueType {
		return staticOperand{display: typeName, nonEntity: true}
	}
	if !strings.Contains(typeName, ", ") && !strings.Contains(typeName, " or ") {
		return knownOperand(typeName)
	}
	if choices := staticTypeChoices(typeName); len(choices) > 1 {
		// Existing numeric operands already have an operator category. Other
		// unions retain their alternatives without claiming one scalar kind.
		if operand := knownOperand(typeName); operand.numeric() {
			return operand
		}
		operand := staticOperand{display: typeName, union: true}
		allLists, allNumbers, allMaps := true, true, true
		for _, choice := range choices {
			member := knownOperand(choice)
			allLists = allLists && member.list()
			allNumbers = allNumbers && member.numeric()
			allMaps = allMaps && containsString(staticCatalogTypeNames["MAP"], choice)
		}
		if allLists {
			operand.kind = "List<T>"
		} else if allNumbers {
			operand.kind = "Number"
		} else if allMaps {
			operand.kind = "Map"
		}
		return operand
	}
	return knownOperand(typeName)
}

// typeName is the static type name of operand: its kind, or a property
// value's type (typedOperand); "" when neither is known.
func (operand staticOperand) typeName() string {
	if operand.union || operand.kind == "" && operand.display == staticPropertyValueType {
		return operand.display
	}
	return operand.kind
}

// operandOfKind is the operand a binding kind describes.
func operandOfKind(kind matchBindingKind) staticOperand {
	switch kind {
	case matchBindingNode:
		return knownOperand("Node")
	case matchBindingRelationship:
		return knownOperand("Relationship")
	case matchBindingNodeList:
		return knownOperand("List<Node>")
	case matchBindingRelationshipList:
		return knownOperand("List<Relationship>")
	case matchBindingPath:
		return knownOperand("Path")
	case matchBindingValue:
		return staticOperand{nonEntity: true}
	}
	return staticOperand{}
}

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
		kinds[name] = operand.bindingKind()
	}
	return kinds
}

// static is the static checks' view of types: the binding kinds, the types of
// variables that aren't entities, and the member types of maps.
func (types typeScope) static(cypher25 bool) staticTypeScope {
	scope := staticTypeScope{kinds: types.kinds(), complete: true, cypher25: cypher25}
	for name, operand := range types {
		if operand.kind == "" && operand.nonInteger {
			if scope.derived == nil {
				scope.derived = make(map[string]staticOperand)
			}
			scope.derived[name] = operand
			if operand.display != "" {
				if scope.values == nil {
					scope.values = make(map[string]string)
				}
				scope.values[name] = operand.display
			}
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
		if isEntityTypeName(types[name].kind) {
			continue
		}
		types[name] = typedOperand(typeName)
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
func projectTypeScope(types typeScope, cypher25 bool, clause string) typeScope {
	return projectTypeScopeIn(types, types.static(cypher25), clause)
}

// projectTypeScopeIn is projectTypeScope for a caller that holds types'
// static view (input) already.
func projectTypeScopeIn(types typeScope, input staticTypeScope, clause string) typeScope {
	return projectLocalTypeScope(types, types, input, clause)
}

func projectLocalTypeScope(types, named typeScope, input staticTypeScope, clause string) typeScope {
	input.complete = false
	projected := make(typeScope)
	types.forEachProjectedBinding(clause, func(name, expression string, inherited bool) {
		if inherited {
			projected[name] = types[name]
			return
		}
		operand := projectedOperand(named, input, expression)
		if simpleSemanticIdentifier(name) == "" {
			// Unaliased, a column is named by its text (WITH {a: 1}); only a
			// name binds a variable whose members are known.
			operand.members = nil
		}
		projected[normalizeProjectionColumnName(name)] = operand
	})
	return projected
}

// forEachProjectedBinding owns projection names and wildcard expansion for
// type inference and dependency proofs. A proof need not allocate or infer
// another type scope just to observe which incoming bindings a * consumes.
func (types typeScope) forEachProjectedBinding(clause string, visit func(name, expression string, inherited bool)) {
	keyword := "WITH"
	if startsWithKeywordFold(strings.TrimSpace(clause), "RETURN") { keyword = "RETURN" }
	body, _ := projectionSemanticBodyAndTail(clause, keyword)
	body, _ = cutDistinct(body)
	for _, raw := range splitTopLevelComma(body) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
		if expression == "*" {
			names := make([]string, 0, len(types))
			for name := range types { names = append(names, name) }
			slices.Sort(names)
			for _, name := range names {
				visit(name, name, true)
			}
			continue
		}
		name := alias
		if name == "" {
			name = simpleSemanticIdentifier(expression)
		}
		if name == "" {
			name = strings.TrimSpace(expression)
		}
		visit(name, expression, false)
	}
}

// projectedOperand is the static type of a projected expression, as Neo4j
// types it before the statement runs: a bare variable's own; null open to
// any kind; coalesce of variables of one kind and a list of nodes or of
// relationships that kind; collect of nodes or relationships their list, a
// count an Integer, any other aggregate a value; a node's or relationship's
// property a value of unknown type; a map member unknown in Cypher 5; and
// any other expression the type the static operator check gives it (a
// literal, arithmetic, a function's result, CASE, a comparison, a map
// projection, a comprehension, a subscript or slice of a typed list).
func projectedOperand(types typeScope, input staticTypeScope, expression string) staticOperand {
	expression = strings.TrimSpace(expression)
	if strings.EqualFold(expression, "null") {
		return staticOperand{}
	}
	if source := simpleSemanticIdentifier(expression); source != "" && !isLiteralKeyword(source) {
		return types[source]
	}
	if kind, ok := entityListLiteralKind(expression, input.kinds); ok {
		return operandOfKind(kind)
	}
	if function, arguments, call := parseFunctionCallWS(expression); call {
		if operand, typed := projectedFunctionOperand(types, input, lowerASCII(function), arguments); typed {
			return operand
		}
		// A function's own result type needs no look at its arguments:
		// their errors are the clause checks' to report.
		if result := staticFunctionResultType(function, arguments); result != "" {
			return knownOperand(result)
		}
	}
	if name, argument, _, aggregate := parsePipelineAggregate(expression); aggregate {
		switch name {
		case "collect":
			if element := projectedOperand(types, input, argument).typeName(); element != "" && element != "Null" {
				return knownOperand(listOfType(element))
			}
			return knownOperand("List<T>")
		case "count":
			return knownOperand("Integer")
		}
		return staticOperand{nonEntity: true}
	}
	if variable, _, property := parseVarPropertyRef(expression); property && simpleSemanticIdentifier(variable) == variable {
		switch input.kinds[variable] {
		case matchBindingNode, matchBindingRelationship:
			return typedOperand(staticPropertyValueType)
		}
		if !input.cypher25 {
			// A map's member (x.node) is checked when the row runs.
			return staticOperand{}
		}
	}
	if inner, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		if operand, typed := projectedListOperand(types, input, inner); typed {
			return operand
		}
	}
	if receiver, index, subscript, postfix := staticPostfixSplit(expression); postfix && subscript {
		// An element of a typed list ([n, o][0], nodes(p)[0]) has its
		// element type; a slice is the list's type.
		list := projectedOperand(types, input, receiver)
		if element := elementOfType(list.typeName()); element != "" {
			if _, _, slice := staticSliceBounds(index); slice {
				return list
			}
			return typedOperand(element)
		}
	}
	operand, err := (staticOperatorChecker{scope: input}).check(expression)
	if err != nil {
		return staticOperand{}
	}
	return operand
}

// unwoundOperand is the static type of the variable UNWIND binds from list:
// a list's element type (a node of a List<Node>, an Integer of a
// List<Integer>), a known non-list value itself (UNWIND 5), unknown for a
// list of unknown elements.
func unwoundOperand(types typeScope, input staticTypeScope, clause string) staticOperand {
	return listElementOperand(types, input, unwindSourceExpression(clause))
}

// listElementOperand is the static type of an element of list (UNWIND's
// variable, a comprehension's or quantifier's local; localBindingKind).
func listElementOperand(types typeScope, input staticTypeScope, list string) staticOperand {
	operand := projectedOperand(types, input, list)
	if element := elementOfType(operand.typeName()); element != "" {
		return typedOperand(element)
	}
	if operand.kind != "" && !strings.Contains(operand.kind, "List<") {
		return operand
	}
	if typeName := staticListElementType(list); typeName != "" {
		return knownOperand(typeName)
	}
	if matchFuncStartAndSuffix(strings.TrimSpace(list), "range") {
		return knownOperand("Integer")
	}
	return staticOperand{}
}

// projectMatchSemanticScope is the binding-kind view of the variables a WITH
// clause projects (projectTypeScope).
func projectMatchSemanticScope(input matchSemanticScope, clause string) matchSemanticScope {
	scope := staticTypeScope{kinds: input}
	return projectTypeScopeIn(typeScopeOf(scope), scope, clause).kinds()
}

// projectStaticValueTypes is the value-type view of the variables a WITH
// clause projects (projectTypeScope).
func projectStaticValueTypes(scope staticTypeScope, clause string) map[string]string {
	return projectTypeScopeIn(typeScopeOf(scope), scope, clause).static(scope.cypher25).values
}

// projectStaticValueMembers is, in a Cypher 25 statement, the member-type
// view of the variables a WITH clause projects (projectTypeScope).
func projectStaticValueMembers(scope staticTypeScope, clause string) map[string]map[string]staticOperand {
	if !scope.cypher25 {
		return nil
	}
	return projectTypeScopeIn(typeScopeOf(scope), scope, clause).static(true).members
}

// carryStaticSubqueryImports models both possible global-aggregate rows. A
// nonempty group carries current operands; an empty group's missing input
// falls back to original imports. Both scopes use the runtime retention owner.
// Keep the type facts shared by both rows and defer only disagreements until
// execution. No cardinality guess is needed for parameterized UNWIND, filters
// or data-dependent MATCH.
func carryStaticSubqueryImports(projected, input, imports typeScope, projection string) {
	var empty typeScope
	if len(imports) > 0 && pipelineProjectionHasOnlyAggregates(returnProjectionPlanFor("RETURN "+projection).projections) {
		empty = maps.Clone(projected)
		carrySubqueryImports(empty, nil, imports)
	}
	carrySubqueryImports(projected, input, imports)
	for name, operand := range empty {
		projected[name] = commonStaticOperand(projected[name], operand)
	}
}

// commonStaticOperand widens differing facts independently: an unrelated map
// member must not erase the Map kind or another member's agreed type. Members
// present in only one alternative have no guaranteed type in the result.
// Exclusions (nonBoolean, nonInteger, nonEntity) hold only when both
// alternatives agree; a deferred predicate check stays deferred if either
// alternative defers it.
func commonStaticOperand(left, right staticOperand) staticOperand {
	common := staticOperand{
		nonBoolean:        left.nonBoolean && right.nonBoolean,
		predicateDeferred: left.predicateDeferred || right.predicateDeferred,
		nonInteger:        left.nonInteger && right.nonInteger,
		nonEntity:         left.nonEntity && right.nonEntity,
		// A union in either alternative keeps only an operator category in
		// kind, so the common operand can't claim that kind as known.
		union: left.union || right.union,
	}
	if left.kind == right.kind {
		common.kind = left.kind
	}
	if left.display == right.display {
		common.display = left.display
	}
	if left.parameter == right.parameter {
		common.parameter = left.parameter
	}
	for name, member := range left.members {
		if other, exists := right.members[name]; exists {
			if common.members == nil {
				common.members = make(map[string]staticOperand)
			}
			common.members[name] = commonStaticOperand(member, other)
		}
	}
	return common
}

// callSubqueryImports is the scope a CALL subquery's body starts from: CALL
// (a, b) { … } imports a and b, CALL (*) { … } and an old-style CALL { … }
// every outer variable (the latter's leading WITH picks them). ok is false
// for a CALL without a body; scoped is set for CALL (…) { }.
func callSubqueryImports(callText string, outer typeScope) (typeScope, bool, bool) {
	if strings.IndexByte(callText, '{') < 0 || !containsFold(callText, "CALL") { return nil, false, false }
	scoped := callSubqueryHasScopeClause(callText)
	names := parseCallSubqueryImportVariables(callText)
	initial := make(typeScope)
	if scoped && names != nil {
		for _, name := range names { if operand, bound := outer[name]; bound { initial[name] = operand } }
	} else { for name, operand := range outer { initial[name] = operand } }
	return initial, scoped, true
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
func (e *StorageExecutor) callSubqueryReturnTypes(callText string, branches []string, outer typeScope, cypher25 bool) (typeScope, error) {
	model, err := e.callQueryScope(callText, outer, nil, cypher25)
	if err != nil || model == nil { return nil, err }
	return model.returned, nil
}
// projectedFunctionOperand is the static type of a call whose result type
// follows from its arguments: coalesce of arguments of one type (open when
// one is null), nodes / relationships of a path, startNode / endNode,
// head / last of a typed list. typed is false for any other call.
func projectedFunctionOperand(types typeScope, input staticTypeScope, function, arguments string) (staticOperand, bool) {
	switch function {
	case "coalesce":
		operand := staticOperand{}
		for index, argument := range splitTopLevelComma(arguments) {
			typed := projectedOperand(types, input, argument)
			if typed.typeName() == "" || (index > 0 && typed.typeName() != operand.typeName()) {
				return staticOperand{}, true
			}
			operand = typed
		}
		return operand, true
	case "nodes":
		return knownOperand("List<Node>"), true
	case "relationships":
		return knownOperand("List<Relationship>"), true
	case "startnode", "endnode":
		return knownOperand("Node"), true
	case "head", "last":
		if element := elementOfType(projectedOperand(types, input, arguments).kind); element != "" {
			return typedOperand(element), true
		}
	}
	return staticOperand{}, false
}

// projectedListOperand is the static type of a list literal or list
// comprehension (inner is the text between the brackets): a list of the one
// type every element has ([p], [coalesce(1, 2)]), a map widened to "Map, Node
// or Relationship" as Neo4j types a list of maps. A comprehension is a list
// of its projection's type, typed with its local as an element of the list
// it iterates (listElementOperand: [at IN range(0, 1) | [at]] is a
// List<List<Integer>>), or of that element's type when it projects nothing
// ([at IN l WHERE …]). typed is false when the elements' types differ or
// aren't known.
func projectedListOperand(types typeScope, input staticTypeScope, inner string) (staticOperand, bool) {
	if variable, listExpression, _, projection, comprehension := parseListComprehension(inner); comprehension {
		element := listElementOperand(types, input, listExpression)
		if strings.TrimSpace(projection) == "" {
			if typeName := element.typeName(); typeName != "" && typeName != "Null" {
				return knownOperand(listOfType(typeName)), true
			}
			return staticOperand{}, false
		}
		scope := make(typeScope, len(types)+1)
		for name, operand := range types {
			scope[name] = operand
		}
		scope[variable] = element
		projected := projectedOperand(scope, scope.static(input.cypher25), projection).typeName()
		if projected == "" || projected == "Null" {
			return staticOperand{}, false
		}
		return knownOperand(listOfType(projected)), true
	}
	if strings.TrimSpace(inner) == "" {
		return staticOperand{}, false
	}
	element := ""
	for _, item := range splitTopLevelComma(inner) {
		typed := projectedOperand(types, input, item).typeName()
		if typed == "Map" {
			typed = "Map, Node or Relationship"
		}
		if typed == "" || typed == "Null" || (element != "" && typed != element) {
			return staticOperand{}, false
		}
		element = typed
	}
	return knownOperand(listOfType(element)), true
}
