package cypher

import "strings"

// staticOperand is an operand's static type. The type is authoritative in
// one place: a single type has its name as operandCategory, and a CASE union
// keeps every alternative in display, with operandCategory naming only the
// operator category its alternatives share (List<T>, Number, Map) or none.
// Consumers ask through this file's methods (typeName, fits, excludes,
// elementOperand, is, known, list, numeric, …) and never read the category
// field themselves (TestStaticOperandCategoryOwner), so no consumer can
// mistake a union's shared category for its type.
type staticOperand struct {
	operandCategory string
	display         string
	parameter       string
	// nonEntity is set for a value known not to be a node, relationship,
	// path or list of them whose type isn't otherwise known (a node's
	// property, an aggregate): the match validator's value kind (#1055).
	nonEntity bool
	// accumulated marks a property value reduce accumulated
	// (reduce(a = n.k, x IN l | a)): a list literal of one holds a value of
	// any type to Neo4j, as [n.k] holds the property value.
	accumulated bool
	// union keeps the complete result type in display when the category can
	// only describe a common operator category (or is unknown to operator rules).
	union bool
	// emptyList marks the literal [], whose elements add no type to a
	// concatenation (xs + [] has xs's type).
	emptyList bool
	// members are the static types of a map literal's entries by key, in a
	// Cypher 25 statement (Neo4j 2026.09 types {a: 2}.a as an Integer;
	// Neo4j 5.26 doesn't): staticMapMember reads them.
	members map[string]staticOperand
	// proven is set only by a rule whose result typing has been verified
	// against Neo4j (proved, provenOperand, composedFrom): every other
	// operand leaves the deriver unknown (gateProven), so it can't cause a
	// rejection Neo4j wouldn't make.
	proven bool
}

// proved marks operand as the result of a verified rule.
func (operand staticOperand) proved() staticOperand {
	operand.proven = true
	return operand
}

// provenOperand is a verified rule's fixed type: a literal's, a predicate's
// Boolean, a declared function result's.
func provenOperand(kind string) staticOperand {
	if kind == "" {
		// No rule typed it: unknown.
		return staticOperand{}
	}
	return knownOperand(kind).proved()
}

// gateProven is the one gate at the deriver's boundary (check): an operand
// no verified rule produced leaves as unknown, with none of its facts.
func gateProven(operand staticOperand) staticOperand {
	if !operand.proven {
		return staticOperand{}
	}
	return operand
}

// composable reports whether a verified composition rule may build on
// operands: each is proven, and none has an alternative whose composition
// Neo4j types differently from its direct use (null: CASE … null … END
// through head or arithmetic; Any: a reduce result through a list). A
// property value composes as the union of the types it may hold; the one
// place Neo4j widens it, a list literal of several, leaves its elements
// unknown (unionListLiteralOperand's least common types).
func composable(operands ...staticOperand) bool {
	for _, operand := range operands {
		if !operand.proven || operand.mayBeNull() || operand.hasAnyAlternative() {
			return false
		}
	}
	return true
}

// joinable reports an operand whose alternatives a join keeps (CASE,
// coalesce, min, max, collect): a proven, non-null type, an Any alternative
// included, which Neo4j unions as a type of its own (min(v) of Any, Integer
// or String is Any, Integer or String; its .day is a type mismatch).
func (operand staticOperand) joinable() bool {
	return operand.proven && !operand.mayBeNull() && operand.typeName() != ""
}

// composedFrom marks a selection of its inputs' alternatives (a union's list
// part, a list's elements, a reversed string or list) proven when every
// input is joinable: an Any alternative stays a type of its own (head of
// Any, Float, Integer, Number, String or List<Float> is a Float).
func (operand staticOperand) composedFrom(inputs ...staticOperand) staticOperand {
	operand.proven = true
	for _, input := range inputs {
		operand.proven = operand.proven && input.joinable()
	}
	return operand
}

// signatureOperand is the static type an operator's signatures give its
// result (n.k - 2 is a Float or an Integer): one proven operand of the type
// lattice, a single type or a union of types, whose alternatives every
// constructor and consumer reads as it reads any other operand's.
func signatureOperand(types []string) staticOperand {
	return typedOperand(joinTypeNames(canonicalTypeOrder(types))).proved()
}

// canonicalTypeOrder is types in the order Neo4j names a union's types
// (Float or Integer, Float, Integer or String): Any, Boolean, the numbers,
// Point, String, Duration and the temporal types, their lists, then
// everything else as given.
func canonicalTypeOrder(types []string) []string {
	ordered := make([]string, 0, len(types))
	for _, name := range append([]string{"Any"}, staticTypeChoices(staticPropertyValueType)...) {
		if containsString(types, name) {
			ordered = append(ordered, name)
		}
	}
	for _, name := range types {
		if !containsString(ordered, name) {
			ordered = append(ordered, name)
		}
	}
	return ordered
}

// isPropertyValue reports a node's or relationship's property value, typed
// as every type a property can hold (staticPropertyValueType).
func (operand staticOperand) isPropertyValue() bool {
	return operand.operandCategory == "" && operand.display == staticPropertyValueType
}

// withoutTemporalListElements is a CASE result's choices as Neo4j 5.26 types
// them: when its branches mix a temporal value or duration (or a list of
// them) with a list of other values, every list's elements are of unknown
// type ("Duration or List<T>" for CASE … duration(…) … [1] END, whose head
// toUpper admits). Branches of temporal lists only (List<Date> or
// List<Duration>), or holding a property value's types, keep theirs. Other
// unions (an operator's result in a list literal: [n.k * 1.5] is a
// List<Float> or List<Duration>) keep every alternative.
func withoutTemporalListElements(choices []string) []string {
	temporalName := func(name string) bool { return name == "Duration" || isTemporalTypeName(name) }
	temporal, otherList := false, false
	for _, choice := range choices {
		element := strings.TrimSuffix(strings.TrimPrefix(choice, "List<"), ">")
		switch {
		case temporalName(choice) || isListTypeName(choice) && temporalName(element):
			temporal = true
		case isListTypeName(choice):
			otherList = true
		}
	}
	if !temporal || !otherList || holdsPropertyValueTypes(choices) {
		return choices
	}
	widened := make([]string, 0, len(choices))
	for _, choice := range choices {
		if isListTypeName(choice) {
			choice = "List<T>"
		}
		widened = appendTypeChoice(widened, choice)
	}
	return widened
}

// holdsPropertyValueTypes reports choices that hold every type a property
// value may have, or a list of each (a union with n.k, [n.k]).
func holdsPropertyValueTypes(choices []string) bool {
	for _, name := range staticTypeChoices(staticPropertyValueType) {
		if !containsString(choices, name) && !containsString(choices, "List<"+name+">") {
			return false
		}
	}
	return true
}

// hasAnyAlternative reports an operand one of whose alternatives is Any
// (the least common type of values that differ).
func (operand staticOperand) hasAnyAlternative() bool {
	return containsString(staticTypeChoices(operand.typeName()), "Any")
}

// staticMapMember is the operand a map with known members gives for key
// (m.key, m['key']), and whether it has one.
func (operand staticOperand) staticMapMember(key string) (staticOperand, bool) {
	member, found := operand.members[key]
	return member, found
}

// knownOperand is the operand of one static type.
func knownOperand(kind string) staticOperand {
	return staticOperand{operandCategory: kind, display: kind}
}

// displayedOperand is the operand of category whose errors name it as display
// (a map typed as "Map, Node or Relationship", a parameter's value).
func displayedOperand(category, display, parameter string) staticOperand {
	return staticOperand{operandCategory: category, display: display, parameter: parameter}
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
			allLists = allLists && isListTypeName(choice)
			allNumbers = allNumbers && isNumericTypeName(choice)
			allMaps = allMaps && containsString(staticCatalogTypeNames["MAP"], choice)
		}
		if allLists {
			operand.operandCategory = "List<T>"
		} else if allNumbers {
			operand.operandCategory = "Number"
		} else if allMaps {
			operand.operandCategory = "Map"
		}
		return operand
	}
	return knownOperand(typeName)
}

// typeName is the static type name of operand: its single type, every
// alternative of a union, or a property value's type (typedOperand); ""
// when none is known.
func (operand staticOperand) typeName() string {
	if operand.union || operand.operandCategory == "" && operand.display == staticPropertyValueType {
		return operand.display
	}
	return operand.operandCategory
}

// is reports whether operand's operator category is name: its single type,
// or the category every alternative of a union shares.
func (operand staticOperand) is(name string) bool { return operand.operandCategory == name }

// known reports an operand whose operator category is known: a single type,
// or a union whose alternatives share a list or map category.
func (operand staticOperand) known() bool {
	return operand.operandCategory != "" && (!operand.union || operand.list() || operand.is("Map"))
}

// fits reports whether operand may have a type accepts takes: its single
// type does, or some alternative of its union does.
//
// A position rejects an operand only on proof: when its type is fully
// derived and no alternative fits. An operand of no known type fits every
// position (the row decides), and every derivation that can't type an
// expression completely returns an unknown operand rather than a partial
// type. (Any, the least common type Neo4j derives for values that differ,
// is a type of its own: it fits only where Any is taken.)
func (operand staticOperand) fits(accepts func(typeName string) bool) bool {
	if operand.proven && operand.isPropertyValue() {
		// A property value (any type a property may hold): one of its
		// display's types.
		for _, choice := range staticTypeChoices(operand.display) {
			if accepts(choice) {
				return true
			}
		}
		return false
	}
	if !operand.union {
		if operand.operandCategory == "" || accepts(operand.operandCategory) {
			return true
		}
		// A category of several choices ("Map, Node or Relationship") fits
		// where one of them does.
		if !strings.Contains(operand.operandCategory, ", ") && !strings.Contains(operand.operandCategory, " or ") {
			return false
		}
	}
	for _, choice := range staticTypeChoices(operand.typeName()) {
		if accepts(choice) {
			return true
		}
	}
	return false
}

// excludes reports an operand statically known to have none of the types
// accepts takes: Null fits every position, and a union is excluded only when
// none of its alternatives fits (Neo4j's compile-time type mismatch).
func (operand staticOperand) excludes(accepts func(typeName string) bool) bool {
	return !operand.fits(func(typeName string) bool { return typeName == "Null" || accepts(typeName) })
}

// elementOperand is the static type of an element of operand as a list: each
// list alternative's element type; unknown when operand isn't a list of
// known elements.
func (operand staticOperand) elementOperand() staticOperand {
	if operand.mayBeNull() {
		// head(CASE … null … [1] END) is any type to Neo4j.
		return staticOperand{}
	}
	list := operand.listPart()
	element := typedOperand(elementOfType(list.typeName()))
	element.proven = list.proven && !element.mayBeNull()
	return gateProven(element)
}

// listPart is the list alternatives of operand, which a list position reads
// (head(v), v[0], v[0..1], a comprehension over v): every alternative of a
// list's type, the list alternatives of a union that may also be a non-list
// value (Neo4j types head of [1] or true as an Integer; the other alternative
// is the row's TypeError), or unknown when none is a list.
func (operand staticOperand) listPart() staticOperand {
	if operand.list() || !operand.union {
		if operand.list() && composable(operand) {
			return operand
		}
		return staticOperand{}
	}
	var lists []string
	for _, choice := range staticTypeChoices(operand.typeName()) {
		if isListTypeName(choice) {
			lists = append(lists, choice)
		}
	}
	if len(lists) == 0 {
		return staticOperand{}
	}
	if operand.mayBeNull() {
		// tail and a slice of a list or null are a list whose elements
		// Neo4j doesn't type (v.a and labels(v) are rejected,
		// toUpper(head(v)) is not).
		if operand.proven && !operand.hasAnyAlternative() {
			return provenOperand("List<T>")
		}
		return staticOperand{}
	}
	return gateProven(typedOperand(joinTypeNames(lists)).composedFrom(operand))
}

// reversedOperand is the static type of reverse(operand): a list
// alternative itself, a string a string; unknown when no alternative is
// either.
func (operand staticOperand) reversedOperand() staticOperand {
	// Null admits reverse without constraining its input to a concrete overload.
	// The declared result remains String or List, never an arbitrary scalar.
	if operand.mayBeNull() || operand.isPropertyValue() {
		return typedOperand("String or List<T>").proved()
	}

	var results []string
	for _, alternative := range operand.alternatives() {
		switch {
		case alternative.list():
			results = appendTypeChoice(results, alternative.typeName())
		case alternative.is("String"):
			results = appendTypeChoice(results, "String")
		case !alternative.known():
			return staticOperand{}
		}
	}
	if len(results) == 0 {
		return staticOperand{}
	}
	// Non-null inputs retain the fitting overloads and their element bounds.
	return gateProven(typedOperand(joinTypeNames(results)).composedFrom(operand))
}

// operandOf is the one static type of expression in scope: every validator
// reads a type through it and decides through fits or excludes. Each call
// types the expression in this scope only, so the same text in another
// scope (a shadowing WITH, another UNION branch) is typed by that scope.
func (scope staticTypeScope) operandOf(expression string) staticOperand {
	operand, err := (staticOperatorChecker{scope: scope}).check(strings.TrimSpace(expression))
	if err != nil {
		return staticOperand{}
	}
	return operand
}

// staticTextType is the static type the operator check derives for
// expression with no variable or parameter facts: a literal's, a
// constructor's, an operator's, a function call's; "" when it derives none.
// A reader without a scope reads an expression's type here.
func staticTextType(expression string) string {
	return (staticTypeScope{}).operandOf(expression).typeName()
}

// staticLiteralTypeName is staticTextType for an expression led by a
// literal, "" for one led by a variable, a parameter or a call (typed with
// its scope).
func staticLiteralTypeName(expression string) string {
	expression = strings.TrimSpace(expression)
	for {
		inner, ok := stripEnclosingExpressionParentheses(expression)
		if !ok {
			break
		}
		expression = strings.TrimSpace(inner)
	}
	if expression == "" || expression[0] == '$' {
		return ""
	}
	if token, _, ok := scanIdentifierToken(expression, 0); ok && !strings.EqualFold(token, "true") && !strings.EqualFold(token, "false") {
		return ""
	}
	return staticTextType(expression)
}

// unwoundOperand is the static type of a value UNWIND binds from operand:
// each list alternative's element type, and each non-list alternative itself
// (UNWIND 5 binds 5); unknown when a list's elements aren't known.
func (operand staticOperand) unwoundOperand() staticOperand {
	typeName := operand.typeName()
	choices := staticTypeChoices(typeName)
	if typeName == "" || typeName == staticPropertyValueType {
		return staticOperand{}
	}
	elements := make([]string, 0, len(choices))
	for _, choice := range choices {
		if !isListTypeName(choice) {
			elements = appendTypeChoice(elements, choice)
			continue
		}
		element := elementOfType(choice)
		if element == "" {
			return staticOperand{}
		}
		for _, item := range staticTypeChoices(element) {
			elements = appendTypeChoice(elements, item)
		}
	}
	return gateProven(typedOperand(joinTypeNames(elements)).composedFrom(operand))
}

// joinedOperand is the one static type of a value that is either left or
// right (a coalesce, a CASE, a concatenated list's elements, a UNION's
// column, an import a global aggregate carries from a nonempty or an empty
// group): the alternatives of both; for one type, or when either is
// unknown, the facts both agree on (commonStaticOperand).
func joinedOperand(left, right staticOperand) staticOperand {
	if left.typeName() == "" || right.typeName() == "" || left.typeName() == right.typeName() {
		return commonStaticOperand(left, right)
	}
	choices := staticTypeChoices(left.typeName())
	for _, choice := range staticTypeChoices(right.typeName()) {
		choices = appendTypeChoice(choices, choice)
	}
	joined := typedOperand(joinTypeNames(choices))
	joined.proven = left.proven && right.proven
	return joined
}

// appendTypeChoice adds choice to choices unless it is already there.
func appendTypeChoice(choices []string, choice string) []string {
	if containsString(choices, choice) {
		return choices
	}
	return append(choices, choice)
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

// operandOfKind is the operand a binding kind describes.
func operandOfKind(kind matchBindingKind) staticOperand {
	switch kind {
	case matchBindingNode:
		return provenOperand("Node")
	case matchBindingRelationship:
		return provenOperand("Relationship")
	case matchBindingNodeList:
		return provenOperand("List<Node>")
	case matchBindingRelationshipList:
		return provenOperand("List<Relationship>")
	case matchBindingPath:
		return provenOperand("Path")
	case matchBindingValue:
		return staticOperand{nonEntity: true, proven: true}
	}
	return staticOperand{}
}

func isListTypeName(typeName string) bool { return strings.HasPrefix(typeName, "List<") }

func isNumericTypeName(typeName string) bool {
	switch typeName {
	case "Integer", "Float", "Number", "Float, Integer or Number":
		return true
	}
	return false
}

func isTemporalTypeName(typeName string) bool {
	switch typeName {
	case "Date", "Time", "LocalTime", "LocalDateTime", "DateTime":
		return true
	}
	return false
}

func (operand staticOperand) numeric() bool {
	return !operand.union && isNumericTypeName(operand.operandCategory)
}

func (operand staticOperand) list() bool { return isListTypeName(operand.operandCategory) }

func (operand staticOperand) duration() bool { return operand.is("Duration") }

// joinsStringStatically reports whether a Cypher 25 string + operand is the
// string joined with the operand's text: a string, number, boolean, point,
// duration or temporal value (a list is list concatenation instead).
func (operand staticOperand) joinsStringStatically() bool {
	switch operand.operandCategory {
	case "String", "Integer", "Float", "Boolean", "Point", "Duration":
		return true
	}
	return operand.temporal()
}

func (operand staticOperand) temporal() bool { return isTemporalTypeName(operand.operandCategory) }

// bindingKind is the match validator's view of an operand: the entity it is
// (a node, a relationship, a list of either, a path), a value that is none of
// them, or unknown. A type of several choices ("Map, Node or Relationship")
// is a value unless one of its choices is a node or relationship, which the
// row decides.
func (operand staticOperand) bindingKind() matchBindingKind {
	if !operand.union {
		switch operand.operandCategory {
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
			if operand.nonEntity {
				return matchBindingValue
			}
			return matchBindingUnknown
		}
	}
	for _, choice := range staticTypeChoices(operand.typeName()) {
		if choice == "Node" || choice == "Relationship" || choice == "Null" {
			// A null alternative is bound as anything (Neo4j admits
			// MATCH (v) for v of null or 1).
			return matchBindingUnknown
		}
	}
	return matchBindingValue
}

// alternatives are the operands of each alternative of a union (one each,
// named as the whole union in errors); an operand of one type is its own.
func (operand staticOperand) alternatives() []staticOperand {
	if !operand.union {
		return []staticOperand{operand}
	}
	choices := staticTypeChoices(operand.display)
	alternatives := make([]staticOperand, 0, len(choices))
	for _, choice := range choices {
		alternative := typedOperand(choice)
		alternative.display, alternative.parameter = operand.display, operand.parameter
		alternative.proven = operand.proven
		alternatives = append(alternatives, alternative)
	}
	return alternatives
}

// emptyListOperand is the type of the list literal [].
func emptyListOperand() staticOperand {
	operand := provenOperand("List<T>")
	operand.emptyList = true
	return operand
}

// leastCommonTypeName is the type Neo4j gives a value that may be of left's
// or right's type (a concatenation's elements): the type itself, a number
// for two numbers, Any otherwise.
func leastCommonTypeName(left, right string) string {
	switch {
	case left == right:
		return left
	case isNumericTypeName(left) && isNumericTypeName(right):
		return "Float, Integer or Number"
	}
	return "Any"
}

// concatenatedOperand is the static type of left + right when one is a list:
// a list whose element is the least common type of the two sides' elements
// (or of a list's element and the other value), for each combination of
// their alternatives, as Neo4j types it. An empty literal adds no element;
// an element of unknown type makes the elements unknown.
func concatenatedOperand(left, right staticOperand) staticOperand {
	if left.emptyList && right.list() {
		return right
	}
	if right.emptyList && left.list() {
		return left
	}
	elements := func(operand staticOperand) []string {
		if !operand.list() {
			return staticTypeChoices(operand.typeName())
		}
		return staticTypeChoices(elementOfType(operand.typeName()))
	}
	leftElements, rightElements := elements(left), elements(right)
	if !composable(left, right) || containsString(leftElements, "") || containsString(rightElements, "") {
		// Still a list, of elements this derivation doesn't type.
		return provenOperand("List<T>")
	}
	var combined []string
	for _, leftElement := range leftElements {
		for _, rightElement := range rightElements {
			if isNumericTypeName(leftElement) && isNumericTypeName(rightElement) && leftElement != rightElement {
				// [1] + [1.5] is "List<Float> or List<Number>": no Integer
				// survives a Float.
				combined = appendTypeChoice(appendTypeChoice(combined, "Float"), "Number")
				continue
			}
			combined = appendTypeChoice(combined, leastCommonTypeName(leftElement, rightElement))
		}
	}
	return typedOperand(listOfType(joinTypeNames(combined))).proved()
}

// listLiteralOperand is the one static type of a list literal from its
// elements' types, as Neo4j types it (rule R8): elements of one type are a
// list of it (a map's covariantly); unequal or union elements follow
// unionListLiteralOperand. A list literal is always a list,
// so an element no rule types (unknown, null, a property value, an Any
// alternative) leaves a list of elements of unknown type.
func listLiteralOperand(elements []staticOperand) staticOperand {
	if len(elements) == 0 {
		return emptyListOperand()
	}
	if len(elements) == 1 && elements[0].proven && strings.Contains(elements[0].typeName(), "Any") {
		// [v] of a reduce result (Any, Boolean or Integer): elements of
		// any type (Neo4j admits [x IN [v] | toUpper(x)]); an Any nested in
		// a list alternative leaves that list's elements unknown, keeping
		// the depth above it ([x] of List<Any> is a list of lists).
		if element := withoutAnyDepth(elements[0].typeName()); element != "" {
			return typedOperand(listOfType(element)).proved()
		}
		return provenOperand("List<T>")
	}
	if len(elements) == 1 && elements[0].proven && elements[0].isPropertyValue() {
		if elements[0].accumulated {
			// [reduce(a = n.k, …)] holds a value of any type to Neo4j
			// (labels(v) after UNWIND of it is admitted).
			return provenOperand("List<T>")
		}
		// [n.k] holds the property value (UNWIND [n.k] AS v binds one).
		return typedOperand(listOfType(staticPropertyValueType)).proved()
	}
	union := false
	for _, element := range elements {
		if !composable(element) {
			return provenOperand("List<T>")
		}
		// A property value is a union of the types a property may hold.
		union = union || element.union || element.isPropertyValue()
	}
	if union {
		return unionListLiteralOperand(elements)
	}
	element := ""
	for _, operand := range elements {
		typed := operand.typeName()
		if typed == "" {
			return provenOperand("List<T>")
		}
		if element != "" && typed != element {
			// [1, 2.5] is a list of numbers, [1, 'a'] of unknown elements.
			return unionListLiteralOperand(elements)
		}
		element = typed
	}
	return typedOperand(listOfType(covariantElementTypeName(element))).proved()
}

// coalescedOperand is the static type of coalesce over arguments: the
// alternatives of every argument, or any type when one is null or unknown
// (Neo4j: coalesce(v, null) takes whatever v does not).
func coalescedOperand(arguments []staticOperand) staticOperand {
	if len(arguments) > 0 && arguments[0].proven && arguments[0].typeName() != "" {
		same := true
		for _, argument := range arguments[1:] {
			same = same && argument.proven && argument.typeName() == arguments[0].typeName()
		}
		if same && !arguments[0].mayBeNull() {
			// coalesce(n.k, n.k) is the property value.
			return arguments[0]
		}
	}
	var result staticOperand
	for index, argument := range arguments {
		if !argument.joinable() {
			return staticOperand{}
		}
		if index == 0 {
			result = argument
			continue
		}
		result = joinedOperand(result, argument)
	}
	return result
}

// staticDerivationKind is how a function's result type follows from its
// arguments' operands.
type staticDerivationKind int

const (
	// derivationJoin: every argument's alternatives (coalesce).
	derivationJoin staticDerivationKind = iota
	// derivationElement: an element of the list argument (head, last).
	derivationElement
	// derivationListPart: the list argument's list alternatives (tail).
	derivationListPart
	// derivationReversed: the list or string argument itself (reverse).
	derivationReversed
	// derivationListOf: a list of the argument's values (collect).
	derivationListOf
	// derivationSame: one of the argument's values (min, max).
	derivationSame
)

// staticFunctionDerivations is the one table of the functions whose result
// type follows from their arguments' operands, by kind; a function whose
// result type doesn't depend on its arguments is in
// staticFunctionResultTypes. Each kind keeps every alternative of its input
// (TestStaticAlternativesNeverCollapse).
var staticFunctionDerivations = map[string]staticDerivationKind{
	"coalesce": derivationJoin,
	"head":     derivationElement, "last": derivationElement,
	"tail":    derivationListPart,
	"reverse": derivationReversed,
	"collect": derivationListOf,
	"min":     derivationSame, "max": derivationSame,
}

// derivedFunctionOperand is the result operand of function (lower case)
// over its arguments' operands by its derivation kind; derived is false for
// a function the table doesn't derive.
func derivedFunctionOperand(function string, arguments []staticOperand) (result staticOperand, derived bool) {
	kind, derived := staticFunctionDerivations[function]
	if !derived || len(arguments) == 0 {
		return staticOperand{}, derived
	}
	first := arguments[0]
	switch kind {
	case derivationJoin:
		return coalescedOperand(arguments), true
	case derivationElement:
		return first.elementOperand(), true
	case derivationListPart:
		result := first.listPart()
		if result.typeName() == "" {
			// tail always returns a list even when admission cannot constrain its input.
			result = provenOperand("List<T>")
		}
		return result, true
	case derivationReversed:
		return first.reversedOperand(), true
	case derivationListOf:
		if first.joinable() {
			return typedOperand(listOfType(first.typeName())).proved(), true
		}
		return provenOperand("List<T>"), true
	case derivationSame:
		if first.joinable() {
			return first, true
		}
	}
	return staticOperand{}, true
}

// commonStaticOperand is joinedOperand of two alternatives of one type, or
// of a typed and an unknown one: only the facts both agree on (the type, its
// display, exclusions, map members).
func commonStaticOperand(left, right staticOperand) staticOperand {
	common := staticOperand{
		nonEntity:   left.nonEntity && right.nonEntity,
		emptyList:   left.emptyList && right.emptyList,
		accumulated: left.accumulated || right.accumulated,
		proven:      left.proven && right.proven,
	}
	if left.operandCategory == right.operandCategory {
		common.operandCategory = left.operandCategory
	}
	if left.display == right.display {
		// The same union on both sides is the common type; different
		// types keep only the operator category they share (List<T>,
		// Number, Map), or none.
		common.display = left.display
		common.union = left.union || right.union
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

// The type predicates the admission checks pass to fits and excludes.

func noStaticType(string) bool { return false }

func isBooleanTypeName(typeName string) bool { return typeName == "Boolean" }

func isStringTypeName(typeName string) bool { return typeName == "String" }

func isIntegerOrNumberTypeName(typeName string) bool {
	return typeName == "Integer" || typeName == "Number"
}

func isDurationOrNumberTypeName(typeName string) bool {
	return typeName == "Duration" || typeName == "Number"
}

// isAddableToNumberOrString is what a number or a string adds: a number, a
// string or a list.
func isAddableToNumberOrString(typeName string) bool {
	return isNumericTypeName(typeName) || typeName == "String" || isListTypeName(typeName)
}

func acceptsPropertyAccess(typeName string) bool { return hasPropertiesTypeName(typeName) }

// acceptsStringKey reports a type a string subscript reads: a map, node or
// relationship, and in Neo4j 5.26 a temporal value or duration.
func acceptsStringKey(typeName string, cypher25 bool) bool {
	switch typeName {
	case "Map", "Node", "Relationship", "Map, Node or Relationship":
		return true
	}
	return !cypher25 && (isTemporalTypeName(typeName) || typeName == "Duration")
}

// isMapNodeOrRelationshipTypeName is what SET x = and SET x += take.
func isMapNodeOrRelationshipTypeName(typeName string) bool {
	switch typeName {
	case "Map", "Node", "Relationship", "Map, Node or Relationship":
		return true
	}
	return false
}

func isNodeTypeName(typeName string) bool { return typeName == "Node" }

func isNodeOrRelationshipTypeName(typeName string) bool {
	return typeName == "Node" || typeName == "Relationship"
}

// isLabelTokenTypeName is what a dynamic label takes: a string or a list of
// strings (a list of unknown elements is checked when the row runs; Neo4j
// rejects a List<Any>, SET n:$([1] + 'a'), and an Any, SET n:$(head([1] +
// ['a']))).
func isLabelTokenTypeName(typeName string) bool {
	switch typeName {
	case "String", "List<String>", "List<T>":
		return true
	}
	return false
}

// isKeyTokenTypeName is what a dynamic property key takes: a string (an Any
// isn't one).
func isKeyTokenTypeName(typeName string) bool { return typeName == "String" }

func (argument staticArgumentType) accepts(typeName string) bool {
	if typeName == "" || typeName == "Null" || len(argument.options) == 0 {
		return true
	}
	if argument.nestedLists {
		// The inner element can itself be a union (List<List<Map>,
		// List<Node> or List<Relationship>>). Its outer element is a list.
		if strings.HasPrefix(typeName, "List<List<") && strings.HasSuffix(typeName, ">>") {
			return true
		}
		// A list of lists among the alternatives is taken
		// (apoc.map.fromPairs([n.xs]): a property may hold a list);
		// otherwise every alternative must be one whose list-element
		// coercion Neo4j leaves to the row (apoc.map.fromPairs(n.xs) is
		// rejected: a property may hold a string).
		choices := staticTypeChoices(typeName)
		for _, choice := range choices {
			if strings.HasPrefix(choice, "List<List<") {
				return true
			}
		}
		for _, choice := range choices {
			switch choice {
			case "List<T>", "List<Boolean>", "List<Float>", "List<Integer>", "List<Number>", "Null":
			default:
				return false
			}
		}
		return true
	}
	if strings.Contains(typeName, ", ") || strings.Contains(typeName, " or ") {
		if choices := staticTypeChoices(typeName); len(choices) > 1 {
			for _, choice := range choices {
				if argument.accepts(choice) {
					return true
				}
			}
			return false
		}
	}
	if typeName == "Number" {
		return argument.accepts("Integer") || argument.accepts("Float")
	}
	isList := strings.HasPrefix(typeName, "List<")
	if isList && argument.acceptsLists || containsString(argument.unlisted, typeName) {
		return true
	}
	for _, option := range argument.options {
		switch {
		case option == typeName:
			return true
		case option == "Float" && typeName == "Integer":
			return true
		case option == "Number" && (typeName == "Integer" || typeName == "Float"):
			return true
		case option == "List<T>" && isList:
			return true
		case typeName == "List<T>" && strings.HasPrefix(option, "List<"):
			return true
		}
	}
	return false
}

// procedureArgumentTypeName is how Neo4j names an argument's static type in a
// procedure call's type mismatch: a list of numbers or booleans is a List<T>,
// while a List<String> or a list of maps keeps its element type.
func procedureArgumentTypeName(typeName string) string {
	if typeName == "List<Float>, List<Integer> or List<Number>" {
		return "List<T>"
	}
	for _, element := range []string{"Integer", "Float", "Boolean", "Number"} {
		typeName = strings.ReplaceAll(typeName, "List<"+element+">", "List<T>")
	}
	return typeName
}

// typeNameIn is the predicate for one of names: a declared signature's
// accumulatorAccepts is the type check of a reduce step against its initial
// value, as Neo4j 5.26 makes it: a type of one of the initial value's
// alternatives (a list's elements covariantly), or one an alternative
// coerces to: a number for a number (reduce(a = 0, x IN [1.5] | a + x) is
// admitted), a Boolean for a list (reduce(a = [1], x IN l | true)). Any takes
// any value. A map takes no node (accumulator is Map but expression has type
// Node), a list no scalar, a List<Integer> no List<Boolean>.
func accumulatorAccepts(initial staticOperand) func(string) bool {
	accepted := staticTypeChoices(initial.typeName())
	return func(choice string) bool {
		for _, typeName := range accepted {
			if accumulatorTakes(typeName, choice) {
				return true
			}
		}
		return false
	}
}

// accumulatorTakes reports whether an accumulator of type accumulator takes
// a step of type step (accumulatorAccepts). A list's elements are taken
// covariantly, without the numeric coercion (reduce(a = [1], x IN [1.5] |
// a + [x]) is "accumulator is List<Integer> but expression has type
// List<Float> or List<Number>").
func accumulatorTakes(accumulator, step string) bool {
	switch {
	case accumulator == step, accumulator == "Any", step == "Any":
		return true
	case isNumericTypeName(accumulator) && isNumericTypeName(step):
		return true
	case isListTypeName(accumulator) && step == "Boolean", accumulator == "Boolean" && isListTypeName(step):
		// A list and a Boolean coerce to each other (reduce(s = true, x IN
		// l | [1]) is admitted).
		return true
	case isListTypeName(accumulator) && isListTypeName(step):
		return listElementsTaken(elementOfType(accumulator), elementOfType(step))
	}
	return false
}

// listElementsTaken reports whether a list of accumulator's elements takes a
// list of step's: an element type of the same type, an Any on either side
// (reduce(a = [1], x IN ['a'] | a + x) is admitted), a Number of a number,
// unknown elements on either side.
func listElementsTaken(accumulator, step string) bool {
	if accumulator == "" || step == "" {
		return true
	}
	for _, element := range staticTypeChoices(step) {
		for _, accepted := range staticTypeChoices(accumulator) {
			switch {
			case accepted == element, accepted == "Any", element == "Any", accepted == "Number" && isNumericTypeName(element):
				return true
			case isListTypeName(accepted) && isListTypeName(element) && listElementsTaken(elementOfType(accepted), elementOfType(element)):
				return true
			}
		}
	}
	return false
}

// accepted types, which a consumer lists and the owner matches.
func typeNameIn(names ...string) func(string) bool {
	return func(typeName string) bool { return containsString(names, typeName) }
}

// isDeleteTargetTypeName is what DELETE takes: an entity, a list of them,
// and a String (NornicDB's delete-by-id extension).
func isDeleteTargetTypeName(typeName string) bool {
	return isEntityTypeName(typeName) || typeName == "String" || isListTypeName(typeName)
}

// unaryOperand is the static type of -operand or +operand, from their
// signatures (an Integer's is an Integer, a Float's a Float) as Neo4j reads
// them: the result of each alternative a signature takes, both for an
// alternative that may be either (null, a number, a property value, an
// operand of no known type), and nothing for any other (Any, a Boolean, the
// temporal values and durations Neo4j 5.26's + admits; -v of an Integer or
// Boolean is an Integer, +v of a Float or Duration a Float). When no
// alternative has a signature (+duration(…)), the result is a number,
// "Float or Integer".
func unaryOperand(operand staticOperand) staticOperand {
	var results []string
	choices := staticTypeChoices(operand.typeName())
	if len(choices) == 0 || operand.isPropertyValue() {
		choices = []string{"Null"}
	}
	for _, choice := range choices {
		switch choice {
		case "Integer", "Float":
			results = appendTypeChoice(results, choice)
		case "Null", "Number":
			results = appendTypeChoice(appendTypeChoice(results, "Float"), "Integer")
		}
	}
	if len(results) == 0 {
		results = []string{"Float", "Integer"}
	}
	return signatureOperand(results)
}

// unionListLiteralOperand is the static type of a list literal of
// composable elements one of which is a union, as Neo4j types it (rule R8):
// a single element keeps its alternatives ([v] for v of [1] or ['a'] is a
// list of List<Integer> or List<String>); elements that are lists on every
// alternative are lists to the depth all their alternatives share ([v, v]
// for v of [[1]] or [['x']] is a list of lists of lists); other elements
// have the least common type of each pair of alternatives, folded left to
// right ([v, v] for v of 1 or 1.5 is a list of numbers). Where a pair's
// least common type is Any ([v, v] for v of 1 or true), the elements'
// type is left unknown.
func unionListLiteralOperand(elements []staticOperand) staticOperand {
	if len(elements) == 1 {
		return typedOperand(listOfType(covariantElementTypeName(elements[0].typeName()))).proved()
	}
	depth := -1
	for _, element := range elements {
		for _, choice := range staticTypeChoices(element.typeName()) {
			if levels := listDepth(choice); depth < 0 || levels < depth {
				depth = levels
			}
		}
	}
	if depth > 0 {
		element := "List<T>"
		for level := 1; level < depth; level++ {
			element = listOfType(element)
		}
		return typedOperand(listOfType(element)).proved()
	}
	var types []string
	for index, element := range elements {
		choices := staticTypeChoices(element.typeName())
		if index == 0 {
			types = choices
			continue
		}
		var combined []string
		for _, left := range types {
			for _, right := range choices {
				combined = appendTypeChoice(combined, leastCommonTypeName(left, right))
			}
		}
		types = combined
	}
	for _, typeName := range types {
		if typeName == "Any" || isListTypeName(typeName) {
			return provenOperand("List<T>")
		}
	}
	return typedOperand(listOfType(covariantElementTypeName(joinTypeNames(types)))).proved()
}

// covariantElementTypeName is how Neo4j reads a list literal's element type
// back: a map alternative may be a node or a relationship
// (labels(head([CASE … {a: 1} … 1 END])) is admitted, MATCH (v) binds it).
func covariantElementTypeName(typeName string) string {
	choices := staticTypeChoices(typeName)
	widened := make([]string, 0, len(choices)+2)
	for _, choice := range choices {
		if choice == "Map" {
			for _, kind := range []string{"Map", "Node", "Relationship"} {
				widened = appendTypeChoice(widened, kind)
			}
			continue
		}
		widened = appendTypeChoice(widened, choice)
	}
	return joinTypeNames(widened)
}

// withoutAnyDepth is typeName with each list alternative that holds Any
// read as a list of unknown elements from that level down; "" when an
// alternative is Any itself.
func withoutAnyDepth(typeName string) string {
	choices := staticTypeChoices(typeName)
	kept := make([]string, 0, len(choices))
	for _, choice := range choices {
		switch {
		case choice == "Any":
			return ""
		case isListTypeName(choice) && strings.Contains(choice, "Any"):
			inner := withoutAnyDepth(elementOfType(choice))
			if inner == "" {
				inner = "T"
			}
			kept = appendTypeChoice(kept, "List<"+inner+">")
		default:
			kept = appendTypeChoice(kept, choice)
		}
	}
	return joinTypeNames(kept)
}

// leastUpperBoundName is Neo4j's least upper bound of two types: the type
// itself, a Number for two numbers, a list of their elements' bound for two
// lists, Any otherwise (rule R11's accumulator, R8's elements).
func leastUpperBoundName(left, right string) string {
	switch {
	case left == right:
		return left
	case isNumericTypeName(left) && isNumericTypeName(right):
		return "Number"
	case isListTypeName(left) && isListTypeName(right):
		leftElement, rightElement := elementOfType(left), elementOfType(right)
		if leftElement == "" || rightElement == "" {
			// A list of elements of unknown type bounds a list of them.
			return "List<T>"
		}
		if len(staticTypeChoices(leftElement)) > 1 || len(staticTypeChoices(rightElement)) > 1 {
			return "List<Any>"
		}
		return listOfType(leastUpperBoundName(leftElement, rightElement))
	}
	return "Any"
}

// leastUpperBoundClosure is the bound of every pair of alternatives of
// typeName (with themselves), as Neo4j types a value that may hold any of
// them in turn: an Integer or Boolean is "Any, Boolean or Integer", a Float
// or Integer a "Float, Integer or Number".
func leastUpperBoundClosure(typeName string) string {
	choices := staticTypeChoices(typeName)
	var closure []string
	for _, left := range choices {
		for _, right := range choices {
			closure = appendTypeChoice(closure, leastUpperBoundName(left, right))
		}
	}
	return joinTypeNames(closure)
}

// listDepth is how many lists deep typeName is: 0 for a value that isn't a
// list, 1 for List<Integer> or List<T>, 2 for List<List<String>>.
func listDepth(typeName string) int {
	depth := 0
	for isListTypeName(typeName) && typeName != "List<T>" {
		depth++
		typeName = strings.TrimSuffix(strings.TrimPrefix(typeName, "List<"), ">")
	}
	if typeName == "List<T>" {
		depth++
	}
	return depth
}

// mayBeNull reports a union one of whose alternatives is null (CASE … ELSE
// null END).
func (operand staticOperand) mayBeNull() bool {
	if !operand.union {
		return operand.operandCategory == "Null"
	}
	return containsString(staticTypeChoices(operand.typeName()), "Null")
}

// isBooleanOrListTypeName is what a predicate position (WHERE, AND, OR, XOR,
// NOT, CASE WHEN) admits in Neo4j's type check: a boolean, or a list, which
// is then rejected by the list coercion check when every alternative is one.
func isBooleanOrListTypeName(typeName string) bool {
	return typeName == "Boolean" || isListTypeName(typeName)
}
