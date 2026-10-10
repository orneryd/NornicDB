package cypher

import (
	"strings"
)

// Compile-time function argument types.
//
// Neo4j checks the static type of every function argument when it compiles a
// statement, and rejects an argument whose type no signature of the function
// accepts with a SyntaxError "Type mismatch: expected X but was Y", whatever
// the data (toInteger(n) for a node n, toUpper(1), left('a', 'b'), …). A
// static type is known for literals, list and map literals, arithmetic over
// them (staticLiteralTypeName), variables bound to nodes, relationships and
// paths, and variables bound to a literal by WITH … AS or UNWIND
// (staticTypeScope). Everything else is left to the row evaluator.

// staticArgumentType is what one argument position of a function accepts at
// compile time: the types as Neo4j names them in its error ("Float or
// Integer"), and the same list split into names. A position without options
// isn't checked.
type staticArgumentType struct {
	expected string
	options  []string
	// acceptsLists is set where Neo4j accepts a list without naming it in
	// the error (toInteger); the evaluator then rejects it at run time.
	acceptsLists bool
	// unlisted are types accepted although the error doesn't name them
	// (staticUnlistedArgumentTypes).
	unlisted []string
}

// staticListAcceptingFunctions accept a list argument at compile time
// although their "Type mismatch" error doesn't list one.
var staticListAcceptingFunctions = map[string]bool{"tointeger": true}

// staticUnlistedArgumentTypes are the Cypher 25 types (VECTOR, UUID) a
// function accepts that its Neo4j 5.26 "Type mismatch" error, the one this
// check writes, doesn't name: toString takes both, size a vector.
var staticUnlistedArgumentTypes = map[string][]string{"tostring": {"Vector", "UUID"}, "size": {"Vector"}}

// staticCatalogTypeNames are the catalog's argument types
// (cypherFunctionCatalog) as Neo4j names them in a compile-time "Type
// mismatch": where a MAP is expected a node or relationship is accepted too.
// Neo4j checks no other type at compile time (POINT, the temporal types,
// ANY), so a position of any other type isn't checked.
var staticCatalogTypeNames = map[string][]string{
	"INTEGER":               {"Integer"},
	"FLOAT":                 {"Float"},
	"STRING":                {"String"},
	"BOOLEAN":               {"Boolean"},
	"DURATION":              {"Duration"},
	"MAP":                   {"Map", "Node", "Relationship"},
	"NODE":                  {"Node"},
	"RELATIONSHIP":          {"Relationship"},
	"PATH":                  {"Path"},
	"LIST<ANY>":             {"List<T>"},
	"LIST<STRING>":          {"List<String>"},
	"LIST<INTEGER | FLOAT>": {"List<Float>", "List<Integer>", "List<Number>"},
	"VECTOR":                {"Vector"},
	"UUID":                  {"UUID"},
}

// staticTypeNameOrder is the order Neo4j lists types in a "Type mismatch"
// error: "Boolean, Float, Integer or String", "Map, Node, Relationship,
// String or List<T>", "Float, Integer or Duration".
var staticTypeNameOrder = []string{
	"Boolean", "Float", "Integer", "Number", "Map", "Node", "Relationship", "Path", "Point", "String",
	"Duration", "Date", "Time", "LocalTime", "LocalDateTime", "DateTime", "Vector", "UUID",
	"List<Float>", "List<Integer>", "List<Number>", "List<String>", "List<T>",
}

// staticArgumentOverrides are positions Neo4j 5.26 checks otherwise than its
// signatures say: toString(input :: ANY) accepts only these types, and every
// argument of trim, whose catalog entries list a trim specification first, is
// a STRING.
var staticArgumentOverrides = map[string][]string{
	"tostring": {"Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime or DateTime"},
	"trim":     {"String", "String", "String"},
}

// staticFunctionArguments are the argument types Neo4j checks at compile
// time, per function (lower-case name) and argument position, from the
// function catalog's signatures: a position accepts what any signature
// accepts there, and isn't checked when some signature takes a type Neo4j
// doesn't check. Checked against Neo4j 5.26 with a literal of a wrong type at
// every position of every function. Positions past the end aren't checked.
var staticFunctionArguments, maxStaticFunctionNameLength = buildStaticFunctionArguments()

func buildStaticFunctionArguments() (map[string][]staticArgumentType, int) {
	positions := make(map[string][][]string)
	unchecked := make(map[string]map[int]bool)
	for _, function := range cypherFunctionCatalog {
		name := lowerASCII(function.name)
		if function.arguments == nil || functionSyntaxForms[name] {
			continue
		}
		for index, argument := range function.arguments {
			for len(positions[name]) <= index {
				positions[name] = append(positions[name], nil)
			}
			options, known := staticCatalogTypeOptions(argument.Type)
			if !known {
				if unchecked[name] == nil {
					unchecked[name] = make(map[int]bool)
				}
				unchecked[name][index] = true
				continue
			}
			for _, option := range options {
				if !containsString(positions[name][index], option) {
					positions[name][index] = append(positions[name][index], option)
				}
			}
		}
	}
	built := make(map[string][]staticArgumentType, len(positions))
	longest := 0
	for name, typed := range positions {
		arguments := make([]staticArgumentType, len(typed))
		checked := false
		for index, options := range typed {
			if override, ok := staticArgumentOverrides[name]; ok && index < len(override) {
				options = staticTypeChoices(override[index])
			} else if unchecked[name][index] {
				continue
			}
			ordered := make([]string, 0, len(options))
			for _, typeName := range staticTypeNameOrder {
				if containsString(options, typeName) {
					ordered = append(ordered, typeName)
				}
			}
			arguments[index] = staticArgumentType{expected: joinTypeNames(ordered), options: ordered, acceptsLists: staticListAcceptingFunctions[name], unlisted: staticUnlistedArgumentTypes[name]}
			checked = true
		}
		if checked {
			built[name] = arguments
			longest = max(longest, len(name))
		}
	}
	return built, longest
}

// staticCatalogTypeOptions names a catalog type ("INTEGER | FLOAT", "LIST<ANY>")
// as the types Neo4j checks it against at compile time
// (staticCatalogTypeNames); known is false when Neo4j doesn't check it.
func staticCatalogTypeOptions(catalogType string) (options []string, known bool) {
	if names, ok := staticCatalogTypeNames[catalogType]; ok {
		return names, true
	}
	for _, part := range strings.Split(catalogType, " | ") {
		names, ok := staticCatalogTypeNames[strings.TrimSpace(part)]
		if !ok {
			return nil, false
		}
		options = append(options, names...)
	}
	return options, true
}

// joinTypeNames writes type names as Neo4j lists them in an error: "Float",
// "Float or Integer", "Map, Node or Relationship".
func joinTypeNames(names []string) string {
	if len(names) < 2 {
		return strings.Join(names, "")
	}
	return strings.Join(names[:len(names)-1], ", ") + " or " + names[len(names)-1]
}

// lookupStaticFunctionArguments finds name in staticFunctionArguments
// case-insensitively without allocating: every identifier followed by "(" in
// a statement (MATCH (, CASE (, …) passes through it.
func lookupStaticFunctionArguments(name string) ([]staticArgumentType, bool) {
	var buffer [64]byte
	if len(name) > maxStaticFunctionNameLength || len(name) > len(buffer) {
		return nil, false
	}
	for i := 0; i < len(name); i++ {
		buffer[i] = asciiLowerByte(name[i])
	}
	arguments, ok := staticFunctionArguments[string(buffer[:len(name)])]
	return arguments, ok
}

// accepts reports whether an argument of static type typeName fits. Neo4j
// coerces an Integer where a Float is expected, so that is accepted too; an
// unknown type ("") always is.
// staticTypeChoices splits a static type that names several possible types
// ("Float, Integer, String or List<T>", the type of 1 + x for an unknown x)
// into them; a single type is itself.
func staticTypeChoices(typeName string) []string {
	return strings.Split(strings.ReplaceAll(typeName, " or ", ", "), ", ")
}

func (argument staticArgumentType) accepts(typeName string) bool {
	if typeName == "" || len(argument.options) == 0 {
		return true
	}
	if strings.Contains(typeName, ", ") || strings.Contains(typeName, " or ") {
		choices := staticTypeChoices(typeName)
		for _, choice := range choices {
			if argument.accepts(choice) {
				return true
			}
		}
		return false
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

// staticArgumentMismatch is Neo4j's compile-time error for an argument of the
// wrong static type.
func staticArgumentMismatch(argument staticArgumentType, typeName string) error {
	return typeNameMismatchError(argument.expected, typeName)
}

// forEachStaticFunctionArgument checks every call to a built-in function in
// text, outside string literals and quoted names, including nested calls: a
// call with an argument count outside the function's arity (functionArities)
// is Neo4j's compile-time SyntaxError, and check is called for every
// argument of a function in staticFunctionArguments. A DISTINCT before an aggregate's
// argument is not part of it, and trim([LEADING | TRAILING | BOTH]
// [characters] FROM source) checks its characters and source (trimFromArguments).
func forEachStaticFunctionArgument(text string, check func(argument staticArgumentType, expression string) error) error {
	// A type after :: or TYPED is no call (x IS :: VECTOR(3)).
	text = maskTypePredicateTypes(text)
	for index := 0; index < len(text); {
		switch text[index] {
		case '\'', '"':
			index = skipQuotedSemanticText(text, index)
			continue
		case '`':
			if end := strings.IndexByte(text[index+1:], '`'); end >= 0 {
				index += end + 2
				continue
			}
			return nil
		}
		name, next, ok := scanIdentifierToken(text, index)
		if !ok {
			index++
			continue
		}
		if index > 0 && (text[index-1] == '.' || text[index-1] == '$' || text[index-1] == ':' || isIdentByte(text[index-1])) {
			index = next
			continue
		}
		if precededByTypeAnnotation(text, index) {
			// `x IS :: DATE` names a type, not a call: in a REQUIRE { ... }
			// block the next entry may start with "(", which must not read
			// as DATE(...).
			index = next
			continue
		}
		// A namespaced name (date.truncate, vector.similarity.cosine) is one
		// function name.
		for next < len(text) && text[next] == '.' && next+1 < len(text) && isIdentStartByte(text[next+1]) {
			_, end, _ := scanIdentifierToken(text, next+1)
			name, next = text[index:end], end
		}
		open := skipSpaces(text, next)
		if open >= len(text) || text[open] != '(' {
			index = next
			continue
		}
		arguments, typed := lookupStaticFunctionArguments(name)
		arity, counted := lookupFunctionArity(name)
		if !typed && !counted {
			index = open + 1
			continue
		}
		closing := findMatchingDelimiter(text, open, '(', ')')
		if closing < 0 {
			return nil
		}
		inner := strings.TrimSpace(text[open+1 : closing])
		inner, _ = cutDistinctArgument(inner)
		if strings.EqualFold(inner, "ALL") {
			// f(all) is f's ALL modifier with no argument, as in Neo4j
			// (count(all) fails "Insufficient parameters"), not a variable
			// named all (#907).
			inner = ""
		}
		if strings.EqualFold(name, "trim") {
			if parameters, fromForm := trimFromArguments(inner); fromForm {
				for _, expression := range parameters {
					if err := check(arguments[0], expression); err != nil {
						return err
					}
				}
				index = open + 1
				continue
			}
		}
		var buffer [8]string
		expressions := appendTopLevelComma(buffer[:0], inner)
		if counted {
			if err := checkFunctionArity(name, arity, len(expressions)); err != nil {
				return err
			}
		}
		if len(expressions) > 0 {
			if err := checkStaticLiteralArguments(name, expressions); err != nil {
				return err
			}
		}
		if typed {
			for position, expression := range expressions {
				if position >= len(arguments) {
					break
				}
				if err := check(arguments[position], expression); err != nil {
					return err
				}
			}
		}
		index = open + 1
	}
	return nil
}

// precededByTypeAnnotation reports whether the identifier at index is written
// as a type: after "::", "TYPED", or the ZONED / LOCAL qualifier of a temporal
// type (IS :: ZONED DATETIME, IS TYPED LOCAL TIME).
func precededByTypeAnnotation(text string, index int) bool {
	end := index
	for end > 0 && isSpaceByte(text[end-1]) {
		end--
	}
	if end >= 2 && text[end-2:end] == "::" {
		return true
	}
	start := end
	for start > 0 && isIdentByte(text[start-1]) {
		start--
	}
	word := text[start:end]
	switch {
	case strings.EqualFold(word, "TYPED"):
		return true
	case strings.EqualFold(word, "ZONED"), strings.EqualFold(word, "LOCAL"):
		return precededByTypeAnnotation(text, start)
	}
	return false
}

// trimFromArguments reads trim's FROM form, trim([LEADING | TRAILING |
// BOTH] [characters] FROM source), and returns its STRING arguments: the
// characters when given, and the source. fromForm is false for trim(source).
func trimFromArguments(inner string) (arguments []string, fromForm bool) {
	from := topLevelKeywordIndex(inner, "FROM")
	if from < 0 {
		return nil, false
	}
	spec := strings.TrimSpace(inner[:from])
	for _, mode := range []string{"LEADING", "TRAILING", "BOTH"} {
		if startsWithKeywordFold(spec, mode) {
			spec = strings.TrimSpace(spec[len(mode):])
			break
		}
	}
	if spec != "" {
		arguments = append(arguments, spec)
	}
	return append(arguments, strings.TrimSpace(inner[from+len("FROM"):])), true
}

// validateStaticFunctionArguments rejects a function called with a literal
// argument whose static type it doesn't accept (labels('x'), toUpper(1),
// keys([1]), length(1 + 1), …) anywhere in the statement: projections, WHERE,
// CASE, ORDER BY, SET values, pattern properties and subquery bodies.
// Variables are checked clause by clause (validateStaticFunctionVariables).
func validateStaticFunctionArguments(cypher string) error {
	return forEachStaticFunctionArgument(cypher, func(argument staticArgumentType, expression string) error {
		typeName := staticLiteralTypeName(expression)
		if typeName == "" {
			typeName = staticValueCallType(expression)
		}
		if !argument.accepts(typeName) {
			return staticArgumentMismatch(argument, typeName)
		}
		return nil
	})
}

// staticTypeScope is what a clause knows about its variables' static types:
// the binding kinds of pattern variables (a node, a relationship, a list of
// relationships, a path) and the literal types of variables bound by WITH …
// AS or UNWIND.
type staticTypeScope struct {
	kinds  matchSemanticScope
	values map[string]string
	params map[string]interface{}
	// complete is set when kinds holds every variable the clause can read
	// (validateMatchSemanticScopes' walk), so an expression naming any
	// other variable reads an undefined one (Neo4j: "Variable `x` not
	// defined").
	complete bool
}

// bound reports whether variable is bound in scope.
func (scope staticTypeScope) bound(variable string) bool {
	if _, bound := scope.kinds[variable]; bound {
		return true
	}
	_, bound := scope.values[variable]
	return bound
}

// typeOf is the static type name of variable, or "" when it isn't known.
func (scope staticTypeScope) typeOf(variable string) string {
	kind, bound := scope.kinds[variable]
	if !bound {
		return scope.values[variable]
	}
	switch kind {
	case matchBindingNode:
		return "Node"
	case matchBindingRelationship:
		return "Relationship"
	case matchBindingRelationshipList:
		return "List<Relationship>"
	case matchBindingNodeList:
		return "List<Node>"
	case matchBindingPath:
		return "Path"
	}
	return scope.values[variable]
}

// staticExpressionType is the static type of expression in scope: a
// literal's type or a variable's.
func (scope staticTypeScope) staticExpressionType(expression string) string {
	if len(scope.params) > 0 && strings.HasPrefix(strings.TrimSpace(expression), "$") {
		return propertyAccessExpressionType(expression, scope.values, scope.params)
	}
	if typeName := staticLiteralTypeName(expression); typeName != "" {
		return typeName
	}
	if variable := simpleSemanticIdentifier(expression); variable != "" {
		return scope.typeOf(variable)
	}
	if mayContainArithmetic(expression) {
		operand, err := (staticOperatorChecker{scope: scope}).check(expression)
		if err == nil {
			if operand.known() {
				return operand.kind
			}
			if operand.nonBoolean {
				return operand.display
			}
		}
	}
	return ""
}

// validateStaticFunctionVariables rejects a function called with a variable
// whose static type it doesn't accept (toInteger(n) for a node n, type(r) for
// a variable-length relationship list, toUpper(x) after UNWIND [1] AS x, …) in
// one clause. A variable the clause binds itself (a list comprehension,
// reduce, any / all / none / single) shadows the scope and is not checked.
func validateStaticFunctionVariables(text string, scope staticTypeScope) error {
	return validateStaticFunctionVariablesIn(text, func() staticTypeScope { return scope })
}

// validateStaticFunctionVariablesIn is validateStaticFunctionVariables with
// the scope built only when a function has a variable argument.
func validateStaticFunctionVariablesIn(text string, scopeOf func() staticTypeScope) error {
	var locals map[string]struct{}
	var scope *staticTypeScope
	return forEachStaticFunctionArgument(text, func(argument staticArgumentType, expression string) error {
		variable := simpleSemanticIdentifier(expression)
		if variable == "" && !mayContainArithmetic(expression) {
			return nil
		}
		if scope == nil {
			built := scopeOf()
			scope = &built
		}
		typeName := scope.staticExpressionType(expression)
		if typeName == "" || argument.accepts(typeName) {
			return nil
		}
		if locals == nil {
			locals = make(map[string]struct{})
			collectListComprehensionBindings(text, locals)
			collectFunctionExpressionBindings(text, locals)
		}
		if _, shadowed := locals[variable]; shadowed {
			return nil
		}
		return staticArgumentMismatch(argument, typeName)
	})
}

// projectStaticValueTypes returns the literal types a WITH clause binds: an
// alias of an expression with a static type keeps that type; WITH * keeps
// every one.
func projectStaticValueTypes(scope staticTypeScope, clause string) map[string]string {
	body, _ := projectionSemanticBodyAndTail(clause, "WITH")
	// Only a literal (or an alias of a variable that already has a literal
	// type) gives a projected value a static type.
	if len(scope.values) == 0 && !strings.ContainsAny(body, "'\"[{0123456789(") && !containsFold(body, "true") && !containsFold(body, "false") {
		return nil
	}
	var values map[string]string
	for _, raw := range splitTopLevelComma(body) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
		if expression == "*" {
			for variable, typeName := range scope.values {
				if values == nil {
					values = make(map[string]string)
				}
				values[variable] = typeName
			}
			continue
		}
		if alias == "" {
			alias = simpleSemanticIdentifier(expression)
		}
		if alias == "" {
			continue
		}
		if typeName := staticLiteralTypeName(expression); typeName != "" {
			if values == nil {
				values = make(map[string]string)
			}
			values[alias] = typeName
			continue
		}
		if variable := simpleSemanticIdentifier(expression); variable != "" {
			if typeName, found := scope.values[variable]; found {
				if values == nil {
					values = make(map[string]string)
				}
				values[alias] = typeName
			}
			continue
		}
		// A computed expression (a function call, arithmetic) keeps the type
		// the operator check infers for it.
		if operand, err := (staticOperatorChecker{scope: scope}).check(expression); err == nil && operand.known() {
			if values == nil {
				values = make(map[string]string)
			}
			values[alias] = operand.kind
		}
	}
	return values
}

// unwindStaticValueType is the element type of an UNWIND over a list literal
// (UNWIND [1, 2] AS x binds an Integer), or "".
func unwindStaticValueType(clause string) string {
	body := strings.TrimSpace(clause[len("UNWIND"):])
	if asIndex := findKeywordIndexInContext(body, "AS"); asIndex >= 0 {
		body = strings.TrimSpace(body[:asIndex])
	}
	if elementType := literalListElementType(body); elementType != "" {
		return elementType
	}
	// A list of computed expressions of one known type ([date('2020-01-01'),
	// date('2021-01-01')]) binds that type, as Neo4j types it before the
	// statement runs; a mixed list ([1, date(…)]) binds no static type.
	return uniformListElementType(body)
}

// literalListElementType is the element type of a list literal of literals,
// as Neo4j names it, "" when the literal typing doesn't type the list.
func literalListElementType(body string) string {
	listType := staticLiteralTypeName(body)
	// A list typed as several list types gives an element of any of their
	// element types, as Neo4j names it: UNWIND [{k: 1}] binds a "Map, Node or
	// Relationship", UNWIND [1, 2.5] a "Float, Integer or Number".
	choices := staticTypeChoices(listType)
	elements := make([]string, 0, len(choices))
	for _, choice := range choices {
		if !strings.HasPrefix(choice, "List<") || choice == "List<T>" {
			return ""
		}
		elements = append(elements, strings.TrimSuffix(strings.TrimPrefix(choice, "List<"), ">"))
	}
	return joinTypeNames(elements)
}

// uniformListElementType is the static type every element of a list literal
// has (a literal's, or a computed element's as the operator check infers it),
// "" when the text isn't a list literal, an element's type is
// unknown or several types, or the elements differ.
func uniformListElementType(list string) string {
	inner, isList := stripEnclosingRowDelimiter(strings.TrimSpace(list), '[', ']')
	if !isList || strings.TrimSpace(inner) == "" {
		return ""
	}
	elementType := ""
	for _, element := range splitTopLevelComma(inner) {
		element = strings.TrimSpace(element)
		typeName := staticLiteralTypeName(element)
		if typeName == "" {
			// A computed element (date('…')) keeps the type the operator check
			// infers for it, as a WITH projection does.
			if operand, err := (staticOperatorChecker{}).check(element); err == nil && operand.known() {
				typeName = operand.kind
			}
		}
		if typeName == "" || len(staticTypeChoices(typeName)) != 1 || (elementType != "" && typeName != elementType) {
			return ""
		}
		elementType = typeName
	}
	return elementType
}
