package cypher

import (
	"fmt"
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

// staticArgumentType is what one argument position of a function accepts: the
// types as Neo4j names them in its error, and the same list split into names.
type staticArgumentType struct {
	expected string
	options  []string
	// acceptsLists is set where Neo4j accepts a list without naming it in
	// the error (toInteger); the evaluator then rejects it at run time.
	acceptsLists bool
}

// staticListAcceptingFunctions accept a list argument at compile time
// although their "Type mismatch" error doesn't list one.
var staticListAcceptingFunctions = map[string]bool{"tointeger": true}

// staticFunctionArguments are the argument types Neo4j accepts, per function
// and argument position; positions past the end are not checked. The expected
// strings are Neo4j 5.26's own.
var staticFunctionArguments = buildStaticFunctionArguments(map[string][]string{
	"tointeger":        {"Boolean, Float, Integer or String"},
	"tofloat":          {"Float, Integer or String"},
	"tostring":         {"Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime or DateTime"},
	"toboolean":        {"Boolean, Integer or String"},
	"abs":              {"Float or Integer"},
	"sign":             {"Float or Integer"},
	"isnan":            {"Float or Integer"},
	"ceil":             {"Float"},
	"floor":            {"Float"},
	"round":            {"Float", "Float, Integer or Number", "String"},
	"sqrt":             {"Float"},
	"exp":              {"Float"},
	"log":              {"Float"},
	"log10":            {"Float"},
	"sin":              {"Float"},
	"cos":              {"Float"},
	"tan":              {"Float"},
	"cot":              {"Float"},
	"asin":             {"Float"},
	"acos":             {"Float"},
	"atan":             {"Float"},
	"atan2":            {"Float", "Float"},
	"degrees":          {"Float"},
	"radians":          {"Float"},
	"haversin":         {"Float"},
	"stdev":            {"Float"},
	"stdevp":           {"Float"},
	"percentilecont":   {"Float", "Float"},
	"percentiledisc":   {"Float or Integer", "Float"},
	"sum":              {"Float, Integer or Duration"},
	"avg":              {"Float, Integer or Duration"},
	"size":             {"String or List<T>"},
	"reverse":          {"String or List<T>"},
	"char_length":      {"String"},
	"character_length": {"String"},
	"toupper":          {"String"},
	"tolower":          {"String"},
	"upper":            {"String"},
	"lower":            {"String"},
	"trim":             {"String"},
	"ltrim":            {"String", "String"},
	"rtrim":            {"String", "String"},
	"btrim":            {"String", "String"},
	"normalize":        {"String"},
	"left":             {"String", "Integer"},
	"right":            {"String", "Integer"},
	"substring":        {"String", "Integer", "Integer"},
	"split":            {"String", "String or List<String>"},
	"replace":          {"String", "String", "String"},
	"head":             {"List<T>"},
	"last":             {"List<T>"},
	"tail":             {"List<T>"},
	"tointegerlist":    {"List<T>"},
	"tofloatlist":      {"List<T>"},
	"tostringlist":     {"List<T>"},
	"tobooleanlist":    {"List<T>"},
	"labels":           {"Node"},
	"type":             {"Relationship"},
	"startnode":        {"Relationship"},
	"endnode":          {"Relationship"},
	"id":               {"Node or Relationship"},
	"elementid":        {"Node or Relationship"},
	"properties":       {"Map, Node or Relationship"},
	"keys":             {"Map, Node or Relationship"},
	"nodes":            {"Path"},
	"relationships":    {"Path"},
	"length":           {"Path"},
})

// maxStaticFunctionNameLength is the longest name in staticFunctionArguments
// ("character_length"), the size of lookupStaticFunctionArguments' buffer.
const maxStaticFunctionNameLength = len("character_length")

func buildStaticFunctionArguments(expected map[string][]string) map[string][]staticArgumentType {
	built := make(map[string][]staticArgumentType, len(expected))
	for function, positions := range expected {
		arguments := make([]staticArgumentType, len(positions))
		for index, types := range positions {
			options := strings.Split(strings.ReplaceAll(types, " or ", ", "), ", ")
			arguments[index] = staticArgumentType{expected: types, options: options, acceptsLists: staticListAcceptingFunctions[function]}
		}
		built[function] = arguments
	}
	return built
}

// lookupStaticFunctionArguments finds name in staticFunctionArguments
// case-insensitively without allocating: every identifier followed by "(" in
// a statement (MATCH (, CASE (, …) passes through it.
func lookupStaticFunctionArguments(name string) ([]staticArgumentType, bool) {
	var buffer [maxStaticFunctionNameLength]byte
	if len(name) > len(buffer) {
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
	if typeName == "" {
		return true
	}
	if choices := staticTypeChoices(typeName); len(choices) > 1 {
		for _, choice := range choices {
			if argument.accepts(choice) {
				return true
			}
		}
		return false
	}
	isList := strings.HasPrefix(typeName, "List<")
	if isList && argument.acceptsLists {
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
		}
	}
	return false
}

// staticArgumentMismatch is Neo4j's compile-time error for an argument of the
// wrong static type.
func staticArgumentMismatch(argument staticArgumentType, typeName string) error {
	return typeNameMismatchError(argument.expected, typeName)
}

// forEachStaticFunctionArgument calls check for every argument of every call
// to a function in staticFunctionArguments in text, outside string literals
// and quoted names, including nested calls. A DISTINCT before an aggregate's
// argument is not part of it, and trim([LEADING | TRAILING | BOTH]
// [characters] FROM source) checks its characters and source (trimFromArguments).
func forEachStaticFunctionArgument(text string, check func(argument staticArgumentType, expression string) error) error {
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
		if index > 0 && (text[index-1] == '.' || text[index-1] == '$' || text[index-1] == ':' || isIdentifierPart(text[index-1])) {
			index = next
			continue
		}
		open := skipSpaces(text, next)
		if open >= len(text) || text[open] != '(' {
			index = next
			continue
		}
		arguments, known := lookupStaticFunctionArguments(name)
		if !known {
			index = open + 1
			continue
		}
		closing := findMatchingDelimiter(text, open, '(', ')')
		if closing < 0 {
			return nil
		}
		inner := strings.TrimSpace(text[open+1 : closing])
		inner, _ = cutDistinctArgument(inner)
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
		minimum, maximum := len(arguments), len(arguments)
		switch lowerASCII(name) {
		case "round":
			minimum = 1
		case "substring":
			minimum = 2
		case "ltrim", "rtrim", "btrim":
			minimum = 1
		case "normalize":
			maximum = 2
		}
		count := 0
		if inner != "" {
			count = len(splitTopLevelComma(inner))
		}
		if count < minimum || count > maximum {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidNumberOfArguments",
				fmt.Sprintf("Invalid number of arguments for function '%s': expected %d to %d, got %d", name, minimum, maximum, count))
		}
		if inner != "" {
			for position, expression := range splitTopLevelComma(inner) {
				if position >= len(arguments) {
					break
				}
				if err := check(arguments[position], strings.TrimSpace(expression)); err != nil {
					return err
				}
			}
		}
		index = open + 1
	}
	return nil
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
		if typeName := staticLiteralTypeName(expression); !argument.accepts(typeName) {
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
	listType := staticLiteralTypeName(body)
	if !strings.HasPrefix(listType, "List<") || strings.Contains(listType, ",") || listType == "List<T>" {
		return ""
	}
	return strings.TrimSuffix(strings.TrimPrefix(listType, "List<"), ">")
}
