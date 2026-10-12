package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
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
	// cypher25 is the check a Cypher 25 statement makes at the position
	// instead, where Neo4j 2026.09 checks it otherwise than 5.26
	// (staticCypher25Arguments); nil where both check it alike.
	cypher25 *staticArgumentType
}

// inVersion is the check a statement of the version (cypher25) makes at the
// argument's position.
func (argument staticArgumentType) inVersion(cypher25 bool) staticArgumentType {
	if cypher25 && argument.cypher25 != nil {
		return *argument.cypher25
	}
	return argument
}

// staticCypher25Arguments are the positions Neo4j 2026.09 checks otherwise
// than 5.26 when it compiles a Cypher 25 statement, as the types it accepts
// there; "" leaves the position unchecked. reverse(1) is left to run time
// (TypeError), with the same catalog signature, and toString also takes a
// map, a graph entity and a list whose elements aren't known to share one
// type (List<Any>: [1, 'a'], [null], []), but not a list known to hold one
// type ([1] is a List<Integer>), which it writes as text at run time
// (cypher25ValueText).
var staticCypher25Arguments = map[string][]string{
	"reverse":  {""},
	"tostring": {"Boolean, Float, Integer, Point, String, UUID, Duration, Date, Time, LocalTime, LocalDateTime, DateTime, Vector, List<Any>, Map, Node, Relationship or Path"},
}

// staticListAcceptingFunctions accept a list argument at compile time
// although their "Type mismatch" error doesn't list one.
var staticListAcceptingFunctions = map[string]bool{"tointeger": true}

// staticUnlistedArgumentTypes are the Cypher 25 types (VECTOR, UUID) a
// function accepts that its Neo4j 5.26 "Type mismatch" error, the one this
// check writes, doesn't name: toString takes both; size, toIntegerList and
// toFloatList a vector (not toStringList or toBooleanList).
var staticUnlistedArgumentTypes = map[string][]string{
	"tostring": {"Vector", "UUID"}, "size": {"Vector"}, "tointegerlist": {"Vector"}, "tofloatlist": {"Vector"},
}

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
// every position of every function. Positions past the end aren't checked,
// nor is a function with a NornicDB extension form, whose entry doesn't list
// its arguments (format(template, values…)): its evaluator rejects what
// neither form takes.
var staticFunctionArguments, staticFunctionArgumentsByCount, maxStaticFunctionNameLength = buildStaticFunctionArguments()

// buildStaticFunctionArguments builds staticFunctionArguments, and for an
// overloaded function whose signatures take different types at a position
// for different argument counts (uuid(name :: STRING), uuid(mostSigBits ::
// INTEGER, leastSigBits :: INTEGER)), the types for each argument count from
// the signatures that take that many: Neo4j picks the overload by argument
// count first, so uuid(1) is a type mismatch (expected String).
func buildStaticFunctionArguments() (map[string][]staticArgumentType, map[string]map[int][]staticArgumentType, int) {
	signatures := make(map[string][][]ProcedureParam)
	extensions := make(map[string]bool)
	for _, function := range cypherFunctionCatalog {
		name := lowerASCII(function.name)
		if function.arguments == nil {
			extensions[name] = true
		}
		if function.arguments == nil || functionSyntaxForms[name] {
			continue
		}
		signatures[name] = append(signatures[name], function.arguments)
	}
	built := make(map[string][]staticArgumentType, len(signatures))
	byCount := make(map[string]map[int][]staticArgumentType)
	longest := 0
	for name, all := range signatures {
		if extensions[name] {
			continue
		}
		arguments, checked := staticArgumentTypesOf(name, all)
		if !checked {
			continue
		}
		attachCypher25Arguments(name, arguments)
		built[name] = arguments
		longest = max(longest, len(name))
		maximum := 0
		for _, signature := range all {
			maximum = max(maximum, len(signature))
		}
		for count := 0; count <= maximum; count++ {
			var taking [][]ProcedureParam
			for _, signature := range all {
				if signatureRequiredArguments(signature) <= count && count <= len(signature) {
					taking = append(taking, signature)
				}
			}
			if len(taking) == 0 || len(taking) == len(all) {
				continue
			}
			if counted, ok := staticArgumentTypesOf(name, taking); ok {
				attachCypher25Arguments(name, counted)
				if byCount[name] == nil {
					byCount[name] = make(map[int][]staticArgumentType)
				}
				byCount[name][count] = counted
			}
		}
	}
	return built, byCount, longest
}

// attachCypher25Arguments gives each argument position of name the type
// Neo4j 2026.09 checks under Cypher 25, where it differs
// (staticCypher25Arguments; "" is unchecked).
func attachCypher25Arguments(name string, arguments []staticArgumentType) {
	for index, accepted := range staticCypher25Arguments[name] {
		if index >= len(arguments) {
			break
		}
		cypher25 := staticArgumentType{}
		if accepted != "" {
			cypher25 = staticArgumentType{expected: accepted, options: staticTypeChoices(accepted)}
		}
		arguments[index].cypher25 = &cypher25
	}
}

// signatureRequiredArguments is how many of a signature's arguments have no
// default (functionArities' rule).
func signatureRequiredArguments(signature []ProcedureParam) int {
	required := 0
	for _, argument := range signature {
		if argument.Default == "" && !argument.Optional {
			required++
		}
	}
	return required
}

// staticArgumentTypesOf is the static check of each argument position of
// name over signatures: a position accepts what any of them accepts there,
// and isn't checked when one takes a type Neo4j doesn't check. checked is
// false when no position is.
func staticArgumentTypesOf(name string, signatures [][]ProcedureParam) ([]staticArgumentType, bool) {
	var typed [][]string
	unchecked := make(map[int]bool)
	for _, signature := range signatures {
		for index, argument := range signature {
			for len(typed) <= index {
				typed = append(typed, nil)
			}
			options, known := staticCatalogTypeOptions(argument.Type)
			if !known {
				unchecked[index] = true
				continue
			}
			for _, option := range options {
				if !containsString(typed[index], option) {
					typed[index] = append(typed[index], option)
				}
			}
		}
	}
	arguments := make([]staticArgumentType, len(typed))
	checked := false
	for index, options := range typed {
		if override, ok := staticArgumentOverrides[name]; ok && index < len(override) {
			options = staticTypeChoices(override[index])
		} else if unchecked[index] {
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
	return arguments, checked
}

// staticCatalogTypeOptions names a catalog type ("INTEGER | FLOAT", "LIST<ANY>")
// as the types Neo4j checks it against at compile time
// (staticCatalogTypeNames); known is false when Neo4j doesn't check it.
func staticCatalogTypeOptions(catalogType string) (options []string, known bool) {
	if names, ok := staticCatalogTypeNames[catalogType]; ok {
		return names, true
	}
	// VECTOR | LIST<INTEGER | FLOAT> is two parts: the bar inside <…> is the
	// list element's.
	for _, part := range splitTopLevelTypeUnion(catalogType) {
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
	if len(name) > maxStaticFunctionNameLength {
		return nil, false
	}
	return lookupLowerASCII(staticFunctionArguments, name)
}

// lookupLowerASCII looks name up in a map keyed by lower-cased names
// without allocating: the key is lowered into a stack buffer, which the
// compiler doesn't copy for a map index. A name longer than 64 bytes is
// not found.
func lookupLowerASCII[V any](table map[string]V, name string) (V, bool) {
	var buffer [64]byte
	if len(name) > len(buffer) {
		var zero V
		return zero, false
	}
	for i := 0; i < len(name); i++ {
		buffer[i] = asciiLowerByte(name[i])
	}
	value, ok := table[string(buffer[:len(name)])]
	return value, ok
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
		if isReduceFormFunction(name) {
			if err := checkReduceForm(name, text, open, check); err != nil {
				return err
			}
			index = open + 1
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
		byCount, _ := lookupLowerASCII(staticFunctionArgumentsByCount, name)
		if overload, found := byCount[len(expressions)]; found {
			arguments = overload
		}
		if strings.EqualFold(name, "format") {
			arguments, typed = staticFormatArguments(expressions), true
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
				if len(arguments[position].options) == 0 {
					continue // a position no type is checked at
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
func validateStaticFunctionArguments(cypher string, cypher25 bool) error {
	return forEachStaticFunctionArgument(cypher, func(argument staticArgumentType, expression string) error {
		argument = argument.inVersion(cypher25)
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

// staticFormatTemporalTypes are the types format()'s value takes in Neo4j.
const staticFormatTemporalTypes = "Duration, Date, Time, LocalTime, LocalDateTime or DateTime"

// staticFormatArguments is the static check of format(value [, pattern]):
// the value is a temporal value or duration, or a string (NornicDB's printf
// form, format(template, values…)), and when the value is known to be
// temporal the pattern is a string (format(1, 'yyyy') and
// format(date(…), 1) are Neo4j's compile-time type mismatch). The printf
// form's values aren't checked.
func staticFormatArguments(expressions []string) []staticArgumentType {
	value := staticTypeChoices(staticFormatTemporalTypes + " or String")
	arguments := []staticArgumentType{{expected: staticFormatTemporalTypes, options: value}}
	if len(expressions) == 2 {
		first := strings.TrimSpace(expressions[0])
		typeName := staticLiteralTypeName(first)
		if function, inner, call := parseFunctionCallWS(first); call && typeName == "" {
			typeName = staticFunctionResultType(function, inner)
		}
		if typeName != "" && containsString(staticTypeChoices(staticFormatTemporalTypes), typeName) {
			arguments = append(arguments, staticArgumentType{expected: "String", options: []string{"String"}})
		}
	}
	return arguments
}

// validateStaticFunctionParameters rejects a function called with a
// parameter whose value's type no signature accepts there, as Neo4j does when
// it compiles a statement with its parameters: coll.sort($l) for l = 1 is
// "Type mismatch for parameter 'l': expected List<T> but was Integer", in
// Cypher 5 and 25 alike. As in Neo4j, a Float parameter and a list's
// elements are checked only at run time, where a value of the wrong type is
// the function's TypeError. It scans the statement only when a parameter
// follows an opening parenthesis or a comma, so parameterized statements on
// hot paths cost one byte scan.
func validateStaticFunctionParameters(cypher string, params map[string]interface{}, cypher25 bool) error {
	if len(params) == 0 || !parameterMayBeFunctionArgument(cypher) {
		return nil
	}
	var unwound map[string]string
	if strings.IndexByte(cypher, '[') >= 0 {
		unwound = unwindParameterBindings(cypher)
	}
	return forEachStaticFunctionArgument(cypher, func(argument staticArgumentType, expression string) error {
		// A Cypher 25 statement's arguments are checked as Neo4j 2026.09
		// checks them (toString takes any value there).
		argument = argument.inVersion(cypher25)
		expression = strings.TrimSpace(expression)
		if parameter, bound := unwound[expression]; bound {
			// UNWIND [$s] AS x binds x to $s's value, typed as Neo4j
			// types it: radians(x) for s = 'xy' is a type mismatch.
			operand := staticParameterOperand(params[parameter])
			if !operand.known() || operand.kind == "Float" || argument.accepts(operand.kind) {
				return nil
			}
			return typeNameMismatchError(argument.expected, operand.display)
		}
		if len(expression) < 2 || expression[0] != '$' {
			return nil
		}
		name, next, ok := scanIdentifierToken(expression, 1)
		if !ok || next != len(expression) {
			return nil
		}
		// A missing parameter (nil) has no static type.
		operand := staticParameterOperand(params[name])
		if !operand.known() || operand.kind == "Float" || argument.accepts(operand.kind) {
			return nil
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType",
			localization.CypherCoreParameterTypeMismatch(name, argument.expected, operand.display))
	})
}

// unwindParameterBindings maps each variable an UNWIND of a one-parameter
// list binds (UNWIND [$s] AS x) to the parameter's name, when the statement
// binds that name nowhere else (no second AS x), so a later binding can't be
// taken for it.
func unwindParameterBindings(cypher string) map[string]string {
	var bindings map[string]string
	for start := 0; ; {
		index := findKeywordIndexInContext(cypher[start:], "UNWIND")
		if index < 0 {
			return bindings
		}
		body := cypher[start+index+len("UNWIND"):]
		start += index + len("UNWIND")
		as := findKeywordIndexInContext(body, "AS")
		if as < 0 {
			continue
		}
		source := strings.TrimSpace(body[:as])
		alias, _, ok := scanIdentifierToken(strings.TrimSpace(body[as+len("AS"):]), 0)
		if !ok || len(source) < 4 || source[0] != '[' || source[len(source)-1] != ']' {
			continue
		}
		parameter := strings.TrimSpace(source[1 : len(source)-1])
		name, next, isParameter := scanIdentifierToken(parameter, 1)
		if !isParameter || parameter[0] != '$' || next != len(parameter) {
			continue
		}
		if strings.Count(cypher, "AS "+alias) != 1 {
			continue
		}
		if bindings == nil {
			bindings = make(map[string]string)
		}
		bindings[alias] = name
	}
}

// parameterMayBeFunctionArgument is validateStaticFunctionParameters' quick
// check: a $ right after "(" or ",", spaces aside.
func parameterMayBeFunctionArgument(cypher string) bool {
	for index := strings.IndexByte(cypher, '$'); index >= 0; {
		before := index
		for before > 0 && isASCIISpace(cypher[before-1]) {
			before--
		}
		if before > 0 && (cypher[before-1] == '(' || cypher[before-1] == ',' || cypher[before-1] == '[') {
			return true
		}
		next := strings.IndexByte(cypher[index+1:], '$')
		if next < 0 {
			return false
		}
		index += 1 + next
	}
	return false
}

// staticTypeScope is what a clause knows about its variables' static types:
// the binding kinds of pattern variables (a node, a relationship, a list of
// relationships, a path) and the literal types of variables bound by WITH …
// AS or UNWIND.
type staticTypeScope struct {
	kinds  matchSemanticScope
	values map[string]string
	// members are the member types of the variables a WITH bound to a map
	// literal in a Cypher 25 statement (projectStaticValueMembers).
	members map[string]map[string]staticOperand
	params  map[string]interface{}
	// complete is set when kinds holds every variable the clause can read
	// (validateMatchSemanticScopes' walk), so an expression naming any
	// other variable reads an undefined one (Neo4j: "Variable `x` not
	// defined").
	complete bool
	// cypher25 is set for a Cypher 25 statement, which the checks hold to
	// Neo4j 2026.09's compile-time rules ('a' + true is a string there); a
	// Cypher 5 statement keeps Neo4j 5.26's (#907).
	cypher25 bool
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

// staticMemberAccessBase is the variable a member read starts from
// (m.a, m['a'], m.a.b), whose member types a Cypher 25 statement knows
// (staticTypeScope.members).
func staticMemberAccessBase(expression string) (string, bool) {
	expression = strings.TrimSpace(expression)
	base, end, ok := scanIdentifierToken(expression, 0)
	if !ok || end == len(expression) {
		return "", false
	}
	for index := end; index < len(expression); {
		switch expression[index] {
		case '.':
			_, next, ok := scanIdentifierToken(expression, index+1)
			if !ok {
				return "", false
			}
			index = next
		case '[':
			closing := findMatchingDelimiter(expression, index, '[', ']')
			if closing < 0 {
				return "", false
			}
			if _, literal := decodeCypherQuotedString(strings.TrimSpace(expression[index+1 : closing])); !literal {
				return "", false
			}
			index = closing + 1
		default:
			return "", false
		}
	}
	return base, true
}

// staticExpressionType is the static type of expression in scope: a
// literal's type or a variable's, or in a Cypher 25 statement a known
// member's (m.a for m bound to {a: 2}).
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
	if _, member := staticMemberAccessBase(expression); member && scope.cypher25 {
		if operand, err := (staticOperatorChecker{scope: scope}).check(expression); err == nil {
			return operand.kind
		}
		return ""
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
		base, property := entityPropertyRead(expression)
		// A call's result type, when it doesn't depend on its arguments
		// (size(properties(m)) is size of a Map, Neo4j's compile-time type
		// mismatch); the call's own arguments are checked as calls of
		// their own.
		function, inner, call := parseFunctionCallWS(strings.TrimSpace(expression))
		result := ""
		if call {
			result = staticFunctionResultType(function, inner)
		}
		switch {
		case property:
			variable = base
		case result != "":
			if !argument.accepts(result) {
				return staticArgumentMismatch(argument, result)
			}
			return nil
		default:
			if memberBase, member := staticMemberAccessBase(expression); member {
				variable = memberBase // its members' types (Cypher 25)
			} else if variable == "" && !mayContainArithmetic(expression) {
				return nil
			}
		}
		if scope == nil {
			built := scopeOf()
			scope = &built
		}
		argument = argument.inVersion(scope.cypher25)
		typeName := scope.staticExpressionType(expression)
		if property {
			// A node's or relationship's property is a property value,
			// never an entity or a map (labels(n.x), id(n.x), point(n.x)
			// are Neo4j's compile-time SyntaxError); another base's member
			// has the type the scope knows for it (a Cypher 25 map's), or
			// none.
			if kind := scope.typeOf(base); kind == "Node" || kind == "Relationship" {
				typeName = staticPropertyValueType
			}
		}
		if typeName == "" || argument.accepts(typeName) {
			return nil
		}
		if locals == nil {
			locals = expressionLocalBindings(text)
		}
		if _, shadowed := locals[variable]; shadowed {
			return nil
		}
		return staticArgumentMismatch(argument, typeName)
	})
}

// staticPropertyValueType is the static type of a node's or relationship's
// property, as Neo4j names it in a type mismatch: any value a property can
// hold.
const staticPropertyValueType = "Boolean, Float, Integer, Number, Point, String, Duration, Date, Time, LocalTime, LocalDateTime, DateTime or List<T>"

// entityPropertyRead reports whether expression reads one property of a
// variable (n.x), and the variable.
func entityPropertyRead(expression string) (string, bool) {
	base, chain, ok := rowPropertyChainShape(strings.TrimSpace(expression))
	if !ok || strings.IndexByte(chain, '.') >= 0 {
		return "", false
	}
	return base, true
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

// projectStaticValueMembers is, for a Cypher 25 statement, the member types
// of each WITH alias whose value is a map literal, or an alias, nested map
// or other expression the operator check types as one ({a: 2} AS m, m AS k):
// Neo4j 2026.09 types m.a and m['a'] from them. WITH * keeps every one.
func projectStaticValueMembers(scope staticTypeScope, clause string) map[string]map[string]staticOperand {
	if !scope.cypher25 {
		return nil
	}
	body, _ := projectionSemanticBodyAndTail(clause, "WITH")
	if len(scope.members) == 0 && strings.IndexByte(body, '{') < 0 {
		return nil
	}
	var members map[string]map[string]staticOperand
	for _, raw := range splitTopLevelComma(body) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
		if expression == "*" {
			for variable, known := range scope.members {
				if members == nil {
					members = make(map[string]map[string]staticOperand)
				}
				members[variable] = known
			}
			continue
		}
		// Unaliased, a column is named by its text (WITH m); only a name
		// binds a variable.
		if simpleSemanticIdentifier(alias) == "" {
			continue
		}
		if operand, err := (staticOperatorChecker{scope: scope}).check(expression); err == nil && operand.members != nil {
			if members == nil {
				members = make(map[string]map[string]staticOperand)
			}
			members[alias] = operand.members
		}
	}
	return members
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

// checkReduceForm is Neo4j's compile-time check of a reduce or allReduce
// call whose parentheses open at open: the call must have the function's
// form (parseReduceForm), a list of a known type must be a list, and an
// allReduce predicate of a known type a boolean.
func checkReduceForm(function, text string, open int, check func(staticArgumentType, string) error) error {
	closing := findMatchingDelimiter(text, open, '(', ')')
	if closing < 0 {
		return nil
	}
	form, ok := parseReduceForm(function, text[open+1:closing])
	if !ok {
		entry := reduceFormEntries[lowerASCII(function)]
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherCoreReduceFormInvalidSyntax(entry.name, entry.signature))
	}
	if err := check(staticArgumentType{expected: "List<T>", options: []string{"List<T>"}}, form.list); err != nil || !form.all {
		return err
	}
	return check(staticArgumentType{expected: "Boolean", options: []string{"Boolean"}}, form.predicate)
}

// reduceFormEntries are the catalog entries of reduce and allReduce, by
// lower-case name, whose signatures their form errors quote.
var reduceFormEntries = func() map[string]cypherFunctionSpec {
	entries := make(map[string]cypherFunctionSpec, 2)
	for _, entry := range cypherFunctionCatalog {
		if isReduceFormFunction(entry.name) {
			entries[lowerASCII(entry.name)] = entry
		}
	}
	return entries
}()
