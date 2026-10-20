package cypher

import (
	"context"
	"maps"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateListOperands rejects, before any route executes the statement, a
// list position whose operand has a static type that is not a list, as
// Neo4j's compile-time type check does ("Type mismatch: expected List<T> but
// was Integer"). A list position is the operand after IN: the IN operator,
// all / any / none / single, list comprehensions, reduce and FOREACH. The
// operands checked are a whole literal (number, string, boolean, map) and a
// whole parameter bound to a non-null value that is not a list ("Type
// mismatch for parameter 'p': …"); `$p.list`, `$p[0]`, `'a' + x` and
// variables are expressions whose type is only known per row, where a value
// that isn't a list is a list of that one value (coerceToUnwindItems). Variables
// bound to a node, relationship or path are checked with their scope
// (graphListOperandTypeError).
func validateListOperands(cypher string, params map[string]interface{}) error {
	return forEachListOperand(cypher, func(start, in int) error {
		// FOREACH (x IN 5 | …) runs once with x = 5 in Neo4j, not a type error.
		if nonListIterationDeclaration(cypher, in) {
			return nil
		}
		end, typeName, parameter := staticListOperand(cypher, start, params)
		if typeName == "" || !wholeListOperand(cypher, end) {
			return nil
		}
		message := localization.CypherCoreListOperandTypeMismatch(typeName)
		if parameter != "" {
			message = localization.CypherCoreListParameterTypeMismatch(parameter, typeName)
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", message)
	})
}

// forEachListOperand calls visit with the offset of each list-position
// operand in text (the operand after an IN keyword outside quotes) and the
// offset of that IN, stopping at the first error.
func forEachListOperand(text string, visit func(start, in int) error) error {
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '\'', '"', '`':
			quote := text[i]
			i++
			for i < len(text) && (text[i] != quote || (quote != '`' && isBackslashEscaped(text, i))) {
				i++
			}
			continue
		case '/':
			if end := queryCommentEnd(text, i); end >= 0 {
				i = end - 1
				continue
			}
		}
		if (text[i] != 'I' && text[i] != 'i') || i+1 >= len(text) || (text[i+1] != 'N' && text[i+1] != 'n') || (i > 0 && (isIdentByte(text[i-1]) || text[i-1] == ':' || text[i-1] == '.')) ||
			i+2 >= len(text) || isIdentByte(text[i+2]) {
			continue
		}
		if err := visit(queryGapEnd(text, i+2), i); err != nil {
			return err
		}
	}
	return nil
}

// wholeListOperand reports whether the operand ending at end is the whole
// list-position operand, not the start of a longer expression (n.list,
// n[0], x + y).
func wholeListOperand(text string, end int) bool {
	next := queryGapEnd(text, end)
	return next >= len(text) || strings.IndexByte(")],}|", text[next]) >= 0 || isIdentByte(text[next])
}

// graphListOperandTypeError is Neo4j's compile-time SyntaxError for a list
// position given a variable bound to a node, relationship or path ("Type
// mismatch: expected List<T> but was Node"), whatever the data: `1 IN n`,
// `[x IN p | …]`, `any(x IN r WHERE …)`, in projections, WHERE, SET values
// and subquery bodies. FOREACH (x IN n | …) is not a type error in Neo4j (it
// runs once). A list element declaration (`[n IN list | …]`) is not itself
// an operand and cannot change the type of a separate `IN n` operand.
func graphListOperandTypeError(text string, scope matchSemanticScope) error {
	return staticListOperandTypeError(text, staticTypeScope{kinds: scope})
}

func staticListOperandTypeError(text string, scope staticTypeScope) error {
	if len(scope.kinds) == 0 && len(scope.values) == 0 {
		return nil
	}
	return forEachListOperand(text, func(start, in int) error {
		end := start
		for end < len(text) && isIdentByte(text[end]) {
			end++
		}
		if end == start || !wholeListOperand(text, end) {
			return nil
		}
		name := text[start:end]
		typeName := scope.typeOf(name)
		if typeName == "" || mayBeListType(typeName) {
			return nil
		}
		if nonListIterationDeclaration(text, in) || localVariableShadowsAt(text, start, name) {
			return nil
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreListOperandTypeMismatch(typeName))
	})
}

// mayBeListType reports whether a static type is a list or names one among
// its choices (a property value's type may be a List<T>).
func mayBeListType(typeName string) bool {
	for _, choice := range staticTypeChoices(typeName) {
		if strings.HasPrefix(choice, "List<") {
			return true
		}
	}
	return false
}

type nativeScopePredicate = bool

// nonListIterationDeclaration selects grammar-owned FOREACH/FOR and map iteration
// positions. IN belongs to the declaration here, rather than a list predicate.
func nonListIterationDeclaration(text string, in int) nativeScopePredicate {
	for _, scope := range fabric.ExpressionScopes(text) {
		if scope.Iteration == in && (scope.Kind == fabric.ExpressionIterationScope || scope.Kind == fabric.ExpressionMapScope) {
			return true
		}
	}
	return false
}

// localBinding is an expression that binds variables of its own, closing at
// text[close]: a list comprehension [v IN list WHERE … | …], a named pattern
// comprehension [p = (a)-->(b) WHERE … | …], all / any / none / single /
// filter (v IN list WHERE …) and reduce / allReduce (acc = initial,
// v IN list | step[, predicate]). It declares count names, each at its
// declared offset, visible from scope (the WHERE or | after the list, the
// pattern after a path's =) to close.
type localBinding struct {
	close, scope, count int
	names               [2]string
	declared            [2]int
	// source is the text span that types each name: the list an element
	// iterates (element), reduce's initial value for its accumulator; a
	// named path (path) is a Path.
	source           [2][2]int
	element          [2]bool
	path             [2]bool
	mapKey, mapValue [2]bool
}

func (binding *localBinding) declare(name string, at, sourceStart, sourceEnd int, element, path bool) {
	index := binding.count
	binding.names[index], binding.declared[index] = name, at
	binding.source[index], binding.element[index], binding.path[index] = [2]int{sourceStart, sourceEnd}, element, path
	binding.count++
}

// localBindingFromNative is a type-consumer view of the shared native descriptor.
// Pattern nodes retain their enclosing query binding when one already exists;
// withPatternComprehensionBindings handles those through the pattern type owner.
func localBindingFromNative(scope fabric.ExpressionScope) localBinding {
	binding := localBinding{close: scope.Close, scope: scope.Visible}
	for _, declaration := range scope.Declarations {
		if declaration.Pattern || declaration.Query {
			continue
		}
		index := binding.count
		binding.declare(declaration.Name, declaration.At, declaration.Source.Start, declaration.Source.End, declaration.Element, declaration.Path)
		binding.mapKey[index], binding.mapValue[index] = declaration.MapKey, declaration.MapValue
	}
	return binding
}

func localBindingAt(text string, open int) (localBinding, bool) {
	for _, scope := range fabric.ExpressionScopes(text) {
		if scope.Open == open {
			binding := localBindingFromNative(scope)
			return binding, binding.count > 0
		}
	}
	return localBinding{}, false
}

// bindingDeclaration is the native argument-body entry used by the evaluators
// and malformed-declaration diagnostics. It never infers a name from IN text.
func bindingDeclaration(text string, from int) (name string, at, list int, ok bool) {
	for _, scope := range fabric.ExpressionScopes(text) {
		if scope.Open == from-1 && len(scope.Declarations) > 0 {
			declaration := scope.Declarations[0]
			return declaration.Name, declaration.At, scope.Input.Start, true
		}
	}
	scope, parsed := fabric.ComprehensionSyntax(text[from:])
	if !parsed || len(scope.Declarations) == 0 {
		return "", 0, 0, false
	}
	declaration := scope.Declarations[0]
	return declaration.Name, declaration.At + from, scope.Input.Start + from, true
}
func localVariableShadowsAt(text string, operand int, name string) nativeScopePredicate {
	_, _, found := localBindingOf(text, operand, name)
	return found
}

func localBindingOf(text string, operand int, name string) (localBinding, int, bool) {
	scopes := fabric.ExpressionScopes(text)
	return localBindingIn(scopes, operand, name)
}

// localBindingOperand is the static type of the local name read at offset
// at of text (localBindingOf): a named path is a Path; an element is its
// list's element (listElementOperand: an Integer of [1], a node of [y]
// for a node y); reduce's accumulator is its initial value's type. The
// source is typed in the scope at its own position, with the locals
// around it typed first (withLocalTypes): in
// all(x IN [y] WHERE all(r IN [x] WHERE …)) [x] holds the outer local's
// node. It is unknown when that type isn't known.
func localBindingOperand(text string, at int, name string, scope staticTypeScope) staticOperand {
	binding, index, ok := localBindingIn(scope.nativeScopesAt(text, at), at, name)
	if !ok {
		return staticOperand{}
	}
	if binding.path[index] {
		return typedOperand("Path")
	}
	span := binding.source[index]
	if span[0] <= 0 || span[1] <= span[0] || span[1] > len(text) {
		return staticOperand{}
	}
	source := strings.TrimSpace(text[span[0]:span[1]])
	inner := withLocalTypes(scope, text, span[0]).nativeRange(text, span[0], span[1])
	types := typeScopeOf(inner)
	if binding.mapKey[index] {
		return knownOperand("String")
	}
	if binding.mapValue[index] {
		sourceType := projectedOperand(types, inner, source)
		var common staticOperand
		first := true
		for _, member := range sourceType.members {
			if first {
				common, first = member, false
			} else {
				common = commonStaticOperand(common, member)
			}
		}
		return common
	}
	if binding.element[index] {
		return listElementOperand(types, inner, source)
	}
	return projectedOperand(types, inner, source)
}

// localBindingKind is localBindingOperand's binding kind.
func localBindingKind(text string, at int, name string, scope staticTypeScope) matchBindingKind {
	return localBindingOperand(text, at, name, scope).bindingKind()
}

// withLocalTypes is scope with each name a local binds around offset at of
// text in place of the outer variable of that name, typed as that local
// (localBindingOperand): in all(r IN [1] WHERE EXISTS { (r)-->() }) the r
// a subquery's pattern reads is an Integer, not the clause's relationship.
// The scope goes through typeScope, the one type of every variable.
func withLocalTypes(scope staticTypeScope, text string, at int) staticTypeScope {
	var types typeScope
	scopes := scope.nativeScopesAt(text, at)
	for name := range nativeLocalNames(scopes, false) {
		if _, _, found := localBindingIn(scopes, at, name); !found {
			continue
		}
		if types == nil {
			types = typeScopeOf(scope)
		}
		types[name] = localBindingOperand(text, at, name, scope)
	}
	if types == nil {
		return scope
	}
	local := types.static(scope.cypher25)
	local.complete, local.params = scope.complete, scope.params
	local.native = scope.native
	return local
}

// localOccurrences retains local declarations during value substitution. It
// shares the immutable native scopes; no row walk reconstructs scope from text.
type localOccurrences struct {
	text, input string
	scopes      []fabric.ExpressionScope
}

func (occurrences *localOccurrences) binds(name string) bool {
	if occurrences == nil {
		return false
	}
	for _, scope := range fabric.ExpressionScopes(occurrences.text) {
		for _, declaration := range scope.Declarations {
			if !declaration.Pattern && !declaration.Query && declaration.Name == name {
				return true
			}
		}
	}
	return false
}

func (occurrences *localOccurrences) keeps(input, name string, at int) bool {
	if occurrences == nil {
		return false
	}
	if input != occurrences.input {
		occurrences.input, occurrences.scopes = input, fabric.ExpressionScopes(input)
	}
	for _, scope := range occurrences.scopes {
		for _, declaration := range scope.Declarations {
			if !declaration.Pattern && !declaration.Query && declaration.Name == name && (declaration.At == at || scope.Visible <= at && at < scope.Close) {
				return true
			}
		}
	}
	return false
}

// localReader answers localVariableShadowsAt for one text, reading the
// names the text binds once (expressionLocalBindings), so a name no binding
// declares costs a map lookup. The reference scanners
// (semanticFreeReferences, expressionFreeVariables) ask it for each
// occurrence they read.
type localReader struct {
	text   string
	names  map[string]struct{}
	scopes []fabric.ExpressionScope
}

func newLocalReader(text string) localReader {
	scopes := fabric.ExpressionScopes(text)
	return localReader{text: text, names: nativeLocalNames(scopes, false), scopes: scopes}
}

// shadows reports whether the occurrence of name at offset at of the text
// reads a local variable of that name.
func (reader localReader) shadows(name string, at int) bool {
	if _, declared := reader.names[name]; !declared {
		return false
	}
	_, _, found := localBindingIn(reader.scopes, at, name)
	return found
}

// withoutTypes is types (static type names by variable) without the names
// a local binds around offset at, for a part checked as text of its own (a
// pattern comprehension, a subquery body); types itself when none is.
func (reader localReader) withoutTypes(types map[string]string, at int) map[string]string {
	var inner map[string]string
	for name := range reader.names {
		if _, typed := types[name]; !typed || !reader.shadows(name, at) {
			continue
		}
		if inner == nil {
			inner = maps.Clone(types)
		}
		delete(inner, name)
	}
	if inner == nil {
		return types
	}
	return inner
}

// withoutLocalVariables is scope without the variables an enclosing
// expression binds around offset at of text (localVariableShadowsAt), and
// whether it bound any of scope's: in all(r IN [[true]] WHERE r[0]) the r
// read is the list's element, whatever the clause binds r to. The names
// come from expressionLocalBindings, read only when a check would fail.
func withoutLocalVariables(scope staticTypeScope, text string, at int) (staticTypeScope, bool) {
	var shadowed []string
	scopes := scope.nativeScopesAt(text, at)
	for name := range nativeLocalNames(scopes, false) {
		_, _, local := localBindingIn(scopes, at, name)
		if scope.bound(name) && local {
			shadowed = append(shadowed, name)
		}
	}
	if len(shadowed) == 0 {
		return scope, false
	}
	local := scope
	local.kinds, local.values, local.members = maps.Clone(scope.kinds), maps.Clone(scope.values), maps.Clone(scope.members)
	local.derived = maps.Clone(scope.derived)
	for _, name := range shadowed {
		delete(local.kinds, name)
		delete(local.values, name)
		delete(local.members, name)
		delete(local.derived, name)
	}
	return local, true
}

// staticListOperand reads the operand of a list position starting at start
// and returns where it ends and, when its type is static and not a list, that
// type's Neo4j name (with the parameter's name for a parameter).
func staticListOperand(cypher string, start int, params map[string]interface{}) (end int, typeName, parameter string) {
	if start >= len(cypher) {
		return start, "", ""
	}
	switch c := cypher[start]; {
	case c == '$':
		end = start + 1
		for end < len(cypher) && isIdentByte(cypher[end]) {
			end++
		}
		name := cypher[start+1 : end]
		value, bound := params[name]
		if name == "" || !bound || value == nil || isCypherListParameter(value) {
			return end, "", ""
		}
		typeName = cypherTypeName(value)
		if typeName == "Map" {
			// A map parameter can stand for a node or relationship.
			typeName = "Map, Node or Relationship"
		}
		return end, typeName, name
	case c == '\'' || c == '"':
		end = start + 1
		for end < len(cypher) && (cypher[end] != c || isBackslashEscaped(cypher, end)) {
			end++
		}
		return end + 1, "String", ""
	case c == '{':
		close := findMatchingDelimiter(cypher, start, '{', '}')
		if close < 0 {
			return start, "", ""
		}
		return close + 1, "Map", ""
	default:
		end = start
		for end < len(cypher) && (isIdentByte(cypher[end]) || cypher[end] == '.' || cypher[end] == '-' && end == start) {
			end++
		}
		word := cypher[start:end]
		switch {
		case strings.EqualFold(word, "true"), strings.EqualFold(word, "false"):
			return end, "Boolean", ""
		case c != '-' && c != '.' && (c < '0' || c > '9'):
			// A call returning a vector or UUID ([x IN vector(…) | x],
			// 1 IN uuid()) is Neo4j's type mismatch; a variable, a property
			// or any other function is known per row.
			if open := queryGapEnd(cypher, end); open < len(cypher) && cypher[open] == '(' {
				if close := findMatchingParen(cypher, open); close > open {
					if result := staticValueCallType(cypher[start : close+1]); result != "" {
						return close + 1, result, ""
					}
				}
			}
			return end, "", ""
		}
		value, literal := parseLiteralValueFromComputedRow(word)
		if !literal || value == nil {
			return end, "", ""
		}
		return end, cypherTypeName(value), ""
	}
}

func isCypherListParameter(value interface{}) bool {
	if _, isBytes := value.([]byte); isBytes {
		return false
	}
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

// validateStaticBooleanOperands rejects statically-known non-boolean operands
// before execution. Expressions whose type depends on a row remain subject to
// runtime validation by the row evaluator.
func (e *StorageExecutor) validateStaticBooleanOperands(ctx context.Context, expression string) error {
	expression = strings.TrimSpace(expression)
	if inner, ok := stripEnclosingExpressionParentheses(expression); ok {
		return e.validateStaticBooleanOperands(ctx, inner)
	}
	if hasPrefixFoldASCII(expression, "NOT ") {
		operand := strings.TrimSpace(expression[len("NOT "):])
		if err := e.validateStaticBooleanOperands(ctx, operand); err != nil {
			return err
		}
		value, known := e.staticBooleanOperandValue(ctx, operand)
		if !known || value == nil {
			return nil
		}
		if _, boolean := value.(bool); !boolean {
			return invalidBooleanOperandError(value)
		}
		return nil
	}
	for _, operator := range []string{" OR ", " XOR ", " AND "} {
		// Only the expression's own operators: not those inside a list, a
		// comprehension, a map or a CASE.
		left, right, found := splitByOperatorOutsideCase(expression, operator, true, true)
		if !found {
			continue
		}
		for _, operand := range []string{left, right} {
			if err := e.validateStaticBooleanOperands(ctx, operand); err != nil {
				return err
			}
			value, known := e.staticBooleanOperandValue(ctx, operand)
			if !known || value == nil {
				continue
			}
			if _, boolean := value.(bool); !boolean {
				return invalidBooleanOperandError(value)
			}
		}
		return nil
	}
	return nil
}

func (e *StorageExecutor) staticBooleanOperandValue(ctx context.Context, expression string) (interface{}, bool) {
	expression = strings.TrimSpace(expression)
	if inner, ok := stripEnclosingExpressionParentheses(expression); ok {
		expression = inner
	}
	if value, literal := parseLiteralValueFromComputedRow(expression); literal {
		return value, true
	}
	if strings.HasPrefix(expression, "{") && strings.HasSuffix(expression, "}") {
		return e.evaluateMapLiteralFromValues(expression, nil), true
	}
	for _, operator := range []string{" OR ", " XOR ", " AND ", "<=", ">=", "<>", "!=", "=", "<", ">"} {
		if _, _, found := splitByOperatorWithOptions(expression, operator, true, true); found {
			return true, true
		}
	}
	upper := upperASCII(expression)
	if strings.HasSuffix(upper, " IS NULL") || strings.HasSuffix(upper, " IS NOT NULL") || strings.HasPrefix(upper, "NOT ") {
		return true, true
	}
	value, defined := e.evaluateExpressionWithContextDefined(ctx, expression, nil, nil)
	if !defined {
		return nil, false
	}
	if text, unresolved := value.(string); unresolved && text == expression && !isWholeCypherQuotedString(expression) {
		return nil, false
	}
	return value, true
}

// validateListComprehensionVariables rejects a property expression where
// Cypher requires a local iterator name, including nested projections.
func validateListComprehensionVariables(text string, inputs ...*semanticStatementInput) error {
	input := matchingSemanticInput(text, inputs...)
	if syntax := input.syntax(); syntax != nil {
		return syntax.Err
	}
	return nil
}
