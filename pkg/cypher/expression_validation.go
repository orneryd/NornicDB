package cypher

import (
	"context"
	"reflect"
	"strings"

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
		if foreachDeclaration(cypher, in) || forIterationDeclaration(cypher, in) {
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
		if typeName == "" || strings.HasPrefix(typeName, "List<") {
			return nil
		}
		if foreachDeclaration(text, in) || forIterationDeclaration(text, in) || localListBindingShadowsOperand(text, start, name) {
			return nil
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreListOperandTypeMismatch(typeName))
	})
}

// foreachDeclaration reports whether the IN at offset in is FOREACH's own
// (`FOREACH (x IN …`).
func foreachDeclaration(text string, in int) bool {
	if !containsFold(text[:in], "FOREACH") {
		return false
	}
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	for from := 0; from < in; {
		start := keywordIndexFrom(text, "FOREACH", from, opts)
		if start < 0 || start >= in {
			return false
		}
		open := queryGapEnd(text, start+len("FOREACH"))
		if open < len(text) && text[open] == '(' {
			_, end, ok := scanIdentifierToken(text, queryGapEnd(text, open+1))
			if ok && queryGapEnd(text, end) == in {
				return true
			}
		}
		from = start + len("FOREACH")
	}
	return false
}

// forIterationDeclaration reports whether the IN at offset in is a FOR
// iteration clause's own (`FOR x IN …`). FOR iterates like UNWIND, coercing a
// non-list operand to a single item, so its operand is not a list type error.
func forIterationDeclaration(text string, in int) bool {
	if !containsFold(text[:in], "FOR") {
		return false
	}
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	for from := 0; from < in; {
		start := keywordIndexFrom(text, "FOR", from, opts)
		if start < 0 || start >= in {
			return false
		}
		nameStart := queryGapEnd(text, start+len("FOR"))
		if nameStart < in {
			if _, nameEnd, ok := scanIdentifierToken(text, nameStart); ok && queryGapEnd(text, nameEnd) == in {
				return true
			}
		}
		from = start + len("FOR")
	}
	return false
}

func localListBindingShadowsOperand(text string, operand int, name string) bool {
	for open := 0; open < operand; open++ {
		switch text[open] {
		case '\'', '"', '`':
			open = skipCypherQuotedText(text, open, text[open]) - 1
			continue
		case '/':
			if end := queryCommentEnd(text, open); end >= 0 {
				open = end - 1
				continue
			}
		}
		isList := text[open] == '['
		if !isList && (text[open] != '(' || !quantifierBeforeParen(text, open)) {
			continue
		}
		declaration := queryGapEnd(text, open+1)
		variable, end, ok := scanIdentifierToken(text, declaration)
		if !ok || variable != name {
			continue
		}
		in := queryGapEnd(text, end)
		if in+2 > operand || !strings.EqualFold(text[in:in+2], "IN") || in+2 < len(text) && isIdentByte(text[in+2]) {
			continue
		}
		brackets, parens, braces := 0, 0, 0
		if isList {
			brackets = 1
		} else {
			parens = 1
		}
		projecting := false
		for index := in + 2; index < operand; index++ {
			switch text[index] {
			case '\'', '"', '`':
				index = skipCypherQuotedText(text, index, text[index]) - 1
				continue
			case '/':
				if commentEnd := queryCommentEnd(text, index); commentEnd >= 0 {
					index = commentEnd - 1
					continue
				}
			case '[':
				brackets++
			case ']':
				brackets--
			case '(':
				parens++
			case ')':
				parens--
			case '{':
				braces++
			case '}':
				braces--
			case '|':
				if isList && brackets == 1 && parens == 0 && braces == 0 {
					projecting = true
				}
			}
			if !isList && parens == 1 && brackets == 0 && braces == 0 && index+5 <= operand && strings.EqualFold(text[index:index+5], "WHERE") &&
				(index == 0 || !isIdentByte(text[index-1])) && (index+5 == len(text) || !isIdentByte(text[index+5])) {
				projecting = true
				index += 4
			}
			if isList && brackets == 0 || !isList && parens == 0 {
				break
			}
		}
		if projecting && (isList && brackets > 0 || !isList && parens > 0) {
			return true
		}
	}
	return false
}

func quantifierBeforeParen(text string, open int) bool {
	end := open
	for end > 0 && isWhitespace(text[end-1]) {
		end--
	}
	start := end
	for start > 0 && isIdentByte(text[start-1]) {
		start--
	}
	if start == end || start > 0 && isIdentByte(text[start-1]) {
		return false
	}
	switch strings.ToLower(text[start:end]) {
	case "any", "all", "none", "single":
		return true
	default:
		return false
	}
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
