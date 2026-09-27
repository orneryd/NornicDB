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
	return forEachListOperand(cypher, func(start, _ int) error {
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
		}
		if (text[i] != 'I' && text[i] != 'i') || i+1 >= len(text) || (text[i+1] != 'N' && text[i+1] != 'n') || (i > 0 && (isIdentByte(text[i-1]) || text[i-1] == ':' || text[i-1] == '.')) ||
			i+2 >= len(text) || isIdentByte(text[i+2]) {
			continue
		}
		if err := visit(skipSpaces(text, i+2), i); err != nil {
			return err
		}
	}
	return nil
}

// wholeListOperand reports whether the operand ending at end is the whole
// list-position operand, not the start of a longer expression (n.list,
// n[0], x + y).
func wholeListOperand(text string, end int) bool {
	next := skipSpaces(text, end)
	return next >= len(text) || strings.IndexByte(")],}|", text[next]) >= 0 || isIdentByte(text[next])
}

// graphListOperandTypeError is Neo4j's compile-time SyntaxError for a list
// position given a variable bound to a node, relationship or path ("Type
// mismatch: expected List<T> but was Node"), whatever the data: `1 IN n`,
// `[x IN p | …]`, `any(x IN r WHERE …)`, in projections, WHERE, SET values
// and subquery bodies. FOREACH (x IN n | …) is not a type error in Neo4j (it
// runs once), and a name the text itself declares as a list element
// (`[n IN list | …]`) names the element, so neither is checked.
func graphListOperandTypeError(text string, scope matchSemanticScope) error {
	if len(scope) == 0 {
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
		var typeName string
		switch scope[name] {
		case matchBindingNode:
			typeName = "Node"
		case matchBindingRelationship:
			typeName = "Relationship"
		case matchBindingPath:
			typeName = "Path"
		default:
			return nil
		}
		if foreachDeclaration(text, in) || declaresListElement(text, name) {
			return nil
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreListOperandTypeMismatch(typeName))
	})
}

// foreachDeclaration reports whether the IN at offset in is FOREACH's own
// (`FOREACH (x IN …`).
func foreachDeclaration(text string, in int) bool {
	i := in - 1
	for i >= 0 && isSpaceByte(text[i]) {
		i--
	}
	for i >= 0 && isIdentByte(text[i]) {
		i--
	}
	for i >= 0 && isSpaceByte(text[i]) {
		i--
	}
	if i < 0 || text[i] != '(' {
		return false
	}
	i--
	for i >= 0 && isSpaceByte(text[i]) {
		i--
	}
	return i+1 >= len("FOREACH") && strings.EqualFold(text[i+1-len("FOREACH"):i+1], "FOREACH") &&
		(i+1 == len("FOREACH") || !isIdentByte(text[i-len("FOREACH")]))
}

// declaresListElement reports whether text has `name IN`: name is then (also)
// a list element variable of a comprehension or quantifier there.
func declaresListElement(text, name string) bool {
	for from := 0; ; {
		index := strings.Index(text[from:], name)
		if index < 0 {
			return false
		}
		index += from
		end := index + len(name)
		from = end
		if (index > 0 && (isIdentByte(text[index-1]) || text[index-1] == '.' || text[index-1] == '$')) || (end < len(text) && isIdentByte(text[end])) {
			continue
		}
		next := skipSpaces(text, end)
		if next+2 <= len(text) && strings.EqualFold(text[next:next+2], "IN") && (next+2 == len(text) || !isIdentByte(text[next+2])) {
			return true
		}
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
			// A variable, property or function: its type is known per row.
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
		left, right, found := splitByOperatorWithOptions(expression, operator, true, false)
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
	upper := strings.ToUpper(expression)
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
