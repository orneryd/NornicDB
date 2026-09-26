package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
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
// that isn't a list is a list of that one value (traversableList).
func validateListOperands(cypher string, params map[string]interface{}) error {
	if !containsFold(cypher, "IN") {
		return nil
	}
	upper := strings.ToUpper(cypher)
	for i := 0; i < len(cypher); i++ {
		switch cypher[i] {
		case '\'', '"', '`':
			quote := cypher[i]
			i++
			for i < len(cypher) && (cypher[i] != quote || (quote != '`' && isBackslashEscaped(cypher, i))) {
				i++
			}
			continue
		}
		if !strings.HasPrefix(upper[i:], "IN") || (i > 0 && (isIdentByte(cypher[i-1]) || cypher[i-1] == ':' || cypher[i-1] == '.')) ||
			i+2 >= len(cypher) || isIdentByte(cypher[i+2]) {
			continue
		}
		start := skipSpaces(cypher, i+2)
		end, typeName, parameter := staticListOperand(cypher, start, params)
		if typeName == "" {
			continue
		}
		if next := skipSpaces(cypher, end); next < len(cypher) && strings.IndexByte(")],}|", cypher[next]) < 0 && !isIdentByte(cypher[next]) {
			// The operand is the start of a longer expression.
			continue
		}
		message := "Type mismatch: expected List<T> but was " + typeName
		if parameter != "" {
			message = fmt.Sprintf("Type mismatch for parameter '%s': expected List<T> but was %s", parameter, typeName)
		}
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", message)
	}
	return nil
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
		typeName = cypherValueTypeName(value)
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
		switch value, literal := parseLiteralValueFromComputedRow(cypher[start:end]); {
		case !literal || value == nil:
			return end, "", ""
		default:
			return end, cypherValueTypeName(value), ""
		}
	}
}

func isCypherListParameter(value interface{}) bool {
	if _, isBytes := value.([]byte); isBytes {
		return false
	}
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

// validateStaticSizeArguments rejects size(PATH) before query execution. A
// path's type is fixed by its MATCH binding, so waiting for a produced row
// would incorrectly make the error depend on whether the graph contains a
// matching path. WITH aliases retain that static type across query horizons.
func validateStaticSizeArguments(cypher string) error {
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	pathVariables := make(map[string]struct{})
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch:
			body := clause.text
			if clause.kind == pipelineClauseOptionalMatch {
				body = strings.TrimSpace(body[len("OPTIONAL MATCH"):])
			} else {
				body = strings.TrimSpace(body[len("MATCH"):])
			}
			if where := topLevelKeywordIndex(body, "WHERE"); where >= 0 {
				body = strings.TrimSpace(body[:where])
			}
			for _, pattern := range splitTopLevelComma(body) {
				if variable := extractPathAssignmentVariable(strings.TrimSpace(pattern)); variable != "" {
					pathVariables[variable] = struct{}{}
				}
			}
		case pipelineClauseWith, pipelineClauseReturn:
			keyword := "RETURN"
			if clause.kind == pipelineClauseWith {
				keyword = "WITH"
			}
			body := strings.TrimSpace(clause.text[len(keyword):])
			end := len(body)
			for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
				if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
					end = index
				}
			}
			body = strings.TrimSpace(body[:end])
			for _, item := range splitTopLevelComma(body) {
				expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
				if staticSizeUsesPath(expression, pathVariables) {
					return typeMismatchError(sizeArgumentTypes, pathTypeMarker{})
				}
			}
			if clause.kind == pipelineClauseWith {
				pathVariables = projectedPathVariables(body, pathVariables)
			}
		}
	}
	return nil
}

func staticSizeUsesPath(expression string, pathVariables map[string]struct{}) bool {
	for offset := 0; offset < len(expression); {
		index := findKeywordIndexInContext(expression[offset:], "size")
		if index < 0 {
			return false
		}
		index += offset
		open := index + len("size")
		for open < len(expression) && (expression[open] == ' ' || expression[open] == '\t' || expression[open] == '\n' || expression[open] == '\r') {
			open++
		}
		if open < len(expression) && expression[open] == '(' {
			if close := findMatchingDelimiter(expression, open, '(', ')'); close >= 0 {
				argument := strings.TrimSpace(expression[open+1 : close])
				if _, isPath := pathVariables[argument]; isPath {
					return true
				}
				offset = close + 1
				continue
			}
		}
		offset = index + len("size")
	}
	return false
}

func projectedPathVariables(body string, current map[string]struct{}) map[string]struct{} {
	next := make(map[string]struct{})
	for _, item := range splitTopLevelComma(body) {
		expression, alias := parseProjectionExprAlias(strings.TrimSpace(item))
		expression = strings.TrimSpace(expression)
		if expression == "*" {
			for variable := range current {
				next[variable] = struct{}{}
			}
			continue
		}
		if _, isPath := current[expression]; !isPath {
			continue
		}
		if alias == "" {
			alias = expression
		}
		next[alias] = struct{}{}
	}
	return next
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
