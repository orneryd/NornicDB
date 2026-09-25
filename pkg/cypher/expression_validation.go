package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

func validateStaticMembershipOperand(expression string) error {
	_, right, matched := splitByOperatorWithOptions(expression, " IN ", true, true)
	if !matched {
		return nil
	}
	right = strings.TrimSpace(right)
	if strings.EqualFold(right, "null") || strings.HasPrefix(right, "[") {
		return nil
	}
	_, scalarLiteral := parseLiteralValueFromComputedRow(right)
	mapLiteral := strings.HasPrefix(right, "{") && strings.HasSuffix(right, "}")
	if !scalarLiteral && !mapLiteral {
		return nil
	}
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidArgumentType",
		fmt.Sprintf("IN requires a LIST on the right-hand side, got %s", right),
	)
}

// validateMembershipParameters rejects `x IN $p` when the parameter $p is bound
// to a non-null value that is not a list, before any route executes the
// statement. Without it the outcome depended on the route: the RETURN evaluator
// raised a type error while every WHERE evaluator (compiled binding predicate,
// index seek, generic fallback) silently treated the value as "no match".
// Only a parameter that is the whole right-hand operand is checked; `$p.list`,
// `$p[0]` or `$p + [1]` are expressions whose type is not the parameter's.
func validateMembershipParameters(cypher string, params map[string]interface{}) error {
	if len(params) == 0 || !strings.Contains(cypher, "$") {
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
		if !strings.HasPrefix(upper[i:], "IN") || (i > 0 && isIdentByte(cypher[i-1])) ||
			i+2 >= len(cypher) || isIdentByte(cypher[i+2]) {
			continue
		}
		j := skipSpaces(cypher, i+2)
		if j >= len(cypher) || cypher[j] != '$' {
			continue
		}
		start := j + 1
		end := start
		for end < len(cypher) && isIdentByte(cypher[end]) {
			end++
		}
		if end == start {
			continue
		}
		next := skipSpaces(cypher, end)
		if next < len(cypher) && strings.IndexByte(")],}|", cypher[next]) < 0 && !isIdentByte(cypher[next]) {
			continue
		}
		name := cypher[start:end]
		value, bound := params[name]
		if !bound || value == nil || isCypherListParameter(value) {
			continue
		}
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidArgumentType",
			fmt.Sprintf("IN requires a LIST on the right-hand side, got $%s = %v", name, value),
		)
	}
	return nil
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
