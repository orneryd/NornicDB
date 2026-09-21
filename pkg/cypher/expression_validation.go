package cypher

import (
	"context"
	"strings"
)

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
