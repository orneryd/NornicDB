package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

func extractProcedureInvocationArguments(ctx context.Context, spec ProcedureSpec, callCypher string) ([]interface{}, error) {
	if procedureCallContainsAggregation(callCypher) {
		return nil, newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"procedure arguments cannot contain aggregate expressions",
		)
	}
	if strings.Index(callCypher, "(") >= 0 {
		args, err := extractCallArguments(callCypher)
		if err != nil {
			return nil, err
		}
		if err := validateProcedureArgCount(spec, args); err != nil {
			return nil, err
		}
		return validateAndCoerceProcedureArguments(spec, args, explicitProcedureArgumentTexts(callCypher))
	}

	params := getParamsFromContext(ctx)
	args := make([]interface{}, 0, len(spec.Params))
	for index, parameter := range spec.Params {
		value, exists := params[parameter.Name]
		if !exists {
			if index >= spec.MinArgs && parameter.Optional {
				continue
			}
			return nil, newSemanticError(
				"Neo.ClientError.Statement.ParameterMissing",
				"MissingParameter",
				fmt.Sprintf("missing implicit procedure parameter %s", parameter.Name),
			)
		}
		args = append(args, value)
	}
	if err := validateProcedureArgCount(spec, args); err != nil {
		return nil, err
	}
	return validateAndCoerceProcedureArguments(spec, args, nil)
}

func validateProcedureArgumentPassingMode(spec ProcedureSpec, callCypher string, hasTail bool) error {
	if len(spec.Params) == 0 || strings.Contains(callCypher, "(") {
		return nil
	}
	if parseYieldClause(callCypher) == nil && !hasTail {
		return nil
	}
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidArgumentPassingMode",
		fmt.Sprintf("procedure %s arguments must be passed explicitly inside a query", spec.Name),
	)
}

func validateProcedureYieldBindings(yield *yieldClause, hasTail bool) error {
	if yield == nil {
		return nil
	}
	if yield.yieldAll && hasTail {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"UnexpectedSyntax",
			"YIELD * is only valid for a standalone procedure call",
		)
	}

	bindings := make(map[string]struct{}, len(yield.items))
	for _, item := range yield.items {
		binding := item.name
		if item.alias != "" {
			binding = item.alias
		}
		binding = normalizeProjectionColumnName(binding)
		if _, exists := bindings[binding]; exists {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"VariableAlreadyBound",
				fmt.Sprintf("variable %s is already bound by this YIELD clause", binding),
			)
		}
		bindings[binding] = struct{}{}
	}
	return nil
}

func explicitProcedureArgumentTexts(callCypher string) []string {
	open := strings.Index(callCypher, "(")
	if open < 0 {
		return nil
	}
	close := findMatchingCallParen(callCypher, open)
	if close < 0 {
		return nil
	}
	body := strings.TrimSpace(callCypher[open+1 : close])
	if body == "" {
		return []string{}
	}
	parts := splitProcedureTopLevelComma(body)
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
}

func validateAndCoerceProcedureArguments(spec ProcedureSpec, args []interface{}, explicitTexts []string) ([]interface{}, error) {
	coerced := append([]interface{}(nil), args...)
	for index := range coerced {
		if index >= len(spec.Params) {
			break
		}
		if explicitTexts != nil && index < len(explicitTexts) && !isStaticallyTypedProcedureArgument(explicitTexts[index]) {
			continue
		}

		parameter := spec.Params[index]
		value, ok := coerceProcedureArgument(parameter, coerced[index])
		if !ok {
			return nil, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				fmt.Sprintf("procedure %s argument %s requires %s but received %s", spec.Name, parameter.Name, parameter.Type, cypherTypeSystemName(coerced[index])),
			)
		}
		coerced[index] = value
	}
	return coerced, nil
}

func isStaticallyTypedProcedureArgument(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	lower := strings.ToLower(text)
	if lower == "null" || lower == "true" || lower == "false" {
		return true
	}
	if (strings.HasPrefix(text, "'") && strings.HasSuffix(text, "'")) ||
		(strings.HasPrefix(text, "\"") && strings.HasSuffix(text, "\"")) {
		return true
	}
	if _, err := strconv.ParseInt(text, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(text, 64); err == nil {
		return true
	}
	return false
}

func coerceProcedureArgument(parameter ProcedureParam, value interface{}) (interface{}, bool) {
	if value == nil {
		return nil, parameter.Optional || strings.EqualFold(parameter.Type, "ANY") || parameter.Type == ""
	}

	switch strings.ToUpper(strings.TrimSpace(parameter.Type)) {
	case "", "ANY":
		return value, true
	case "STRING":
		_, ok := value.(string)
		return value, ok
	case "BOOLEAN", "BOOL":
		_, ok := value.(bool)
		return value, ok
	case "INTEGER":
		return value, isIntegerProcedureValue(value)
	case "FLOAT":
		if converted, ok := procedureFloat64(value); ok {
			return converted, true
		}
		return value, false
	case "NUMBER":
		return value, isIntegerProcedureValue(value) || isFloatProcedureValue(value)
	default:
		return value, true
	}
}

func procedureFloat64(value interface{}) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	case float32:
		return float64(number), true
	case float64:
		return number, true
	default:
		return 0, false
	}
}

func isIntegerProcedureValue(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func isFloatProcedureValue(value interface{}) bool {
	switch value.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}

func procedureCallContainsAggregation(callCypher string) bool {
	open := strings.Index(callCypher, "(")
	if open < 0 {
		return false
	}
	close := findMatchingCallParen(callCypher, open)
	if close < 0 {
		return false
	}
	body := callCypher[open+1 : close]
	for _, name := range []string{"count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp", "percentilecont", "percentiledisc"} {
		if findKeywordIndexInContext(body, name) >= 0 && strings.Contains(strings.ToLower(body), name+"(") {
			return true
		}
	}
	return false
}
