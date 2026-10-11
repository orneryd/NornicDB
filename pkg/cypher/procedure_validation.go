package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateProcedureCallArguments rejects an aggregate in a procedure call's
// arguments, as Neo4j does when it compiles the statement: whether the call
// runs on its own or as a clause of a larger query, and whatever rows reach
// it.
func validateProcedureCallArguments(callCypher string) error {
	if procedureCallContainsAggregation(callCypher) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"procedure arguments cannot contain aggregate expressions",
		)
	}
	return nil
}

func extractProcedureInvocationArguments(ctx context.Context, spec ProcedureSpec, callCypher string) ([]interface{}, error) {
	if err := validateProcedureCallArguments(callCypher); err != nil {
		return nil, err
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

func (e *StorageExecutor) extractBoundProcedureInvocationArguments(ctx context.Context, spec ProcedureSpec, callCypher string) ([]interface{}, error) {
	ctx = withExpressionFailureSlot(ctx)
	bindings := valueBindingsFromContext(ctx)
	texts := explicitProcedureArgumentTexts(callCypher)
	if texts == nil {
		return extractProcedureInvocationArguments(ctx, spec, callCypher)
	}
	if bindings == nil {
		bindings = make(map[string]interface{})
		bindParameterRow(ctx, pipelineRow(bindings))
	}
	if err := validateProcedureCallArguments(callCypher); err != nil {
		return nil, err
	}
	args := make([]interface{}, len(texts))
	if err := validateProcedureArgCount(spec, args); err != nil {
		return nil, err
	}
	for index, text := range texts {
		value, resolved := e.evaluateRowExpressionWithContext(ctx, text, pipelineRow(bindings))
		if !resolved {
			pipelineItemUnevaluable(ctx, text)
			return nil, getExpressionFailure(ctx)
		}
		if err := getExpressionFailure(ctx); err != nil {
			return nil, err
		}
		args[index] = value
	}
	return validateAndCoerceProcedureArguments(spec, args, texts)
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

// validateYieldModifiers checks a YIELD's WHERE / ORDER BY / SKIP / LIMIT
// as Neo4j does when it compiles the statement: a standalone call (nothing
// after the YIELD) can't have any of them, a WHERE must come before
// ORDER BY / SKIP / LIMIT, and SKIP / LIMIT follow the same rules as in a
// WITH or RETURN.
func (e *StorageExecutor) validateYieldModifiers(yield *yieldClause, hasTail bool) error {
	if yield == nil || !yield.hasModifiers() {
		return nil
	}
	if !hasTail {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
			localization.CypherCoreStandaloneCallModifiers())
	}
	if yield.misplacedWhere {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
			localization.CypherCoreYieldWhereMisplaced())
	}
	if yield.skip != "" {
		if err := e.validateStaticPaginationExpression("SKIP", yield.skip); err != nil {
			return err
		}
	}
	if yield.limit != "" {
		if err := e.validateStaticPaginationExpression("LIMIT", yield.limit); err != nil {
			return err
		}
	}
	return nil
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

// staticProcedureArgumentTypeError is Neo4j's compile-time error for a
// procedure argument whose static type its parameter can't take: a literal, a
// variable bound to one by WITH, or a pattern variable (CALL
// db.resampleIndex(1.5), WITH 1.5 AS v CALL db.resampleIndex(v), MATCH (n)
// CALL db.resampleIndex(n)). Neo4j types a value bound by UNWIND only when the
// call runs, so those are left to the call. Only the parameter types the call
// itself checks (coerceProcedureArgument) are checked here: a NODE, MAP or
// LIST parameter also takes NornicDB's other forms (a node id, a label, a
// request's short form).
func staticProcedureArgumentTypeError(clause string, scope staticTypeScope, unwound map[string]struct{}) error {
	procedure, found := globalProcedureRegistry.Get(extractProcedureName(clause))
	if !found {
		return nil
	}
	texts := explicitProcedureArgumentTexts(clause)
	if len(texts) == 0 {
		return nil
	}
	if len(unwound) > 0 && len(scope.values) > 0 {
		values := make(map[string]string, len(scope.values))
		for name, typeName := range scope.values {
			if _, fromUnwind := unwound[name]; !fromUnwind {
				values[name] = typeName
			}
		}
		scope.values = values
	}
	for index, text := range texts {
		if index >= len(procedure.Spec.Params) {
			break
		}
		typeName := scope.staticExpressionType(text)
		if typeName == "" {
			continue
		}
		if expected, accepted := procedureParameterAcceptsStaticType(procedure.Spec.Params[index].Type, typeName); !accepted {
			return typeNameMismatchError(expected, procedureArgumentTypeName(typeName))
		}
	}
	return nil
}

// procedureParameterAcceptsStaticType reports whether a parameter of
// parameterType takes an argument of static type typeName (any one of its
// choices, for a type such as "Float, Integer or Number"), and the type
// Neo4j's error names as expected. An INTEGER is a FLOAT argument, as when the
// call runs.
func procedureParameterAcceptsStaticType(parameterType, typeName string) (string, bool) {
	var expected string
	var accepted []string
	switch upperASCII(strings.TrimSpace(parameterType)) {
	case "STRING":
		expected, accepted = "String", []string{"String"}
	case "BOOLEAN", "BOOL":
		expected, accepted = "Boolean", []string{"Boolean"}
	case "INTEGER":
		expected, accepted = "Integer", []string{"Integer", "Number"}
	case "FLOAT":
		expected, accepted = "Float", []string{"Float", "Integer", "Number"}
	case "NUMBER":
		expected, accepted = "Number", []string{"Number", "Float", "Integer"}
	default:
		return "", true
	}
	for _, choice := range staticTypeChoices(typeName) {
		if choice == "Any" || containsString(accepted, choice) {
			return expected, true
		}
	}
	return expected, false
}

// procedureArgumentTypeName is how Neo4j names an argument's static type in a
// procedure call's type mismatch: a list of numbers or booleans is a List<T>,
// while a List<String> or a list of maps keeps its element type.
func procedureArgumentTypeName(typeName string) string {
	if typeName == "List<Float>, List<Integer> or List<Number>" {
		return "List<T>"
	}
	for _, element := range []string{"Integer", "Float", "Boolean", "Number"} {
		typeName = strings.ReplaceAll(typeName, "List<"+element+">", "List<T>")
	}
	return typeName
}

func isStaticallyTypedProcedureArgument(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	lower := lowerASCII(text)
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

// coerceProcedureArgument checks and converts one argument against its
// parameter's type. null is a value of every type, as in Neo4j: the call
// goes ahead and the procedure decides what null means (its own failure is
// ProcedureCallFailed).
func coerceProcedureArgument(parameter ProcedureParam, value interface{}) (interface{}, bool) {
	if value == nil {
		return nil, true
	}

	switch upperASCII(strings.TrimSpace(parameter.Type)) {
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
		if findKeywordIndexInContext(body, name) >= 0 && strings.Contains(lowerASCII(body), name+"(") {
			return true
		}
	}
	return false
}
