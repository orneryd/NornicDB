package cypher

import (
	"context"
	"fmt"
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
					return newSemanticError(
						"Neo.ClientError.Statement.SyntaxError",
						"InvalidArgumentType",
						"size() does not accept PATH values; use length()",
					)
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
