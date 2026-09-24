package cypher

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// validateSetSemanticScopes validates SET expressions against the clause
// horizon before execution. This is deliberately independent of row count:
// an undefined variable is a compile-time error even when MATCH yields no rows.
func (e *StorageExecutor) validateSetSemanticScopes(cypher string) error {
	if !containsKeywordOutsideStrings(cypher, "SET") {
		return nil
	}
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	scope := newSemanticBindingScope()
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseMerge, pipelineClauseCreate:
			for _, name := range extractNodeVariables(clause.text) {
				scope.bind(name)
			}
			for _, name := range extractRelationshipVariables(clause.text) {
				scope.bind(name)
			}
		case pipelineClauseWith:
			scope = projectedBindingScope(scope, clause.text)
		case pipelineClauseUnwind:
			if alias := unwindBindingName(clause.text); alias != "" {
				scope.bind(alias)
			}
		case pipelineClauseSet:
			if err := e.validateSetClauseScope(scope, clause.text); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateSetClauseScope is the statement-level check for one SET clause,
// shared by every route (MATCH, CREATE, MERGE, pipeline, fast paths): the
// assignment shapes (validatePipelineSetAssignments), bound target and value
// variables, and known functions in the assigned values.
func (e *StorageExecutor) validateSetClauseScope(scope *semanticBindingScope, clause string) error {
	body := strings.TrimSpace(clause[len("SET"):])
	assignments := e.splitSetAssignments(body)
	if err := validatePipelineSetAssignments(assignments); err != nil {
		return err
	}
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		operator := strings.Index(assignment, "+=")
		operatorWidth := 2
		if operator < 0 {
			operator = strings.Index(assignment, "=")
			operatorWidth = 1
		}
		if operator < 0 {
			// Label assignment `n:Label[:Label2]`: the variable before the
			// first colon must be bound, as for a property assignment.
			if colon := strings.Index(assignment, ":"); colon > 0 {
				target := normalizeProjectionColumnName(assignment[:colon])
				if isValidIdentifier(target) && !scope.contains(target) {
					return createUndefinedVariableError(target)
				}
			}
			continue
		}
		target, _, _ := parseSetAssignmentTarget(assignment[:operator])
		if target != "" && !scope.contains(target) {
			return createUndefinedVariableError(target)
		}
		expression := strings.TrimSpace(assignment[operator+operatorWidth:])
		if missing := firstUndefinedSetExpressionVariable(expression, scope); missing != "" {
			return createUndefinedVariableError(missing)
		}
		if err := validateKnownFunctionsInExpression(expression); err != nil {
			return err
		}
	}
	return nil
}

func firstUndefinedSetExpressionVariable(expression string, scope *semanticBindingScope) string {
	for _, name := range expressionFreeVariables(expression) {
		if _, exists := scope.names[name]; !exists {
			return name
		}
	}
	return ""
}

// expressionFreeVariables returns the variables an expression reads, in order
// of appearance: identifiers outside string literals that are not parameters,
// property or map keys, function names, keywords, or names the expression binds
// itself (list comprehension iterators, reduce / all / any / none / single).
// It is the one reference scanner for the static SET and CREATE checks.
func expressionFreeVariables(expression string) []string {
	locals := make(map[string]struct{})
	collectListComprehensionBindings(expression, locals)
	collectFunctionExpressionBindings(expression, locals)
	var names []string
	for index := 0; index < len(expression); {
		character := expression[index]
		if character == '\'' || character == '"' || character == '`' {
			quote := character
			index++
			for index < len(expression) {
				if expression[index] == quote {
					if quote == '`' && index+1 < len(expression) && expression[index+1] == '`' {
						index += 2
						continue
					}
					index++
					break
				}
				if expression[index] == '\\' && quote != '`' && index+1 < len(expression) {
					index += 2
					continue
				}
				index++
			}
			continue
		}
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		previous := previousSetExpressionByte(expression, index)
		following := nextSetExpressionByte(expression, next)
		upper := strings.ToUpper(name)
		if previous != '$' && previous != '.' && following != '(' && following != ':' && !setExpressionKeyword(upper) {
			if _, exists := locals[name]; !exists {
				names = append(names, name)
			}
		}
		index = next
	}
	return names
}

func collectFunctionExpressionBindings(expression string, bindings map[string]struct{}) {
	lower := strings.ToLower(expression)
	for _, functionName := range []string{"reduce", "all", "any", "none", "single", "filter"} {
		searchFrom := 0
		for searchFrom < len(expression) {
			relative := strings.Index(lower[searchFrom:], functionName)
			if relative < 0 {
				break
			}
			nameStart := searchFrom + relative
			open := nameStart + len(functionName)
			for open < len(expression) && isWhitespace(expression[open]) {
				open++
			}
			if open >= len(expression) || expression[open] != '(' {
				searchFrom = nameStart + len(functionName)
				continue
			}
			close := findMatchingParen(expression, open)
			if close < 0 {
				break
			}
			inner := strings.TrimSpace(expression[open+1 : close])
			if functionName == "reduce" {
				parts := splitTopLevelComma(inner)
				if len(parts) == 2 {
					if accumulator, _, ok := scanIdentifierToken(strings.TrimSpace(parts[0]), 0); ok {
						bindings[accumulator] = struct{}{}
					}
					collectLeadingIteratorBinding(parts[1], bindings)
				}
			} else {
				collectLeadingIteratorBinding(inner, bindings)
			}
			searchFrom = close + 1
		}
	}
}

func collectLeadingIteratorBinding(expression string, bindings map[string]struct{}) {
	expression = strings.TrimSpace(expression)
	name, next, ok := scanIdentifierToken(expression, 0)
	if !ok {
		return
	}
	for next < len(expression) && isWhitespace(expression[next]) {
		next++
	}
	if next+2 <= len(expression) && strings.EqualFold(expression[next:next+2], "IN") &&
		(next+2 == len(expression) || isWhitespace(expression[next+2]) || expression[next+2] == '(') {
		bindings[name] = struct{}{}
	}
}

func collectListComprehensionBindings(expression string, bindings map[string]struct{}) {
	for index := 0; index < len(expression); index++ {
		if expression[index] != '[' {
			continue
		}
		start := index + 1
		for start < len(expression) && isWhitespace(expression[start]) {
			start++
		}
		name, next, ok := scanIdentifierToken(expression, start)
		if !ok {
			continue
		}
		for next < len(expression) && isWhitespace(expression[next]) {
			next++
		}
		if next+2 <= len(expression) && strings.EqualFold(expression[next:next+2], "IN") &&
			(next+2 == len(expression) || isWhitespace(expression[next+2])) {
			bindings[name] = struct{}{}
		}
	}
}

func previousSetExpressionByte(text string, index int) byte {
	for index--; index >= 0; index-- {
		if !isWhitespace(text[index]) {
			return text[index]
		}
	}
	return 0
}

func nextSetExpressionByte(text string, index int) byte {
	for index < len(text) {
		if !isWhitespace(text[index]) {
			return text[index]
		}
		index++
	}
	return 0
}

func setExpressionKeyword(token string) bool {
	switch token {
	case "NULL", "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END",
		"IN", "WHERE", "AND", "OR", "XOR", "NOT", "IS", "STARTS", "ENDS",
		"WITH", "CONTAINS", "DISTINCT", "AS":
		return true
	default:
		return false
	}
}

func validateSetPropertyValue(value interface{}) error {
	if value == nil {
		return nil
	}
	if _, invalid := value.(*storage.Node); invalid {
		return invalidSetPropertyType(value)
	}
	if _, invalid := value.(*storage.Edge); invalid {
		return invalidSetPropertyType(value)
	}
	if _, invalid := value.(*PathResult); invalid {
		return invalidSetPropertyType(value)
	}
	switch value.(type) {
	case []float64, []float32, []int64, []int, []int32, []string, []bool:
		return nil // typed lists of primitives (e.g. embeddings) are valid as-is
	}
	typeOf := reflect.TypeOf(value)
	if typeOf != nil && typeOf.Kind() == reflect.Map {
		return invalidSetPropertyType(value)
	}
	if items, list := toInterfaceSlice(value); list {
		for _, item := range items {
			itemType := reflect.TypeOf(item)
			if itemType != nil && (itemType.Kind() == reflect.Map || itemType.Kind() == reflect.Slice || itemType.Kind() == reflect.Array) {
				return invalidSetPropertyType(value)
			}
			if _, invalid := item.(*storage.Node); invalid {
				return invalidSetPropertyType(value)
			}
			if _, invalid := item.(*storage.Edge); invalid {
				return invalidSetPropertyType(value)
			}
		}
	}
	return nil
}

func invalidSetPropertyType(value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidPropertyType",
		fmt.Sprintf("SET property value has unsupported type %T", value),
	)
}
