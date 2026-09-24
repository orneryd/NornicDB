package cypher

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func validateGraphFunctionSemanticTypes(expression string, scope matchSemanticScope) error {
	expression = strings.TrimSpace(expression)
	// (labels(x)) is checked like labels(x), as the conversion-function
	// validator does.
	if inner, ok := stripEnclosingExpressionParentheses(expression); ok {
		return validateGraphFunctionSemanticTypes(inner, scope)
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expression, '[', ']'); enclosed {
		_, listExpression, predicate, projection, comprehension := parseListComprehension(inner)
		if comprehension {
			for _, part := range []string{listExpression, predicate, projection} {
				if err := validateGraphFunctionSemanticTypes(part, scope); err != nil {
					return err
				}
			}
			return nil
		}
		for _, item := range splitTopLevelComma(inner) {
			if err := validateGraphFunctionSemanticTypes(item, scope); err != nil {
				return err
			}
		}
		return nil
	}
	function, argument, ok := parseFunctionCallWS(expression)
	if !ok {
		return nil
	}
	for _, item := range splitTopLevelComma(argument) {
		if err := validateGraphFunctionSemanticTypes(item, scope); err != nil {
			return err
		}
	}
	if strings.EqualFold(function, "properties") {
		if staticallyRejectsPropertyAccess(argument) {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				"properties() received a statically incompatible argument",
			)
		}
		return nil
	}
	if strings.EqualFold(function, "length") {
		variable := simpleSemanticIdentifier(argument)
		if variable == "" {
			return nil
		}
		switch scope[variable] {
		case matchBindingNode, matchBindingRelationship, matchBindingNodeList, matchBindingRelationshipList:
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				fmt.Sprintf("length() received %s with an incompatible static type", variable),
			)
		default:
			return nil
		}
	}
	if !strings.EqualFold(function, "labels") && !strings.EqualFold(function, "type") {
		return nil
	}
	// A literal argument has a static type, so labels('x') / type(1) are
	// compile-time type errors on every route, as in Neo4j (null is allowed).
	if typeName := staticLiteralTypeName(argument); typeName != "" {
		expected := "Node"
		if strings.EqualFold(function, "type") {
			expected = "Relationship"
		}
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidArgumentType",
			fmt.Sprintf("Type mismatch: expected %s but was %s", expected, typeName),
		)
	}
	variable := simpleSemanticIdentifier(argument)
	if variable == "" {
		return nil
	}
	kind := scope[variable]
	invalid := false
	if strings.EqualFold(function, "labels") {
		invalid = kind == matchBindingPath || kind == matchBindingRelationship || kind == matchBindingRelationshipList || kind == matchBindingNodeList
	} else {
		invalid = kind == matchBindingPath || kind == matchBindingNode || kind == matchBindingRelationshipList || kind == matchBindingNodeList
	}
	if invalid {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidArgumentType",
			fmt.Sprintf("%s() received %s with an incompatible static type", function, variable),
		)
	}
	return nil
}

func (e *StorageExecutor) validatePipelineGraphFunctionArguments(rows []pipelineRow, clause, keyword string) error {
	body := strings.TrimSpace(clause)
	if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
		return nil
	}
	body = strings.TrimSpace(body[len(keyword):])
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		body = strings.TrimSpace(body[len("DISTINCT "):])
	}
	end := len(body)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
			end = index
		}
	}
	for _, item := range splitTopLevelComma(strings.TrimSpace(body[:end])) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		for _, row := range rows {
			if err := e.validateRowGraphFunctionArguments(expression, row); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateRowGraphFunctionArguments(expression string, row pipelineRow) error {
	expression = strings.TrimSpace(expression)
	// (labels(x)) is checked like labels(x), as the conversion-function
	// validator does.
	if inner, ok := stripEnclosingExpressionParentheses(expression); ok {
		return e.validateRowGraphFunctionArguments(inner, row)
	}
	if inner, enclosed := stripEnclosingRowDelimiter(expression, '[', ']'); enclosed {
		variable, listExpression, predicate, projection, comprehension := parseListComprehension(inner)
		if comprehension {
			if err := e.validateRowGraphFunctionArguments(listExpression, row); err != nil {
				return err
			}
			listValue, evaluated := e.evaluateRowExpression(listExpression, row)
			if !evaluated || listValue == nil {
				return nil
			}
			valueType := reflect.TypeOf(listValue)
			if valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array {
				return nil
			}
			for _, item := range toAnySlice(listValue) {
				scope := make(pipelineRow, len(row)+1)
				for name, value := range row {
					scope[name] = value
				}
				scope[variable] = item
				for _, part := range []string{predicate, projection} {
					if err := e.validateRowGraphFunctionArguments(part, scope); err != nil {
						return err
					}
				}
			}
			return nil
		}
		for _, item := range splitTopLevelComma(inner) {
			if err := e.validateRowGraphFunctionArguments(item, row); err != nil {
				return err
			}
		}
		return nil
	}
	function, argument, ok := parseFunctionCallWS(expression)
	if !ok {
		return nil
	}
	for _, item := range splitTopLevelComma(argument) {
		if err := e.validateRowGraphFunctionArguments(item, row); err != nil {
			return err
		}
	}
	if !strings.EqualFold(function, "labels") && !strings.EqualFold(function, "type") {
		return nil
	}
	value, evaluated := e.evaluateRowExpression(argument, row)
	if !evaluated || value == nil {
		return nil
	}
	if strings.EqualFold(function, "labels") {
		if node, valid := value.(*storage.Node); valid && node != nil {
			return nil
		}
	} else if relationship, valid := value.(*storage.Edge); valid && relationship != nil {
		return nil
	}
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidArgumentValue",
		fmt.Sprintf("%s() received an invalid %T argument", function, value),
	)
}

// staticLiteralTypeName returns the Cypher type name of a literal expression
// (String, Integer, Float, Boolean, List, Map), or "" when the expression is not
// a non-null literal.
func staticLiteralTypeName(expression string) string {
	expression = strings.TrimSpace(expression)
	for {
		inner, ok := stripEnclosingExpressionParentheses(expression)
		if !ok {
			break
		}
		expression = strings.TrimSpace(inner)
	}
	if _, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		if _, _, _, _, comprehension := parseListComprehension(expression[1 : len(expression)-1]); comprehension {
			return ""
		}
		return "List"
	}
	if strings.HasPrefix(expression, "{") && strings.HasSuffix(expression, "}") {
		return "Map"
	}
	value, literal := parseLiteralValueFromComputedRow(expression)
	if !literal {
		return ""
	}
	switch value.(type) {
	case string:
		return "String"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "Integer"
	case float32, float64:
		return "Float"
	case bool:
		return "Boolean"
	default:
		return ""
	}
}
