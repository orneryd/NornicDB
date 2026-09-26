package cypher

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

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
	return invalidFunctionArgument(function, value)
}

// invalidFunctionArgument is the TypeError for a function argument of the
// wrong type, shared by RETURN-row validation and the expression evaluator
// (functionEvaluationFailure).
func invalidFunctionArgument(function string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidArgumentValue",
		fmt.Sprintf("%s() received an invalid %s argument", strings.ToLower(function), cypherTypeName(value)),
	)
}

// staticLiteralTypeName returns the Cypher type name of an expression whose
// type is known without data, as Neo4j names it in type errors: String,
// Integer, Float, Boolean, Map, List<…> for a list literal, and the result type
// of arithmetic / string concatenation over such operands (1 + 1 is an
// Integer, 'a' + 'b' a String). It returns "" for null, a list comprehension,
// and any expression involving variables, parameters or function calls.
func staticLiteralTypeName(expression string) string {
	expression = strings.TrimSpace(expression)
	for {
		inner, ok := stripEnclosingExpressionParentheses(expression)
		if !ok {
			break
		}
		expression = strings.TrimSpace(inner)
	}
	if expression == "" || expression[0] == '$' {
		return ""
	}
	// An expression that starts with a variable or a function call has no
	// static type; only the boolean literals start with a letter.
	if token, _, ok := scanIdentifierToken(expression, 0); ok &&
		!strings.EqualFold(token, "true") && !strings.EqualFold(token, "false") {
		return ""
	}
	if inner, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList {
		if _, _, _, _, comprehension := parseListComprehension(inner); comprehension {
			return ""
		}
		return staticListTypeName(inner)
	}
	if strings.HasPrefix(expression, "{") && strings.HasSuffix(expression, "}") &&
		findMatchingDelimiter(expression, 0, '{', '}') == len(expression)-1 {
		return "Map"
	}
	if typeName := staticArithmeticTypeName(expression); typeName != "" {
		return typeName
	}
	value, literal := parseLiteralValueFromComputedRow(expression)
	if !literal {
		return ""
	}
	switch kind := cypherValueKindOf(value); kind {
	case valueKindString, valueKindInteger, valueKindFloat, valueKindBoolean:
		return valueTypeNames[kind].cypher
	default:
		return ""
	}
}

// staticListTypeName names a list literal's type from its elements, as
// Neo4j does: List<Integer>, List<List<String>>, …; List<T> when the list is
// empty, holds null, or mixes unrelated types.
func staticListTypeName(inner string) string {
	if strings.TrimSpace(inner) == "" {
		return "List<T>"
	}
	elementType := ""
	numeric := false
	for _, item := range splitTopLevelComma(inner) {
		itemType := staticLiteralTypeName(item)
		if itemType == "" {
			return "List<T>"
		}
		switch {
		case elementType == "" || elementType == itemType:
			elementType = itemType
		case (elementType == "Integer" || elementType == "Float") && (itemType == "Integer" || itemType == "Float"):
			numeric = true
		default:
			return "List<T>"
		}
	}
	if numeric {
		return "List<Float>, List<Integer> or List<Number>"
	}
	if elementType == "Map" {
		return "List<Map>, List<Node> or List<Relationship>"
	}
	return "List<" + elementType + ">"
}

// staticArithmeticTypeName types a top-level +, -, *, /, % or ^ over operands
// with a static type: numbers give Integer (both Integer, except ^) or Float,
// and + with a String operand gives String. It returns "" otherwise.
func staticArithmeticTypeName(expression string) string {
	for _, operator := range []string{"+", "-", "*", "/", "%", "^"} {
		left, right, ok := splitByOperatorWithOptions(expression, operator, false, true)
		if !ok || left == "" || right == "" {
			continue
		}
		leftType, rightType := staticLiteralTypeName(left), staticLiteralTypeName(right)
		if leftType == "" || rightType == "" {
			return ""
		}
		numeric := func(typeName string) bool { return typeName == "Integer" || typeName == "Float" }
		switch {
		case operator == "+" && (leftType == "String" || rightType == "String") &&
			(numeric(leftType) || leftType == "String") && (numeric(rightType) || rightType == "String"):
			return "String"
		case numeric(leftType) && numeric(rightType):
			if leftType == "Integer" && rightType == "Integer" && operator != "^" {
				return "Integer"
			}
			return "Float"
		default:
			return ""
		}
	}
	return ""
}

// functionEvaluationFailure records an error returned by a registry function
// as the expression failure of ctx, so it fails the statement instead of
// evaluating to null.
func functionEvaluationFailure(ctx context.Context, err error) {
	var argumentError *cypherfn.ArgumentTypeError
	if errors.As(err, &argumentError) {
		err = invalidFunctionArgument(argumentError.Function, argumentError.Value)
	}
	err = typeMismatchFromFunctionError(err)
	recordExpressionFailure(ctx, err)
}
