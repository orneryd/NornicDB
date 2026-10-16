package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// invalidFunctionArgument is the TypeError for a function argument of the
// wrong type, shared by RETURN-row validation and the expression evaluator
// (functionEvaluationFailure).
func invalidFunctionArgument(function string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidArgumentValue",
		fmt.Sprintf("%s() received an invalid %s argument", lowerASCII(function), cypherTypeName(value)),
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
	recordExpressionFailure(ctx, functionEvaluationError(err))
}

// functionEvaluationError is the statement error for an error a registry
// function returned, with Neo4j's status and message, for either evaluator.
func functionEvaluationError(err error) error {
	var selection *cypherfn.GraphSelectionContextError
	if errors.As(err, &selection) {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.Message{Fallback: selection.Error()})
	}
	var argumentError *cypherfn.ArgumentTypeError
	if errors.As(err, &argumentError) {
		err = invalidFunctionArgument(argumentError.Function, argumentError.Value)
	}
	var unknown *cypherfn.UnknownFunctionError
	if errors.As(err, &unknown) {
		err = graphFunctionUnknownError(unknown.Function)
	}
	var notFound *cypherfn.GraphNotFoundError
	if errors.As(err, &notFound) {
		err = graphNotFoundError(notFound.Name)
	}
	var count *cypherfn.ParameterCountError
	if errors.As(err, &count) {
		err = functionParameterCountError(count.Function, count.TooMany)
	}
	return typeMismatchFromFunctionError(err)
}

// errRowArgumentUnresolved stops a registry function whose argument the row
// evaluator can't resolve; the call is then unresolved, not an error.
var errRowArgumentUnresolved = errors.New("row argument unresolved")

// evaluateRowGraphFunction evaluates graph.names() / graph.propertiesByName()
// and GDS's functions (isGDSFunctionName) in the row evaluator with their one
// implementation, the function registry (pkg/cypher/fn), and its errors as
// the other evaluator reports them.
func (e *StorageExecutor) evaluateRowGraphFunction(function, argument string, values map[string]interface{}) (interface{}, bool, error) {
	var args []string
	if strings.TrimSpace(argument) != "" {
		args = splitTopLevelComma(argument)
	}
	value, found, err := cypherfn.EvaluateFunction(function, args, cypherfn.Context{
		Storage:  e.storage,
		Cypher25: rowIsCypher25(values),
		Eval: func(expression string) (interface{}, error) {
			value, resolved, err := e.evaluateRowValue(expression, values)
			if err != nil {
				return nil, err
			}
			if !resolved {
				return nil, errRowArgumentUnresolved
			}
			return value, nil
		},
		Graphs: e,
	})
	if !found || errors.Is(err, errRowArgumentUnresolved) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, functionEvaluationError(err)
	}
	return value, true, nil
}

// graphFunctionUnknownError is Neo4j's SyntaxError for a composite graph
// function (graph.names, graph.propertiesByName) outside a composite
// database: "Unknown function '<name>'".
func graphFunctionUnknownError(function string) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnknownFunction",
		localization.CypherCommandRoutingGraphFunctionUnknown(function))
}

// functionParameterCountError is Neo4j's SyntaxError for a call with too
// many or too few arguments: "Too many parameters for function '<name>'",
// "Insufficient parameters for function '<name>'".
func functionParameterCountError(function string, tooMany bool) error {
	message := localization.CypherCommandRoutingFunctionInsufficientParameters(function)
	if tooMany {
		message = localization.CypherCommandRoutingFunctionTooManyParameters(function)
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "FunctionParameterCount", message)
}

// graphNotFoundError is Neo4j's DatabaseNotFound "Graph not found: <name>"
// for a graph name that isn't one of the composite database's graphs.
func graphNotFoundError(name string) error {
	return localizedStatusError("Neo.ClientError.Database.DatabaseNotFound", "DatabaseNotFound",
		localization.CypherCommandRoutingGraphNotFound(name))
}
