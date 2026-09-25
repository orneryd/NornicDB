package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Runtime type errors.
//
// An operand whose type is only known from the data (a property, a value a
// row carries) and that an operator can't take is a Neo4j TypeError at run
// time, worded by the operator: "Cannot add `Long` and `Map`", "Cannot
// subtract `Long` from `String`", "Type mismatch: expected a map but was
// Long(1)". Operands whose type is known when the statement is compiled
// (literals, pattern variables) are rejected before it runs, with a
// SyntaxError (validateStaticOperatorTypes). Names and value renderings are
// Neo4j's own (its storage value classes: Long, Double, String, Boolean, Map,
// LongArray for a stored list of integers, …).

// runtimeTypeError is a Neo4j TypeError raised while a statement runs.
func runtimeTypeError(message string) error {
	return newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", message)
}

// neo4jValueTypeName is the name Neo4j gives a value's type in runtime
// errors.
func neo4jValueTypeName(value interface{}) string {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "Long"
	case float32, float64:
		return "Double"
	case string:
		return "String"
	case bool:
		return "Boolean"
	case *storage.Node:
		return "NodeIdReference"
	case *storage.Edge:
		return "RelationshipReference"
	case *PathResult, PathResult:
		return "Path"
	case CypherDate, *CypherDate:
		return "Date"
	case CypherTime, *CypherTime:
		return "Time"
	case CypherLocalTime, *CypherLocalTime:
		return "LocalTime"
	case CypherLocalDateTime, *CypherLocalDateTime:
		return "LocalDateTime"
	case CypherDateTime, *CypherDateTime:
		return "DateTime"
	case CypherDuration, *CypherDuration:
		return "Duration"
	case map[string]interface{}:
		if _, isPath := v["_pathResult"]; isPath {
			return "Path"
		}
		return "Map"
	case nil:
		return "NoValue"
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Map:
		return "Map"
	case reflect.Slice, reflect.Array:
		// A list of one scalar type is a stored array (LongArray,
		// StringArray, …); any other list is a List.
		items := toAnySlice(value)
		element := ""
		for _, item := range items {
			name := neo4jValueTypeName(item)
			switch name {
			case "Long", "Double", "String", "Boolean":
			default:
				return "List"
			}
			if element != "" && element != name {
				return "List"
			}
			element = name
		}
		if element == "" {
			return "List"
		}
		return element + "Array"
	}
	return fmt.Sprintf("%T", value)
}

// neo4jValueRepr renders a value the way Neo4j's runtime errors show it:
// Long(1), Double(1.500000e+00), String("x"), Boolean('true'),
// LongArray[1, 2], List{Long(1), String("a")}.
func neo4jValueRepr(value interface{}) string {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("Long(%d)", v)
	case float32, float64:
		return fmt.Sprintf("Double(%e)", v)
	case string:
		return fmt.Sprintf("String(%q)", v)
	case bool:
		return fmt.Sprintf("Boolean('%t')", v)
	case *storage.Node:
		if v != nil {
			return "(" + string(v.ID) + ")"
		}
	case *storage.Edge:
		if v != nil {
			return "-[" + string(v.ID) + "]-"
		}
	}
	if value == nil {
		return "NO_VALUE"
	}
	if kind := reflect.TypeOf(value).Kind(); kind == reflect.Slice || kind == reflect.Array {
		items := toAnySlice(value)
		typeName := neo4jValueTypeName(value)
		parts := make([]string, len(items))
		for index, item := range items {
			if typeName == "List" {
				parts[index] = neo4jValueRepr(item)
			} else {
				parts[index] = fmt.Sprint(item)
			}
		}
		if typeName == "List" {
			return "List{" + strings.Join(parts, ", ") + "}"
		}
		return typeName + "[" + strings.Join(parts, ", ") + "]"
	}
	return neo4jValueTypeName(value)
}

// isRuntimeNumber reports whether v is a Cypher INTEGER or FLOAT value.
func isRuntimeNumber(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	}
	return false
}

func isRuntimeDuration(v interface{}) bool {
	switch v.(type) {
	case CypherDuration, *CypherDuration:
		return true
	}
	return false
}

func isRuntimeTemporal(v interface{}) bool {
	switch v.(type) {
	case CypherDate, *CypherDate, CypherTime, *CypherTime, CypherLocalTime, *CypherLocalTime,
		CypherLocalDateTime, *CypherLocalDateTime, CypherDateTime, *CypherDateTime:
		return true
	}
	return false
}

func isRuntimeList(v interface{}) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case string, map[string]interface{}:
		return false
	}
	kind := reflect.TypeOf(v).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

// runtimeArithmeticTypeError is the TypeError of left op right when an
// operand is of a type the operator can't take, or nil. Null operands make the
// result null and are never an error. A String next to a temporal or duration
// operand is left alone: NornicDB can carry temporal values as strings.
func runtimeArithmeticTypeError(op byte, left, right interface{}) error {
	if left == nil || right == nil {
		return nil
	}
	_, leftString := left.(string)
	_, rightString := right.(string)
	temporalOperand := isRuntimeTemporal(left) || isRuntimeTemporal(right) || isRuntimeDuration(left) || isRuntimeDuration(right)
	if (leftString || rightString) && temporalOperand {
		return nil
	}
	leftNumber, rightNumber := isRuntimeNumber(left), isRuntimeNumber(right)
	valid := false
	switch op {
	case '+':
		valid = (leftNumber && rightNumber) ||
			(leftString && (rightString || rightNumber)) || (rightString && leftNumber) ||
			isRuntimeList(left) || isRuntimeList(right) ||
			(isRuntimeDuration(left) && (isRuntimeDuration(right) || isRuntimeTemporal(right))) ||
			(isRuntimeTemporal(left) && isRuntimeDuration(right))
	case '-':
		valid = (leftNumber && rightNumber) ||
			((isRuntimeDuration(left) || isRuntimeTemporal(left)) && isRuntimeDuration(right))
	case '*':
		valid = (leftNumber && rightNumber) || (isRuntimeDuration(left) && rightNumber) || (leftNumber && isRuntimeDuration(right))
	case '/':
		valid = (leftNumber && rightNumber) || (isRuntimeDuration(left) && rightNumber)
	case '%', '^':
		valid = leftNumber && rightNumber
	default:
		return nil
	}
	if valid {
		return nil
	}
	leftName, rightName := neo4jValueTypeName(left), neo4jValueTypeName(right)
	switch op {
	case '+':
		return runtimeTypeError(fmt.Sprintf("Cannot add `%s` and `%s`", leftName, rightName))
	case '-':
		return runtimeTypeError(fmt.Sprintf("Cannot subtract `%s` from `%s`", rightName, leftName))
	case '*':
		return runtimeTypeError(fmt.Sprintf("Cannot multiply `%s` and `%s`", leftName, rightName))
	case '/':
		return runtimeTypeError(fmt.Sprintf("Cannot divide `%s` by `%s`", leftName, rightName))
	case '%':
		return runtimeTypeError(fmt.Sprintf("Cannot calculate modulus of `%s` and `%s`", leftName, rightName))
	default:
		return runtimeTypeError(fmt.Sprintf("Cannot raise `%s` to the power of `%s`", leftName, rightName))
	}
}

// unaryMinusTypeError is the TypeError of -value (0 - value in Neo4j) for a
// non-null value that isn't a number or a duration, or nil.
func unaryMinusTypeError(value interface{}) error {
	if value == nil || isRuntimeNumber(value) || isRuntimeDuration(value) {
		return nil
	}
	return runtimeTypeError(fmt.Sprintf("Cannot subtract `%s` from `Long`", neo4jValueTypeName(value)))
}

// propertyAccessTypeError is the TypeError of value.key for a non-null value
// that has no properties (a number, a string, a boolean, a list), or nil.
func propertyAccessTypeError(value interface{}) error {
	switch value.(type) {
	case nil, *storage.Node, *storage.Edge, map[string]interface{}:
		return nil
	}
	if isRuntimeTemporal(value) || isRuntimeDuration(value) {
		return nil
	}
	if _, isMap := toStringAnyMap(value); isMap {
		return nil
	}
	return runtimeTypeError(fmt.Sprintf("Type mismatch: expected a map but was %s", neo4jValueRepr(value)))
}

// subscriptReceiverError is the TypeError of base[index] for a base that is
// neither a list nor a map.
func subscriptReceiverError(base, index interface{}) error {
	repr := neo4jValueRepr(base)
	return runtimeTypeError(fmt.Sprintf("`%s` is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key (found: %s[%s])", repr, repr, neo4jValueRepr(index)))
}

// recordRowOperatorFailure records the error of an expression the row
// evaluator could not resolve because an operator got an operand it can't
// take: a division by zero or an arithmetic TypeError, a unary minus of a
// non-number, or a property access on a value without properties. It looks
// through parentheses, operands, function arguments and list comprehensions
// for the failing operator and reports whether it recorded one.
func (e *StorageExecutor) recordRowOperatorFailure(ctx context.Context, expr string, values pipelineRow) bool {
	expression := strings.TrimSpace(expr)
	for {
		inner, enclosed := stripEnclosingExpressionParentheses(expression)
		if !enclosed {
			break
		}
		expression = strings.TrimSpace(inner)
	}
	if len(expression) >= 2 && expression[0] == '[' && expression[len(expression)-1] == ']' {
		if variable, list, predicate, projection, comprehension := parseListComprehension(expression[1 : len(expression)-1]); comprehension {
			return e.recordComprehensionOperatorFailure(ctx, variable, list, predicate, projection, values)
		}
	}
	for _, keyword := range [...]string{"OR", "XOR", "AND"} {
		if index := topLevelKeywordIndex(expression, keyword); index > 0 {
			return e.recordUnresolvedOperandFailure(ctx, values, expression[:index], expression[index+len(keyword):])
		}
	}
	if startsWithKeywordFold(expression, "NOT") {
		return e.recordUnresolvedOperandFailure(ctx, values, expression[len("NOT"):])
	}
	if operands, _, comparison := splitComparisonChain(expression); comparison {
		return e.recordUnresolvedOperandFailure(ctx, values, operands...)
	}
	if expression == "" || !isOperatorExpressionText(expression) {
		return false
	}
	for _, tier := range []string{"+-", "*/%", "^"} {
		left, right, operator, arithmetic := splitRowArithmeticTier(expression, tier)
		if !arithmetic {
			continue
		}
		if !isOperandExpressionText(left) || !isOperandExpressionText(right) {
			return false
		}
		leftValue, leftOK := e.evaluateRowExpression(left, values)
		if !leftOK {
			return e.recordRowOperatorFailure(ctx, left, values)
		}
		rightValue, rightOK := e.evaluateRowExpression(right, values)
		if !rightOK {
			return e.recordRowOperatorFailure(ctx, right, values)
		}
		if divisor, numeric := toFloat64(rightValue); (operator == '/' || operator == '%') && isRuntimeNumber(leftValue) && isRuntimeNumber(rightValue) && numeric && divisor == 0 {
			recordExpressionFailure(ctx, newSemanticError("Neo.ClientError.Statement.ArithmeticError", "DivisionByZero", "/ by zero"))
			return true
		}
		if err := arithmeticError(operator, leftValue, rightValue); err != nil {
			recordExpressionFailure(ctx, err)
			return true
		}
		return false
	}
	if expression[0] == '-' && len(expression) > 1 {
		operand := strings.TrimSpace(expression[1:])
		value, ok := e.evaluateRowExpression(operand, values)
		if !ok {
			return e.recordRowOperatorFailure(ctx, operand, values)
		}
		if err := unaryMinusTypeError(value); err != nil {
			recordExpressionFailure(ctx, err)
			return true
		}
		return false
	}
	if function, arguments, call := parseFunctionCallWS(expression); call && function != "" {
		for _, argument := range splitTopLevelComma(arguments) {
			if _, ok := e.evaluateRowExpression(argument, values); !ok {
				if e.recordRowOperatorFailure(ctx, argument, values) {
					return true
				}
			}
		}
		return false
	}
	if dot := strings.LastIndex(expression, "."); dot > 0 && isSimplePropertyAccess(expression) {
		base, ok := e.evaluateRowExpression(strings.TrimSpace(expression[:dot]), values)
		if !ok {
			return e.recordRowOperatorFailure(ctx, expression[:dot], values)
		}
		if err := propertyAccessTypeError(base); err != nil {
			recordExpressionFailure(ctx, err)
			return true
		}
	}
	return false
}

// recordUnresolvedOperandFailure is recordRowOperatorFailure for the operands
// of a boolean or comparison operator: it looks into each operand the row
// evaluator can't resolve.
func (e *StorageExecutor) recordUnresolvedOperandFailure(ctx context.Context, values pipelineRow, operands ...string) bool {
	for _, operand := range operands {
		if _, ok := e.evaluateRowExpression(operand, values); !ok && e.recordRowOperatorFailure(ctx, operand, values) {
			return true
		}
	}
	return false
}

// recordComprehensionOperatorFailure is recordRowOperatorFailure for
// [variable IN list WHERE predicate | projection]: it looks for the failing
// operator in the list, then in the predicate and projection of each element,
// as evaluateRowListComprehension evaluates them.
func (e *StorageExecutor) recordComprehensionOperatorFailure(ctx context.Context, variable, list, predicate, projection string, values pipelineRow) bool {
	listValue, ok := e.evaluateRowExpression(list, values)
	if !ok {
		return e.recordRowOperatorFailure(ctx, list, values)
	}
	for _, item := range toAnySlice(listValue) {
		scope := make(pipelineRow, len(values)+1)
		for name, value := range values {
			scope[name] = value
		}
		scope[variable] = item
		if predicate != "" {
			condition, evaluated := e.evaluateRowExpression(predicate, scope)
			if !evaluated {
				return e.recordRowOperatorFailure(ctx, predicate, scope)
			}
			if matches, isBoolean := condition.(bool); condition == nil || (isBoolean && !matches) {
				continue
			}
		}
		if projection != "" {
			if _, evaluated := e.evaluateRowExpression(projection, scope); !evaluated {
				return e.recordRowOperatorFailure(ctx, projection, scope)
			}
		}
	}
	return false
}

// isOperatorExpressionText reports whether text can be an expression whose
// operators recordRowOperatorFailure may report: not a relationship pattern
// ((a)-[:R]->(b)) or a map projection (n {.*, …}), whose - and * aren't
// arithmetic.
func isOperatorExpressionText(text string) bool {
	text = strings.TrimSpace(text)
	// A list or map literal is a value, not an operator expression, even
	// when its elements are.
	if _, enclosed := stripEnclosingRowDelimiter(text, '[', ']'); enclosed {
		return false
	}
	if strings.HasPrefix(text, "{") && findMatchingDelimiter(text, 0, '{', '}') == len(text)-1 {
		return false
	}
	// -[1] is a unary minus of a list, not a relationship.
	if strings.HasPrefix(text, "-") {
		if _, enclosed := stripEnclosingRowDelimiter(strings.TrimSpace(text[1:]), '[', ']'); enclosed {
			return true
		}
	}
	for _, token := range []string{"->", "<-", "-[", "]-", "--", "{."} {
		if containsOutsideStrings(text, token) {
			return false
		}
	}
	if brace := strings.IndexByte(text, '{'); brace > 0 {
		if before := strings.TrimSpace(text[:brace]); before != "" && isIdentifierPart(before[len(before)-1]) {
			return false
		}
	}
	return true
}

// isOperandExpressionText reports whether text is a whole operand: a literal,
// a parameter, a variable or property chain, a function call, a
// parenthesised expression, a list or map literal, a unary minus of one of
// those, or arithmetic over them.
func isOperandExpressionText(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	if _, enclosed := stripEnclosingRowDelimiter(text, '[', ']'); enclosed {
		return true
	}
	if text[0] == '{' && findMatchingDelimiter(text, 0, '{', '}') == len(text)-1 {
		return true
	}
	if !isOperatorExpressionText(text) {
		return false
	}
	if _, literal := parseLiteralValueFromComputedRow(text); literal {
		return true
	}
	if text[0] == '-' || text[0] == '+' {
		return isOperandExpressionText(text[1:])
	}
	if text[0] == '$' {
		return simpleSemanticIdentifier(text[1:]) != ""
	}
	if simpleSemanticIdentifier(text) != "" || isSimplePropertyAccess(text) {
		return true
	}
	if _, enclosed := stripEnclosingExpressionParentheses(text); enclosed {
		return true
	}
	if _, _, call := parseFunctionCallWS(text); call {
		return true
	}
	for _, tier := range []string{"+-", "*/%", "^"} {
		if left, right, _, arithmetic := splitRowArithmeticTier(text, tier); arithmetic {
			return isOperandExpressionText(left) && isOperandExpressionText(right)
		}
	}
	return false
}

// isSimplePropertyAccess reports whether expression is a chain of names
// (a.b.c) and nothing else.
func isSimplePropertyAccess(expression string) bool {
	parts := strings.Split(expression, ".")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if simpleSemanticIdentifier(name) == "" {
			return false
		}
	}
	return true
}
