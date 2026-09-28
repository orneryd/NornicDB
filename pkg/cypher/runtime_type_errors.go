package cypher

import (
	"fmt"
	"reflect"
	"sort"
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

func predicateTruthFromValue(value interface{}) (cypherTruth, error) {
	switch boolean := value.(type) {
	case bool:
		return truthOf(boolean), nil
	case nil:
		return truthUnknown, nil
	default:
		return truthFalse, newSemanticError(
			"Neo.ClientError.Statement.TypeError",
			"TypeMismatch",
			fmt.Sprintf("Type mismatch: expected Boolean but was %s", cypherTypeName(value)),
		)
	}
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
	if m, isMap := value.(map[string]interface{}); isMap {
		keys := make([]string, 0, len(m))
		for key := range m {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for index, key := range keys {
			parts[index] = key + " -> " + neo4jValueRepr(m[key])
		}
		return "Map{" + strings.Join(parts, ", ") + "}"
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
	if _, _, chain := rowPropertyChainShape(text); simpleSemanticIdentifier(text) != "" || chain {
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
