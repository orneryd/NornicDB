package cypher

import (
	"fmt"
	"math"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Value-level Cypher arithmetic shared by every evaluator (add, subtract,
// multiply, divide, modulo and the context-aware callers that report
// errors). Neo4j semantics (#462):
//
//   - an INTEGER op INTEGER is exact 64-bit arithmetic: no round trip through
//     float64 (9007199254740993 + 1 = 9007199254740994) and an overflow is
//     ArithmeticError "long overflow"; / truncates toward zero and % keeps
//     the dividend's sign;
//   - an INTEGER op FLOAT (or FLOAT op FLOAT) is IEEE float arithmetic;
//   - a null operand makes the result null (strings and lists included:
//     'a' + null and [1] + null are null);
//   - booleans, maps, nodes and relationships are not arithmetic operands.

// cypherIntegerOperand returns v as an int64 when it is a Cypher INTEGER.
func cypherIntegerOperand(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint:
		if uint64(n) <= math.MaxInt64 {
			return int64(n), true
		}
	case uint64:
		if n <= math.MaxInt64 {
			return int64(n), true
		}
	}
	return 0, false
}

// longOverflowError is Neo4j's error for INTEGER arithmetic outside int64.
func longOverflowError() error {
	return newSemanticError("Neo.ClientError.Statement.ArithmeticError", "IntegerOverflow", "long overflow")
}

// exactIntegerArithmetic applies op to two INTEGERs. ok is false for a
// division or modulo by zero (callers report "/ by zero"); err is the
// overflow error.
func exactIntegerArithmetic(op byte, a, b int64) (result int64, ok bool, err error) {
	switch op {
	case '+':
		result = a + b
		if (a > 0 && b > 0 && result < 0) || (a < 0 && b < 0 && result >= 0) {
			return 0, true, longOverflowError()
		}
	case '-':
		result = a - b
		if (a >= 0 && b < 0 && result < 0) || (a < 0 && b > 0 && result >= 0) {
			return 0, true, longOverflowError()
		}
	case '*':
		if a == 0 || b == 0 {
			return 0, true, nil
		}
		result = a * b
		if result/b != a || (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) {
			return 0, true, longOverflowError()
		}
	case '/':
		if b == 0 {
			return 0, false, nil
		}
		if a == math.MinInt64 && b == -1 {
			return 0, true, longOverflowError()
		}
		result = a / b
	case '%':
		if b == 0 {
			return 0, false, nil
		}
		if b == -1 {
			return 0, true, nil
		}
		result = a % b
	default:
		return 0, false, nil
	}
	return result, true, nil
}

// numericArithmetic applies op to two numbers: exact for two INTEGERs,
// float64 otherwise. handled is false when either operand is not a number
// or an INTEGER division/modulo is by zero.
func numericArithmetic(op byte, left, right interface{}) (value interface{}, handled bool, err error) {
	if a, leftInt := cypherIntegerOperand(left); leftInt {
		if b, rightInt := cypherIntegerOperand(right); rightInt {
			result, ok, overflow := exactIntegerArithmetic(op, a, b)
			if overflow != nil {
				return nil, true, overflow
			}
			if !ok {
				return nil, false, nil
			}
			return result, true, nil
		}
	}
	l, leftNumeric := toFloat64(left)
	r, rightNumeric := toFloat64(right)
	if !leftNumeric || !rightNumeric || isNonNumericArithmeticOperand(left) || isNonNumericArithmeticOperand(right) {
		return nil, false, nil
	}
	if divisionByZero(op, left, right) {
		return nil, false, nil
	}
	switch op {
	case '+':
		return l + r, true, nil
	case '-':
		return l - r, true, nil
	case '*':
		return l * r, true, nil
	case '/':
		// IEEE 754: x / 0.0 is ±Inf and 0.0 / 0.0 is NaN.
		return l / r, true, nil
	case '%':
		// IEEE 754 remainder: x % 0.0 is NaN.
		return math.Mod(l, r), true, nil
	}
	return nil, false, nil
}

// divisionByZeroError is Neo4j's error for an INTEGER / or % by zero.
func divisionByZeroError() error {
	return newSemanticError("Neo.ClientError.Statement.ArithmeticError", "DivisionByZero", "/ by zero")
}

// divisionByZero reports Neo4j's "/ by zero" ArithmeticError for numeric
// operands: a / whose divisor is an INTEGER zero, whatever the dividend's
// type, and a % of two INTEGERs with a zero divisor. A FLOAT 0.0 divisor, or
// a % with a FLOAT operand, is IEEE 754 (±Infinity or NaN). A / of
// literal-only operands is folded instead (foldedDivisionByZero).
func divisionByZero(op byte, left, right interface{}) bool {
	if op != '/' && op != '%' {
		return false
	}
	if _, numeric := toFloat64(left); !numeric || left == nil || isNonNumericArithmeticOperand(left) {
		return false
	}
	divisor, rightInt := cypherIntegerOperand(right)
	if !rightInt || divisor != 0 {
		return false
	}
	if op == '%' {
		_, leftInt := cypherIntegerOperand(left)
		return leftInt
	}
	return true
}

// foldedDivisionByZero is Neo4j's compile-time folding of a / whose operands
// are literal-only expressions (numbers, arithmetic operators, parentheses):
// a FLOAT divided by an INTEGER zero is ±Infinity (NaN for 0.0 / 0) rather
// than the "/ by zero" error. An INTEGER dividend still fails, and an operand
// with a variable, parameter or function call is not folded:
// RETURN 1.0 / (1 - 1) is Infinity, RETURN toFloat(1) / 0 fails.
func foldedDivisionByZero(leftExpr, rightExpr string, left, right interface{}) (interface{}, bool) {
	if !divisionByZero('/', left, right) {
		return nil, false
	}
	if _, leftInt := cypherIntegerOperand(left); leftInt {
		return nil, false
	}
	if !isConstantNumericExpression(leftExpr) || !isConstantNumericExpression(rightExpr) {
		return nil, false
	}
	dividend, _ := toFloat64(left)
	zero := 0.0
	return dividend / zero, true
}

// isConstantNumericExpression reports an expression made only of number
// literals (decimal, float, exponent, hex, octal), arithmetic operators and
// parentheses.
func isConstantNumericExpression(expr string) bool {
	numbers := 0
	for i := 0; i < len(expr); {
		ch := expr[i]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '(' || ch == ')' ||
			ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '%' || ch == '^':
			i++
		case ch >= '0' && ch <= '9' || ch == '.' && i+1 < len(expr) && expr[i+1] >= '0' && expr[i+1] <= '9':
			numbers++
			start := i
			i++
			for i < len(expr) {
				c := expr[i]
				if c >= '0' && c <= '9' || c == '.' || c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
					i++
				} else if (c == '+' || c == '-') && (expr[i-1] == 'e' || expr[i-1] == 'E') && !strings.ContainsAny(expr[start:i], "xX") {
					i++
				} else {
					break
				}
			}
		default:
			return false
		}
	}
	return numbers > 0
}

// isNonNumericArithmeticOperand reports values toFloat64 may accept but that
// are not Cypher numbers (booleans, numeric strings).
func isNonNumericArithmeticOperand(v interface{}) bool {
	switch v.(type) {
	case bool, string:
		return true
	}
	return false
}

// arithmeticOperandTypeError is Neo4j's Type mismatch error for a value that
// is never an operand of arithmetic: a boolean, a map, a node or a
// relationship. Strings are left to the + and temporal rules (NornicDB also
// carries dates as strings).
func arithmeticOperandTypeError(op byte, left, right interface{}) error {
	if left == nil || right == nil {
		return nil
	}
	for _, operand := range []interface{}{left, right} {
		switch operand.(type) {
		case bool, *storage.Node, *storage.Edge:
			return arithmeticTypeMismatch(op, operand)
		case map[string]interface{}:
			if _, isPath := operand.(map[string]interface{})["_pathResult"]; !isPath {
				return arithmeticTypeMismatch(op, operand)
			}
		}
	}
	return nil
}

func arithmeticTypeMismatch(op byte, operand interface{}) error {
	expected := "Float, Integer, Duration, Date, Time, LocalTime, LocalDateTime or DateTime"
	if op == '+' {
		expected = "Float, Integer, String or List<T>"
	}
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidArgumentType",
		fmt.Sprintf("Type mismatch: expected %s but was %s", expected, cypherValueTypeName(operand)),
	)
}

// arithmeticError is the statement error of left op right, or nil: an
// INTEGER overflow or an operand type error. Context-aware evaluators record
// it (recordExpressionFailure); the value helpers return null for it.
func arithmeticError(op byte, left, right interface{}) error {
	if left == nil || right == nil {
		return nil
	}
	if err := arithmeticOperandTypeError(op, left, right); err != nil {
		return err
	}
	if _, _, err := numericArithmetic(op, left, right); err != nil {
		return err
	}
	return nil
}
