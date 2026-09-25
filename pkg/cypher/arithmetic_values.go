package cypher

import (
	"math"
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
//   - any other operand type is a TypeError (runtimeArithmeticTypeError).

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
		if r == 0 {
			return nil, false, nil
		}
		return math.Mod(l, r), true, nil
	}
	return nil, false, nil
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

// arithmeticError is the statement error of left op right, or nil: an
// INTEGER overflow or an operand type error (runtimeArithmeticTypeError, a
// TypeError: operand types known at compile time were rejected before the
// statement ran). Context-aware evaluators record it
// (recordExpressionFailure); the value helpers return null for it.
func arithmeticError(op byte, left, right interface{}) error {
	if left == nil || right == nil {
		return nil
	}
	if err := runtimeArithmeticTypeError(op, left, right); err != nil {
		return err
	}
	if _, _, err := numericArithmetic(op, left, right); err != nil {
		return err
	}
	return nil
}
