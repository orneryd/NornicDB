package cypher

import (
	"math"
	"strings"
)

// evaluateRowMathFunction evaluates scalar math functions for the converged
// row-expression operator. The boolean results are (matched, resolved), which
// lets the caller distinguish a non-math function from an invalid argument.
func (e *StorageExecutor) evaluateRowMathFunction(function, argument string, values map[string]interface{}) (interface{}, bool, bool) {
	name := strings.ToLower(function)
	if name == "pi" || name == "e" {
		if strings.TrimSpace(argument) != "" {
			return nil, true, false
		}
		if name == "pi" {
			return math.Pi, true, true
		}
		return math.E, true, true
	}

	var unary func(float64) float64
	switch name {
	case "sin":
		unary = math.Sin
	case "cos":
		unary = math.Cos
	case "tan":
		unary = math.Tan
	case "cot":
		unary = rowMathCot
	case "asin":
		unary = math.Asin
	case "acos":
		unary = math.Acos
	case "atan":
		unary = math.Atan
	case "exp":
		unary = math.Exp
	case "log":
		unary = math.Log
	case "log10":
		unary = math.Log10
	case "sqrt":
		unary = math.Sqrt
	case "ceil", "ceiling":
		unary = math.Ceil
	case "floor":
		unary = math.Floor
	case "radians":
		unary = rowMathRadians
	case "degrees":
		unary = rowMathDegrees
	case "haversin":
		unary = rowMathHaversin
	case "sinh":
		unary = math.Sinh
	case "cosh":
		unary = math.Cosh
	case "tanh":
		unary = math.Tanh
	case "coth":
		unary = rowMathCoth
	}
	if unary != nil {
		value, resolved := e.evaluateRowExpression(strings.TrimSpace(argument), values)
		if !resolved {
			return nil, true, false
		}
		if value == nil {
			return nil, true, true
		}
		number, numeric := toFloat64(value)
		if !numeric {
			return nil, true, false
		}
		return unary(number), true, true
	}

	if name != "atan2" && name != "power" {
		return nil, false, false
	}
	parts := e.splitFunctionArgs(argument)
	if len(parts) != 2 {
		return nil, true, false
	}
	left, leftResolved := e.evaluateRowExpression(strings.TrimSpace(parts[0]), values)
	right, rightResolved := e.evaluateRowExpression(strings.TrimSpace(parts[1]), values)
	if !leftResolved || !rightResolved {
		return nil, true, false
	}
	if left == nil || right == nil {
		return nil, true, true
	}
	leftNumber, leftNumeric := toFloat64(left)
	rightNumber, rightNumeric := toFloat64(right)
	if !leftNumeric || !rightNumeric {
		return nil, true, false
	}
	if name == "atan2" {
		return math.Atan2(leftNumber, rightNumber), true, true
	}
	return math.Pow(leftNumber, rightNumber), true, true
}

func rowMathCot(value float64) float64 { return 1 / math.Tan(value) }

func rowMathRadians(value float64) float64 { return value * math.Pi / 180 }

func rowMathDegrees(value float64) float64 { return value * 180 / math.Pi }

func rowMathHaversin(value float64) float64 { return (1 - math.Cos(value)) / 2 }

func rowMathCoth(value float64) float64 {
	sinh := math.Sinh(value)
	if sinh == 0 {
		return math.NaN()
	}
	return math.Cosh(value) / sinh
}
