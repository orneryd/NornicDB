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
	if name == "round" {
		parts := e.splitFunctionArgs(argument)
		if len(parts) < 1 || len(parts) > 3 {
			return nil, true, false
		}
		value, resolved := e.evaluateRowExpression(strings.TrimSpace(parts[0]), values)
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
		precision := 0
		if len(parts) >= 2 {
			precisionValue, ok := e.evaluateRowExpression(strings.TrimSpace(parts[1]), values)
			if !ok {
				return nil, true, false
			}
			var precisionOK bool
			precision, precisionOK = toInt(precisionValue)
			if !precisionOK || precision < 0 {
				return nil, true, false
			}
		}
		mode := "HALF_UP"
		if len(parts) == 3 {
			modeValue, ok := e.evaluateRowExpression(strings.TrimSpace(parts[2]), values)
			if !ok {
				return nil, true, false
			}
			if modeValue == nil {
				return nil, true, true
			}
			var modeOK bool
			mode, modeOK = modeValue.(string)
			if !modeOK {
				return nil, true, false
			}
		}
		if len(parts) == 1 || (len(parts) == 2 && precision == 0) {
			return float64(math.Floor(number + 0.5)), true, true
		}
		factor := math.Pow10(precision)
		rounded, ok := roundRowNumber(number*factor, mode)
		if !ok {
			return nil, true, false
		}
		return rounded / factor, true, true
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

func roundRowNumber(value float64, mode string) (float64, bool) {
	switch mode {
	case "CEILING":
		return math.Ceil(value), true
	case "FLOOR":
		return math.Floor(value), true
	case "UP":
		if value < 0 {
			return math.Floor(value), true
		}
		return math.Ceil(value), true
	case "DOWN":
		return math.Trunc(value), true
	case "HALF_EVEN":
		return math.RoundToEven(value), true
	case "HALF_UP":
		return math.Round(value), true
	case "HALF_DOWN":
		absolute := math.Abs(value)
		integer, fraction := math.Modf(absolute)
		if fraction > 0.5 {
			integer++
		}
		return math.Copysign(integer, value), true
	case "UNNECESSARY":
		if value != math.Trunc(value) {
			return 0, false
		}
		return value, true
	default:
		return 0, false
	}
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
