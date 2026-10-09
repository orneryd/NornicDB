package cypher

import (
	"github.com/orneryd/nornicdb/pkg/math/angle"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
)

// evaluateRowMathFunction evaluates scalar math functions for the converged
// row-expression operator. The boolean results are (matched, resolved), which
// lets the caller distinguish a non-math function from an invalid argument.
func (e *StorageExecutor) evaluateRowMathFunction(function, argument string, values map[string]interface{}) (interface{}, bool, bool, error) {
	name := lowerASCII(function)
	if !isSharedMathFunction(name) {
		return nil, false, false, nil
	}
	value, resolved, err := e.evaluateRowGraphFunction(function, argument, values)
	return value, true, resolved, err
}

func isSharedMathFunction(name string) bool {
	switch name {
	case "pi", "e", "round", "sin", "cos", "tan", "cot", "asin", "acos", "atan", "atan2",
		"exp", "log", "log10", "sqrt", "ceil", "ceiling", "floor", "degrees", "radians", "haversin",
		"sinh", "cosh", "tanh", "coth", "power":
		return true
	default:
		return false
	}
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

func rowMathDegrees(value float64) float64 { return angle.ToDegrees(value) }

func rowMathHaversin(value float64) float64 { return (1 - math.Cos(value)) / 2 }

func rowMathCoth(value float64) float64 {
	sinh := math.Sinh(value)
	if sinh == 0 {
		return math.NaN()
	}
	return math.Cosh(value) / sinh
}
