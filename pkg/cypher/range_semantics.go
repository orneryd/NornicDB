package cypher

import (
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"iter"
)

// evaluateCypherRange builds range()'s list, every evaluator's one builder.
// The list counts against the statement's memory limit before it is built
// (budget, #1343); it is given back once built, and the row or list that
// keeps it counts it there.
func evaluateCypherRange(budget *expressionBudget, arguments []interface{}) ([]interface{}, error) {
	sequence, err := newCypherRange(arguments)
	if err != nil {
		return nil, err
	}
	if budget.active() {
		// Each element is an interface slot and its boxed integer.
		if err := budget.holdSlots(sequence.length(), valueSlotBytes+8); err != nil {
			return nil, err
		}
		defer budget.release()
	}
	values := make([]interface{}, 0)
	for value := range sequence.values() {
		values = append(values, value)
	}
	return values, nil
}

type cypherRange struct{ start, end, step int64 }

func newCypherRange(arguments []interface{}) (cypherRange, error) {
	if len(arguments) != 2 && len(arguments) != 3 {
		return cypherRange{}, invalidRangeArgumentType("range() requires two or three INTEGER arguments", nil)
	}
	start, ok := rangeBound(arguments[0])
	if !ok {
		return cypherRange{}, invalidRangeArgumentType("range() start must be an INTEGER", arguments[0])
	}
	end, ok := rangeBound(arguments[1])
	if !ok {
		return cypherRange{}, invalidRangeArgumentType("range() end must be an INTEGER", arguments[1])
	}
	step := int64(1)
	if len(arguments) == 3 {
		step, ok = rangeBound(arguments[2])
		if !ok {
			return cypherRange{}, invalidRangeArgumentType("range() step must be an INTEGER", arguments[2])
		}
	}
	if step == 0 {
		return cypherRange{}, newSemanticError(
			"Neo.ClientError.Statement.ArgumentError",
			"NumberOutOfRange",
			"range() step must not be zero",
		)
	}

	return cypherRange{start: start, end: end, step: step}, nil
}

// length is the number of values the range has, capped at maxReservation.
func (sequence cypherRange) length() int64 {
	var span, step uint64
	switch {
	case sequence.step > 0 && sequence.end >= sequence.start:
		span, step = uint64(sequence.end)-uint64(sequence.start), uint64(sequence.step)
	case sequence.step < 0 && sequence.end <= sequence.start:
		span, step = uint64(sequence.start)-uint64(sequence.end), uint64(-(sequence.step+1))+1
	default:
		return 0
	}
	// The quotient saturates before the range's first value is added: a
	// range as wide as the integers (span/step = MaxUint64) would wrap to 0.
	steps := span / step
	if steps >= uint64(maxReservation) {
		return maxReservation
	}
	return int64(steps) + 1
}

func (sequence cypherRange) values() iter.Seq[int64] {
	return func(yield func(int64) bool) {
		for value := sequence.start; (sequence.step > 0 && value <= sequence.end) || (sequence.step < 0 && value >= sequence.end); {
			if !yield(value) {
				return
			}
			next := value + sequence.step
			if (sequence.step > 0 && next < value) || (sequence.step < 0 && next > value) {
				return
			}
			value = next
		}
	}
}

func cypherIntegerValue(value interface{}) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case uint:
		if uint64(number) > math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	case uint8:
		return int64(number), true
	case uint16:
		return int64(number), true
	case uint32:
		return int64(number), true
	case uint64:
		if number > math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	default:
		return 0, false
	}
}

// rangeBound is a range() start, end or step: an integer, or a float
// truncated toward zero (range(n.f, 3) for n.f = 1.5 is [1, 2, 3], -1.5 is
// -1), as Neo4j reads a float from a property; a float literal is the
// static check's SyntaxError first. Anything else isn't a bound.
func rangeBound(value interface{}) (int64, bool) {
	if integer, ok := cypherIntegerValue(value); ok {
		return integer, true
	}
	number, isFloat := cypherFloatValue(value)
	if !isFloat || number != number || number >= 9.223372036854775807e18 || number <= -9.223372036854775808e18 {
		return 0, false
	}
	return int64(number), true
}

func invalidRangeArgumentType(message string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidArgumentType",
		fmt.Sprintf("%s, got %T", message, value),
	)
}

func namedFunctionArguments(expression, function string) []string {
	return lexicalNamedArguments(expression, function)
}
