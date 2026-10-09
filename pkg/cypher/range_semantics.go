package cypher

import (
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"iter"
	"strings"
)

func evaluateCypherRange(arguments []interface{}) ([]interface{}, error) {
	sequence, err := newCypherRange(arguments)
	if err != nil {
		return nil, err
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
	start, ok := cypherIntegerValue(arguments[0])
	if !ok {
		return cypherRange{}, invalidRangeArgumentType("range() start must be an INTEGER", arguments[0])
	}
	end, ok := cypherIntegerValue(arguments[1])
	if !ok {
		return cypherRange{}, invalidRangeArgumentType("range() end must be an INTEGER", arguments[1])
	}
	step := int64(1)
	if len(arguments) == 3 {
		step, ok = cypherIntegerValue(arguments[2])
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

func invalidRangeArgumentType(message string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidArgumentType",
		fmt.Sprintf("%s, got %T", message, value),
	)
}

func (e *StorageExecutor) validateRangeCalls(expression string, row pipelineRow) error {
	for _, argumentText := range namedFunctionArguments(expression, "range") {
		parts := e.splitFunctionArgs(argumentText)
		arguments := make([]interface{}, len(parts))
		for index, part := range parts {
			value, resolved, err := e.evaluateRowValue(strings.TrimSpace(part), row)
			if err != nil {
				return err
			}
			if !resolved {
				arguments = nil
				break
			}
			arguments[index] = value
		}
		if arguments == nil {
			continue
		}
		if _, err := newCypherRange(arguments); err != nil {
			return err
		}
	}
	return nil
}

func (e *StorageExecutor) validatePipelineRangeArguments(rows []pipelineRow, clause, keyword string) error {
	expressions := projectionExpressions(clause, keyword)
	if len(expressions) == 0 {
		return nil
	}
	if len(rows) == 0 {
		rows = []pipelineRow{{}}
	}
	for _, expression := range expressions {
		for _, row := range rows {
			if err := e.validateRangeCalls(expression, row); err != nil {
				return err
			}
		}
	}
	return nil
}

func namedFunctionArguments(expression, function string) []string {
	lower := lowerASCII(expression)
	function = lowerASCII(function)
	arguments := make([]string, 0)
	for index := 0; index < len(lower); {
		quote := lower[index]
		if quote == '\'' || quote == '"' || quote == '`' {
			index++
			for index < len(lower) {
				if lower[index] == quote && !isBackslashEscaped(lower, index) {
					index++
					break
				}
				index++
			}
			continue
		}
		if !strings.HasPrefix(lower[index:], function) || (index > 0 && isIdentByte(lower[index-1])) {
			index++
			continue
		}
		open := index + len(function)
		if open < len(lower) && isIdentByte(lower[open]) {
			index++
			continue
		}
		for open < len(lower) && isWhitespace(lower[open]) {
			open++
		}
		if open >= len(lower) || lower[open] != '(' {
			index++
			continue
		}
		close := findMatchingDelimiter(expression, open, '(', ')')
		if close < 0 {
			return arguments
		}
		arguments = append(arguments, expression[open+1:close])
		index = close + 1
	}
	return arguments
}
