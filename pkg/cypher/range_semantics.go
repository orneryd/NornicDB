package cypher

import (
	"fmt"
	"math"
	"strings"
)

func evaluateCypherRange(arguments []interface{}) ([]interface{}, error) {
	if len(arguments) != 2 && len(arguments) != 3 {
		return nil, invalidRangeArgumentType("range() requires two or three INTEGER arguments", nil)
	}
	start, ok := cypherIntegerValue(arguments[0])
	if !ok {
		return nil, invalidRangeArgumentType("range() start must be an INTEGER", arguments[0])
	}
	end, ok := cypherIntegerValue(arguments[1])
	if !ok {
		return nil, invalidRangeArgumentType("range() end must be an INTEGER", arguments[1])
	}
	step := int64(1)
	if len(arguments) == 3 {
		step, ok = cypherIntegerValue(arguments[2])
		if !ok {
			return nil, invalidRangeArgumentType("range() step must be an INTEGER", arguments[2])
		}
	}
	if step == 0 {
		return nil, newSemanticError(
			"Neo.ClientError.Statement.ArgumentError",
			"NumberOutOfRange",
			"range() step must not be zero",
		)
	}

	values := make([]interface{}, 0)
	if step > 0 {
		for value := start; value <= end; {
			values = append(values, value)
			next := value + step
			if next < value {
				break
			}
			value = next
		}
		return values, nil
	}
	for value := start; value >= end; {
		values = append(values, value)
		next := value + step
		if next > value {
			break
		}
		value = next
	}
	return values, nil
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
			value, resolved := e.evaluateRowExpression(strings.TrimSpace(part), row)
			if !resolved {
				arguments = nil
				break
			}
			arguments[index] = value
		}
		if arguments == nil {
			continue
		}
		if _, err := evaluateCypherRange(arguments); err != nil {
			return err
		}
	}
	return nil
}

func (e *StorageExecutor) validatePipelineRangeArguments(rows []pipelineRow, clause, keyword string) error {
	body := strings.TrimSpace(clause)
	if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
		return nil
	}
	body = strings.TrimSpace(body[len(keyword):])
	expressions := make([]string, 0)
	if strings.EqualFold(keyword, "UNWIND") {
		if asIndex := topLevelKeywordIndex(body, "AS"); asIndex > 0 {
			expressions = append(expressions, strings.TrimSpace(body[:asIndex]))
		}
	} else {
		end := len(body)
		for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
				end = index
			}
		}
		for _, item := range splitTopLevelComma(strings.TrimSpace(body[:end])) {
			expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
			expressions = append(expressions, expression)
		}
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
	lower := strings.ToLower(expression)
	function = strings.ToLower(function)
	arguments := make([]string, 0)
	for index := 0; index < len(lower); {
		quote := lower[index]
		if quote == '\'' || quote == '"' || quote == '`' {
			index++
			for index < len(lower) {
				if lower[index] == quote && (index == 0 || lower[index-1] != '\\') {
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
