// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
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
	return valueconvert.IntegerValue(value)
}

// rangeBound is a range() start, end or step: an integer, or a float
// truncated toward zero (range(n.f, 3) for n.f = 1.5 is [1, 2, 3], -1.5 is
// -1), as Neo4j reads a float from a property; a float literal is the
// static check's SyntaxError first. Anything else isn't a bound.
func rangeBound(value interface{}) (int64, bool) {
	return valueconvert.Int64(value, valueconvert.IntegerFloat)
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

func (e *StorageExecutor) validatePipelineRangeArguments(ctx context.Context, rows []pipelineRow, clause pipelineClause, keyword string) error {
	return e.validatePipelineRangeExpressions(ctx, rows, projectionExpressions(ctx, clause, keyword))
}

func (e *StorageExecutor) validatePipelineRangeExpressions(ctx context.Context, rows []pipelineRow, expressions []string) error {
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
		if isCypherQuoteByte(lower[index]) {
			index = skipCypherQuotedText(expression, index, expression[index])
			continue
		}
		if lower[index] == '/' {
			if end := queryCommentEnd(expression, index); end >= 0 {
				index = end
				continue
			}
		}
		if !strings.HasPrefix(lower[index:], function) || !functionNameStartsAt(expression, index) {
			index++
			continue
		}
		open := index + len(function)
		if open < len(lower) && cyphertext.NamePartAt(lower, open) {
			index++
			continue
		}
		for open < len(lower) && cyphertext.IsWhitespaceAt(lower, open) {
			open++
		}
		if open >= len(lower) || lower[open] != '(' {
			index++
			continue
		}
		close := cyphertext.FindMatchingDelimiter(expression, open, '(', ')', cyphertext.OpeningDelimiter)
		if close < 0 {
			return arguments
		}
		arguments = append(arguments, expression[open+1:close])
		index = close + 1
	}
	return arguments
}
