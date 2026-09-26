package cypher

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
)

func (e *StorageExecutor) evaluatePipelinePercentile(ctx context.Context, rows []pipelineRow, name, expression string, distinct bool) (interface{}, bool) {
	arguments := e.splitFunctionArgs(expression)
	if len(arguments) != 2 {
		return nil, false
	}

	percentile := float64(0)
	if len(rows) > 0 {
		value, resolved := e.evaluateRowExpressionWithContext(ctx, strings.TrimSpace(arguments[1]), rows[0])
		if !resolved {
			return nil, false
		}
		var numeric bool
		percentile, _, numeric = pipelineAggregateNumber(value)
		if !numeric || math.IsNaN(percentile) || percentile < 0 || percentile > 1 {
			return nil, false
		}
	}

	type percentileValue struct {
		numeric  float64
		original interface{}
	}
	values := make([]percentileValue, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		value, resolved := e.evaluateRowExpressionWithContext(ctx, strings.TrimSpace(arguments[0]), row)
		if !resolved {
			return nil, false
		}
		if value == nil {
			continue
		}
		numeric, _, valid := pipelineAggregateNumber(value)
		if !valid {
			return nil, false
		}
		if distinct {
			key := pipelineValueKey(value)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
		}
		values = append(values, percentileValue{numeric: numeric, original: value})
	}
	if len(values) == 0 {
		return nil, true
	}
	sort.SliceStable(values, func(left, right int) bool { return values[left].numeric < values[right].numeric })

	if name == "percentiledisc" {
		index := int(math.Ceil(percentile*float64(len(values)))) - 1
		if index < 0 {
			index = 0
		}
		return values[index].original, true
	}
	position := percentile * float64(len(values)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return values[lower].numeric, true
	}
	fraction := position - float64(lower)
	return values[lower].numeric + (values[upper].numeric-values[lower].numeric)*fraction, true
}

func (e *StorageExecutor) validatePercentileCalls(expression string, row pipelineRow) error {
	for _, function := range []string{"percentilecont", "percentiledisc"} {
		for _, argumentText := range namedFunctionArguments(expression, function) {
			arguments := e.splitFunctionArgs(argumentText)
			if len(arguments) != 2 {
				continue
			}
			value, resolved, err := e.evaluateRowValue(strings.TrimSpace(arguments[1]), row)
			if err != nil {
				return err
			}
			if !resolved {
				continue
			}
			percentile, _, numeric := pipelineAggregateNumber(value)
			if !numeric || math.IsNaN(percentile) || percentile < 0 || percentile > 1 {
				return newSemanticError(
					"Neo.ClientError.Statement.ArgumentError",
					"NumberOutOfRange",
					fmt.Sprintf("%s() percentile must be between 0.0 and 1.0", function),
				)
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validatePipelinePercentileArguments(rows []pipelineRow, clause, keyword string) error {
	body := strings.TrimSpace(clause)
	if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
		return nil
	}
	body = strings.TrimSpace(body[len(keyword):])
	end := len(body)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
			end = index
		}
	}
	if len(rows) == 0 {
		rows = []pipelineRow{{}}
	}
	for _, item := range splitTopLevelComma(strings.TrimSpace(body[:end])) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		for _, row := range rows {
			if err := e.validatePercentileCalls(expression, row); err != nil {
				return err
			}
		}
	}
	return nil
}
