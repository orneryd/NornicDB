package cypher

import (
	"context"
	"math"
	"strings"
)

type pipelineRowSource func(func(pipelineRow) bool) bool

func pipelineClauseAggregates(clause pipelineClause) bool {
	if clause.kind != pipelineClauseWith && clause.kind != pipelineClauseReturn {
		return false
	}
	body := pipelineClauseBody(clause.text, "RETURN")
	if clause.kind == pipelineClauseWith {
		body = pipelineClauseBody(clause.text, "WITH")
	}
	if where := topLevelKeywordIndex(body, "WHERE"); where >= 0 {
		body = body[:where]
	}
	return returnProjectionPlanFor("RETURN " + body).hasAggregate
}

func pipelineRowsSource(rows []pipelineRow) pipelineRowSource {
	return func(yield func(pipelineRow) bool) bool {
		for _, row := range rows {
			if !yield(row) {
				break
			}
		}
		return true
	}
}

type pipelineAggregateProjection struct {
	expression string
	states     []pipelineAggregateState
}

type pipelineAggregateGroup struct {
	first       pipelineRow
	projections []pipelineAggregateProjection
}

func (e *StorageExecutor) pipelineAggregateGroups(ctx context.Context, source pipelineRowSource, projections []returnProjection) ([]*pipelineAggregateGroup, bool) {
	templates := make([]pipelineAggregateProjection, len(projections))
	validationClauses := make([]string, 0)
	allAggregates := len(projections) > 0
	for index, projection := range projections {
		allAggregates = allAggregates && projection.isAggr
		needsValidation := false
		var rewritten strings.Builder
		last := 0
		for offset, span := range findAggregateSpans(projection.expr) {
			name, inner, distinct, ok := parsePipelineAggregate(projection.expr[span.start:span.end])
			if !ok {
				return nil, false
			}
			templates[index].states = append(templates[index].states, pipelineAggregateState{name: name, expression: inner, distinct: distinct})
			needsValidation = needsValidation || strings.ContainsAny(inner, "([") || name == "percentilecont" || name == "percentiledisc"
			rewritten.WriteString(projection.expr[last:span.start])
			rewritten.WriteString(traversalAggPlaceholder(offset))
			last = span.end
		}
		rewritten.WriteString(projection.expr[last:])
		templates[index].expression = rewritten.String()
		if needsValidation || strings.ContainsAny(templates[index].expression, "([") {
			validationClauses = append(validationClauses, "WITH "+projection.expr)
		}
	}
	newGroup := func(row pipelineRow) *pipelineAggregateGroup {
		group := &pipelineAggregateGroup{first: make(pipelineRow, len(row)), projections: make([]pipelineAggregateProjection, len(templates))}
		for name, value := range row {
			group.first[name] = value
		}
		for index, template := range templates {
			group.projections[index] = pipelineAggregateProjection{expression: template.expression, states: append([]pipelineAggregateState(nil), template.states...)}
		}
		return group
	}
	groups := make(map[string]*pipelineAggregateGroup)
	ordered := make([]*pipelineAggregateGroup, 0)
	valid := true
	completed := source(func(row pipelineRow) bool {
		if err := ctx.Err(); err != nil {
			recordExpressionFailure(ctx, err)
			valid = false
			return false
		}
		for _, clause := range validationClauses {
			if err := e.validatePipelineProjectionValues([]pipelineRow{row}, clause, "WITH"); err != nil {
				recordExpressionFailure(ctx, err)
				valid = false
				return false
			}
		}
		key := ""
		if !allAggregates {
			parts := make([]string, 0, len(projections))
			for _, projection := range projections {
				if projection.isAggr {
					continue
				}
				value, ok := e.evaluateRowExpressionWithContext(ctx, projection.expr, row)
				if !ok {
					pipelineItemUnevaluable(ctx, projection.expr)
					valid = false
					return false
				}
				parts = append(parts, pipelineValueKey(value))
			}
			key = strings.Join(parts, "\x1f")
		}
		group := groups[key]
		if group == nil {
			group = newGroup(row)
			groups[key] = group
			ordered = append(ordered, group)
		}
		for index := range group.projections {
			for offset := range group.projections[index].states {
				if !group.projections[index].states[offset].add(ctx, e, row) {
					valid = false
					return false
				}
			}
		}
		return true
	})
	if !completed || !valid {
		return nil, false
	}
	if len(ordered) == 0 && allAggregates {
		ordered = append(ordered, newGroup(nil))
	}
	return ordered, true
}

func (group *pipelineAggregateGroup) value(ctx context.Context, executor *StorageExecutor, index int) (interface{}, bool) {
	projection := &group.projections[index]
	if len(projection.states) == 0 {
		return executor.evaluateRowExpressionWithContext(ctx, projection.expression, group.first)
	}
	values := make(pipelineRow, len(group.first)+len(projection.states))
	for name, value := range group.first {
		values[name] = value
	}
	for offset := range projection.states {
		value, ok := projection.states[offset].result(ctx, executor)
		if !ok {
			return nil, false
		}
		values[traversalAggPlaceholder(offset)] = value
	}
	return executor.evaluateRowExpressionWithContext(ctx, projection.expression, values)
}

type pipelineAggregateState struct {
	name, expression string
	distinct         bool
	seen             map[string]struct{}
	count            int64
	integerTotal     int64
	floatingTotal    float64
	hasFloat         bool
	selected         interface{}
	values           []interface{}
	mean, squared    float64
	percentileRows   []pipelineRow
}

func (state *pipelineAggregateState) add(ctx context.Context, executor *StorageExecutor, row pipelineRow) bool {
	if state.name == "count" && state.expression == "*" {
		state.count++
		return true
	}
	if state.name == "percentilecont" || state.name == "percentiledisc" {
		arguments := executor.splitFunctionArgs(state.expression)
		if len(arguments) != 2 {
			return false
		}
		stored := pipelineRow{}
		if len(state.percentileRows) == 0 {
			if err := executor.validatePercentileCalls(state.name+"("+state.expression+")", row); err != nil {
				recordExpressionFailure(ctx, err)
				return false
			}
		} else {
			arguments = arguments[:1]
		}
		for _, argument := range arguments {
			argument = strings.TrimSpace(argument)
			value, ok := executor.evaluateRowExpressionWithContext(ctx, argument, row)
			if !ok {
				return false
			}
			stored[argument] = value
		}
		state.percentileRows = append(state.percentileRows, stored)
		return true
	}
	value, ok := row[state.expression]
	if !ok {
		value, ok = executor.evaluateRowExpressionWithContext(ctx, state.expression, row)
	}
	if !ok {
		return false
	}
	if value == nil {
		return true
	}
	if state.distinct {
		if state.seen == nil {
			state.seen = make(map[string]struct{})
		}
		key := pipelineValueKey(value)
		if _, exists := state.seen[key]; exists {
			return true
		}
		state.seen[key] = struct{}{}
	}
	switch state.name {
	case "count":
		state.count++
	case "collect":
		state.values = append(state.values, value)
	case "sum", "avg", "stdev", "stdevp":
		numeric, integer, valid := pipelineAggregateNumber(value)
		if !valid {
			return true
		}
		state.count++
		if state.name == "sum" {
			if integer && !state.hasFloat {
				state.integerTotal += int64(numeric)
			} else {
				if !state.hasFloat {
					state.floatingTotal = float64(state.integerTotal)
					state.hasFloat = true
				}
				state.floatingTotal += numeric
			}
		} else if state.name == "avg" {
			state.floatingTotal += numeric
		} else {
			delta := numeric - state.mean
			state.mean += delta / float64(state.count)
			state.squared += delta * (numeric - state.mean)
		}
	case "min", "max":
		if state.selected == nil {
			state.selected = value
		} else {
			comparison := compareValuesForSort(value, state.selected)
			if state.name == "min" && comparison < 0 || state.name == "max" && comparison > 0 {
				state.selected = value
			}
		}
	default:
		return false
	}
	return true
}

func (state *pipelineAggregateState) result(ctx context.Context, executor *StorageExecutor) (interface{}, bool) {
	switch state.name {
	case "count":
		return state.count, true
	case "sum":
		if state.hasFloat {
			return state.floatingTotal, true
		}
		return state.integerTotal, true
	case "avg":
		if state.count == 0 {
			return nil, true
		}
		return state.floatingTotal / float64(state.count), true
	case "min", "max":
		return state.selected, true
	case "collect":
		if state.values == nil {
			return []interface{}{}, true
		}
		return state.values, true
	case "stdev", "stdevp":
		if state.count == 0 {
			return nil, true
		}
		if state.count < 2 {
			return float64(0), true
		}
		divisor := state.count
		if state.name == "stdev" {
			divisor--
		}
		return math.Sqrt(state.squared / float64(divisor)), true
	case "percentilecont", "percentiledisc":
		return executor.evaluatePipelinePercentile(ctx, state.percentileRows, state.name, state.expression, state.distinct)
	default:
		return nil, false
	}
}
