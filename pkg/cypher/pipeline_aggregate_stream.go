package cypher

import (
	"context"
	"math"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
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

// pipelineAggregateGroups aggregates the rows of source into groups. Rows are
// validated as they stream in, unless rowsValidated says the caller already
// validated exactly these rows; validating them again doubled the cost of
// aggregating projections (#823).
func (e *StorageExecutor) pipelineAggregateGroups(ctx context.Context, source pipelineRowSource, projections []returnProjection, rowsValidated bool) ([]*pipelineAggregateGroup, bool) {
	templates := make([]pipelineAggregateProjection, len(projections))
	validationClauses := make([]string, 0)
	allAggregates := len(projections) > 0
	groupingCount := 0
	for index, projection := range projections {
		allAggregates = allAggregates && projection.isAggr
		if !projection.isAggr {
			groupingCount++
		}
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
		if !rowsValidated && (needsValidation || strings.ContainsAny(templates[index].expression, "([")) {
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
	grouping := struct {
		ordered      []*pipelineAggregateGroup
		stringGroups map[string]*pipelineAggregateGroup
	}{ordered: make([]*pipelineAggregateGroup, 0)}
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
		lookup := groups
		if !allAggregates {
			var parts []string
			if groupingCount > 1 {
				parts = make([]string, 0, len(projections))
			}
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
				if groupingCount == 1 {
					if text, ok := value.(string); ok {
						key = text
						if grouping.stringGroups == nil {
							grouping.stringGroups = make(map[string]*pipelineAggregateGroup)
						}
						lookup = grouping.stringGroups
					} else {
						key = pipelineValueKey(value)
					}
				} else {
					parts = append(parts, pipelineValueKey(value))
				}
			}
			if groupingCount > 1 {
				key = strings.Join(parts, "\x1f")
			}
		}
		group := lookup[key]
		if group == nil {
			group = newGroup(row)
			lookup[key] = group
			grouping.ordered = append(grouping.ordered, group)
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
	if len(grouping.ordered) == 0 && allAggregates {
		grouping.ordered = append(grouping.ordered, newGroup(nil))
	}
	return grouping.ordered, true
}

func cartesianAggregateWorkers(rows, complexity, cores int) int {
	if rows < 65536 || cores <= 1 {
		return 1
	}
	chunk := 16384 / max(1, complexity)
	return max(1, min(cores, rows/max(256, chunk)))
}

func cartesianAggregateProperty(expression string, patterns []struct {
	variable string
	nodes    []*storage.Node
}) (int, string, bool) {
	separator := strings.IndexByte(expression, '.')
	if separator <= 0 || separator == len(expression)-1 {
		return 0, "", false
	}
	property := expression[separator+1:]
	for _, character := range property {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_') {
			return 0, "", false
		}
	}
	for index, pattern := range patterns {
		if pattern.variable == expression[:separator] {
			return index, property, true
		}
	}
	return 0, "", false
}

func (e *StorageExecutor) tryCartesianAggregatePartitions(ctx context.Context, patterns []struct {
	variable string
	nodes    []*storage.Node
}, plan *returnProjectionPlan, forcedWorkers int) ([]*pipelineAggregateGroup, bool, error) {
	if !plan.valid || !plan.hasAggregate {
		return nil, false, nil
	}
	rows := 1
	for _, pattern := range patterns {
		if len(pattern.nodes) == 0 || rows > int(^uint(0)>>1)/len(pattern.nodes) {
			return nil, false, nil
		}
		rows *= len(pattern.nodes)
	}
	if rows < 65536 && forcedWorkers == 0 {
		return nil, false, nil
	}
	for index, pattern := range patterns {
		for prior := 0; prior < index; prior++ {
			if pattern.variable == patterns[prior].variable {
				return nil, false, nil
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	complexity := 0
	for _, projection := range plan.projections {
		if !projection.isAggr {
			if _, _, ok := cartesianAggregateProperty(projection.expr, patterns); !ok {
				return nil, false, nil
			}
			complexity += 2
			continue
		}
		spans := findAggregateSpans(projection.expr)
		if len(spans) == 0 {
			return nil, false, nil
		}
		for _, span := range spans {
			name, expression, distinct, ok := parsePipelineAggregate(projection.expr[span.start:span.end])
			if !ok || distinct || (name != "count" && name != "sum") {
				return nil, false, nil
			}
			complexity++
			if name == "count" && expression == "*" {
				continue
			}
			if _, err := strconv.ParseInt(expression, 10, 64); err == nil {
				continue
			}
			patternIndex, property, ok := cartesianAggregateProperty(expression, patterns)
			if !ok {
				return nil, false, nil
			}
			if name == "sum" {
				complexity++
				for _, node := range patterns[patternIndex].nodes {
					if node == nil || node.Properties[property] == nil {
						continue
					}
					_, _, integer, valid := pipelineAggregateNumber(node.Properties[property])
					if !valid || !integer {
						return nil, false, nil
					}
				}
			}
		}
	}
	workers := cartesianAggregateWorkers(rows, complexity, runtime.GOMAXPROCS(0))
	if forcedWorkers > 0 {
		workers = min(rows, forcedWorkers)
	}
	type partition struct {
		groups []*pipelineAggregateGroup
		err    error
	}
	jobs := workers
	if workers > 1 {
		jobs = min(rows, workers*4)
	}
	partitions := make([]partition, workers)
	var next atomic.Int64
	run := func(worker int) {
		workerContext := context.WithValue(ctx, expressionFailureKey{}, &expressionFailure{})
		source := func(yield func(pipelineRow) bool) bool {
			values := make(pipelineRow, len(patterns))
			for {
				job := int(next.Add(1) - 1)
				if job >= jobs {
					return true
				}
				width, remainder := rows/jobs, rows%jobs
				start := job*width + min(job, remainder)
				end := start + width
				if job < remainder {
					end++
				}
				for ordinal := start; ordinal < end; ordinal++ {
					position := ordinal
					for index := len(patterns) - 1; index >= 0; index-- {
						pattern := patterns[index]
						node := pattern.nodes[position%len(pattern.nodes)]
						if node == nil {
							values[pattern.variable] = nil
						} else {
							values[pattern.variable] = node
						}
						position /= len(pattern.nodes)
					}
					if !yield(values) {
						return true
					}
				}
			}
		}
		groups, valid := e.pipelineAggregateGroups(workerContext, source, plan.projections, true)
		partitions[worker].groups = groups
		if !valid {
			partitions[worker].err = getExpressionFailure(workerContext)
			if partitions[worker].err == nil {
				partitions[worker].err = newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidAggregate", "invalid Cartesian aggregate")
			}
		}
	}
	if workers == 1 {
		run(0)
	} else {
		var wait sync.WaitGroup
		wait.Add(workers)
		for worker := 0; worker < workers; worker++ {
			go func() {
				defer wait.Done()
				run(worker)
			}()
		}
		wait.Wait()
	}
	if workers == 1 {
		return partitions[0].groups, true, partitions[0].err
	}
	positions := make([]map[*storage.Node]int, len(patterns))
	for index, pattern := range patterns {
		positions[index] = make(map[*storage.Node]int, len(pattern.nodes))
		for position, node := range pattern.nodes {
			if _, exists := positions[index][node]; !exists {
				positions[index][node] = position
			}
		}
	}
	type mergedGroup struct {
		group   *pipelineAggregateGroup
		ordinal int
	}
	groups := make(map[string]*mergedGroup)
	var ordered []*mergedGroup
	for _, partition := range partitions {
		if partition.err != nil {
			return nil, true, partition.err
		}
		for _, partial := range partition.groups {
			if len(partial.first) == 0 {
				continue
			}
			ordinal := 0
			for index, pattern := range patterns {
				node, _ := partial.first[pattern.variable].(*storage.Node)
				ordinal = ordinal*len(pattern.nodes) + positions[index][node]
			}
			var parts []string
			for _, projection := range plan.projections {
				if !projection.isAggr {
					value, ok := e.evaluateRowExpressionWithContext(ctx, projection.expr, partial.first)
					if !ok {
						return nil, true, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidAggregate", "invalid Cartesian grouping expression")
					}
					parts = append(parts, pipelineValueKey(value))
				}
			}
			key := strings.Join(parts, "\x1f")
			merged := groups[key]
			if merged == nil {
				merged = &mergedGroup{group: partial, ordinal: ordinal}
				groups[key] = merged
				ordered = append(ordered, merged)
				continue
			}
			group := merged.group
			if ordinal < merged.ordinal {
				merged.ordinal = ordinal
				group.first = partial.first
			}
			for index := range group.projections {
				for stateIndex := range group.projections[index].states {
					state := &group.projections[index].states[stateIndex]
					other := &partial.projections[index].states[stateIndex]
					state.count += other.count
					state.integerTotal += other.integerTotal
				}
			}
		}
	}
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].ordinal < ordered[right].ordinal })
	result := make([]*pipelineAggregateGroup, len(ordered))
	for index, merged := range ordered {
		result[index] = merged.group
	}
	return result, true, nil
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
		numeric, exactInteger, integer, valid := pipelineAggregateNumber(value)
		if !valid {
			return true
		}
		state.count++
		if state.name == "sum" {
			if integer && !state.hasFloat {
				state.integerTotal += exactInteger
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
