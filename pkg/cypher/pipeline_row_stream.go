package cypher

import (
	"context"
	"iter"
	"strings"
)

type pipelineRowProjection struct{ expression, alias string }
type pipelineRowWith struct {
	clause, where string
	star          bool
	projections   []pipelineRowProjection
}

func parsePipelineRowWith(clause string) (pipelineRowWith, bool) {
	body := pipelineClauseBody(clause, "WITH")
	for _, modifier := range []string{"DISTINCT", "ORDER BY", "SKIP", "LIMIT"} {
		if topLevelKeywordIndex(body, modifier) >= 0 {
			return pipelineRowWith{}, false
		}
	}
	plan := pipelineRowWith{clause: clause}
	if index := topLevelKeywordIndex(body, "WHERE"); index >= 0 {
		plan.where = strings.TrimSpace(body[index+len("WHERE"):])
		body = strings.TrimSpace(body[:index])
	}
	plan.star = strings.TrimSpace(body) == "*"
	if !plan.star {
		for _, item := range splitTopLevelComma(body) {
			item = strings.TrimSpace(item)
			if item == "" || item == "{}" {
				continue
			}
			expression, alias := parseProjectionExprAlias(item)
			if expression == "" || alias == "" || pipelineExpressionContainsAggregate(expression) {
				return pipelineRowWith{}, false
			}
			plan.projections = append(plan.projections, pipelineRowProjection{expression, alias})
		}
		if len(plan.projections) == 0 {
			return pipelineRowWith{}, false
		}
	}
	return plan, true
}

func (e *StorageExecutor) pipelineProjectWithRow(ctx context.Context, row pipelineRow, plan pipelineRowWith, projected, scope pipelineRow) (pipelineRow, bool, bool) {
	if projected == nil {
		projected = pipelineRow{}
	} else {
		clear(projected)
	}
	for name, value := range row {
		if plan.star || strings.HasPrefix(name, "$") {
			projected[name] = value
		}
	}
	for _, projection := range plan.projections {
		value, found := row[projection.expression]
		if !found {
			value, found = e.evaluateRowExpressionWithContext(ctx, projection.expression, row)
		}
		if !found {
			pipelineItemUnevaluable(ctx, projection.expression)
			return nil, false, false
		}
		projected[projection.alias] = value
	}
	if plan.where == "" && scope == nil {
		return projected, true, true
	}
	if scope == nil {
		scope = pipelineRow{}
	} else {
		clear(scope)
	}
	for name, value := range row {
		scope[name] = value
	}
	for name, value := range projected {
		scope[name] = value
	}
	if plan.where == "" {
		return projected, true, true
	}
	return projected, e.evaluateWithWhereCondition(ctx, plan.where, scope), getExpressionFailure(ctx) == nil
}

func pipelineUnwindUsesRange(clause string) bool {
	expression, _, ok := splitUnwindBody(pipelineClauseBody(clause, "UNWIND"))
	function, _, call := parseFunctionCallWS(strings.TrimSpace(expression))
	return ok && call && strings.EqualFold(function, "range")
}

func (e *StorageExecutor) pipelineUnwindValues(ctx context.Context, expression string, row pipelineRow) (iter.Seq[interface{}], bool) {
	function, arguments, call := parseFunctionCallWS(strings.TrimSpace(expression))
	if call && strings.EqualFold(function, "range") {
		parts := e.splitFunctionArgs(arguments)
		values := make([]interface{}, len(parts))
		for index, part := range parts {
			value, resolved := e.evaluateRowExpressionWithContext(ctx, strings.TrimSpace(part), row)
			if !resolved {
				pipelineItemUnevaluable(ctx, part)
				return nil, false
			}
			values[index] = value
		}
		sequence, err := newCypherRange(values)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil, false
		}
		return func(yield func(interface{}) bool) {
			for value := range sequence.values() {
				if !yield(value) {
					return
				}
			}
		}, true
	}
	items, ok := e.evaluateListForPipelineWithContext(ctx, expression, row)
	if !ok {
		pipelineItemUnevaluable(ctx, expression)
		return nil, false
	}
	return func(yield func(interface{}) bool) {
		for _, value := range items {
			if !yield(value) {
				return
			}
		}
	}, true
}

func (e *StorageExecutor) validatePipelineWithRows(rows []pipelineRow, clause string) error {
	if err := e.validatePipelinePercentileArguments(rows, clause, "WITH"); err != nil {
		return err
	}
	return e.validatePipelineProjectionValues(rows, clause, "WITH")
}

func (e *StorageExecutor) validatePipelineProjectionValues(rows []pipelineRow, clause, keyword string) error {
	for _, validate := range []func([]pipelineRow, string, string) error{
		e.validatePipelineRangeArguments,
		e.validatePipelineConversionArguments, e.validatePipelineGraphFunctionArguments,
		e.validatePipelineProjectionSubscripts, e.validatePipelineSizeArguments,
	} {
		if err := validate(rows, clause, keyword); err != nil {
			return err
		}
	}
	return nil
}

func (e *StorageExecutor) pipelineApplyUnwindPrefix(ctx context.Context, rows []pipelineRow, clauses []pipelineClause) ([]pipelineRow, int, bool) {
	source, consumed, ok := e.pipelineUnwindSource(ctx, rows, clauses)
	if !ok {
		return nil, 0, false
	}
	out, ok := materializePipelineSource(source)
	return out, consumed, ok
}

func materializePipelineSource(source pipelineRowSource) ([]pipelineRow, bool) {
	out := make([]pipelineRow, 0)
	ok := source(func(row pipelineRow) bool {
		retained := make(pipelineRow, len(row))
		for name, value := range row {
			retained[name] = value
		}
		out = append(out, retained)
		return true
	})
	return out, ok
}

func (e *StorageExecutor) pipelineUnwindSource(ctx context.Context, rows []pipelineRow, clauses []pipelineClause) (pipelineRowSource, int, bool) {
	expression, alias, ok := splitUnwindBody(pipelineClauseBody(clauses[0].text, "UNWIND"))
	if !ok {
		return nil, 0, false
	}
	plans := make([]pipelineRowWith, 0)
	for _, clause := range clauses[1:] {
		if clause.kind != pipelineClauseWith {
			break
		}
		plan, local := parsePipelineRowWith(clause.text)
		if !local {
			break
		}
		if err := e.validatePipelineWithRows(rows, clause.text); err != nil {
			recordExpressionFailure(ctx, err)
			return nil, 0, false
		}
		plans = append(plans, plan)
	}
	projected := make([]pipelineRow, len(plans))
	scopes := make([]pipelineRow, len(plans))
	for index := range plans {
		projected[index], scopes[index] = pipelineRow{}, pipelineRow{}
	}
	return func(yield func(pipelineRow) bool) bool {
		for _, input := range rows {
			values, ok := e.pipelineUnwindValues(ctx, expression, input)
			if !ok {
				return false
			}
			child := make(pipelineRow, len(input)+1)
			for name, value := range input {
				child[name] = value
			}
			for value := range values {
				if err := ctx.Err(); err != nil {
					recordExpressionFailure(ctx, err)
					return false
				}
				child[alias] = value
				current, accepted := child, true
				for index, plan := range plans {
					if strings.ContainsAny(plan.clause, "([") {
						if err := e.validatePipelineWithRows([]pipelineRow{current}, plan.clause); err != nil {
							recordExpressionFailure(ctx, err)
							return false
						}
					}
					var resolved bool
					current, accepted, resolved = e.pipelineProjectWithRow(ctx, current, plan, projected[index], scopes[index])
					if !resolved {
						return false
					}
					if !accepted {
						break
					}
				}
				if !accepted {
					continue
				}
				if !yield(current) {
					return true
				}
			}
		}
		return true
	}, len(plans), true
}
