package cypher

import (
	"context"
	"iter"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type pipelineRowProjection struct{ expression, alias string }
type pipelineRowWith struct {
	clause, where string
	star          bool
	projections   []pipelineRowProjection
}

type pipelineRowWithParse struct {
	plan      pipelineRowWith
	supported bool
}

var pipelineRowWithPlans = newBoundedCache[string, pipelineRowWithParse](4096)

func parsePipelineRowWith(clause string) (pipelineRowWith, bool) {
	if parsed, cached := pipelineRowWithPlans.get(clause); cached {
		return parsed.plan, parsed.supported
	}
	plan, supported := compilePipelineRowWith(clause)
	pipelineRowWithPlans.put(clause, pipelineRowWithParse{plan: plan, supported: supported})
	return plan, supported
}

func compilePipelineRowWith(clause string) (pipelineRowWith, bool) {
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (WITH with WHERE with = 3, #894).
	body := strings.TrimSpace(clause)
	if startsWithDistinct(pipelineClauseBody(body, "WITH")) {
		return pipelineRowWith{}, false
	}
	for _, modifier := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if topLevelKeywordIndex(body, modifier) >= 0 {
			return pipelineRowWith{}, false
		}
	}
	plan := pipelineRowWith{clause: clause}
	if index := topLevelKeywordIndex(body, "WHERE"); index >= 0 {
		plan.where = strings.TrimSpace(body[index+len("WHERE"):])
		body = strings.TrimSpace(body[:index])
	}
	body = pipelineClauseBody(body, "WITH")
	items := splitTopLevelComma(body)
	// WITH *, items keeps every variable and adds the items (#883).
	plan.star = len(items) > 0 && strings.TrimSpace(items[0]) == "*"
	if plan.star {
		items = items[1:]
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" || item == "{}" {
			continue
		}
		expression, alias := parseProjectionExprAlias(item)
		if expression == "" || pipelineExpressionContainsAggregate(expression) {
			return pipelineRowWith{}, false
		}
		plan.projections = append(plan.projections, pipelineRowProjection{expression, alias})
	}
	if len(plan.projections) == 0 && (!plan.star || len(items) > 0) {
		return pipelineRowWith{}, false
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
	expression, _, ok := parsePipelineIteration(clause)
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

func (e *StorageExecutor) pipelineProjectReturnRow(ctx context.Context, projections []returnProjection, row pipelineRow) ([]interface{}, bool) {
	values := make([]interface{}, len(projections))
	for index, projection := range projections {
		value, evaluated := e.evaluateRowExpressionWithContext(ctx, projection.expr, row)
		if !evaluated {
			pipelineItemUnevaluable(ctx, projection.expr)
			return nil, false
		}
		values[index] = value
	}
	return values, true
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

func (e *StorageExecutor) pipelineNodeProductSource(ctx context.Context, rows []pipelineRow, clause string) (pipelineRowSource, bool, error) {
	if len(splitTopLevelComma(pipelineClauseBody(clause, "MATCH"))) < 2 {
		return nil, false, nil
	}
	return e.pipelineNodeMatchSource(ctx, pipelineRowsSource(rows), clause, nil)
}

func (e *StorageExecutor) pipelineNodeMatchSource(ctx context.Context, inputSource pipelineRowSource, clause string, readTail []string) (pipelineRowSource, bool, error) {
	return e.pipelineNodeMatchSourceWithHint(ctx, inputSource, clause, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1, readTail: readTail})
}

type pipelineNodeMatchSourcePlan struct {
	templates      []*pipelineNodeMatchTemplate
	supported      bool
	earlyLimitSafe bool
}

var pipelineNodeMatchSourcePlans = newBoundedCache[string, pipelineNodeMatchSourcePlan](4096)

func (e *StorageExecutor) compilePipelineNodeMatchSource(clause string) pipelineNodeMatchSourcePlan {
	plan := pipelineNodeMatchSourcePlan{earlyLimitSafe: true}
	body := pipelineClauseBody(clause, "MATCH")
	if topLevelKeywordIndex(body, "WHERE") >= 0 {
		return plan
	}
	patterns := splitTopLevelComma(body)
	if len(patterns) == 0 {
		return plan
	}
	templates := make([]*pipelineNodeMatchTemplate, len(patterns))
	variables := make(map[string]struct{}, len(patterns))
	for index, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if !strings.HasPrefix(pattern, "(") || findMatchingParen(pattern, 0) != len(pattern)-1 {
			return plan
		}
		template := e.pipelineNodeMatchTemplateFor("MATCH " + pattern)
		if !template.usable {
			return plan
		}
		if template.labelErr != nil {
			return plan
		}
		templates[index] = template
		if _, repeated := variables[template.variable]; repeated {
			plan.earlyLimitSafe = false
		}
		variables[template.variable] = struct{}{}
	}
	for _, template := range templates {
		for _, property := range template.properties {
			for _, reference := range semanticExpressionReferences(property.expr) {
				variable := strings.SplitN(reference, ".", 2)[0]
				if _, dependent := variables[variable]; dependent {
					return plan
				}
			}
		}
	}
	plan.templates, plan.supported = templates, true
	return plan
}

func (e *StorageExecutor) pipelineNodeMatchSourceWithHint(ctx context.Context, inputSource pipelineRowSource, clause string, hint pipelineMatchPhysicalHint) (pipelineRowSource, bool, error) {
	plan, cached := pipelineNodeMatchSourcePlans.get(clause)
	if !cached {
		plan = e.compilePipelineNodeMatchSource(clause)
		pipelineNodeMatchSourcePlans.put(clause, plan)
	}
	if !plan.supported {
		return nil, false, nil
	}
	if !plan.earlyLimitSafe {
		hint.limit, hint.earlyLimit = -1, -1
	}
	templates := plan.templates
	if hint.streamScan && len(templates) == 1 {
		return e.pipelineStreamedNodeMatchSource(ctx, inputSource, templates[0], hint), true, nil
	}
	type candidateCache struct {
		key          string
		nodes        []*storage.Node
		initialized  bool
		alternatives map[string][]*storage.Node
	}
	caches := make([]candidateCache, len(templates))
	prefetched, _ := ctx.Value(pipelinePrefetchedNodeCandidatesKey{}).(map[nodeBatchMatchKey]map[string]*storage.Node)
	return func(yield func(pipelineRow) bool) bool {
		valid := true
		current := make(pipelineRow, len(templates))
		resolved := make([]nodePatternInfo, len(templates))
		candidates := make([][]*storage.Node, len(templates))
		completed := inputSource(func(input pipelineRow) bool {
			clear(current)
			for name, value := range input {
				current[name] = value
			}
			candidateCtx := ctx
			candidatesBound := false
			for index, template := range templates {
				pattern, ok := template.node(ctx, e, input)
				if !ok {
					pipelineItemUnevaluable(ctx, template.pattern)
					valid = false
					return false
				}
				resolved[index] = pattern
				key, keyed := "", false
				prefetchedPattern := false
				if len(pattern.labels) == 1 && len(pattern.properties) == 1 {
					_, prefetchedPattern = prefetched[nodeBatchMatchKey{label: pattern.labels[0], prop: template.properties[0].key}]
				}
				if !prefetchedPattern {
					key, keyed = pipelinePropertiesKey(pattern.properties)
				}
				cache := &caches[index]
				nodes, cached := cache.nodes, keyed && cache.initialized && cache.key == key
				if keyed && !cached && cache.alternatives != nil {
					nodes, cached = cache.alternatives[key]
				}
				if !keyed || !cached {
					if !candidatesBound && len(input) > 0 {
						candidateCtx = withValueBindings(ctx, input)
						candidatesBound = true
					}
					var err error
					nodes, _, err = e.collectPipelineInitialNodeCandidates(candidateCtx, pattern, "", hint)
					if err != nil {
						recordExpressionFailure(ctx, err)
						valid = false
						return false
					}
					if keyed {
						if !cache.initialized {
							cache.key, cache.nodes, cache.initialized = key, nodes, true
						} else {
							if cache.alternatives == nil {
								cache.alternatives = make(map[string][]*storage.Node)
							}
							cache.alternatives[key] = nodes
						}
					}
				}
				candidates[index] = nodes
			}
			var visit func(int) bool
			visit = func(index int) bool {
				if err := ctx.Err(); err != nil {
					recordExpressionFailure(ctx, err)
					valid = false
					return false
				}
				if index == len(resolved) {
					return yield(current)
				}
				pattern := resolved[index]
				if value, bound := current[pattern.variable]; bound {
					node, typed := value.(*storage.Node)
					if !typed {
						if err := boundPatternNodeValueError(pattern.variable, value); err != nil {
							recordExpressionFailure(ctx, err)
							valid = false
							return false
						}
					}
					if !typed || node == nil || !pipelineNodeMatchesPattern(node, pattern) {
						return true
					}
					return visit(index + 1)
				}
				for _, node := range candidates[index] {
					if !pipelineNodeMatchesPattern(node, pattern) {
						continue
					}
					current[pattern.variable] = node
					accepted := visit(index + 1)
					delete(current, pattern.variable)
					if !accepted {
						return false
					}
				}
				return true
			}
			return visit(0)
		})
		return completed && valid
	}, true, nil
}

// pipelineStreamedNodeMatchSource is the source of a one-pattern node MATCH
// of a streamed statement (#939): it reads the pattern's candidates as the
// rows are consumed (visitPipelineInitialNodeCandidates) instead of
// collecting them first, with the same matching as
// pipelineNodeMatchSourceWithHint.
func (e *StorageExecutor) pipelineStreamedNodeMatchSource(ctx context.Context, inputSource pipelineRowSource, template *pipelineNodeMatchTemplate, hint pipelineMatchPhysicalHint) pipelineRowSource {
	return func(yield func(pipelineRow) bool) bool {
		valid := true
		current := make(pipelineRow)
		completed := inputSource(func(input pipelineRow) bool {
			clear(current)
			for name, value := range input {
				current[name] = value
			}
			pattern, ok := template.node(ctx, e, input)
			if !ok {
				pipelineItemUnevaluable(ctx, template.pattern)
				valid = false
				return false
			}
			// The MATCH is the statement's first clause and its input binds no
			// variable (hint.streamScan), so the pattern's variable is unbound.
			candidateCtx := ctx
			if len(input) > 0 {
				candidateCtx = withValueBindings(ctx, input)
			}
			accepted := true
			err := e.visitPipelineInitialNodeCandidates(candidateCtx, pattern, hint, func(node *storage.Node) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !pipelineNodeMatchesPattern(node, pattern) {
					return nil
				}
				current[pattern.variable] = node
				accepted = yield(current)
				delete(current, pattern.variable)
				if !accepted {
					return storage.ErrIterationStopped
				}
				return nil
			})
			if err != nil {
				recordExpressionFailure(ctx, err)
				valid = false
				return false
			}
			return accepted
		})
		return completed && valid
	}
}

func (e *StorageExecutor) pipelineWithRowSource(ctx context.Context, input pipelineRowSource, plan pipelineRowWith) pipelineRowSource {
	return func(yield func(pipelineRow) bool) bool {
		projected, scope := pipelineRow{}, pipelineRow{}
		valid := true
		completed := input(func(row pipelineRow) bool {
			if err := ctx.Err(); err != nil {
				recordExpressionFailure(ctx, err)
				valid = false
				return false
			}
			if strings.ContainsAny(plan.clause, "([") {
				if err := e.validatePipelineWithRows([]pipelineRow{row}, plan.clause); err != nil {
					recordExpressionFailure(ctx, err)
					valid = false
					return false
				}
			}
			current, accepted, resolved := e.pipelineProjectWithRow(ctx, row, plan, projected, scope)
			if !resolved {
				valid = false
				return false
			}
			return !accepted || yield(current)
		})
		return completed && valid
	}
}

func (e *StorageExecutor) pipelineWithWindowSource(ctx context.Context, input pipelineRowSource, rows []pipelineRow, clause string) (pipelineRowSource, bool) {
	end := len(clause)
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(clause, keyword); index >= 0 && index < end {
			end = index
		}
	}
	if end == len(clause) || topLevelKeywordIndex(clause, "WHERE") >= 0 {
		return nil, false
	}
	plan, ok := parsePipelineRowWith(clause[:end])
	if !ok {
		return nil, false
	}
	skip, limit := 0, -1
	if topLevelKeywordIndex(clause, "SKIP") >= 0 {
		skip, ok = e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(clause, "SKIP"), rows)
		if !ok {
			return nil, false
		}
	}
	if topLevelKeywordIndex(clause, "LIMIT") >= 0 {
		limit, ok = e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(clause, "LIMIT"), rows)
		if !ok {
			return nil, false
		}
	}
	projected := e.pipelineWithRowSource(ctx, input, plan)
	return func(yield func(pipelineRow) bool) bool {
		if limit == 0 {
			return true
		}
		seen, kept := 0, 0
		return projected(func(row pipelineRow) bool {
			if seen < skip {
				seen++
				return true
			}
			kept++
			return yield(row) && (limit < 0 || kept < limit)
		})
	}, true
}

func (e *StorageExecutor) pipelineUnwindSource(ctx context.Context, rows []pipelineRow, clauses []pipelineClause) (pipelineRowSource, int, bool) {
	expression, alias, ok := parsePipelineIteration(clauses[0].text)
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
	source := pipelineRowSource(func(yield func(pipelineRow) bool) bool {
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
				if !yield(child) {
					return true
				}
			}
		}
		return true
	})
	for _, plan := range plans {
		source = e.pipelineWithRowSource(ctx, source, plan)
	}
	return source, len(plans), true
}

// projectionExpressionCache holds projectionExpressions results by clause.
var projectionExpressionCache = newBoundedCache[string, []string](4096)

// projectionExpressions returns the projected expressions of a projection
// clause given as text with its keyword: the items of RETURN or WITH, after
// DISTINCT and before any WHERE, ORDER BY, SKIP or LIMIT ("*" projects
// nothing), or UNWIND's list expression. The row validators
// (validatePipelineProjectionValues, validatePipelinePercentileArguments)
// all read projections through it, so they agree on what a projection is.
// Results are cached by clause text: the streaming aggregation validates
// every row on its own, and parsing the clause per row and per validator
// doubled nested-aggregation time (#823). The returned slice is shared.
func projectionExpressions(clause, keyword string) []string {
	key := keyword + "\x00" + clause
	if cached, ok := projectionExpressionCache.get(key); ok {
		return cached
	}
	expressions := parseProjectionExpressions(clause, keyword)
	projectionExpressionCache.put(key, expressions)
	return expressions
}

func parseProjectionExpressions(clause, keyword string) []string {
	if strings.EqualFold(keyword, "UNWIND") {
		if expression, _, ok := parsePipelineIteration(clause); ok {
			return []string{expression}
		}
		return nil
	}
	body := strings.TrimSpace(clause)
	if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
		return nil
	}
	body, _ = projectionSemanticBodyAndTail(body, keyword)
	if body == "" || body == "*" {
		return nil
	}
	items := splitTopLevelComma(body)
	expressions := make([]string, 0, len(items))
	for _, item := range items {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		expressions = append(expressions, expression)
	}
	return expressions
}
