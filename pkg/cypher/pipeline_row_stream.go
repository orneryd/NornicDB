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

	// Bound on the plan copy for one invocation, never in the plan cache.
	imports pipelineRow
	// Bound only on the invocation copy: eager consumers retain rows/scopes;
	// streaming consumers borrow one map until their yield returns.
	retainRows     bool
	skipValidation bool
	observeScope   func(projected, scope pipelineRow)
}

// withImports binds one invocation's CALL imports to a copy of plan; the
// cached plan itself never holds imports.
func (plan pipelineRowWith) withImports(imports pipelineRow) pipelineRowWith {
	plan.imports = imports
	return plan
}

type pipelineRowWithParse struct {
	plan      pipelineRowWith
	supported bool
}

var pipelineRowWithPlans = newBoundedCache[string, pipelineRowWithParse](4096, boundedCacheStringShardIndex)

func parsePipelineRowWith(ctx context.Context, clause pipelineClause) (pipelineRowWith, bool) {
	if parsed, cached := pipelineRowWithPlans.get(clause.text); cached {
		return parsed.plan, parsed.supported
	}
	plan, supported := compilePipelineRowWith(ctx, clause, false)
	pipelineRowWithPlans.put(clause.text, pipelineRowWithParse{plan: plan, supported: supported})
	return plan, supported
}

// compilePipelineRowWith plans a WITH clause's row projection from the items
// its retained syntax owns; item text is read for identity, never reparsed as
// a RETURN clause. withoutWindow relaxes only the SKIP / LIMIT decline, for a
// caller that applies the clause's window itself.
func compilePipelineRowWith(ctx context.Context, clause pipelineClause, withoutWindow bool) (pipelineRowWith, bool) {
	// The clause text is scanned with its keyword, which tells a keyword-named
	// first item from a clause (WITH with WHERE with = 3, #894).
	body := strings.TrimSpace(clause.text)
	if startsWithDistinct(pipelineClauseBody(body, "WITH")) {
		return pipelineRowWith{}, false
	}
	for _, modifier := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if withoutWindow && modifier != "ORDER BY" {
			continue
		}
		if topLevelKeywordIndex(ctx, body, modifier) >= 0 {
			return pipelineRowWith{}, false
		}
	}
	plan := pipelineRowWith{clause: clause.text}
	if index := topLevelKeywordIndex(ctx, body, "WHERE"); index >= 0 {
		plan.where = strings.TrimSpace(body[index+len("WHERE"):])
	}
	items := projectionItemsFromSyntax(clause.syntax)
	// WITH *, items keeps every variable and adds the items (#883).
	plan.star = len(items) > 0 && items[0].star
	if plan.star {
		items = items[1:]
	}
	for _, item := range items {
		raw := ""
		if item.syntax.Valid() {
			raw = strings.TrimSpace(item.syntax.Text())
		}
		if raw == "" || raw == "{}" {
			continue
		}
		if item.expression == "" || pipelineExpressionContainsAggregate(item.expression) {
			return pipelineRowWith{}, false
		}
		plan.projections = append(plan.projections, pipelineRowProjection{item.expression, item.alias})
	}
	if len(plan.projections) == 0 && (!plan.star || len(items) > 0) {
		return pipelineRowWith{}, false
	}
	return plan, true
}

// pipelineUnwindUsesRange reports a statement whose leading UNWIND lists
// range(...). It reads raw statement text, before the statement is split into
// clauses: the native scanner admits the clause and its expression.
func pipelineUnwindUsesRange(ctx context.Context, clause string) bool {
	product, found := splitUnwindBody(ctx, clause)
	if !found {
		return false
	}
	var callFacts1 functionCallFacts
	call := inspectFunctionCall(strings.TrimSpace(product.expression), qualifiedFunctionCall, "", &callFacts1)
	function := callFacts1.name
	return call && strings.EqualFold(function, "range")
}

func (e *StorageExecutor) pipelineUnwindValues(ctx context.Context, expression string, row pipelineRow) (iter.Seq[interface{}], bool) {
	var callFacts2 functionCallFacts
	call := inspectFunctionCall(strings.TrimSpace(expression), qualifiedFunctionCall, "", &callFacts2)
	function, arguments := callFacts2.name, callFacts2.inner
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

func (e *StorageExecutor) validatePipelineWithRows(ctx context.Context, rows []pipelineRow, clause pipelineClause) error {
	return e.validatePipelineWithExpressions(ctx, rows, projectionExpressions(ctx, clause, "WITH"))
}

// validatePipelineWithExpressions runs the WITH argument checks over the
// clause's own projection expressions. The row operator passes its planned
// items so an executing clause rereads no clause text per row.
func (e *StorageExecutor) validatePipelineWithExpressions(ctx context.Context, rows []pipelineRow, expressions []string) error {
	if err := e.validatePipelinePercentileExpressions(ctx, rows, expressions); err != nil {
		return err
	}
	return e.validatePipelineProjectionExpressions(ctx, rows, expressions)
}

func (e *StorageExecutor) validatePipelineProjectionValues(ctx context.Context, rows []pipelineRow, clause pipelineClause, keyword string) error {
	return e.validatePipelineProjectionExpressions(ctx, rows, projectionExpressions(ctx, clause, keyword))
}

func (e *StorageExecutor) validatePipelineProjectionExpressions(ctx context.Context, rows []pipelineRow, expressions []string) error {
	if err := e.validatePipelineRangeExpressions(ctx, rows, expressions); err != nil {
		return err
	}
	for _, expression := range expressions {
		for _, row := range rows {
			if err := e.validateRowSubscriptTypes(ctx, expression, row); err != nil {
				return err
			}
		}
	}
	return e.validatePipelineSizeExpressions(ctx, rows, expressions)
}

func (e *StorageExecutor) pipelineApplyUnwindPrefix(ctx context.Context, rows []pipelineRow, clauses []pipelineClause) ([]pipelineRow, int, bool) {
	source, consumed, ok := e.pipelineUnwindSource(ctx, rows, clauses)
	if !ok {
		return nil, 0, false
	}
	out, ok := materializePipelineSource(ctx, source)
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

// materializePipelineSource collects the rows of source. Each kept row
// counts against the statement's memory limit; going over stops the
// collection, and the statement fails with the limit's error (#1343).
func materializePipelineSource(ctx context.Context, source pipelineRowSource) ([]pipelineRow, bool) {
	// One variable for what the callback updates: the rows, and the memory
	// they take (sampled sizes from one account, #1343).
	collected := struct {
		out      []pipelineRow
		memory   memoryAccount
		estimate rowEstimator
	}{out: make([]pipelineRow, 0)}
	collected.memory.scope = memoryScopeFromContext(ctx)
	collected.estimate.known = collected.memory.scope.knownLists()
	ok := source(func(row pipelineRow) bool {
		retained := make(pipelineRow, len(row))
		if collected.memory.scope == nil {
			for name, value := range row {
				retained[name] = value
			}
		} else {
			// The copy's walk sizes the row too.
			size := int64(pipelineRowExtraBytes + mapHeaderBytes)
			for name, value := range row {
				retained[name] = value
				size += collected.estimate.entry(name, value)
			}
			if collected.memory.grow(size) != nil {
				return false
			}
		}
		collected.out = append(collected.out, retained)
		return true
	})
	collected.memory.close()
	if collected.memory.scope != nil && collected.memory.scope.tracker.err() != nil {
		return nil, false
	}
	return collected.out, ok
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

var pipelineNodeMatchSourcePlans = newBoundedCache[string, pipelineNodeMatchSourcePlan](4096, boundedCacheStringShardIndex)

func (e *StorageExecutor) compilePipelineNodeMatchSource(ctx context.Context, clause string) pipelineNodeMatchSourcePlan {
	plan := pipelineNodeMatchSourcePlan{earlyLimitSafe: true}
	body := pipelineClauseBody(clause, "MATCH")
	if topLevelKeywordIndex(ctx, body, "WHERE") >= 0 {
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
		template := e.pipelineNodeMatchTemplateFor(ctx, "MATCH "+pattern)
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
			if !forEachSemanticExpressionReference(property.expr, func(variable, _ string, _ bool, _ int) bool {
				_, dependent := variables[variable]
				return !dependent
			}) {
				return plan
			}
		}
	}
	plan.templates, plan.supported = templates, true
	return plan
}

func (e *StorageExecutor) pipelineNodeMatchSourceWithHint(ctx context.Context, inputSource pipelineRowSource, clause string, hint pipelineMatchPhysicalHint) (pipelineRowSource, bool, error) {
	if source, supported, err := e.pipelineTraversalSource(ctx, inputSource, clause, hint); supported || err != nil {
		return source, supported, err
	}
	plan, cached := pipelineNodeMatchSourcePlans.get(clause)
	if !cached {
		plan = e.compilePipelineNodeMatchSource(ctx, clause)
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
	prefetched, _ := ctx.Value(pipelinePrefetchedNodeCandidatesKey{}).(nodeBatchCandidates)
	return func(yield func(pipelineRow) bool) bool {
		valid := true
		current := make(pipelineRow, len(templates))
		resolved := make([]nodePatternInfo, len(templates))
		candidates := make([]struct {
			nodes []*storage.Node
			ready bool
		}, len(templates))
		completed := inputSource(func(input pipelineRow) bool {
			clear(current)
			clear(candidates)
			for name, value := range input {
				current[name] = value
			}
			candidateCtx := ctx
			candidatesBound := false
			for index, template := range templates {
				pattern, ok, evalErr := template.node(ctx, e, input)
				if evalErr != nil {
					recordExpressionFailure(ctx, evalErr)
					valid = false
					return false
				}
				if !ok {
					pipelineItemUnevaluable(ctx, template.pattern)
					valid = false
					return false
				}
				resolved[index] = pattern
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
					if !typed || node == nil || !e.pipelineNodeMatchesPattern(ctx, node, pattern) {
						return true
					}
					return visit(index + 1)
				}
				// Only unbound visits discover candidates. Earlier patterns may
				// bind this variable or reject the row before any scan is needed.
				if !candidates[index].ready {
					template := templates[index]
					key, keyed := "", false
					prefetchedPattern := false
					var prefetchedBatch map[string][]*storage.Node
					if len(pattern.labels) == 1 && len(pattern.properties) == 1 {
						prefetchedBatch, prefetchedPattern = prefetched[nodeBatchMatchKey{label: pattern.labels[0], prop: template.properties[0].key}]
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
						if prefetchedPattern {
							// The batch prefetch already index-seeked every
							// distinct value of this UNWIND batch: the row's own
							// value hits it directly instead of an index seek
							// per row (see buildRelationshipBatchNodeMatchIndex).
							value, _ := pattern.properties[template.properties[0].key]
							// The list is the prefetch's own and is only read here.
							nodes = prefetchedBatch[propEqKeyBatch(normalizePropValue(value))]
						} else {
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
					}
					candidates[index].nodes = nodes
					candidates[index].ready = true
				}
				for _, node := range candidates[index].nodes {
					if !e.pipelineNodeMatchesPattern(ctx, node, pattern) {
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
			pattern, ok, evalErr := template.node(ctx, e, input)
			if evalErr != nil {
				recordExpressionFailure(ctx, evalErr)
				valid = false
				return false
			}
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
				if !e.pipelineNodeMatchesPattern(ctx, node, pattern) {
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
		plan := plan.withImports(pipelineImports(ctx))
		observeScope := plan.observeScope
		needsScope := plan.where != "" || observeScope != nil
		validateRows := !plan.skipValidation && strings.ContainsAny(plan.clause, "([")
		var expressions []string
		if validateRows {
			expressions = make([]string, 0, len(plan.projections))
			for _, projection := range plan.projections {
				expressions = append(expressions, projection.expression)
			}
		}
		var projected, scope pipelineRow
		if !plan.retainRows {
			projected = pipelineRow{}
			if needsScope {
				scope = pipelineRow{}
			}
		}
		valid := true
		completed := input(func(row pipelineRow) bool {
			// Borrow the run's buffers by value. Retained rows allocate their
			// own maps below; reassigning these callback-local variables must
			// not force two mutable map-handle cells onto the heap per run.
			projected, scope := projected, scope
			if err := ctx.Err(); err != nil {
				recordExpressionFailure(ctx, err)
				valid = false
				return false
			}
			if validateRows {
				if err := e.validatePipelineWithExpressions(ctx, []pipelineRow{row}, expressions); err != nil {
					recordExpressionFailure(ctx, err)
					valid = false
					return false
				}
			}
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
					valid = false
					return false
				}
				projected[projection.alias] = value
			}
			carryPipelineRowImports(projected, row, plan.imports)
			if needsScope {
				if scope == nil {
					if observeScope != nil {
						scope = make(pipelineRow, len(row)+len(plan.projections))
					} else {
						scope = pipelineRow{}
					}
				} else {
					clear(scope)
				}
				for name, value := range row {
					scope[name] = value
				}
				for name, value := range projected {
					scope[name] = value
				}
				if plan.where != "" {
					accepted := e.evaluateWithWhereCondition(ctx, plan.where, scope)
					if getExpressionFailure(ctx) != nil {
						valid = false
						return false
					}
					if !accepted {
						return true
					}
				}
				if observeScope != nil {
					observeScope(projected, scope)
				}
			}
			return yield(projected)
		})
		return completed && valid
	}
}

func (e *StorageExecutor) pipelineWithWindowSource(ctx context.Context, input pipelineRowSource, rows []pipelineRow, projection pipelineClause) (pipelineRowSource, bool) {
	clause := projection.text
	end := len(clause)
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(ctx, clause, keyword); index >= 0 && index < end {
			end = index
		}
	}
	if end == len(clause) || topLevelKeywordIndex(ctx, clause, "WHERE") >= 0 {
		return nil, false
	}
	plan, ok := compilePipelineRowWith(ctx, projection, true)
	if !ok {
		return nil, false
	}
	skip, limit := 0, -1
	if topLevelKeywordIndex(ctx, clause, "SKIP") >= 0 {
		skip, ok = e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(ctx, clause, "SKIP"), rows)
		if !ok {
			return nil, false
		}
	}
	if topLevelKeywordIndex(ctx, clause, "LIMIT") >= 0 {
		limit, ok = e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(ctx, clause, "LIMIT"), rows)
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
	iteration, ok := iterationClauseFor(clauses[0])
	if err := ctx.Err(); err != nil {
		recordExpressionFailure(ctx, err)
		return nil, 0, false
	}
	if !ok {
		return nil, 0, false
	}
	expression, alias := iteration.expression, iteration.variable
	plans := make([]pipelineRowWith, 0)
	for _, clause := range clauses[1:] {
		if clause.kind != pipelineClauseWith {
			break
		}
		plan, local := parsePipelineRowWith(ctx, clause)
		if !local {
			break
		}
		// The projection reads the rows produced by UNWIND and earlier WITHs.
		// pipelineWithRowSource validates it against those actual rows.
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
var projectionExpressionCache = newBoundedCache[string, []string](4096, boundedCacheStringShardIndex)

// projectionExpressions returns the projected expressions of a projection
// clause: the items of RETURN or WITH from the clause's retained syntax, after
// DISTINCT and before any WHERE, ORDER BY, SKIP or LIMIT ("*" projects
// nothing). UNWIND's list expression comes from the clause's retained node
// (iterationClauseFor), not from this projection reader. The row validators
// (validatePipelineProjectionValues, validatePipelinePercentileArguments)
// all read projections through it, so they agree on what a projection is.
// Results are cached by clause text: the streaming aggregation validates
// every row on its own, and parsing the clause per row and per validator
// doubled nested-aggregation time (#823). The returned slice is shared.
func projectionExpressions(ctx context.Context, clause pipelineClause, keyword string) []string {
	key := keyword + "\x00" + clause.text
	if cached, ok := projectionExpressionCache.get(key); ok {
		return cached
	}
	var expressions []string
parse:
	for {
		body := strings.TrimSpace(clause.text)
		if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
			break parse
		}
		body, _ = projectionSemanticBodyAndTail(ctx, body, keyword)
		if body == "" || body == "*" {
			break parse
		}
		// The clause's retained items are its expressions; a * entry adds none
		// of its own (the * of *, items stands for the variables in scope).
		items := projectionItemsFromSyntax(clause.syntax)
		if len(items) > 0 && items[0].star {
			items = items[1:]
		}
		expressions = make([]string, 0, len(items))
		for _, item := range items {
			expressions = append(expressions, item.expression)
		}
		break
	}
	projectionExpressionCache.put(key, expressions)
	return expressions
}
