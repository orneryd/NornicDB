package cypher

import (
	"context"
	"iter"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type pipelineRowProjection struct {
	expression, alias string
	view              braceView
}
type pipelineRowWith struct {
	clause, where   string
	view, whereView braceView
	star            bool
	projections     []pipelineRowProjection

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

var pipelineRowWithPlans = newBoundedCache[*cyphertext.Source, pipelineRowWithParse](4096, func(source *cyphertext.Source) int { return boundedCacheStringShardIndex(source.Text) })

func parsePipelineRowWith(ctx context.Context, clause braceView) (pipelineRowWith, bool) {
	if parsed, cached := pipelineRowWithPlans.get(clause.source); cached {
		return parsed.plan, parsed.supported
	}
	plan, supported := compilePipelineRowWith(ctx, clause)
	pipelineRowWithPlans.put(clause.source, pipelineRowWithParse{plan: plan, supported: supported})
	return plan, supported
}

func compilePipelineRowWith(ctx context.Context, clause braceView) (pipelineRowWith, bool) {
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (WITH with WHERE with = 3, #894).
	bodyView := clause.trim()
	body := bodyView.text()
	if startsWithDistinct(bodyView.clauseBody("WITH")) {
		return pipelineRowWith{}, false
	}
	for _, modifier := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if topLevelKeywordIndex(bodyView, modifier) >= 0 {
			return pipelineRowWith{}, false
		}
	}
	plan := pipelineRowWith{clause: clause.text(), view: clause}
	if index := topLevelKeywordIndex(bodyView, "WHERE"); index >= 0 {
		plan.where = strings.TrimSpace(body[index+len("WHERE"):])
		plan.whereView = bodyView.window(index+len("WHERE"), len(body)).trim()
		bodyView = bodyView.window(0, index).trim()
	}
	bodyView = bodyView.clauseBody("WITH")
	items := bodyView.commaViews()
	// WITH *, items keeps every variable and adds the items (#883).
	plan.star = len(items) > 0 && items[0].text() == "*"
	if plan.star {
		items = items[1:]
	}
	for _, item := range items {
		if item.empty() || item.text() == "{}" {
			continue
		}
		expression, alias, valid := projectionItemView(ctx, item)
		if !valid || expression.empty() || pipelineExpressionContainsAggregate(expression) {
			return pipelineRowWith{}, false
		}
		plan.projections = append(plan.projections, pipelineRowProjection{expression: expression.text(), alias: alias, view: expression})
	}
	if len(plan.projections) == 0 && (!plan.star || len(items) > 0) {
		return pipelineRowWith{}, false
	}
	return plan, true
}

func pipelineUnwindUsesRange(clause braceView) bool {
	expression, _, ok := parsePipelineIteration(clause)
	var callFacts1 functionCallFacts
	call := inspectFunctionCall(strings.TrimSpace(expression), qualifiedFunctionCall, "", &callFacts1)
	function := callFacts1.name
	return ok && call && strings.EqualFold(function, "range")
}

func (e *StorageExecutor) pipelineUnwindValues(ctx context.Context, expression braceView, row pipelineRow) (iter.Seq[interface{}], bool) {
	expression = expression.trim()
	var callFacts2 functionCallFacts
	call := inspectFunctionCall(expression.text(), qualifiedFunctionCall, "", &callFacts2)
	function := callFacts2.name
	if call && strings.EqualFold(function, "range") {
		parts := expression.functionArguments().commaViews()
		values := make([]interface{}, len(parts))
		for index, part := range parts {
			value, resolved := e.evaluateRowExpressionWithContext(ctx, part, row)
			if !resolved {
				pipelineItemUnevaluable(ctx, part.text())
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
		pipelineItemUnevaluable(ctx, expression.text())
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

func (e *StorageExecutor) validatePipelineWithRows(ctx context.Context, rows []pipelineRow, clause braceView) error {
	if err := e.validatePipelinePercentileArguments(ctx, rows, clause, "WITH"); err != nil {
		return err
	}
	return e.validatePipelineProjectionValues(ctx, rows, clause, "WITH")
}

func (e *StorageExecutor) validatePipelineProjectionValues(ctx context.Context, rows []pipelineRow, clause braceView, keyword string) error {
	if err := e.validatePipelineRangeArguments(ctx, rows, clause, keyword); err != nil {
		return err
	}
	for _, expression := range projectionExpressions(ctx, clause, keyword) {
		for _, row := range rows {
			if err := e.validateRowSubscriptTypes(expression, row); err != nil {
				return err
			}
		}
	}
	return e.validatePipelineSizeArguments(ctx, rows, clause, keyword)
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
		value, evaluated := e.evaluateRowExpressionWithContext(ctx, projection.view, row)
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

func (e *StorageExecutor) pipelineNodeProductSource(ctx context.Context, rows []pipelineRow, clause braceView) (pipelineRowSource, bool, error) {
	if len(clause.clauseBody("MATCH").commaViews()) < 2 {
		return nil, false, nil
	}
	return e.pipelineNodeMatchSource(ctx, pipelineRowsSource(rows), clause, nil)
}

func (e *StorageExecutor) pipelineNodeMatchSource(ctx context.Context, inputSource pipelineRowSource, clause braceView, readTail []braceView) (pipelineRowSource, bool, error) {
	return e.pipelineNodeMatchSourceWithHint(ctx, inputSource, clause, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1, readTail: readTail})
}

type pipelineNodeMatchSourcePlan struct {
	view           braceView
	templates      []*pipelineNodeMatchTemplate
	supported      bool
	earlyLimitSafe bool
}

var pipelineNodeMatchSourcePlans = newBoundedCache[string, pipelineNodeMatchSourcePlan](4096, boundedCacheStringShardIndex)

func (e *StorageExecutor) compilePipelineNodeMatchSource(clause braceView) pipelineNodeMatchSourcePlan {
	plan := pipelineNodeMatchSourcePlan{view: clause, earlyLimitSafe: true}
	body := clause.clauseBody("MATCH")
	if topLevelKeywordIndex(body, "WHERE") >= 0 {
		return plan
	}
	patterns := body.commaViews()
	if len(patterns) == 0 {
		return plan
	}
	templates := make([]*pipelineNodeMatchTemplate, len(patterns))
	variables := make(map[string]struct{}, len(patterns))
	for index, pattern := range patterns {
		pattern = pattern.trim()
		if !strings.HasPrefix(pattern.text(), "(") || findMatchingParen(pattern.text(), 0) != len(pattern.text())-1 {
			return plan
		}
		template := e.pipelineNodeMatchTemplateFor(pattern.wrap("MATCH ", ""))
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
			for _, reference := range semanticExpressionReferences(property.view) {
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

func (e *StorageExecutor) pipelineNodeMatchSourceWithHint(ctx context.Context, inputSource pipelineRowSource, clause braceView, hint pipelineMatchPhysicalHint) (pipelineRowSource, bool, error) {
	ctx = withBraceView(ctx, clause)
	if source, supported, err := e.pipelineTraversalSource(ctx, inputSource, clause, hint); supported || err != nil {
		return source, supported, err
	}
	plan, cached := pipelineNodeMatchSourcePlans.get(clause.text())
	if !cached {
		plan = e.compilePipelineNodeMatchSource(clause)
		pipelineNodeMatchSourcePlans.put(clause.text(), plan)
	} else if plan.supported {
		// Author: #1663 lane. Replay cached copy recipes onto this invocation,
		// including each generated MATCH prefix; never retain another input's facts.
		templates := make([]*pipelineNodeMatchTemplate, len(plan.templates))
		for index, template := range plan.templates {
			copy := *template
			copy.view = braceView{syntax: clause.syntax, source: template.view.source.Rebase(plan.view.source, clause.source)}
			copy.properties = rebasePropertyExpressions(template.properties, plan.view, clause)
			templates[index] = &copy
		}
		plan.view, plan.templates = clause, templates
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
					if !typed || node == nil || !e.pipelineNodeMatchesPattern(node, pattern) {
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
					var prefetchedBatch map[string]*storage.Node
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
							if node, ok := prefetchedBatch[propEqKeyBatch(normalizePropValue(value))]; ok {
								nodes = []*storage.Node{node}
							} else {
								nodes = nil
							}
						} else {
							if !candidatesBound && len(input) > 0 {
								candidateCtx = withValueBindings(ctx, input)
								candidatesBound = true
							}
							var err error
							nodes, _, err = e.collectPipelineInitialNodeCandidates(candidateCtx, pattern, braceView{}, hint)
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
					if !e.pipelineNodeMatchesPattern(node, pattern) {
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
				if !e.pipelineNodeMatchesPattern(node, pattern) {
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
				if err := e.validatePipelineWithRows(ctx, []pipelineRow{row}, plan.view); err != nil {
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
					value, found = e.evaluateRowExpressionWithContext(ctx, projection.view, row)
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
					accepted := e.evaluateWithWhereCondition(ctx, plan.whereView, scope)
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

func (e *StorageExecutor) pipelineWithWindowSource(ctx context.Context, input pipelineRowSource, rows []pipelineRow, view braceView) (pipelineRowSource, bool) {
	clause := view.text()
	end := len(clause)
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(view, keyword); index >= 0 && index < end {
			end = index
		}
	}
	if end == len(clause) || topLevelKeywordIndex(view, "WHERE") >= 0 {
		return nil, false
	}
	plan, ok := parsePipelineRowWith(ctx, view.window(0, end))
	if !ok {
		return nil, false
	}
	skip, limit := 0, -1
	if topLevelKeywordIndex(view, "SKIP") >= 0 {
		skip, ok = e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(view, "SKIP"), rows)
		if !ok {
			return nil, false
		}
	}
	if topLevelKeywordIndex(view, "LIMIT") >= 0 {
		limit, ok = e.evaluatePipelinePagination(ctx, pipelinePaginationExpression(view, "LIMIT"), rows)
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
	expression, alias, ok := parsePipelineIterationSource(clauses[0].input(ctx))
	if !ok {
		return nil, 0, false
	}
	plans := make([]pipelineRowWith, 0)
	for _, clause := range clauses[1:] {
		if clause.kind != pipelineClauseWith {
			break
		}
		plan, local := parsePipelineRowWith(ctx, clause.input(ctx))
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

// projectionExpressionCache retains each expression's original Source identity.
type projectionExpressionKey struct {
	source  *cyphertext.Source
	keyword string
}

var projectionExpressionCache = newBoundedCache[projectionExpressionKey, []braceView](4096, func(key projectionExpressionKey) int { return boundedCacheStringShardIndex(key.source.Text) })

func projectionExpressions(ctx context.Context, clause braceView, keyword string) []braceView {
	key := projectionExpressionKey{clause.source, keyword}
	if cached, ok := projectionExpressionCache.get(key); ok {
		return cached
	}
	var expressions []braceView
	if keyword == "UNWIND" {
		if expression, _, ok := parsePipelineIterationSource(clause); ok {
			expressions = []braceView{expression}
		}
	} else {
		body, _ := projectionSemanticSourceAndTail(clause, keyword)
		for _, item := range body.commaViews() {
			if item.text() == "*" {
				continue
			}
			expression, _, ok := projectionItemView(ctx, item)
			if ok {
				expressions = append(expressions, expression)
			}
		}
	}
	projectionExpressionCache.put(key, expressions)
	return expressions
}
