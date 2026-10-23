package cypher

import (
	"context"
	"strings"
	"sync"
)

// returnProjection is one RETURN item: its expression, its column name, and
// for an aggregating item the aggregate (aggregateName empty when the
// aggregate is nested in a larger expression, aggregateExpr then being the
// whole expression).
type returnProjection struct {
	expr          string
	alias         string
	isAggr        bool
	aggregateName string
	aggregateExpr string
	distinct      bool
}

// returnProjectionPlan is a RETURN clause parsed for pipelineApplyReturn:
// its items, columns, DISTINCT and trailing ORDER BY / SKIP / LIMIT. A plan
// depends only on the clause text, so it is parsed once per text
// (returnProjectionPlanFor). valid is false when the clause has no items.
type returnProjectionPlan struct {
	valid bool
	star  bool
	// starItems are the items after a leading * (RETURN *, x AS y, #883);
	// the * stands for every variable in scope, in name order.
	starItems    []string
	distinct     bool
	modifiers    string
	projections  []returnProjection
	columns      []string
	hasAggregate bool
}

// returnProjectionPlans caches parsed RETURN clauses by text. Plans are
// immutable once cached; the cache is cleared when it reaches
// returnProjectionPlanLimit entries, which bounds it for workloads with
// unbounded distinct query texts.
var returnProjectionPlans = struct {
	sync.RWMutex
	plans map[string]*returnProjectionPlan
}{plans: make(map[string]*returnProjectionPlan)}

const returnProjectionPlanLimit = 4096

func returnProjectionPlanFor(clause string) *returnProjectionPlan {
	returnProjectionPlans.RLock()
	plan, cached := returnProjectionPlans.plans[clause]
	returnProjectionPlans.RUnlock()
	if cached {
		return plan
	}
	plan = parseReturnProjectionPlan(clause)
	returnProjectionPlans.Lock()
	if len(returnProjectionPlans.plans) >= returnProjectionPlanLimit {
		returnProjectionPlans.plans = make(map[string]*returnProjectionPlan)
	}
	returnProjectionPlans.plans[clause] = plan
	returnProjectionPlans.Unlock()
	return plan
}

func parseReturnProjectionPlan(clause string) *returnProjectionPlan {
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (RETURN by ORDER BY by, #894).
	body := lexicalExpressionText(clause)
	modifierStart := len(body)
	if cut := firstTopLevelModifierIndex(body); cut >= 0 {
		modifierStart = cut
	}
	plan := &returnProjectionPlan{modifiers: strings.TrimSpace(body[modifierStart:])}
	body, plan.distinct = cutDistinct(pipelineClauseBody(body[:modifierStart], "RETURN"))
	items := splitTopLevelComma(body)
	if len(items) > 0 && strings.TrimSpace(items[0]) == "*" {
		plan.valid, plan.star = true, true
		for _, item := range items[1:] {
			plan.starItems = append(plan.starItems, strings.TrimSpace(item))
		}
		return plan
	}
	for _, rawItem := range items {
		// Semantic validation rejects an empty item (RETURN 1,,2).
		// Same alias parsing as WITH (parseProjectionExprAlias, #547).
		expr, alias := parseProjectionExprAlias(strings.TrimSpace(rawItem))
		plan.addProjection(expr, alias)
	}
	plan.valid = len(plan.projections) > 0
	return plan
}

// withStarExpanded is the plan of RETURN *, items with the * written out
// (starProjectionItems, #883).
func (plan *returnProjectionPlan) withStarExpanded(columns []string) *returnProjectionPlan {
	expanded := &returnProjectionPlan{valid: true, distinct: plan.distinct, modifiers: plan.modifiers}
	for _, item := range starProjectionItems(columns, plan.starItems) {
		expanded.addProjection(parseProjectionExprAlias(item))
	}
	return expanded
}

// bindInputs checks the executable items against their input scope before the
// program publishes output names. The canonical expression checker owns local
// bindings and free-variable reads; this does not start the semantic/type walk.
// The caller expands wildcards first and supplies a known scope.
func (plan *returnProjectionPlan) bindInputs(input map[string]struct{}) error {
	bindings := make(matchSemanticScope, len(input))
	for name := range input {
		bindings[name] = matchBindingUnknown
	}
	for _, item := range plan.projections {
		if err := undefinedExpressionVariable(bindings, item.expr); err != nil {
			return err
		}
	}
	return nil
}

// starProjectionItems writes out the * of `WITH *, items` or
// `RETURN *, items` (#883), as Neo4j orders the columns: each of columns, in
// the given (name) order, as a variable item, except a column one of items
// names (`RETURN *, a + 1 AS a` has the one column a), then items.
func starProjectionItems(columns, items []string) []string {
	named := make(map[string]bool, len(items))
	for _, item := range items {
		_, alias := parseProjectionExprAlias(item)
		named[alias] = true
	}
	out := make([]string, 0, len(columns)+len(items))
	for _, column := range columns {
		if !named[column] {
			variable := projectionVariableText(column)
			out = append(out, variable+" AS "+variable)
		}
	}
	return append(out, items...)
}

// projectionVariableText is a variable as an expression: backtick-quoted when
// it isn't a plain identifier.
func projectionVariableText(name string) string {
	if isSimpleIdentifier(name) && !strings.Contains(name, "`") {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (plan *returnProjectionPlan) addProjection(expr, alias string) {
	aggregateName, aggregateExpr, distinct, isAggr := parsePipelineAggregate(expr)
	if !isAggr && pipelineExpressionContainsAggregate(expr) {
		isAggr = true
		aggregateExpr = expr
	}
	plan.hasAggregate = plan.hasAggregate || isAggr
	plan.projections = append(plan.projections, returnProjection{expr: expr, alias: alias, isAggr: isAggr, aggregateName: aggregateName, aggregateExpr: aggregateExpr, distinct: distinct})
	plan.columns = append(plan.columns, alias)
}

func returnProjectionPlanFromItems(items []returnItem) *returnProjectionPlan {
	plan := &returnProjectionPlan{}
	for _, item := range items {
		plan.addProjection(item.expr, item.column())
	}
	plan.valid = len(plan.projections) > 0
	return plan
}

// pipelineReturnSourceColumns is a legacy semantic/source-spelling adapter of
// execution's projection plan, not another RETURN parser. The semantic walk
// still uses nil for bare star and a star marker for mixed projections; the
// execution header path expands wildcards through the complete statement owner.
func pipelineReturnSourceColumns(clause string) []string {
	plan := returnProjectionPlanFor(clause)
	if !plan.valid {
		return nil
	}
	if plan.star {
		if len(plan.starItems) == 0 {
			return nil
		}
		return append([]string{"*"}, plan.withStarExpanded(nil).columns...)
	}
	return append([]string{}, plan.columns...)
}
func parseProjectionExprAlias(item string) (string, string) {
	trimmed := lexicalExpressionText(item)
	if trimmed == "" {
		return "", ""
	}
	asIdx := projectionAliasIndex(trimmed)
	if asIdx < 0 {
		// A projected variable is named by the variable itself: `x` is the
		// column x, as in Neo4j. Other expressions are named by their text.
		if name := simpleSemanticIdentifier(trimmed); name != "" {
			return trimmed, name
		}
		return trimmed, trimmed
	}
	expr := strings.TrimSpace(trimmed[:asIdx])
	alias := normalizeProjectionColumnName(trimmed[asIdx+len("AS"):])
	return expr, alias
}

// projectionAliasIndex is the index of the AS that introduces item's alias:
// the first AS outside strings, parentheses, brackets, braces and CASE … END
// blocks, with whitespace on both sides. An AS inside a nested expression
// (COLLECT { UNWIND l AS y RETURN y }, 'a AS b', CASE WHEN true THEN as END,
// where as is a variable; #1162) is not the alias; -1 when there is none.
func projectionAliasIndex(item string) int {
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	for from := 0; from < len(item); {
		index := keywordIndexFrom(item, "AS", from, opts)
		if index < 0 {
			return -1
		}
		end := index + len("AS")
		if index > 0 && isWhitespace(item[index-1]) && end < len(item) && isWhitespace(item[end]) && !insideCaseBlock(item, index) {
			return index
		}
		from = end
	}
	return -1
}

// buildColumnsFromReturnItems builds column names from parsed return items
func (e *StorageExecutor) buildColumnsFromReturnItems(items []returnItem) []string {
	columns := make([]string, len(items))
	for i, item := range items {
		columns[i] = item.column()
	}
	return columns
}

// parseYieldClause extracts YIELD information from a CALL statement.
// Handles: YIELD *, YIELD a, b, YIELD a AS x, b AS y, YIELD a WHERE a.score > 0.5,
// YIELD a ORDER BY a SKIP 1 LIMIT 2. Keywords are found at the top level, so
// a WHERE / ORDER inside a subquery, a list or a string, and the WITH of
// STARTS WITH, belong to the expression they are in.
func parseYieldClause(cypher string) *yieldClause {
	// Normalize whitespace: replace newlines/tabs with spaces for keyword detection
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")
	yieldIdx := findKeywordIndexInContext(normalized, "YIELD")
	if yieldIdx == -1 {
		return nil
	}
	result := &yieldClause{items: []yieldItem{}}

	afterYield := strings.TrimSpace(normalized[yieldIdx+len("YIELD"):])
	if len(afterYield) > 0 && afterYield[0] == '*' {
		result.yieldAll = true
		afterYield = strings.TrimSpace(afterYield[1:])
	}

	// Limit YIELD parsing to the CALL-clause scope only. Anything after the first
	// outer clause boundary (and a RETURN) belongs to the subsequent query.
	yieldScope := scopeYieldToCallClause(afterYield)
	if returnIdx := topLevelKeywordIndex(yieldScope, "RETURN"); returnIdx >= 0 {
		yieldScope = strings.TrimSpace(yieldScope[:returnIdx])
	}
	whereIdx := topLevelKeywordIndex(yieldScope, "WHERE")
	orderIdx := topLevelKeywordIndex(yieldScope, "ORDER")
	skipIdx := topLevelKeywordIndex(yieldScope, "SKIP")
	limitIdx := topLevelKeywordIndex(yieldScope, "LIMIT")
	positions := []int{whereIdx, orderIdx, skipIdx, limitIdx}

	// part returns the text of the keyword at start, up to the next keyword.
	part := func(start, keywordLen int) string {
		end := len(yieldScope)
		for _, idx := range positions {
			if idx > start && idx < end {
				end = idx
			}
		}
		return strings.TrimSpace(yieldScope[start+keywordLen : end])
	}
	if whereIdx >= 0 {
		result.where = part(whereIdx, len("WHERE"))
		for _, idx := range []int{orderIdx, skipIdx, limitIdx} {
			if idx >= 0 && idx < whereIdx {
				result.misplacedWhere = true
			}
		}
	}
	if orderIdx >= 0 {
		orderBy := part(orderIdx, len("ORDER"))
		if len(orderBy) >= len("BY") && strings.EqualFold(orderBy[:len("BY")], "BY") {
			orderBy = strings.TrimSpace(orderBy[len("BY"):])
		}
		result.orderBy = orderBy
	}
	if skipIdx >= 0 {
		result.skip = part(skipIdx, len("SKIP"))
	}
	if limitIdx >= 0 {
		result.limit = part(limitIdx, len("LIMIT"))
	}

	// Parse yield items (if not YIELD *)
	if !result.yieldAll {
		itemsEnd := len(yieldScope)
		for _, idx := range positions {
			if idx != -1 && idx < itemsEnd {
				itemsEnd = idx
			}
		}
		itemsStr := strings.TrimSpace(yieldScope[:itemsEnd])
		if itemsStr != "" {
			// Split by comma, respecting AS keyword
			for _, item := range strings.Split(itemsStr, ",") {
				item = strings.TrimSpace(item)
				if item == "" {
					continue
				}
				yi := yieldItem{}
				upperItem := upperASCII(item)
				if asIdx := strings.Index(upperItem, " AS "); asIdx != -1 {
					yi.name = strings.TrimSpace(item[:asIdx])
					yi.alias = strings.TrimSpace(item[asIdx+4:])
				} else {
					yi.name = item
				}
				result.items = append(result.items, yi)
			}
		}
	}

	return result
}

// callTailWithProjectionColumns consumes execution's WITH projection plan.
func callTailWithProjectionColumns(withClause string) ([]string, bool) {
	projection, _ := splitWithProjection(withClause)
	plan := returnProjectionPlanFor("RETURN " + pipelineClauseBody(projection, "WITH"))
	if !plan.valid || plan.star {
		return nil, false
	}
	for _, item := range plan.projections {
		if item.expr == "" {
			return nil, false
		}
	}
	return append([]string{}, plan.columns...), true
}

// projectionPlanScope is execution's WITH scope transfer. Header metadata and
// row execution consume this one cached projection plan; neither reparses aliases.
func projectionPlanScope(plan *returnProjectionPlan, previous map[string]struct{}) map[string]struct{} {
	if !plan.valid {
		return nil
	}
	if plan.star {
		if previous == nil {
			return nil
		}
		plan = plan.withStarExpanded(pipelineScopeColumns(previous))
	}
	next := make(map[string]struct{}, len(plan.columns))
	for _, name := range plan.columns {
		next[name] = struct{}{}
	}
	return next
}

func pipelineProjectionScope(_ context.Context, previous map[string]struct{}, clause string, imports pipelineRow) map[string]struct{} {
	projection, _ := splitWithProjection(clause)
	plan := returnProjectionPlanFor("RETURN " + pipelineClauseBody(projection, "WITH"))
	next := projectionPlanScope(plan, previous)
	if next == nil {
		return nil
	}
	for name := range imports {
		next[name] = struct{}{}
	}
	return next
}
