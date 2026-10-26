package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"sync"
)

// returnProjection is one RETURN item: its expression, its column name, and
// for an aggregating item the aggregate (aggregateName empty when the
// aggregate is nested in a larger expression, aggregateExpr then being the
// whole expression).
type returnProjection struct {
	view, aggregateView            braceView
	expr                           string
	variable                       string
	direct                         bool
	alias                          string
	isAggr                         bool
	aggregateName                  string
	aggregateExpr                  string
	distinct                       bool
	definitionStart, definitionEnd int
}

// returnProjectionPlan is a RETURN clause parsed for pipelineApplyReturn:
// its items, columns, DISTINCT and trailing ORDER BY / SKIP / LIMIT. A plan
// depends only on the clause text, so it is parsed once per text
// (returnProjectionPlanFor). valid is false when the clause has no items.
type returnProjectionPlan struct {
	view  braceView
	valid bool
	star  bool
	// starItems are the items after a leading * (RETURN *, x AS y, #883);
	// the * stands for every variable in scope, in name order.
	starItems       []string
	starProjections []returnProjection
	distinct        bool
	modifiers       string
	modifiersView   braceView
	projections     []returnProjection
	columns         []string
	hasAggregate    bool
}

// returnProjectionPlans caches parsed RETURN clauses by text. Plans are
// immutable once cached; the cache is cleared when it reaches
// returnProjectionPlanLimit entries, which bounds it for workloads with
// unbounded distinct query texts.
var returnProjectionPlans = struct {
	sync.RWMutex
	plans map[*cyphertext.Source]*returnProjectionPlan
}{plans: make(map[*cyphertext.Source]*returnProjectionPlan)}

const returnProjectionPlanLimit = 4096

func returnProjectionPlanFor(ctx context.Context, clause braceView, retained ...pipelineClause) *returnProjectionPlan {
	if len(retained) != 0 {
		origin := retained[0]
		program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
		if !ok || program == nil || !origin.syntax.Valid() || program.syntax.Source() != origin.syntax.Source() {
			return nil
		}
		plan := program.projections[origin.syntax.ID()]
		if plan == nil || !plan.valid {
			return nil
		}
		return plan.inView(origin.grammarView)
	}
	if ctx.Err() != nil {
		return &returnProjectionPlan{}
	}
	returnProjectionPlans.RLock()
	plan, cached := returnProjectionPlans.plans[clause.source]
	returnProjectionPlans.RUnlock()
	if cached {
		return plan
	}
	if plan = tryFlatReturnProjection(ctx, clause); plan == nil {
		if node, ok := canonicalQueryClause(ctx, clause); ok {
			plan = projectionPlanFromSyntax(clause, node)
		} else {
			plan = &returnProjectionPlan{}
		}
	}
	if ctx.Err() != nil {
		return &returnProjectionPlan{}
	}
	returnProjectionPlans.Lock()
	if ctx.Err() != nil {
		returnProjectionPlans.Unlock()
		return &returnProjectionPlan{}
	}
	if len(returnProjectionPlans.plans) >= returnProjectionPlanLimit {
		returnProjectionPlans.plans = make(map[*cyphertext.Source]*returnProjectionPlan)
	}
	returnProjectionPlans.plans[clause.source] = plan
	returnProjectionPlans.Unlock()
	return plan
}

// tryFlatReturnProjection keeps ordinary scalar RETURN clauses on the short
// scanner path. Nested expressions, quoted identifiers, strings, wildcards,
// modifiers, and UNION clauses use the canonical syntax owner above.
func tryFlatReturnProjection(ctx context.Context, clauseView braceView) *returnProjectionPlan {
	clause := strings.TrimSpace(clauseView.text())
	const keyword = "RETURN"
	if len(clause) <= len(keyword) || !strings.EqualFold(clause[:len(keyword)], keyword) || cyphertext.NamePartAt(clause, len(keyword)) {
		return nil
	}
	rawBodyView := clauseView.trim().window(len(keyword), len(clause))
	body := rawBodyView.text()
	if strings.ContainsAny(body, "\x60()[]{}\r\n*'\"") || firstTopLevelModifierIndex(rawBodyView) >= 0 || topLevelKeywordIndex(rawBodyView, "UNION") >= 0 {
		return nil
	}
	plan := &returnProjectionPlan{view: clauseView}
	bodyView, distinct := cutDistinct(clauseView.clauseBody("RETURN"))
	plan.distinct = distinct
	for _, rawItem := range bodyView.commaViews() {
		item := readProjectionItemSource(ctx, rawItem, projectionAdmitted)
		if !item.valid {
			return nil
		}
		plan.addProjection(item.view, item.alias)
	}
	plan.valid = len(plan.projections) > 0
	return plan
}

// withStarExpanded is the plan of RETURN *, items with the * written out
// (starProjectionItems, #883).
func (plan *returnProjectionPlan) withStarExpanded(columns []string) *returnProjectionPlan {
	expanded := &returnProjectionPlan{view: plan.view, valid: plan.valid, distinct: plan.distinct, modifiers: plan.modifiers, modifiersView: plan.modifiersView}
	named := make(map[string]bool, len(plan.starProjections))
	for _, projection := range plan.starProjections {
		named[projection.alias] = true
	}
	for _, column := range columns {
		if named[column] {
			continue
		}
		expanded.addProjection(plan.view.generated(cyphertext.PrintIdentifier(column, cyphertext.IsIdentifier, cyphertext.AcceptedSource)), column)
		last := &expanded.projections[len(expanded.projections)-1]
		last.variable = column
		last.direct = true
		last.definitionStart, last.definitionEnd = -1, -1
	}
	for _, projection := range plan.starProjections {
		expanded.projections = append(expanded.projections, projection)
		expanded.columns = append(expanded.columns, projection.alias)
		expanded.hasAggregate = expanded.hasAggregate || projection.isAggr
	}
	return expanded
}

// starProjectionItems writes out the * of `WITH *, items` or
// `RETURN *, items` (#883), as Neo4j orders the columns: each of columns, in
// the given (name) order, as a variable item, except a column one of items
// names (`RETURN *, a + 1 AS a` has the one column a), then items.
func starProjectionItems(ctx context.Context, source braceView, columns []string, items []braceView) []braceView {
	named := make(map[string]bool, len(items))
	for _, item := range items {
		alias := readProjectionItemSource(ctx, item, projectionAdmitted).alias
		named[alias] = true
	}
	out := make([]braceView, 0, len(columns)+len(items))
	for _, column := range columns {
		if !named[column] {
			variable := cyphertext.PrintIdentifier(column, cyphertext.IsIdentifier, cyphertext.AcceptedSource)
			out = append(out, source.generated(variable+" AS "+variable))
		}
	}
	return append(out, items...)
}

func (plan *returnProjectionPlan) inView(view braceView) *returnProjectionPlan {
	if plan.view.source == view.source {
		return plan
	}
	clone := *plan
	project := func(input braceView) braceView {
		if input.source == nil {
			return input
		}
		return braceView{syntax: view.syntax, source: input.source.Rebase(plan.view.source, view.source)}
	}
	clone.view = view
	clone.modifiersView = project(plan.modifiersView)
	clone.projections = append([]returnProjection(nil), plan.projections...)
	clone.starProjections = append([]returnProjection(nil), plan.starProjections...)
	for _, items := range [][]returnProjection{clone.projections, clone.starProjections} {
		for index := range items {
			items[index].view = project(items[index].view)
			items[index].aggregateView = project(items[index].aggregateView)
		}
	}
	return &clone
}

func (plan *returnProjectionPlan) addProjection(view braceView, alias string) {
	expr := view.text()
	aggregateName, aggregateView, distinct, isAggr := parsePipelineAggregate(view)
	aggregateExpr := ""
	if isAggr {
		aggregateExpr = aggregateView.text()
	}
	if !isAggr && pipelineExpressionContainsAggregate(view) {
		isAggr = true
		aggregateView = view
		aggregateExpr = expr
	}
	plan.hasAggregate = plan.hasAggregate || isAggr
	plan.projections = append(plan.projections, returnProjection{view: view, aggregateView: aggregateView, expr: expr, alias: alias, isAggr: isAggr, aggregateName: aggregateName, aggregateExpr: aggregateExpr, distinct: distinct})
	plan.columns = append(plan.columns, alias)
}

// pipelineReturnSourceColumns is a legacy semantic/source-spelling adapter of
// execution's projection plan, not another RETURN parser. The semantic walk
// still uses nil for bare star and a star marker for mixed projections; the
// execution header path expands wildcards through the complete statement owner.
func pipelineReturnSourceColumns(ctx context.Context, clause braceView) []string {
	plan := returnProjectionPlanFor(ctx, clause)
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

// projectionItemView is a transport adapter for the canonical projection item product.
func projectionItemView(ctx context.Context, item braceView) (braceView, string, bool) {
	product := readProjectionItemSource(ctx, item, projectionAdmitted)
	return product.view, product.alias, product.valid
}

func parseProjectionExprAlias(ctx context.Context, item braceView) (string, string) {
	product := readProjectionItemSource(ctx, item, projectionAdmitted)
	return product.expression, product.alias
}

func projectionAliasIndex(ctx context.Context, item braceView) int {
	return readProjectionItemSource(ctx, item, projectionAdmitted).as
}

// buildColumnsFromReturnItems builds column names from parsed return items
func (e *StorageExecutor) buildColumnsFromReturnItems(items []returnItem) []string {
	columns := make([]string, len(items))
	for i, item := range items {
		columns[i] = item.column()
	}
	return columns
}

// parseYieldClause is execution's compatibility adapter of the canonical node.
func parseYieldClause(ctx context.Context, source braceView) *yieldClause {
	clause, ok := canonicalQueryClause(ctx, source)
	if !ok {
		return nil
	}
	return yieldFromSyntax(source, clause)
}

// callTailWithProjectionColumns consumes execution's WITH projection plan.
func callTailWithProjectionColumns(ctx context.Context, withClause braceView) ([]string, bool) {
	projection, _ := projectionSemanticSourceAndTail(withClause, "WITH")
	plan := returnProjectionPlanFor(ctx, projection.wrap("RETURN ", ""))
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

func pipelineProjectionScope(ctx context.Context, previous map[string]struct{}, clause braceView, imports pipelineRow) map[string]struct{} {
	projection, _ := projectionSemanticSourceAndTail(clause, "WITH")
	plan := returnProjectionPlanFor(ctx, projection.wrap("RETURN ", ""))
	next := projectionPlanScope(plan, previous)
	if next == nil {
		return nil
	}
	for name := range imports {
		next[name] = struct{}{}
	}
	return next
}

// returnShapeItems returns the trimmed projection items of query's RETURN clause
// with the trailing ORDER BY / SKIP / LIMIT clauses removed. It is the single
// owner of that RETURN-location, trailing-clause trimming and comma-splitting
// step for the narrow optimized return shapes below; each shape keeps its own
// arity and expression grammar on top of the returned items.
//
// query is the canonical statement text supplied to pattern detection. The RETURN
// offset is kept on its own so only the clause body below it is trimmed.
func returnShapeItems(view braceView) []string {
	query := view.text()
	returnAt := findKeywordIndex(view, "RETURN")
	if returnAt < 0 {
		return nil
	}
	returnView := view.window(returnAt+len("RETURN"), len(query)).trim()
	returnPart := returnView.text()

	// Strip ORDER BY / SKIP / LIMIT.
	end := len(returnPart)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnView, keyword); idx >= 0 && idx < end {
			end = idx
		}
	}
	returnView = returnView.window(0, end).trim()
	returnPart = returnView.text()
	if returnPart == "" {
		return nil
	}

	var items []string
	for _, item := range returnView.commaViews() {
		items = append(items, item.text())
	}
	return items
}

// trimReturnItemAlias strips the established space-delimited "AS" alias from one RETURN item. The
// surrounding spaces are part of the match, as before canonicalization.
func trimReturnItemAlias(item string) string {
	item = strings.TrimSpace(item)
	if asIdx := strings.Index(upperASCII(item), " AS "); asIdx > 0 {
		item = strings.TrimSpace(item[:asIdx])
	}
	return item
}

// restoreResultColumns preserves final RETURN spelling after quoted variables
// have executed with internal identities. Parsed aliases are already decoded.
func (names *quotedVariableNames) restoreResultColumns(result *ExecuteResult, parseItems func(braceView) []returnItem) {
	if start, end, ok := finalReturnBody(names.canonicalView); ok {
		canonicalView := names.canonicalView.window(start, end)
		bodyView, _ := cutDistinct(canonicalView.trim())
		body := bodyView.text()
		if strings.HasPrefix(body, "*") && (len(body) == 1 || !cyphertext.NamePartAt(body, 1)) && topLevelKeywordIndex(bodyView, "UNION") < 0 {
			sortColumnsByName(result)
		} else {
			originalSpan, mapped := names.OriginalSpan(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExactBoundaries)
			if !mapped {
				panic("cypher: RETURN presentation lost its original extent")
			}
			items := parseItems(names.originalView.spanWindow(originalSpan))
			canonicalItems := parseItems(canonicalView)
			if len(items) == len(result.Columns) && len(canonicalItems) == len(items) {
				for index, item := range items {
					if item.alias != strings.TrimSpace(item.expr) {
						result.Columns[index] = item.alias
					} else if name, isName := isOneSymbolicName(item.expr); isName {
						result.Columns[index] = name
					} else {
						result.Columns[index] = strings.TrimSpace(item.expr)
					}
				}
			}
		}
	}
}
