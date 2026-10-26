package cypher

import (
	"context"
	"strings"
	"sync"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// returnProjection is one RETURN item: its expression, its column name, and
// for an aggregating item the aggregate (aggregateName empty when the
// aggregate is nested in a larger expression, aggregateExpr then being the
// whole expression).
type returnProjection struct {
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
	valid bool
	star  bool
	// starItems are the items after a leading * (RETURN *, x AS y, #883);
	// the * stands for every variable in scope, in name order.
	starItems       []string
	starProjections []returnProjection
	distinct        bool
	modifiers       string
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
	plans map[string]*returnProjectionPlan
}{plans: make(map[string]*returnProjectionPlan)}

const returnProjectionPlanLimit = 4096

func returnProjectionPlanFor(ctx context.Context, clause string, retained ...pipelineClause) *returnProjectionPlan {
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
		return plan
	}
	if ctx.Err() != nil {
		return &returnProjectionPlan{}
	}
	returnProjectionPlans.RLock()
	plan, cached := returnProjectionPlans.plans[clause]
	returnProjectionPlans.RUnlock()
	if cached {
		return plan
	}
	plan = parseReturnProjectionPlan(ctx, clause)
	if ctx.Err() != nil {
		return &returnProjectionPlan{}
	}
	returnProjectionPlans.Lock()
	if ctx.Err() != nil {
		returnProjectionPlans.Unlock()
		return &returnProjectionPlan{}
	}
	if len(returnProjectionPlans.plans) >= returnProjectionPlanLimit {
		returnProjectionPlans.plans = make(map[string]*returnProjectionPlan)
	}
	returnProjectionPlans.plans[clause] = plan
	returnProjectionPlans.Unlock()
	return plan
}

func parseReturnProjectionPlan(ctx context.Context, clause string) *returnProjectionPlan {
	node, ok := canonicalQueryClause(ctx, clause)
	if !ok {
		return &returnProjectionPlan{}
	}
	return projectionPlanFromSyntax(node)
}

// withStarExpanded is the plan of RETURN *, items with the * written out
// (starProjectionItems, #883).
func (plan *returnProjectionPlan) withStarExpanded(columns []string) *returnProjectionPlan {
	expanded := &returnProjectionPlan{valid: plan.valid, distinct: plan.distinct, modifiers: plan.modifiers}
	named := make(map[string]bool, len(plan.starProjections))
	for _, projection := range plan.starProjections {
		named[projection.alias] = true
	}
	for _, column := range columns {
		if named[column] {
			continue
		}
		expanded.addProjection(projectionVariableText(column), column)
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
func starProjectionItems(ctx context.Context, columns, items []string) []string {
	named := make(map[string]bool, len(items))
	for _, item := range items {
		_, alias := parseProjectionExprAlias(ctx, item)
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

// pipelineReturnSourceColumns is a legacy semantic/source-spelling adapter of
// execution's projection plan, not another RETURN parser. The semantic walk
// still uses nil for bare star and a star marker for mixed projections; the
// execution header path expands wildcards through the complete statement owner.
func pipelineReturnSourceColumns(ctx context.Context, clause string) []string {
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
func parseProjectionExprAlias(ctx context.Context, item string) (string, string) {
	node, ok := canonicalQueryClause(ctx, "RETURN "+item)
	if !ok {
		return "", ""
	}
	plan := projectionPlanFromSyntax(node)
	if plan.valid && plan.star && len(plan.starItems) == 0 {
		return "*", "*"
	}
	if !plan.valid || plan.star || len(plan.projections) != 1 {
		return "", ""
	}
	projection := plan.projections[0]
	return projection.expr, projection.alias
}

// projectionAliasIndex is a source-spelling adapter of the retained AS token.
func projectionAliasIndex(ctx context.Context, item string) int {
	const prefix = "RETURN "
	node, ok := canonicalQueryClause(ctx, prefix+item)
	if !ok {
		return -1
	}
	items := node.ChildRule(cypherantlr.CypherParserRULE_projectionBody).ChildRule(cypherantlr.CypherParserRULE_projectionItems)
	if !items.Valid() {
		return -1
	}
	projection := items.ChildRule(cypherantlr.CypherParserRULE_projectionItem)
	if !projection.Valid() {
		return -1
	}
	token, ok := projection.DirectToken(cypherantlr.CypherLexerAS)
	if !ok {
		return -1
	}
	start, _ := token.Span()
	return start - len(prefix)
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
func parseYieldClause(ctx context.Context, source string) *yieldClause {
	clause, ok := canonicalQueryClause(ctx, source)
	if !ok {
		return nil
	}
	return yieldFromSyntax(clause)
}

// callTailWithProjectionColumns consumes execution's WITH projection plan.
func callTailWithProjectionColumns(ctx context.Context, withClause string) ([]string, bool) {
	projection, _ := splitWithProjection(withClause)
	plan := returnProjectionPlanFor(ctx, "RETURN "+pipelineClauseBody(projection, "WITH"))
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
