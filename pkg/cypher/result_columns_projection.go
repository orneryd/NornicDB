package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"strings"
	"sync"
)

// returnProjection is one RETURN item: its expression, its column name, and
// for an aggregating item the aggregate (aggregateName empty when the
// aggregate is nested in a larger expression, aggregateExpr then being the
// whole expression).
type returnProjection struct {
	// syntax is the expression retained by the canonical projection constructor.
	syntax cypherantlr.SyntaxNode
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
	if plan = tryFlatReturnProjection(ctx, clause); plan == nil {
		if node, ok := canonicalQueryClause(ctx, clause); ok {
			plan = projectionPlanFromSyntax(node)
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
		returnProjectionPlans.plans = make(map[string]*returnProjectionPlan)
	}
	returnProjectionPlans.plans[clause] = plan
	returnProjectionPlans.Unlock()
	return plan
}

// tryFlatReturnProjection keeps ordinary scalar RETURN clauses on the short
// scanner path. Nested expressions, quoted identifiers, strings, wildcards,
// modifiers, and UNION clauses use the canonical syntax owner above.
func tryFlatReturnProjection(ctx context.Context, clause string) *returnProjectionPlan {
	clause = strings.TrimSpace(clause)
	const keyword = "RETURN"
	if len(clause) <= len(keyword) || !strings.EqualFold(clause[:len(keyword)], keyword) || cyphertext.NamePartAt(clause, len(keyword)) {
		return nil
	}
	body := clause[len(keyword):]
	if strings.ContainsAny(body, "\x60()[]{}\r\n*'\"") || firstTopLevelModifierIndex(body) >= 0 || topLevelKeywordIndex(body, "UNION") >= 0 {
		return nil
	}
	plan := &returnProjectionPlan{}
	body, plan.distinct = cutDistinct(pipelineClauseBody(clause, "RETURN"))
	for _, rawItem := range splitTopLevelComma(body) {
		itemSource := readProjectionItemSource(ctx, strings.TrimSpace(rawItem), projectionAdmitted)
		expr, alias := itemSource.expression, itemSource.alias
		plan.addProjection(expr, alias)
	}
	plan.valid = len(plan.projections) > 0
	return plan
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
		expanded.addProjection(cyphertext.PrintIdentifier(column, cyphertext.IsIdentifier, cyphertext.AcceptedSource), column)
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
		alias := readProjectionItemSource(ctx, item, projectionAdmitted).alias
		named[alias] = true
	}
	out := make([]string, 0, len(columns)+len(items))
	for _, column := range columns {
		if !named[column] {
			variable := cyphertext.PrintIdentifier(column, cyphertext.IsIdentifier, cyphertext.AcceptedSource)
			out = append(out, variable+" AS "+variable)
		}
	}
	return append(out, items...)
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

// returnShapeItems returns the trimmed projection items of query's RETURN clause
// with the trailing ORDER BY / SKIP / LIMIT clauses removed. It is the single
// owner of that RETURN-location, trailing-clause trimming and comma-splitting
// step for the narrow optimized return shapes below; each shape keeps its own
// arity and expression grammar on top of the returned items.
//
// query is the canonical statement text supplied to pattern detection. The RETURN
// offset is kept on its own so only the clause body below it is trimmed.
func returnShapeItems(query string) []string {
	returnAt := findKeywordIndex(query, "RETURN")
	if returnAt < 0 {
		return nil
	}
	returnPart := strings.TrimSpace(query[returnAt+len("RETURN"):])

	// Strip ORDER BY / SKIP / LIMIT.
	end := len(returnPart)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnPart, keyword); idx >= 0 && idx < end {
			end = idx
		}
	}
	returnPart = strings.TrimSpace(returnPart[:end])
	if returnPart == "" {
		return nil
	}

	return splitTopLevelComma(returnPart)
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
func (names *quotedVariableNames) restoreResultColumns(result *ExecuteResult, parseItems func(string) []returnItem, declared ...resultmetadata.Projection) {
	if len(declared) != 0 {
		projection := declared[0]
		if !projection.WrittenWildcard {
			projection.WrittenNames.Visit(func(columns []string) {
				if len(columns) == len(result.Columns) {
					result.Columns = columns
				}
			}, func() {})
			return
		}
		// Membership and the explicit items come from construction. Preserve
		// this naming owner's existing wildcard presentation order and row
		// permutation without parsing the projection or observing row keys.
		projection.Names.Visit(func(columns []string) {
			projection.WrittenNames.Visit(func(explicit []string) {
				if len(columns) != len(result.Columns) || len(explicit) > len(columns) {
					return
				}
				prefix := len(columns) - len(explicit)
				for index, column := range columns[:prefix] {
					if original, found := names.internal[column]; found {
						columns[index] = original.name
					}
				}
				copy(columns[prefix:], explicit)
				result.Columns = columns
				sortColumnsByName(result)
			}, func() {})
		}, func() {})
		return
	}
	if start, end, ok := finalReturnBody(names.Text); ok {
		body, _ := cutDistinct(strings.TrimSpace(names.Text[start:end]))
		if strings.HasPrefix(body, "*") && (len(body) == 1 || !cyphertext.NamePartAt(body, 1)) && topLevelKeywordIndex(body, "UNION") < 0 {
			sortColumnsByName(result)
		} else {
			originalText, _ := names.OriginalText(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExactBoundaries, func(text string) string { return names.restoreSourceText(text, quotedNamesOnly) })
			items := parseItems(originalText)
			canonicalItems := parseItems(names.Text[start:end])
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
