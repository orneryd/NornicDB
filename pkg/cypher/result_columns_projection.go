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
	expr     string
	variable string
	// direct preserves a variable identity, including parenthesized identities.
	direct bool
	// grouped distinguishes that identity from a bare pass-through projection.
	grouped bool
	// named records that the item's column is a name - an explicit AS alias or
	// a bare variable identity - instead of the item's expression text as
	// written. A name is a decoded identity, so a source rewrite never maps it
	// back to the spelling the client wrote.
	named                          bool
	alias                          string
	isAggr                         bool
	aggregateName                  string
	aggregateExpr                  string
	distinct                       bool
	definitionStart, definitionEnd int
	// expressionStart is where the item's evaluation expression starts in the
	// clause's own text, or -1 when the plan was built without the statement's
	// retained syntax. A PROFILE statement binds it as the row's expression
	// origin, so a program an expression starts is reported at the occurrence
	// that was written; the alias definition span is a different position and
	// is not used for it.
	expressionStart int
}

// returnProjectionPlan is a RETURN clause parsed for pipelineApplyReturn:
// its items, columns, DISTINCT and trailing ORDER BY / SKIP / LIMIT. A plan
// depends only on the clause text, so it is parsed once per text
// (returnProjectionPlanFor). valid is false when the clause has no items.
type returnProjectionPlan struct {
	err   error
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
	// parameterPassthrough proves the entire cached source is one plain RETURN
	// of bare parameters. Its admission cannot depend on their runtime types.
	parameterPassthrough bool
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
		if !ok || !boundStatementAddresses(program, origin.syntax) {
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
	plan.parameterPassthrough = plan.provesParameterPassthrough(ctx, clause)
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

func (plan *returnProjectionPlan) provesParameterPassthrough(ctx context.Context, source string) bool {
	if !plan.valid || plan.star || plan.distinct || plan.modifiers != "" || len(plan.projections) == 0 {
		return false
	}
	for _, projection := range plan.projections {
		whole := false
		cyphertext.ParameterReferences(projection.expr, func(_ string, start, end int) {
			whole = start == 0 && end == len(projection.expr)
		})
		if !whole {
			return false
		}
	}
	// canonicalQueryClause can return the first clause of a larger statement.
	// The shared pipeline reader proves the whole source has one branch and
	// one RETURN; no UNION, importing clause or tail can inherit this proof.
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(ctx, source)
	return ok && len(clauses) == 1 && clauses[0].kind == pipelineClauseReturn
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
	if strings.ContainsAny(body, "\x60()[]{}\r\n*'\"") || firstTopLevelModifierIndex(ctx, body) >= 0 || topLevelKeywordIndex(ctx, body, "UNION") >= 0 {
		return nil
	}
	plan := &returnProjectionPlan{}
	body, plan.distinct = cutDistinct(pipelineClauseBody(clause, "RETURN"))
	for _, rawItem := range splitTopLevelComma(body) {
		itemSource := readProjectionItemSource(ctx, strings.TrimSpace(rawItem), projectionAdmitted)
		expr, alias := itemSource.expression, itemSource.alias
		plan.addProjection(expr, alias)
		plan.projections[len(plan.projections)-1].named = projectionItemNamesColumn(itemSource.syntax)
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
		last.named = true
		last.definitionStart, last.definitionEnd = -1, -1
	}
	for _, projection := range plan.starProjections {
		expanded.projections = append(expanded.projections, projection)
		expanded.columns = append(expanded.columns, projection.alias)
		expanded.hasAggregate = expanded.hasAggregate || projection.isAggr
	}
	return expanded
}

// namesColumn reports whether the plan names the result column at index: an
// alias or a bare variable names its column, and a star names each column it
// expands from the statement's scope. Any other item's column is its
// expression text as written.
func (plan *returnProjectionPlan) namesColumn(index, columns int) bool {
	if plan == nil || !plan.valid || index < 0 || index >= columns {
		return false
	}
	if plan.star {
		// The star's expanded scope columns precede the items written after it.
		first := columns - len(plan.starProjections)
		if index < first {
			return true
		}
		index -= first
		return index >= 0 && index < len(plan.starProjections) && plan.starProjections[index].named
	}
	return len(plan.projections) == columns && plan.projections[index].named
}

// statementNamesColumn reports whether the statement names the result column at
// index instead of carrying expression text there. Only an expression column is
// source spelling a rewrite maps back, so this is the one question column
// restoration asks. A statement with no column-naming RETURN (CALL ... YIELD,
// SHOW ...) names every column it produces.
func statementNamesColumn(ctx context.Context, statement string, columns, index int) bool {
	start, end, ok := columnReturnBody(ctx, statement)
	if !ok {
		return true
	}
	return returnProjectionPlanFor(ctx, "RETURN "+strings.TrimSpace(statement[start:end])).namesColumn(index, columns)
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
	plan.projections = append(plan.projections, returnProjection{expr: expr, alias: alias, isAggr: isAggr, aggregateName: aggregateName, aggregateExpr: aggregateExpr, distinct: distinct, expressionStart: -1})
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
	projection, _ := splitWithProjection(ctx, withClause)
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

// pipelineProjectionScope is execution's WITH scope transfer: the one owner
// every pipeline scope consumer uses to establish the variables a WITH clause
// exports. The transfer is metadata, so it reports the caller's context
// failure instead of degrading the projection: a statement canceled between a
// WITH projection and its scope update stops here with the caller's own error
// instead of continuing with an absent scope (nor20). A clause whose
// projection cannot be established is an unresolved statement, never an empty
// scope.
func pipelineProjectionScope(ctx context.Context, previous map[string]struct{}, clause string, imports pipelineRow) (map[string]struct{}, error) {
	if err := pipelineScopeFailure(ctx); err != nil {
		return nil, err
	}
	projection, _ := splitWithProjection(ctx, clause)
	plan := returnProjectionPlanFor(ctx, "RETURN "+pipelineClauseBody(projection, "WITH"))
	if !plan.valid {
		// returnProjectionPlanFor answers a canceled read with no plan: the
		// context failure is the statement's outcome, not absent metadata.
		if err := pipelineScopeFailure(ctx); err != nil {
			return nil, err
		}
		return nil, unresolvedStatementError()
	}
	next := projectionPlanScope(plan, previous)
	if next == nil {
		return nil, unresolvedStatementError()
	}
	for name := range imports {
		next[name] = struct{}{}
	}
	return next, nil
}

// pipelineScopeFailure is the scope transfer's context observation: the
// caller's cancellation stops the statement, and a failure the statement
// already recorded is reported ahead of it, as pipelineDecline does.
func pipelineScopeFailure(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	if failure := getExpressionFailure(ctx); failure != nil {
		return failure
	}
	return err
}

// restoreResultColumns preserves final RETURN spelling after quoted variables
// have executed with internal identities: a named column is the decoded
// identity its canonical item carries, and an expression column the text the
// client wrote.
func (names *quotedVariableNames) restoreResultColumns(ctx context.Context, result *ExecuteResult, parseItems func(string) []returnItem) {
	if start, end, ok := columnReturnBody(ctx, names.Text); ok {
		body, _ := cutDistinct(strings.TrimSpace(names.Text[start:end]))
		if strings.HasPrefix(body, "*") && (len(body) == 1 || !cyphertext.NamePartAt(body, 1)) && topLevelKeywordIndex(ctx, body, "UNION") < 0 {
			sortColumnsByName(result)
		} else {
			originalText, _ := names.OriginalText(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExactBoundaries, func(text string) string { return names.restoreSourceText(text, quotedNamesOnly) })
			items := parseItems(originalText)
			canonical := returnProjectionPlanFor(ctx, "RETURN "+names.Text[start:end])
			if len(items) == len(result.Columns) && len(canonical.projections) == len(items) {
				for index, item := range items {
					if canonical.namesColumn(index, len(items)) {
						// The statement names this column, so its text is the decoded
						// identity the canonical item carries, never the written spelling
						// the original item keeps for expression columns.
						result.Columns[index] = names.restoreColumn(canonical.projections[index].alias)
					} else {
						result.Columns[index] = item.alias
					}
				}
			}
		}
	}
}
