// Author of retained syntax cache identity: Codex R17 implementation agent.
package cypher

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/internal/cyphertext"
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
type projectionPlanKey struct{ accepted, submitted string }

var returnProjectionPlans = struct {
	sync.RWMutex
	plans map[projectionPlanKey]*returnProjectionPlan
}{plans: make(map[projectionPlanKey]*returnProjectionPlan)}

const returnProjectionPlanLimit = 4096

func returnProjectionPlanFor(ctx context.Context, clause pipelineClause) *returnProjectionPlan {
	program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if !ok || program == nil || !clause.syntax.Valid() || program.syntax.Source() != clause.syntax.Source() || program.syntax.Origin() != clause.syntax.Origin() {
		return nil
	}
	plan := program.projections[clause.syntax.ID()]
	if plan == nil || !plan.valid {
		return nil
	}
	return plan
}

// projectionPlanOf is the plan of one retained RETURN or WITH occurrence. The
// plan is built only by projectionPlanFromSyntax; the cache is keyed by the
// occurrence's own source text, which fully determines its plan. Every
// consumer acquires a plan here (projectionPlanOfNode); the bound program
// keeps only its scope-resolved derivations of these plans.
// Author: Claude R17 implementation agent, 30 October 2026.
func projectionPlanOf(ctx context.Context, clause cypherantlr.SyntaxNode) *returnProjectionPlan {
	if ctx.Err() != nil {
		return absentProjectionPlan()
	}
	return projectionPlanOfNode(clause)
}

// projectionPlanOfNode is projectionPlanOf for readers without a context.
func projectionPlanOfNode(clause cypherantlr.SyntaxNode) *returnProjectionPlan {
	if !clause.Valid() {
		return absentProjectionPlan()
	}
	key := projectionPlanKey{accepted: clause.Code(), submitted: clause.SubmittedText()}
	returnProjectionPlans.RLock()
	plan, cached := returnProjectionPlans.plans[key]
	returnProjectionPlans.RUnlock()
	if cached {
		return plan
	}
	plan = projectionPlanFromSyntax(clause)
	returnProjectionPlans.Lock()
	if len(returnProjectionPlans.plans) >= returnProjectionPlanLimit {
		returnProjectionPlans.plans = make(map[projectionPlanKey]*returnProjectionPlan)
	}
	returnProjectionPlans.plans[key] = plan
	returnProjectionPlans.Unlock()
	return plan
}

// absentProjectionPlan is the invalid plan of a missing projection occurrence.
func absentProjectionPlan() *returnProjectionPlan {
	return &returnProjectionPlan{}
}

// returnPlanOf is the plan of a retained RETURN occurrence, nil when present is
// false (a body without RETURN).
func returnPlanOf(ctx context.Context, clause pipelineClause, present bool) *returnProjectionPlan {
	if !present {
		return nil
	}
	return projectionPlanOf(ctx, clause.syntax)
}

// projectionPlanAt is the plan of the RETURN or WITH occurrence at offset at of
// statement (projectionClauseAt). Absent an occurrence, the plan is invalid.
func projectionPlanAt(ctx context.Context, statement string, at int) *returnProjectionPlan {
	clause, ok := projectionClauseAt(ctx, statement, at)
	if !ok {
		return absentProjectionPlan()
	}
	return projectionPlanOf(ctx, clause)
}

// withStarExpanded is the plan of RETURN *, items with the * written out
// through the canonical wildcard expansion (#883).
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
func pipelineReturnSourceColumns(ctx context.Context, clause pipelineClause) []string {
	plan := projectionPlanOf(ctx, clause.syntax)
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
	clause, ok := leadingCallClause(ctx, source)
	if !ok {
		return nil
	}
	return yieldFromSyntax(clause)
}

// callTailWithProjectionColumns consumes execution's WITH projection plan.
func callTailWithProjectionColumns(ctx context.Context, withClause pipelineClause) ([]string, bool) {
	plan := projectionPlanOf(ctx, withClause.syntax)
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
func pipelineProjectionScope(ctx context.Context, previous map[string]struct{}, clause pipelineClause, imports pipelineRow) (map[string]struct{}, error) {
	if err := pipelineScopeFailure(ctx); err != nil {
		return nil, err
	}
	plan := returnProjectionPlanFor(ctx, clause)
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

// restoreResultColumns orders wildcard columns after binding identities have
// been restored. Explicit projection labels belong to their retained syntax
// occurrences; this boundary does not read original/canonical RETURN copies.
func (names *quotedVariableNames) restoreResultColumns(result *ExecuteResult) {
	if start, end, ok := finalReturnBody(names.Text); ok {
		body, _ := cutDistinct(strings.TrimSpace(names.Text[start:end]))
		if strings.HasPrefix(body, "*") && (len(body) == 1 || !cyphertext.NamePartAt(body, 1)) && topLevelKeywordIndex(body, "UNION") < 0 {
			sortColumnsByName(result)
		}
	}
}
