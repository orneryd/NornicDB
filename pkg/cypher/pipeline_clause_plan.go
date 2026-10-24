package cypher

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
)

// A correlated CALL subquery runs its body once for every outer row, and each
// clause reads its own text for each run: the WHERE's conjuncts, the
// pattern's structure, the quantified path, the projection's aliases, the
// body's RETURN columns. The text is the same for every row; only the row's
// values change. pipelineClausePlans is the statement's parsed form of those
// texts: one plan per text, built on first use and shared by every row and
// every nested subquery of the statement. Each analysis in a plan is the
// result of the existing parser, run once; nothing here parses on its own.
// The CALL operator installs the plans (withPipelineClausePlans). Without
// them in the context every analysis parses its text, as before.
type pipelineClausePlans struct {
	mu    sync.Mutex
	plans map[string]*pipelineClausePlan
}

// pipelineClausePlanLimit bounds a statement's plans: a body that builds a
// new text for each row gets no plan beyond it, and parses that text as it
// would without plans.
const pipelineClausePlanLimit = 1024

type pipelineClausePlansKey struct{}

// pipelineClauseAnalysis names one analysis of a clause text.
type pipelineClauseAnalysis int

const (
	clauseAnalysisPushdownTerms pipelineClauseAnalysis = iota
	clauseAnalysisMatchWhere
	clauseAnalysisPatternStructure
	clauseAnalysisQuantifiedPath
	clauseAnalysisLeadingNodeNamed
	clauseAnalysisCallSubquery
	clauseAnalysisReturnColumns
	clauseAnalysisWrite
	clauseAnalysisUnionBranches
	clauseAnalysisPatternBindingsMatch
	clauseAnalysisPatternBindingsOptionalMatch
	clauseAnalysisPatternBindingsCreate
	clauseAnalysisPatternBindingsMerge
	clauseAnalysisProjectionNames
	clauseAnalysisAggregates
	clauseAnalysisMatchParts
	clauseAnalysisMatchBodyPartCount
	clauseAnalysisReferences
	clauseAnalysisIndexedEquality
	clauseAnalysisSubqueryQueryExists
	clauseAnalysisSubqueryQueryCount
	clauseAnalysisSubqueryQueryCollect
	clauseAnalysisBoundChain
	clauseAnalysisLogicalPlan
	clauseAnalysisOperandFacts
	clauseAnalysisTopLevelAnd
	clauseAnalysisTopLevelOr
	clauseAnalysisMaySubquery
	clauseAnalysisOrTerms
	clauseAnalysisMatchBodyWhere
	clauseAnalysisReturnSourceColumns
	clauseAnalysisLimitModifier
	clauseAnalysisSkipModifier
	clauseAnalysisBoundNode
	clauseAnalysisRelationshipEqualities
	clauseAnalysisCount
)

// pipelineClausePlan holds the analyses of one clause text. A text is read
// by few of the analyses, so each one's slot is allocated on its first use:
// a plan costs a pointer per analysis, not a built value per analysis.
type pipelineClausePlan struct {
	slots [clauseAnalysisCount]atomic.Pointer[pipelineClauseAnalysisSlot]
}

// pipelineClauseAnalysisSlot is one analysis of a plan, built once.
type pipelineClauseAnalysisSlot struct {
	once  sync.Once
	value any
}

// slot returns kind's slot, allocating it on first use.
func (plan *pipelineClausePlan) slot(kind pipelineClauseAnalysis) *pipelineClauseAnalysisSlot {
	if slot := plan.slots[kind].Load(); slot != nil {
		return slot
	}
	plan.slots[kind].CompareAndSwap(nil, &pipelineClauseAnalysisSlot{})
	return plan.slots[kind].Load()
}

// withPipelineClausePlans returns ctx with a statement's clause plans,
// adding them when ctx has none yet.
func withPipelineClausePlans(ctx context.Context) context.Context {
	if _, ok := ctx.Value(pipelineClausePlansKey{}).(*pipelineClausePlans); ok {
		return ctx
	}
	return context.WithValue(ctx, pipelineClausePlansKey{}, &pipelineClausePlans{plans: make(map[string]*pipelineClausePlan)})
}

// pipelineClausePlanFor returns the plan of text, nil without plans in ctx
// or past pipelineClausePlanLimit.
func pipelineClausePlanFor(ctx context.Context, text string) *pipelineClausePlan {
	if ctx == nil {
		return nil
	}
	plans, _ := ctx.Value(pipelineClausePlansKey{}).(*pipelineClausePlans)
	if plans == nil {
		return nil
	}
	plans.mu.Lock()
	defer plans.mu.Unlock()
	plan := plans.plans[text]
	if plan == nil {
		if len(plans.plans) >= pipelineClausePlanLimit {
			return nil
		}
		plan = &pipelineClausePlan{}
		plans.plans[text] = plan
	}
	return plan
}

// pipelineClausePlanBuildHook, when set, sees each analysis a plan builds
// (tests count parses with it).
var pipelineClausePlanBuildHook atomic.Pointer[func(pipelineClauseAnalysis, string)]

// clauseAnalysisFor returns parse(text) from text's plan in ctx, parsing on
// the plan's first use, or parse(text) without a plan. The result is shared
// by every caller of the statement: callers never modify it.
func clauseAnalysisFor[T any](ctx context.Context, kind pipelineClauseAnalysis, text string, parse func(string) T) T {
	plan := pipelineClausePlanFor(ctx, text)
	if plan == nil {
		return parse(text)
	}
	return plannedAnalysis(plan, kind, text, parse)
}

// plannedAnalysis is clauseAnalysisFor with text's plan in hand.
func plannedAnalysis[T any](plan *pipelineClausePlan, kind pipelineClauseAnalysis, text string, parse func(string) T) T {
	slot := plan.slot(kind)
	slot.once.Do(func() {
		if hook := pipelineClausePlanBuildHook.Load(); hook != nil {
			(*hook)(kind, text)
		}
		slot.value = parse(text)
	})
	value, _ := slot.value.(T)
	return value
}

// withStatementClausePlans adds clause plans to the context of a statement
// that may run a subquery for each row: a CALL subquery, or an EXISTS /
// COUNT / COLLECT subquery expression, whose body every row runs again. A
// statement without one runs without plans.
func withStatementClausePlans(ctx context.Context, cypher string) context.Context {
	if strings.IndexByte(cypher, '{') < 0 || !mayContainSubqueryExpression(cypher) && !containsFold(cypher, "CALL") {
		return ctx
	}
	return withPipelineClausePlans(ctx)
}
