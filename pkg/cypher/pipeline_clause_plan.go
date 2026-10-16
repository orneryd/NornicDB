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
	clauseAnalysisCount
)

// pipelineClausePlan holds the analyses of one clause text.
type pipelineClausePlan struct {
	once   [clauseAnalysisCount]sync.Once
	values [clauseAnalysisCount]any
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
	plan.once[kind].Do(func() {
		if hook := pipelineClausePlanBuildHook.Load(); hook != nil {
			(*hook)(kind, text)
		}
		plan.values[kind] = parse(text)
	})
	value, _ := plan.values[kind].(T)
	return value
}

// pipelinePushdownTerms is a WHERE's top-level AND conjuncts, trimmed, with
// which outer variables each one reads.
type pipelinePushdownTerms struct {
	terms []string
	// references[i] is terms[i]'s references; nil for terms parsed without
	// a plan.
	references []*pipelineVariableReferences
}

func parsePipelinePushdownTerms(where string) pipelinePushdownTerms {
	conjuncts := splitTopLevelAndConjuncts(where)
	terms := make([]string, 0, len(conjuncts))
	for _, term := range conjuncts {
		if term = strings.TrimSpace(term); term != "" {
			terms = append(terms, term)
		}
	}
	return pipelinePushdownTerms{terms: terms}
}

func pipelinePushdownTermsFor(ctx context.Context, where string) pipelinePushdownTerms {
	plan := pipelineClausePlanFor(ctx, where)
	if plan == nil {
		return parsePipelinePushdownTerms(where)
	}
	return plannedAnalysis(plan, clauseAnalysisPushdownTerms, where, func(where string) pipelinePushdownTerms {
		terms := parsePipelinePushdownTerms(where)
		terms.references = make([]*pipelineVariableReferences, len(terms.terms))
		for index, term := range terms.terms {
			terms.references[index] = newPipelineVariableReferences(term)
		}
		return terms
	})
}

// referencesVariable reports referencesVariable(terms[index], name).
func (t pipelinePushdownTerms) referencesVariable(index int, name string) bool {
	if t.references == nil {
		return referencesVariable(t.terms[index], name)
	}
	return t.references[index].references(name)
}

// pipelineVariableReferences is which variables a text reads
// (referencesVariable), filled as rows ask about each name.
type pipelineVariableReferences struct {
	text  string
	mu    sync.Mutex
	known map[string]bool
}

func newPipelineVariableReferences(text string) *pipelineVariableReferences {
	return &pipelineVariableReferences{text: text, known: make(map[string]bool)}
}

func (r *pipelineVariableReferences) references(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	referenced, known := r.known[name]
	if !known {
		referenced = referencesVariable(r.text, name)
		r.known[name] = referenced
	}
	return referenced
}

// referencesVariableFor is referencesVariable(text, name), from text's plan
// in ctx when there is one.
func referencesVariableFor(ctx context.Context, text, name string) bool {
	plan := pipelineClausePlanFor(ctx, text)
	if plan == nil {
		return referencesVariable(text, name)
	}
	return plannedAnalysis(plan, clauseAnalysisReferences, text, newPipelineVariableReferences).references(name)
}

// pipelineMatchWhere is a MATCH clause's pattern and its WHERE, whitespace
// normalized.
type pipelineMatchWhere struct {
	pattern string
	where   string
}

func splitPipelineMatchWhere(clause string) pipelineMatchWhere {
	split := pipelineMatchWhere{pattern: strings.TrimSpace(clause[len("MATCH"):])}
	if whereIndex := topLevelKeywordIndex(split.pattern, "WHERE"); whereIndex >= 0 {
		split.where = normalizePipelineWhitespace(split.pattern[whereIndex+len("WHERE"):])
		split.pattern = strings.TrimSpace(split.pattern[:whereIndex])
	}
	return split
}

func pipelineMatchWhereFor(ctx context.Context, clause string) pipelineMatchWhere {
	return clauseAnalysisFor(ctx, clauseAnalysisMatchWhere, clause, splitPipelineMatchWhere)
}

func matchPatternStructureFor(ctx context.Context, pattern string) string {
	return clauseAnalysisFor(ctx, clauseAnalysisPatternStructure, pattern, matchPatternStructure)
}

func ensureLeadingNodeNamedFor(ctx context.Context, pattern string) string {
	return clauseAnalysisFor(ctx, clauseAnalysisLeadingNodeNamed, pattern, ensureLeadingNodeNamed)
}

// quantifiedPathParse is parseQuantifiedPathMatch's result.
type quantifiedPathParse struct {
	match *quantifiedPathMatch
	ok    bool
	err   error
}

func quantifiedPathMatchFor(ctx context.Context, body string) (*quantifiedPathMatch, bool, error) {
	parsed := clauseAnalysisFor(ctx, clauseAnalysisQuantifiedPath, body, func(body string) quantifiedPathParse {
		match, ok, err := parseQuantifiedPathMatch(body)
		return quantifiedPathParse{match: match, ok: ok, err: err}
	})
	return parsed.match, parsed.ok, parsed.err
}

// callSubqueryParse is a CALL subquery clause read once: parseCallSubquery's
// parts, its scope clause and the variables that clause imports.
type callSubqueryParse struct {
	body           string
	afterCall      string
	inTransactions bool
	batchSize      int
	scoped         bool
	scopedImports  []string
}

func (e *StorageExecutor) callSubqueryParseFor(ctx context.Context, clause string) callSubqueryParse {
	return clauseAnalysisFor(ctx, clauseAnalysisCallSubquery, clause, func(clause string) callSubqueryParse {
		var parsed callSubqueryParse
		parsed.body, parsed.afterCall, parsed.inTransactions, parsed.batchSize = e.parseCallSubquery(clause)
		callScope := readCallSubqueryScope(clause)
		parsed.scoped, parsed.scopedImports = callScope.scoped, callScope.names
		return parsed
	})
}

func (e *StorageExecutor) inferTopLevelReturnColumnsFor(ctx context.Context, query string) []string {
	return clauseAnalysisFor(ctx, clauseAnalysisReturnColumns, query, e.inferTopLevelReturnColumns)
}

func callSubqueryQueryIsWriteFor(ctx context.Context, query string) bool {
	return clauseAnalysisFor(ctx, clauseAnalysisWrite, query, callSubqueryQueryIsWrite)
}

// unionBranchesParse is parseTopLevelUnionBranches' result.
type unionBranchesParse struct {
	branches []string
	all      bool
	mixed    bool
	ok       bool
}

func parseTopLevelUnionBranchesFor(ctx context.Context, query string) ([]string, bool, bool, bool) {
	parsed := clauseAnalysisFor(ctx, clauseAnalysisUnionBranches, query, func(query string) unionBranchesParse {
		var parsed unionBranchesParse
		parsed.branches, parsed.all, parsed.mixed, parsed.ok = parseTopLevelUnionBranches(query)
		return parsed
	})
	return parsed.branches, parsed.all, parsed.mixed, parsed.ok
}

// plannedPatternBindings returns the variables a pattern clause binds
// (pipelinePatternBindingNames), from the clause's plan.
func (e *StorageExecutor) plannedPatternBindings(plan *pipelineClausePlan, clause, keyword string) []string {
	parse := func(clause string) []string { return e.pipelinePatternBindingNames(clause, keyword) }
	switch keyword {
	case "MATCH":
		return plannedAnalysis(plan, clauseAnalysisPatternBindingsMatch, clause, parse)
	case "OPTIONAL MATCH":
		return plannedAnalysis(plan, clauseAnalysisPatternBindingsOptionalMatch, clause, parse)
	case "CREATE":
		return plannedAnalysis(plan, clauseAnalysisPatternBindingsCreate, clause, parse)
	case "MERGE":
		return plannedAnalysis(plan, clauseAnalysisPatternBindingsMerge, clause, parse)
	}
	return parse(clause)
}

func pipelineClauseAggregatesFor(ctx context.Context, clause pipelineClause) bool {
	if clause.kind != pipelineClauseWith && clause.kind != pipelineClauseReturn {
		return false
	}
	plan := pipelineClausePlanFor(ctx, clause.text)
	if plan == nil {
		return pipelineClauseAggregates(clause)
	}
	return plannedAnalysis(plan, clauseAnalysisAggregates, clause.text, func(string) bool { return pipelineClauseAggregates(clause) })
}

// pipelineMatchParts is a MATCH body's pattern parts: the comma-separated
// parts before its WHERE, which starts at patternEnd (len(body) without one).
type pipelineMatchParts struct {
	patternEnd int
	parts      []string
}

func splitPipelineMatchParts(body string) pipelineMatchParts {
	split := pipelineMatchParts{patternEnd: len(body)}
	if where := topLevelKeywordIndex(body, "WHERE"); where >= 0 {
		split.patternEnd = where
	}
	split.parts = splitTopLevelComma(body[:split.patternEnd])
	return split
}

func pipelineMatchPartsFor(ctx context.Context, body string) pipelineMatchParts {
	return clauseAnalysisFor(ctx, clauseAnalysisMatchParts, body, splitPipelineMatchParts)
}

// pipelineMatchBodyPartCountFor counts the top-level comma-separated parts of
// a MATCH clause's whole body.
func pipelineMatchBodyPartCountFor(ctx context.Context, clause string) int {
	return clauseAnalysisFor(ctx, clauseAnalysisMatchBodyPartCount, clause, func(clause string) int {
		return len(splitTopLevelComma(pipelineClauseBody(clause, "MATCH")))
	})
}

// simpleIndexedEqualityShapes is one WHERE's parseSimpleIndexedEqualityShape
// by variable, filled as callers ask.
type simpleIndexedEqualityShapes struct {
	where      string
	mu         sync.Mutex
	byVariable map[string]simpleIndexedEqualityShape
}

func simpleIndexedEqualityShapeFor(ctx context.Context, variable, where string) simpleIndexedEqualityShape {
	plan := pipelineClausePlanFor(ctx, where)
	if plan == nil {
		return parseSimpleIndexedEqualityShape(variable, where)
	}
	shapes := plannedAnalysis(plan, clauseAnalysisIndexedEquality, where, func(where string) *simpleIndexedEqualityShapes {
		return &simpleIndexedEqualityShapes{where: where, byVariable: make(map[string]simpleIndexedEqualityShape)}
	})
	shapes.mu.Lock()
	defer shapes.mu.Unlock()
	shape, known := shapes.byVariable[variable]
	if !known {
		shape = parseSimpleIndexedEqualityShape(variable, where)
		shapes.byVariable[variable] = shape
	}
	return shape
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
