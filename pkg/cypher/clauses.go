// Cypher clause implementations for NornicDB.
// This file contains implementations for WITH, UNWIND, UNION, OPTIONAL MATCH,
// FOREACH, and LOAD CSV clauses.

package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func prevWordEqualsIgnoreCase(s string, pos int, word string) bool {
	if pos <= 0 {
		return false
	}
	i := lastLiveByte(s, pos)
	if i < 0 {
		return false
	}
	end := i + 1
	for i >= 0 && cyphertext.NamePartAt(s, i) {
		i--
	}
	start := i + 1
	if end-start != len(word) {
		return false
	}
	for j := 0; j < len(word); j++ {
		if asciiUpper(s[start+j]) != asciiUpper(word[j]) {
			return false
		}
	}
	return true
}

// findKeywordNotInBrackets finds the index of a keyword that is NOT inside brackets [] or parentheses ()
// This is used to avoid matching keywords inside list comprehensions like [x IN list WHERE x > 2]
// The keyword should be in the format " KEYWORD " with leading/trailing spaces.
// This function normalizes whitespace (tabs, newlines) to match.
func findKeywordNotInBrackets(view braceView, keyword string) int {
	s := view.text()
	opts := defaultKeywordScanOpts(view)
	opts.SkipBraces = false
	opts.Boundary = keywordBoundaryWhitespace

	keywordCore := strings.TrimSpace(keyword)
	if keywordCore == "" {
		return -1
	}
	return keywordIndexFrom(s, keywordCore, 0, opts)
}

// ========================================
// WITH Clause
// ========================================

func normalizeInterfaceMap(input map[interface{}]interface{}) map[string]interface{} {
	output := make(map[string]interface{}, len(input))
	for k, v := range input {
		key := fmt.Sprintf("%v", k)
		output[key] = v
	}
	return output
}

// evaluateWithWhere evaluates typed WITH bindings through the shared row predicate.
func (e *StorageExecutor) evaluateWithWhere(ctx context.Context, view braceView, boundVars map[string]interface{}) (bool, error) {
	view = view.trim()
	expr := view.text()
	ctx = withBraceView(ctx, view)
	if expr == "" {
		return true, nil
	}
	ctx = withExpressionFailureSlot(ctx)
	if plan := planRowPredicate(view); plan != nil && plan.complete {
		accepted := e.evaluateRowPredicatePartScope(ctx, &plan.root, compiledRowScope{values: boundVars, parameters: getParamsFromContext(ctx)}, nil)
		return accepted, getExpressionFailure(ctx)
	}
	values := boundVars
	if parameters := parameterRowValues(ctx); len(parameters) > 0 {
		values = make(map[string]interface{}, len(boundVars)+len(parameters))
		for name, value := range boundVars {
			values[name] = value
		}
		for name, value := range parameters {
			values[name] = value
		}
	}
	accepted := e.evaluateRowPredicate(ctx, view, values)
	return accepted, getExpressionFailure(ctx)
}

// aggregateFnNames lists the aggregating function names recognized
// by isAggregateExpression / aggregateIdentity. The set matches what
// the rest of the executor (functions.go) treats as aggregates.
var aggregateFnNames = []string{"collect", "count", "sum", "avg", "min", "max", "stdev", "stdevp"}

// isAggregateExpression reports whether expr is an aggregating Cypher
// expression at the top level. Recognizes the canonical forms
// `collect(...)`, `count(...)`, etc., case-insensitively.
func isAggregateExpression(expr string) bool {
	trimmed := lowerASCII(strings.TrimSpace(expr))
	for _, fn := range aggregateFnNames {
		if strings.HasPrefix(trimmed, fn+"(") || strings.HasPrefix(trimmed, fn+" (") {
			return true
		}
	}
	return false
}

// aggregateIdentity returns the value an aggregating expression
// produces when applied to an empty row set. Cypher specifies:
//
//	collect(...) → []
//	count(...)   → 0
//	sum(...)     → 0
//	avg(...)     → null
//	min(...)     → null
//	max(...)     → null
//
// stdev / stdevp follow the avg convention (null on empty input).
func aggregateIdentity(expr string) interface{} {
	trimmed := lowerASCII(strings.TrimSpace(expr))
	switch {
	case strings.HasPrefix(trimmed, "collect("), strings.HasPrefix(trimmed, "collect ("):
		return []interface{}{}
	case strings.HasPrefix(trimmed, "count("), strings.HasPrefix(trimmed, "count ("):
		return int64(0)
	case strings.HasPrefix(trimmed, "sum("), strings.HasPrefix(trimmed, "sum ("):
		return int64(0)
	}
	return nil
}

// rowHasAggregate reports whether any return item is an aggregating
// expression. Used to decide whether a WHERE-filtered WITH should emit
// one row (with aggregation identity values) or zero rows.
func rowHasAggregate(items []returnItem) bool {
	for _, item := range items {
		if isAggregateExpression(item.expr) {
			return true
		}
	}
	return false
}

// splitWithItems splits WITH expressions respecting nested brackets and quotes
func (e *StorageExecutor) splitWithItems(expr string) []string {
	return splitTopLevelComma(expr)
}

type unwindSimpleSetAssignment struct {
	prop     string
	expr     string
	mergeMap bool
	// Author: #1663 lane. Property-map metadata keeps its consumed record view.
	mapInput braceView
}

type unwindMergeChainNodePlan struct {
	mergeVar            string
	labels              []string
	matchAssignments    []unwindSimpleSetAssignment
	setAssignments      []unwindSimpleSetAssignment
	onCreateAssignments []unwindSimpleSetAssignment
	onMatchAssignments  []unwindSimpleSetAssignment
}

type unwindMergeChainLookupPlan struct {
	varName          string
	labels           []string
	matchAssignments []unwindSimpleSetAssignment
	setAssignments   []unwindSimpleSetAssignment
	optional         bool
	anyLabel         bool
}

type unwindMergeChainWithAssignment struct {
	alias string
	expr  string
}

type unwindMergeChainWithPlan struct {
	assignments []unwindMergeChainWithAssignment
	projection  pipelineRowWith
}

type unwindMergeChainWherePlan struct {
	clause string
}

type unwindMergeChainRelationshipPlan struct {
	fromVar          string
	toVar            string
	relVar           string
	relType          string
	matchAssignments []unwindSimpleSetAssignment
	setAssignments   []unwindSimpleSetAssignment
}

type unwindMergeChainStep struct {
	node         *unwindMergeChainNodePlan
	lookup       *unwindMergeChainLookupPlan
	with         *unwindMergeChainWithPlan
	where        *unwindMergeChainWherePlan
	relationship *unwindMergeChainRelationshipPlan
}

type unwindMergeChainPlan struct {
	input     braceView
	supported bool
	simple    bool
	steps     []unwindMergeChainStep
}

func parseUnwindSimpleMergeMatchAssignments(raw braceView) ([]unwindSimpleSetAssignment, bool) {
	entries, valid := raw.keyedEntries()
	if !valid || len(entries) == 0 {
		return nil, false
	}
	out := make([]unwindSimpleSetAssignment, 0, len(entries))
	for _, entry := range entries {
		out = append(out, unwindSimpleSetAssignment{prop: entry.name, expr: entry.value.text(), mapInput: raw})
	}
	return out, true
}

func parseUnwindSimpleSetAssignments(raw, mergeVar string) ([]unwindSimpleSetAssignment, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, true
	}
	assignments := splitTopLevelComma(trimmed)
	out := make([]unwindSimpleSetAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		a := strings.TrimSpace(assignment)
		if a == "" {
			continue
		}
		if plusEqIdx := strings.Index(a, "+="); plusEqIdx > 0 {
			left := strings.TrimSpace(a[:plusEqIdx])
			right := strings.TrimSpace(a[plusEqIdx+2:])
			if left != mergeVar || right == "" {
				return nil, false
			}
			out = append(out, unwindSimpleSetAssignment{
				expr:     right,
				mergeMap: true,
			})
			continue
		}
		eqIdx := strings.Index(a, "=")
		if eqIdx <= 0 {
			return nil, false
		}
		left := strings.TrimSpace(a[:eqIdx])
		right := strings.TrimSpace(a[eqIdx+1:])
		dotIdx := strings.Index(left, ".")
		if dotIdx <= 0 || dotIdx == len(left)-1 {
			return nil, false
		}
		leftVar := strings.TrimSpace(left[:dotIdx])
		prop := strings.TrimSpace(left[dotIdx+1:])
		if leftVar != mergeVar || prop == "" {
			return nil, false
		}
		out = append(out, unwindSimpleSetAssignment{
			prop: prop,
			expr: right,
		})
	}
	return out, true
}

func parseSimpleUnwindMergeClause(mergePart braceView) (string, []string, []unwindSimpleSetAssignment, bool) {
	return parseUnwindNodePatternClause(mergePart, "MERGE")
}

func parseUnwindNodePatternClause(clause braceView, keyword string) (string, []string, []unwindSimpleSetAssignment, bool) {
	varName, labels, assignments, _, ok := parseUnwindNodePatternClauseInternal(clause, keyword, false)
	return varName, labels, assignments, ok
}

func parseUnwindNodePatternClauseInternal(clause braceView, keyword string, allowAlternatives bool) (string, []string, []unwindSimpleSetAssignment, bool, bool) {
	clause = clause.trim()
	trimmed := clause.text()
	if !startsWithKeywordFold(trimmed, keyword) {
		return "", nil, nil, false, false
	}
	restView := clause.window(len(keyword), len(trimmed)).trim()
	rest := restView.text()
	if !strings.HasPrefix(rest, "(") {
		return "", nil, nil, false, false
	}
	parenEnd := findMatchingParen(rest, 0)
	if parenEnd < 0 {
		return "", nil, nil, false, false
	}
	// MATCH (v {…}) WHERE v:A|B is the form the label-expression rewrite
	// gives MATCH (v:A|B {…}) (#860); a lookup reads its alternatives.
	where := strings.TrimSpace(rest[parenEnd+1:])
	if where != "" && (!allowAlternatives || !startsWithKeywordFold(where, "WHERE")) {
		return "", nil, nil, false, false
	}
	nodeView := restView.window(1, parenEnd).trim()
	nodePattern := nodeView.text()
	braceStart, braceEnd, fact, found := nodeView.firstBrace()
	if !found || fact.kind != patternPropertyBrace {
		return "", nil, nil, false, false
	}
	if braceEnd < 0 || strings.TrimSpace(nodePattern[braceEnd+1:]) != "" {
		return "", nil, nil, false, false
	}
	head := strings.TrimSpace(nodePattern[:braceStart])
	propMap := nodeView.window(braceStart, braceEnd+1)
	if head == "" || strings.TrimSpace(nodePattern[braceStart+1:braceEnd]) == "" {
		return "", nil, nil, false, false
	}
	parts := strings.Split(head, ":")
	if len(parts) < 2 && where == "" || len(parts) >= 2 && where != "" {
		return "", nil, nil, false, false
	}
	mergeVar := strings.TrimSpace(parts[0])
	if !isSimpleIdentifier(mergeVar) {
		return "", nil, nil, false, false
	}
	labels := make([]string, 0, len(parts)-1)
	anyLabel := false
	if where != "" {
		variable, chain, hasLabels := splitNodeHead(strings.TrimSpace(where[len("WHERE"):]))
		expr, ok := parseLabelExpression(chain)
		if !hasLabels || variable != mergeVar || !ok {
			return "", nil, nil, false, false
		}
		alternatives, ok := expr.alternatives()
		if !ok {
			return "", nil, nil, false, false
		}
		labels, anyLabel = alternatives, len(alternatives) > 1
	}
	for i := 1; i < len(parts); i++ {
		label := strings.TrimSpace(parts[i])
		if strings.Contains(label, "|") {
			if !allowAlternatives || anyLabel || len(parts) != 2 {
				return "", nil, nil, false, false
			}
			for _, alternative := range strings.Split(label, "|") {
				alternative = strings.TrimSpace(alternative)
				if !isSimpleIdentifier(alternative) {
					return "", nil, nil, false, false
				}
				labels = append(labels, alternative)
			}
			anyLabel = true
			continue
		}
		if !isSimpleIdentifier(label) {
			return "", nil, nil, false, false
		}
		labels = append(labels, label)
	}
	if len(labels) == 0 {
		return "", nil, nil, false, false
	}
	assignments, ok := parseUnwindSimpleMergeMatchAssignments(propMap)
	if !ok {
		return "", nil, nil, false, false
	}
	return mergeVar, labels, assignments, anyLabel, true
}

func parseUnwindLookupClause(clause braceView) (unwindMergeChainLookupPlan, bool) {
	optional := startsWithKeywordFold(clause.trim().text(), "OPTIONAL MATCH")
	keyword := "MATCH"
	if optional {
		keyword = "OPTIONAL MATCH"
	}
	varName, labels, assignments, anyLabel, ok := parseUnwindNodePatternClauseInternal(clause, keyword, true)
	if !ok {
		return unwindMergeChainLookupPlan{}, false
	}
	return unwindMergeChainLookupPlan{
		varName:          varName,
		labels:           labels,
		matchAssignments: assignments,
		optional:         optional,
		anyLabel:         anyLabel,
	}, true
}

func unwindLookupSetHasUniqueAnchor(schema *storage.SchemaManager, lookupPlan unwindMergeChainLookupPlan) bool {
	if schema == nil || lookupPlan.anyLabel || len(lookupPlan.labels) != 1 || len(lookupPlan.matchAssignments) == 0 {
		return false
	}
	matchedProps := make(map[string]struct{}, len(lookupPlan.matchAssignments))
	for _, assignment := range lookupPlan.matchAssignments {
		matchedProps[assignment.prop] = struct{}{}
	}
	for _, constraint := range schema.GetConstraintsForLabels([]string{lookupPlan.labels[0]}) {
		if constraint.EffectiveEntityType() != storage.ConstraintEntityNode {
			continue
		}
		if constraint.Type != storage.ConstraintUnique && constraint.Type != storage.ConstraintNodeKey {
			continue
		}
		if len(constraint.Properties) == 0 {
			continue
		}
		allConstraintPropsMatched := true
		for _, prop := range constraint.Properties {
			if _, ok := matchedProps[prop]; !ok {
				allConstraintPropsMatched = false
				break
			}
		}
		if allConstraintPropsMatched {
			return true
		}
	}
	return false
}

func unwindMergeChainMatchSetLookupsAreUnique(store storage.Engine, plan unwindMergeChainPlan) bool {
	schema := store.GetSchema()
	for _, step := range plan.steps {
		if step.lookup == nil || len(step.lookup.setAssignments) == 0 {
			continue
		}
		if !unwindLookupSetHasUniqueAnchor(schema, *step.lookup) {
			return false
		}
	}
	return true
}

func unwindMergeLabelsKey(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	sorted := append([]string(nil), labels...)
	sort.Strings(sorted)
	return strings.Join(sorted, ":")
}

func parseUnwindWithClause(ctx context.Context, clause braceView) (unwindMergeChainWithPlan, bool) {
	clause = clause.trim()
	trimmed := clause.text()
	if !startsWithKeywordFold(trimmed, "WITH") {
		return unwindMergeChainWithPlan{}, false
	}
	body := clause.clauseBody("WITH")
	if body.empty() {
		return unwindMergeChainWithPlan{}, false
	}
	projectionPlan := returnProjectionPlanFor(ctx, body.wrap("RETURN ", ""))
	if !projectionPlan.valid || projectionPlan.star || projectionPlan.distinct || projectionPlan.hasAggregate || projectionPlan.modifiers != "" {
		return unwindMergeChainWithPlan{}, false
	}
	plan := unwindMergeChainWithPlan{projection: pipelineRowWith{clause: trimmed, view: clause}}
	for _, projection := range projectionPlan.projections {
		plan.projection.projections = append(plan.projection.projections, pipelineRowProjection{expression: projection.expr, alias: projection.alias, view: projection.view})
		if name := simpleSemanticIdentifier(projection.expr); name != "" && name == projection.alias {
			continue
		}
		if projection.expr == "" || !isSimpleIdentifier(projection.alias) {
			return unwindMergeChainWithPlan{}, false
		}
		plan.assignments = append(plan.assignments, unwindMergeChainWithAssignment{alias: projection.alias, expr: projection.expr})
	}
	return plan, true
}

func parseUnwindWhereClause(clause string) (unwindMergeChainWherePlan, bool) {
	trimmed := strings.TrimSpace(clause)
	if !startsWithKeywordFold(trimmed, "WHERE") {
		return unwindMergeChainWherePlan{}, false
	}
	body := strings.TrimSpace(trimmed[len("WHERE"):])
	if body == "" {
		return unwindMergeChainWherePlan{}, false
	}
	return unwindMergeChainWherePlan{clause: body}, true
}

func (e *StorageExecutor) cachedUnwindMergeChainPlan(ctx context.Context, mutationPart braceView) unwindMergeChainPlan {
	if ctx.Err() != nil {
		return unwindMergeChainPlan{}
	}
	mutationPart = mutationPart.trim()
	key := mutationPart.text()
	cache := e.unwindMergeChainPlanCache
	if cache == nil {
		cache = &unwindMergeChainPlanCache{plans: make(map[string]unwindMergeChainPlan, 128)}
		e.unwindMergeChainPlanCache = cache
	}
	cache.mu.RLock()
	if plan, ok := cache.plans[key]; ok {
		cache.mu.RUnlock()
		return rebaseUnwindMergeChainPlan(plan, mutationPart)
	}
	cache.mu.RUnlock()

	plan := parseUnwindMergeChainPattern(ctx, mutationPart)
	if ctx.Err() != nil {
		return unwindMergeChainPlan{}
	}
	cache.mu.Lock()
	if ctx.Err() != nil {
		cache.mu.Unlock()
		return unwindMergeChainPlan{}
	}
	cache.plans[key] = plan
	cache.mu.Unlock()
	return plan
}

func isComparableInterfaceValue(v interface{}) bool {
	if v == nil {
		return true
	}
	t := reflect.TypeOf(v)
	return t.Comparable()
}

func parseSimpleCountReturn(ctx context.Context, returnPart braceView, mergeVar string) (alias string, ok bool) {
	returnPart = returnPart.trim()
	r := returnPart.text()
	if r == "" {
		return "", true
	}
	if !startsWithKeywordFold(r, "RETURN") {
		return "", false
	}
	plan := returnProjectionPlanFor(ctx, returnPart)
	if !plan.valid || plan.star || plan.distinct || plan.modifiers != "" || len(plan.projections) != 1 || plan.columns[0] == "" {
		return "", false
	}
	projection := plan.projections[0]
	if projection.aggregateName != "count" || projection.distinct || simpleSemanticIdentifier(projection.aggregateExpr) != mergeVar {
		return "", false
	}
	return plan.columns[0], true
}

func parseUnwindBatchCountReturn(ctx context.Context, returnPart braceView) (alias string, ok bool) {
	returnPart = returnPart.trim()
	r := returnPart.text()
	if r == "" {
		return "", true
	}
	if !startsWithKeywordFold(r, "RETURN") {
		return "", false
	}
	plan := returnProjectionPlanFor(ctx, returnPart)
	if !plan.valid || plan.star || plan.distinct || plan.modifiers != "" || len(plan.projections) != 1 || plan.columns[0] == "" {
		return "", false
	}
	projection := plan.projections[0]
	if projection.aggregateName != "count" || projection.distinct {
		return "", false
	}
	if projection.aggregateExpr != "*" && simpleSemanticIdentifier(projection.aggregateExpr) == "" {
		return "", false
	}
	return plan.columns[0], true
}

func countResultRows(result *ExecuteResult) int64 {
	if result == nil || len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return 0
	}
	integer, ok := valueconvert.Int64(result.Rows[0][0], valueconvert.IntegerFloat)
	if !ok {
		return 0
	}
	return integer
}

func splitUnwindMergeChainClauses(input braceView) ([]braceView, bool) {
	input = input.trim()
	trimmed := input.text()
	if trimmed == "" {
		return nil, false
	}
	keywords := []string{"ON CREATE SET", "ON MATCH SET", "OPTIONAL MATCH", "MATCH", "MERGE", "WITH", "WHERE", "SET"}
	var clauses []braceView
	for trimmed != "" {
		keyword := ""
		for _, candidate := range keywords {
			if startsWithKeywordFold(trimmed, candidate) {
				keyword = candidate
				break
			}
		}
		if keyword == "" {
			return nil, false
		}
		next := len(trimmed)
		for _, candidate := range keywords {
			if idx := findKeywordIndexInContext(input.window(len(keyword), len(trimmed)), candidate); idx >= 0 {
				pos := len(keyword) + idx
				if pos < next {
					next = pos
				}
			}
		}
		clauses = append(clauses, input.window(0, next).trim())
		input = input.window(next, len(trimmed)).trim()
		trimmed = input.text()
	}
	return clauses, len(clauses) > 0
}

func parseUnwindMergeRelationshipClause(clause braceView) (unwindMergeChainRelationshipPlan, bool) {
	clause = clause.trim()
	trimmed := clause.text()
	if !startsWithKeywordFold(trimmed, "MERGE") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	restView := clause.clauseBody("MERGE")
	rest := restView.text()
	if !strings.HasPrefix(rest, "(") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	fromEnd := findMatchingParen(rest, 0)
	if fromEnd <= 1 {
		return unwindMergeChainRelationshipPlan{}, false
	}
	fromVar := strings.TrimSpace(rest[1:fromEnd])
	afterFromView := restView.window(fromEnd+1, len(rest)).trim()
	afterFrom := afterFromView.text()
	if !isSimpleIdentifier(fromVar) || !strings.HasPrefix(afterFrom, "-") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	openBracket := strings.Index(afterFrom, "[")
	closeBracket := strings.Index(afterFrom, "]")
	if openBracket < 0 || closeBracket <= openBracket {
		return unwindMergeChainRelationshipPlan{}, false
	}
	relInnerView := afterFromView.window(openBracket+1, closeBracket).trim()
	relInner := relInnerView.text()
	var matchAssignments []unwindSimpleSetAssignment
	if span, found := relInnerView.patternProperty(blankRelationshipExpressions(relInner)); found {
		if strings.TrimSpace(relInner[span.End:]) != "" {
			return unwindMergeChainRelationshipPlan{}, false
		}
		matchProperties := relInnerView.window(span.Start, span.End)
		if strings.TrimSpace(relInner[span.Start+1:span.End-1]) != "" {
			var ok bool
			matchAssignments, ok = parseUnwindSimpleMergeMatchAssignments(matchProperties)
			if !ok {
				return unwindMergeChainRelationshipPlan{}, false
			}
		}
		relInner = strings.TrimSpace(relInner[:span.Start])
	}
	var relVar string
	var relType string
	switch {
	case strings.HasPrefix(relInner, ":"):
		relType = strings.TrimSpace(relInner[1:])
	default:
		colonIdx := strings.Index(relInner, ":")
		if colonIdx <= 0 || colonIdx == len(relInner)-1 {
			return unwindMergeChainRelationshipPlan{}, false
		}
		relVar = strings.TrimSpace(relInner[:colonIdx])
		relType = strings.TrimSpace(relInner[colonIdx+1:])
		if !isSimpleIdentifier(relVar) {
			return unwindMergeChainRelationshipPlan{}, false
		}
	}
	if relType == "" {
		return unwindMergeChainRelationshipPlan{}, false
	}
	afterRel := strings.TrimSpace(afterFrom[closeBracket+1:])
	if !strings.HasPrefix(afterRel, "->") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	afterArrow := strings.TrimSpace(afterRel[2:])
	if !strings.HasPrefix(afterArrow, "(") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	toEnd := findMatchingParen(afterArrow, 0)
	if toEnd != len(afterArrow)-1 {
		return unwindMergeChainRelationshipPlan{}, false
	}
	toVar := strings.TrimSpace(afterArrow[1:toEnd])
	if !isSimpleIdentifier(relType) || !isSimpleIdentifier(toVar) {
		return unwindMergeChainRelationshipPlan{}, false
	}
	return unwindMergeChainRelationshipPlan{
		fromVar:          fromVar,
		toVar:            toVar,
		relVar:           relVar,
		relType:          relType,
		matchAssignments: matchAssignments,
	}, true
}

func splitUnwindCompoundMutationStages(restQuery braceView, unwindParamName, unwindVar string) ([]braceView, bool) {
	restQuery = restQuery.trim()
	trimmed := restQuery.text()
	if trimmed == "" || unwindParamName == "" || unwindVar == "" {
		return nil, false
	}
	skipSpace := func(pos int) int {
		for pos < len(trimmed) && cyphertext.IsWhitespaceAt(trimmed, pos) {
			pos++
		}
		return pos
	}

	stages := make([]braceView, 0, 4)
	start := 0
	searchFrom := 0
	for {
		withIdx := keywordIndexFrom(trimmed, "WITH", searchFrom, defaultKeywordScanOpts(restQuery))
		if withIdx < 0 {
			break
		}

		pos := skipSpace(withIdx + len("WITH"))
		if pos >= len(trimmed) || trimmed[pos] != '$' {
			searchFrom = withIdx + len("WITH")
			continue
		}
		paramName, trailing, ok := parseIdentifierToken(trimmed[pos+1:])
		if !ok || !strings.EqualFold(paramName, unwindParamName) {
			searchFrom = withIdx + len("WITH")
			continue
		}
		nextPos := pos + 1 + len(paramName)
		_ = trailing

		pos = skipSpace(nextPos)
		if !startsWithKeywordFold(trimmed[pos:], "AS") {
			searchFrom = withIdx + len("WITH")
			continue
		}
		pos = skipSpace(pos + len("AS"))
		aliasOne, _, ok := parseIdentifierToken(trimmed[pos:])
		if !ok {
			searchFrom = withIdx + len("WITH")
			continue
		}
		nextPos = pos + len(aliasOne)

		pos = skipSpace(nextPos)
		if !startsWithKeywordFold(trimmed[pos:], "UNWIND") {
			searchFrom = withIdx + len("WITH")
			continue
		}
		pos = skipSpace(pos + len("UNWIND"))
		aliasTwo, _, ok := parseIdentifierToken(trimmed[pos:])
		if !ok || !strings.EqualFold(aliasOne, aliasTwo) {
			searchFrom = withIdx + len("WITH")
			continue
		}
		nextPos = pos + len(aliasTwo)

		pos = skipSpace(nextPos)
		if !startsWithKeywordFold(trimmed[pos:], "AS") {
			searchFrom = withIdx + len("WITH")
			continue
		}
		pos = skipSpace(pos + len("AS"))
		stageVar, _, ok := parseIdentifierToken(trimmed[pos:])
		if !ok || !strings.EqualFold(stageVar, unwindVar) {
			searchFrom = withIdx + len("WITH")
			continue
		}
		nextPos = pos + len(stageVar)

		stage := restQuery.window(start, withIdx).trim()
		if stage.empty() {
			return nil, false
		}
		stages = append(stages, stage)
		start = nextPos
		searchFrom = nextPos
	}
	if len(stages) == 0 {
		return nil, false
	}
	last := restQuery.window(start, len(trimmed)).trim()
	if last.empty() {
		return nil, false
	}
	stages = append(stages, last)
	return stages, len(stages) > 1
}

func (e *StorageExecutor) executeUnwindCompoundMutationBatch(ctx context.Context, unwindVar, unwindParamName string, items []interface{}, restQuery braceView) (*ExecuteResult, bool, error) {
	stages, ok := splitUnwindCompoundMutationStages(restQuery, unwindParamName, unwindVar)
	if !ok {
		return nil, false, nil
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	for _, stage := range stages {
		fast, supported, err := e.executeUnwindMergeChainBatch(ctx, unwindVar, items, stage, stage.window(len(stage.text()), len(stage.text())))
		if err != nil {
			return nil, true, err
		}
		if !supported {
			return nil, false, nil
		}
		if fast != nil {
			addQueryStats(result.Stats, fast.Stats)
		}
	}
	e.markCompoundQueryFastPathUsed()
	return result, true, nil
}

func parseUnwindMergeChainPattern(ctx context.Context, mutationPart braceView) unwindMergeChainPlan {
	mutationPart = mutationPart.trim()
	plan := unwindMergeChainPlan{supported: false, input: mutationPart}
	mutation := mutationPart.text()
	if !startsWithKeywordFold(mutation, "MERGE") && !startsWithKeywordFold(mutation, "MATCH") && !startsWithKeywordFold(mutation, "OPTIONAL MATCH") {
		return plan
	}
	if findKeywordIndexInContext(mutationPart, "CALL") >= 0 ||
		findKeywordIndexInContext(mutationPart, "DELETE") >= 0 ||
		findKeywordIndexInContext(mutationPart, "DETACH") >= 0 ||
		findKeywordIndexInContext(mutationPart, "REMOVE") >= 0 ||
		findKeywordIndexInContext(mutationPart, "FOREACH") >= 0 ||
		findKeywordIndexInContext(mutationPart, "UNWIND") >= 0 ||
		findKeywordIndexInContext(mutationPart, "RETURN") >= 0 {
		return plan
	}
	clauses, ok := splitUnwindMergeChainClauses(mutationPart)
	if !ok || len(clauses) == 0 {
		return plan
	}
	boundVars := make(map[string]struct{})
	hasMutation := false
	for i := 0; i < len(clauses); i++ {
		clauseView := clauses[i]
		clause := clauseView.text()
		if !startsWithKeywordFold(clause, "MERGE") {
			lookupPlan, ok := parseUnwindLookupClause(clauseView)
			if !ok && i+1 < len(clauses) && startsWithKeywordFold(clauses[i+1].text(), "WHERE") {
				// MATCH (v {…}) WHERE v:A|B, the rewritten MATCH (v:A|B {…}) (#860).
				if lookupPlan, ok = parseUnwindLookupClause(mutationPart.assembleCode([]braceView{clauseView, clauses[i+1]}, " ")); ok {
					i++
				}
			}
			if ok {
				for i+1 < len(clauses) && startsWithKeywordFold(clauses[i+1].text(), "SET") {
					parsed, ok := parseUnwindSimpleSetAssignments(clauses[i+1].clauseBody("SET").text(), lookupPlan.varName)
					if !ok {
						return unwindMergeChainPlan{}
					}
					lookupPlan.setAssignments = append(lookupPlan.setAssignments, parsed...)
					hasMutation = true
					i++
				}
				plan.steps = append(plan.steps, unwindMergeChainStep{lookup: &lookupPlan})
				boundVars[lookupPlan.varName] = struct{}{}
				continue
			}
			if withPlan, ok := parseUnwindWithClause(ctx, clauseView); ok {
				for _, assignment := range withPlan.assignments {
					boundVars[assignment.alias] = struct{}{}
				}
				plan.steps = append(plan.steps, unwindMergeChainStep{with: &withPlan})
				continue
			}
			if wherePlan, ok := parseUnwindWhereClause(clause); ok {
				plan.steps = append(plan.steps, unwindMergeChainStep{where: &wherePlan})
				continue
			}
			return unwindMergeChainPlan{}
		}
		mergeVar, labels, assignments, ok := parseSimpleUnwindMergeClause(clauseView)
		if ok {
			nodePlan := &unwindMergeChainNodePlan{
				mergeVar:         mergeVar,
				labels:           labels,
				matchAssignments: assignments,
			}
			for i+1 < len(clauses) {
				nextClause := clauses[i+1].text()
				switch {
				case startsWithKeywordFold(nextClause, "SET"):
					parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(nextClause[len("SET"):]), mergeVar)
					if !ok {
						return unwindMergeChainPlan{}
					}
					nodePlan.setAssignments = append(nodePlan.setAssignments, parsed...)
					i++
				case startsWithKeywordFold(nextClause, "ON CREATE SET"):
					parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(nextClause[len("ON CREATE SET"):]), mergeVar)
					if !ok {
						return unwindMergeChainPlan{}
					}
					nodePlan.onCreateAssignments = append(nodePlan.onCreateAssignments, parsed...)
					i++
				case startsWithKeywordFold(nextClause, "ON MATCH SET"):
					parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(nextClause[len("ON MATCH SET"):]), mergeVar)
					if !ok {
						return unwindMergeChainPlan{}
					}
					nodePlan.onMatchAssignments = append(nodePlan.onMatchAssignments, parsed...)
					i++
				default:
					goto nodeDone
				}
			}
		nodeDone:
			plan.steps = append(plan.steps, unwindMergeChainStep{node: nodePlan})
			boundVars[mergeVar] = struct{}{}
			hasMutation = true
			continue
		}
		relPlan, ok := parseUnwindMergeRelationshipClause(clauseView)
		if !ok {
			return unwindMergeChainPlan{}
		}
		for i+1 < len(clauses) {
			nextClause := clauses[i+1].text()
			if !startsWithKeywordFold(nextClause, "SET") || relPlan.relVar == "" {
				break
			}
			parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(nextClause[len("SET"):]), relPlan.relVar)
			if !ok {
				return unwindMergeChainPlan{}
			}
			relPlan.setAssignments = append(relPlan.setAssignments, parsed...)
			i++
		}
		if _, exists := boundVars[relPlan.fromVar]; !exists {
			return unwindMergeChainPlan{}
		}
		if _, exists := boundVars[relPlan.toVar]; !exists {
			return unwindMergeChainPlan{}
		}
		for i+1 < len(clauses) && startsWithKeywordFold(clauses[i+1].text(), "SET") {
			if relPlan.relVar == "" {
				return unwindMergeChainPlan{}
			}
			parsed, ok := parseUnwindSimpleSetAssignments(clauses[i+1].clauseBody("SET").text(), relPlan.relVar)
			if !ok {
				return unwindMergeChainPlan{}
			}
			relPlan.setAssignments = append(relPlan.setAssignments, parsed...)
			i++
		}
		plan.steps = append(plan.steps, unwindMergeChainStep{relationship: &relPlan})
		hasMutation = true
	}
	if len(plan.steps) == 0 || !hasMutation {
		return unwindMergeChainPlan{}
	}
	plan.simple = len(plan.steps) == 1 && plan.steps[0].node != nil
	plan.supported = true
	return plan
}

func canonicalUnwindMergeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]interface{}, 0, len(keys))
		for _, k := range keys {
			out = append(out, []interface{}{k, canonicalUnwindMergeValue(val[k])})
		}
		return map[string]interface{}{"type": "map", "entries": out}
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, item := range val {
			out[i] = canonicalUnwindMergeValue(item)
		}
		return map[string]interface{}{"type": "list", "items": out}
	case string:
		return map[string]interface{}{"type": "string", "value": val}
	case bool:
		return map[string]interface{}{"type": "bool", "value": val}
	case int:
		return map[string]interface{}{"type": "int", "value": strconv.FormatInt(int64(val), 10)}
	case int8:
		return map[string]interface{}{"type": "int8", "value": strconv.FormatInt(int64(val), 10)}
	case int16:
		return map[string]interface{}{"type": "int16", "value": strconv.FormatInt(int64(val), 10)}
	case int32:
		return map[string]interface{}{"type": "int32", "value": strconv.FormatInt(int64(val), 10)}
	case int64:
		return map[string]interface{}{"type": "int64", "value": strconv.FormatInt(val, 10)}
	case uint:
		return map[string]interface{}{"type": "uint", "value": strconv.FormatUint(uint64(val), 10)}
	case uint8:
		return map[string]interface{}{"type": "uint8", "value": strconv.FormatUint(uint64(val), 10)}
	case uint16:
		return map[string]interface{}{"type": "uint16", "value": strconv.FormatUint(uint64(val), 10)}
	case uint32:
		return map[string]interface{}{"type": "uint32", "value": strconv.FormatUint(uint64(val), 10)}
	case uint64:
		return map[string]interface{}{"type": "uint64", "value": strconv.FormatUint(val, 10)}
	case float32:
		return map[string]interface{}{"type": "float32", "value": strconv.FormatFloat(float64(val), 'g', -1, 32)}
	case float64:
		return map[string]interface{}{"type": "float64", "value": strconv.FormatFloat(val, 'g', -1, 64)}
	case nil:
		return map[string]interface{}{"type": "nil"}
	default:
		rv := reflect.ValueOf(v)
		if rv.IsValid() && rv.Kind() == reflect.Slice {
			out := make([]interface{}, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				out[i] = canonicalUnwindMergeValue(rv.Index(i).Interface())
			}
			return map[string]interface{}{"type": rv.Type().String(), "items": out}
		}
		return map[string]interface{}{"type": fmt.Sprintf("%T", v), "value": fmt.Sprintf("%#v", v)}
	}
}

func unwindMergeKey(label string, props map[string]interface{}) string {
	propNames := mergePropertyNamesSorted(props)
	entries := make([]interface{}, 0, len(propNames))
	for _, prop := range propNames {
		entries = append(entries, []interface{}{prop, canonicalUnwindMergeValue(props[prop])})
	}
	encoded, err := json.Marshal(map[string]interface{}{
		"label":   label,
		"entries": entries,
	})
	if err != nil {
		return fmt.Sprintf("%s|%v", label, propNames)
	}
	return string(encoded)
}

// requireUnwindMergeChainParameters reports a SET value in the batch plan that
// is a bare $name parameter not supplied with the query, once per statement,
// as requireSetParameter does on every other SET route.
func requireUnwindMergeChainParameters(ctx context.Context, plan unwindMergeChainPlan) error {
	check := func(assignments []unwindSimpleSetAssignment) error {
		for _, assignment := range assignments {
			if err := requireSetParameter(ctx, assignment.expr); err != nil {
				return err
			}
		}
		return nil
	}
	for _, step := range plan.steps {
		var groups [][]unwindSimpleSetAssignment
		if step.node != nil {
			groups = append(groups, step.node.setAssignments, step.node.onCreateAssignments, step.node.onMatchAssignments)
		}
		if step.lookup != nil {
			groups = append(groups, step.lookup.setAssignments)
		}
		if step.relationship != nil {
			groups = append(groups, step.relationship.setAssignments)
		}
		for _, group := range groups {
			if err := check(group); err != nil {
				return err
			}
		}
	}
	return nil
}

// setNodePropertyIfChanged writes one SET value through setNodeProperty and
// reports whether the node changed (a null removes the key).
func setNodePropertyIfChanged(node *storage.Node, prop string, value interface{}) bool {
	cur, exists := node.Properties[prop]
	if value == nil && !exists || value != nil && exists && reflect.DeepEqual(cur, value) {
		return false
	}
	setNodeProperty(node, prop, value)
	return true
}

// setRelationshipPropertyIfChanged is setNodePropertyIfChanged for a
// relationship.
func setRelationshipPropertyIfChanged(edge *storage.Edge, prop string, value interface{}) bool {
	cur, exists := edge.Properties[prop]
	if value == nil && !exists || value != nil && exists && reflect.DeepEqual(cur, value) {
		return false
	}
	setRelationshipProperty(edge, prop, value)
	return true
}

func (e *StorageExecutor) executeUnwindMergeChainBatch(ctx context.Context, unwindVar string, items []interface{}, mutationPart, returnPart braceView) (*ExecuteResult, bool, error) {
	plan := e.cachedUnwindMergeChainPlan(ctx, mutationPart)
	if !plan.supported {
		return nil, false, nil
	}
	if _, supported := parseUnwindBatchCountReturn(ctx, returnPart); !supported {
		return nil, false, nil
	}
	if err := requireUnwindMergeChainParameters(ctx, plan); err != nil {
		return nil, true, err
	}
	e.markUnwindMergeChainBatchUsed()
	if plan.simple {
		e.markUnwindSimpleMergeBatchUsed()
	}
	original := braceView{syntax: mutationPart.syntax, source: mutationPart.source.OriginalView()}
	return e.executeUnwindRowsPipeline(ctx, unwindVar, items, original.assembleCode([]braceView{mutationPart, returnPart}, " ").trim())
}

func (e *StorageExecutor) executeUnwindFixedChainLinkBatch(ctx context.Context, unwindVar string, items []interface{}, restQuery braceView) (*ExecuteResult, bool, error) {
	returnIdx := topLevelKeywordIndex(restQuery, "RETURN")
	if returnIdx <= 0 {
		return nil, false, nil
	}
	mutationPart := restQuery.window(0, returnIdx).trim()
	returnPart := restQuery.window(returnIdx, len(restQuery.text())).trim()
	_, ok := parseSimpleCountReturn(ctx, returnPart, "o")
	if !ok {
		return nil, false, nil
	}

	normalized := mutationPart.joinFields()

	type rootSpec struct {
		varName  string
		label    string
		byID     bool
		propName string
		rowField string
	}
	type hopSpec struct {
		label      string
		propName   string
		rowField   string
		depthByVar map[string]int
	}
	parseRowFieldRef := func(expr string) (string, string, bool) {
		trimmed := strings.TrimSpace(expr)
		dotIdx := strings.Index(trimmed, ".")
		if dotIdx <= 0 || dotIdx == len(trimmed)-1 {
			return "", "", false
		}
		base := strings.TrimSpace(trimmed[:dotIdx])
		field := strings.TrimSpace(trimmed[dotIdx+1:])
		if !isSimpleIdentifier(base) || !isSimpleIdentifier(field) {
			return "", "", false
		}
		return base, field, true
	}
	parseIdentifierNodePattern := func(pattern braceView) (string, string, string, string, bool) {
		pattern = pattern.trim()
		trimmed := pattern.text()
		if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
			pattern = pattern.window(1, len(trimmed)-1).trim()
			trimmed = pattern.text()
		}
		head := trimmed
		props := pattern.window(0, 0)
		if span, found := pattern.patternProperty(blankRelationshipExpressions(trimmed)); found {
			if span.End != len(trimmed) {
				return "", "", "", "", false
			}
			head = strings.TrimSpace(trimmed[:span.Start])
			props = pattern.window(span.Start, span.End)
		}
		parts := strings.Split(head, ":")
		if len(parts) != 2 {
			return "", "", "", "", false
		}
		varName := strings.TrimSpace(parts[0])
		label := strings.TrimSpace(parts[1])
		if !isSimpleIdentifier(varName) || !isSimpleIdentifier(label) {
			return "", "", "", "", false
		}
		if props.empty() || strings.TrimSpace(props.text()[1:len(props.text())-1]) == "" {
			return varName, label, "", "", true
		}
		entries, valid := props.keyedEntries()
		if !valid || len(entries) != 1 {
			return "", "", "", "", false
		}
		propName, propExpr := entries[0].name, entries[0].value.text()
		if !isSimpleIdentifier(propName) {
			return "", "", "", "", false
		}
		return varName, label, propName, propExpr, true
	}
	parseRowFieldDepthExpr := func(expr string) (string, string, int, bool) {
		parts := strings.SplitN(strings.TrimSpace(expr), "+", 2)
		if len(parts) != 2 {
			return "", "", 0, false
		}
		rowVar, rowField, ok := parseRowFieldRef(parts[0])
		if !ok {
			return "", "", 0, false
		}
		suffix := strings.TrimSpace(parts[1])
		if len(suffix) < 4 {
			return "", "", 0, false
		}
		if (!isCypherSingleQuoteByte(suffix[0]) || !isCypherSingleQuoteByte(suffix[len(suffix)-1])) && (!isCypherDoubleQuoteByte(suffix[0]) || !isCypherDoubleQuoteByte(suffix[len(suffix)-1])) {
			return "", "", 0, false
		}
		literal := suffix[1 : len(suffix)-1]
		if !strings.HasPrefix(literal, ":") {
			return "", "", 0, false
		}
		depth, err := strconv.Atoi(strings.TrimSpace(literal[1:]))
		if err != nil || depth <= 0 {
			return "", "", 0, false
		}
		return rowVar, rowField, depth, true
	}
	parseMergeClause := func(clause string) (string, string, string, bool) {
		trimmed := strings.TrimSpace(clause)
		if !strings.HasPrefix(upperASCII(trimmed), "MERGE ") {
			return "", "", "", false
		}
		body := strings.TrimSpace(trimmed[len("MERGE "):])
		if !strings.HasPrefix(body, "(") {
			return "", "", "", false
		}
		fromEnd := findMatchingParen(body, 0)
		if fromEnd <= 1 {
			return "", "", "", false
		}
		fromVar := strings.TrimSpace(body[1:fromEnd])
		rest := strings.TrimSpace(body[fromEnd+1:])
		if !isSimpleIdentifier(fromVar) || !strings.HasPrefix(rest, "-") {
			return "", "", "", false
		}
		openBracket := strings.Index(rest, "[")
		closeBracket := strings.Index(rest, "]")
		if openBracket < 0 || closeBracket <= openBracket {
			return "", "", "", false
		}
		relInner := strings.TrimSpace(rest[openBracket+1 : closeBracket])
		if !strings.HasPrefix(relInner, ":") {
			return "", "", "", false
		}
		relType := strings.TrimSpace(relInner[1:])
		afterRel := strings.TrimSpace(rest[closeBracket+1:])
		if !strings.HasPrefix(afterRel, "->") {
			return "", "", "", false
		}
		afterArrow := strings.TrimSpace(afterRel[2:])
		if !strings.HasPrefix(afterArrow, "(") {
			return "", "", "", false
		}
		toEnd := findMatchingParen(afterArrow, 0)
		if toEnd != len(afterArrow)-1 {
			return "", "", "", false
		}
		toVar := strings.TrimSpace(afterArrow[1:toEnd])
		if !isSimpleIdentifier(relType) || !isSimpleIdentifier(toVar) {
			return "", "", "", false
		}
		return lowerASCII(fromVar), relType, lowerASCII(toVar), true
	}
	splitMutationClauses := func(input braceView) ([]braceView, bool) {
		input = input.trim()
		trimmed := input.text()
		if trimmed == "" {
			return nil, false
		}
		var clauses []braceView
		for trimmed != "" {
			upper := upperASCII(trimmed)
			var keyword string
			switch {
			case strings.HasPrefix(upper, "MATCH "):
				keyword = "MATCH"
			case strings.HasPrefix(upper, "MERGE "):
				keyword = "MERGE"
			default:
				return nil, false
			}
			next := len(trimmed)
			for _, candidate := range []string{"MATCH", "MERGE"} {
				if idx := findKeywordIndexInContext(input.window(len(keyword), len(trimmed)), candidate); idx >= 0 {
					pos := len(keyword) + idx
					if pos < next {
						next = pos
					}
				}
			}
			clauses = append(clauses, input.window(0, next).trim())
			input = input.window(next, len(trimmed)).trim()
			trimmed = input.text()
		}
		return clauses, true
	}
	mutationClauses, ok := splitMutationClauses(normalized)
	if !ok || len(mutationClauses) < 3 {
		return nil, false, nil
	}
	parseRootSpec := func(clause braceView) (rootSpec, bool) {
		clause = clause.trim()
		trimmed := clause.text()
		if !strings.HasPrefix(upperASCII(trimmed), "MATCH ") {
			return rootSpec{}, false
		}
		bodyView := clause.window(len("MATCH "), len(trimmed)).trim()
		body := bodyView.text()
		if !strings.HasPrefix(body, "(") {
			return rootSpec{}, false
		}
		closeIdx := findMatchingParen(body, 0)
		if closeIdx < 0 {
			return rootSpec{}, false
		}
		varName, label, propName, propExpr, ok := parseIdentifierNodePattern(bodyView.window(0, closeIdx+1))
		if !ok {
			return rootSpec{}, false
		}
		rest := strings.TrimSpace(body[closeIdx+1:])
		var out rootSpec
		out.varName = lowerASCII(varName)
		out.label = label
		if propName != "" {
			rowVar, rowField, ok := parseRowFieldRef(propExpr)
			if !ok || !strings.EqualFold(rowVar, unwindVar) {
				return rootSpec{}, false
			}
			out.propName = propName
			out.rowField = rowField
			return out, rest == ""
		}
		if rest == "" || !strings.HasPrefix(upperASCII(rest), "WHERE ") {
			return rootSpec{}, false
		}
		whereExpr := strings.TrimSpace(rest[len("WHERE "):])
		parts := strings.SplitN(whereExpr, "=", 2)
		if len(parts) != 2 {
			return rootSpec{}, false
		}
		left := strings.TrimSpace(parts[0])
		right := strings.TrimSpace(parts[1])
		lowerLeft := lowerASCII(left)
		if !strings.HasPrefix(lowerLeft, "elementid(") || !strings.HasSuffix(left, ")") {
			return rootSpec{}, false
		}
		whereVar := strings.TrimSpace(left[len("elementId(") : len(left)-1])
		rowVar, rowField, ok := parseRowFieldRef(right)
		if !ok || !strings.EqualFold(whereVar, varName) || !strings.EqualFold(rowVar, unwindVar) {
			return rootSpec{}, false
		}
		out.byID = true
		out.rowField = rowField
		return out, true
	}
	root, ok := parseRootSpec(mutationClauses[0])
	if !ok {
		return nil, false, nil
	}
	parseHopSpec := func(clauses []braceView) (hopSpec, bool) {
		var out hopSpec
		out.depthByVar = make(map[string]int)
		seenDepth := make(map[int]bool)
		for _, clause := range clauses {
			clause = clause.trim()
			trimmed := clause.text()
			if !strings.HasPrefix(upperASCII(trimmed), "MATCH ") {
				return hopSpec{}, false
			}
			bodyView := clause.window(len("MATCH "), len(trimmed)).trim()
			body := bodyView.text()
			if strings.Contains(upperASCII(body), " WHERE ") {
				return hopSpec{}, false
			}
			varName, label, propName, propExpr, ok := parseIdentifierNodePattern(bodyView)
			if !ok || propName == "" {
				return hopSpec{}, false
			}
			rowVar, rowField, depth, ok := parseRowFieldDepthExpr(propExpr)
			if !ok || !strings.EqualFold(rowVar, unwindVar) {
				return hopSpec{}, false
			}
			if out.label == "" {
				out.label = label
				out.propName = propName
				out.rowField = rowField
			}
			if out.label != label || out.propName != propName || out.rowField != rowField {
				return hopSpec{}, false
			}
			lowerVar := lowerASCII(varName)
			if prev, exists := out.depthByVar[lowerVar]; exists && prev != depth {
				return hopSpec{}, false
			}
			out.depthByVar[lowerVar] = depth
			seenDepth[depth] = true
		}
		if len(out.depthByVar) == 0 {
			return hopSpec{}, false
		}
		for i := 1; i <= len(seenDepth); i++ {
			if !seenDepth[i] {
				return hopSpec{}, false
			}
		}
		return out, true
	}
	firstMergeIdx := -1
	for idx, clause := range mutationClauses {
		if strings.HasPrefix(upperASCII(clause.text()), "MERGE ") {
			firstMergeIdx = idx
			break
		}
	}
	if firstMergeIdx <= 1 || firstMergeIdx >= len(mutationClauses) {
		return nil, false, nil
	}
	hop, ok := parseHopSpec(mutationClauses[1:firstMergeIdx])
	if !ok {
		return nil, false, nil
	}
	relType := ""
	nextByFrom := make(map[string]string)
	for _, clause := range mutationClauses[firstMergeIdx:] {
		from, rel, to, ok := parseMergeClause(clause.text())
		if !ok {
			return nil, false, nil
		}
		if relType == "" {
			relType = rel
		}
		if rel != relType {
			return nil, false, nil
		}
		if prev, exists := nextByFrom[from]; exists && prev != to {
			return nil, false, nil
		}
		nextByFrom[from] = to
	}
	firstHopVar, ok := nextByFrom[root.varName]
	if !ok {
		return nil, false, nil
	}
	chainVars := make([]string, 0, len(hop.depthByVar))
	seenVar := make(map[string]bool)
	cur := firstHopVar
	for {
		if seenVar[cur] {
			return nil, false, nil
		}
		seenVar[cur] = true
		chainVars = append(chainVars, cur)
		next, ok := nextByFrom[cur]
		if !ok {
			break
		}
		cur = next
	}
	if len(chainVars) == 0 || len(chainVars) != len(hop.depthByVar) {
		return nil, false, nil
	}
	for _, v := range chainVars {
		if _, ok := hop.depthByVar[v]; !ok {
			return nil, false, nil
		}
	}

	result, handled, err := e.executeUnwindRowsPipeline(ctx, unwindVar, items, restQuery)
	if handled && err == nil {
		e.markUnwindFixedChainLinkBatchUsed()
	}
	return result, handled, err
}

func rewriteUnwindCorrelationToIn(input braceView, variable string, paramName string) (braceView, bool) {
	query := input.text()
	if strings.TrimSpace(query) == "" || strings.TrimSpace(variable) == "" || strings.TrimSpace(paramName) == "" {
		return braceView{}, false
	}
	// Preserve join correlation semantics:
	//   a.prop = unwindVar AND b.prop = unwindVar
	// => a.prop IN $items AND b.prop = a.prop
	// so we do not produce cross-key cartesian joins.
	type equalityMatch struct {
		start  int
		end    int
		lhsEnd int
	}
	matches := make([]equalityMatch, 0, 2)
	for i := 0; i < len(query); i++ {
		if end, quoted := skipCypherLexicalToken(query, i); quoted {
			i = end - 1
			continue
		}
		ch := query[i]
		if ch != '=' {
			continue
		}

		lhsEnd := i
		for lhsEnd > 0 && cyphertext.IsWhitespaceAt(query, lhsEnd-1) {
			lhsEnd--
		}
		lhsStart := lhsEnd
		for lhsStart > 0 {
			prev := query[lhsStart-1]
			if cyphertext.NamePartAt(query, lhsStart-1) || prev == '.' {
				lhsStart--
				continue
			}
			break
		}
		lhs := strings.TrimSpace(query[lhsStart:lhsEnd])
		if lhs == "" || !isSimplePropertyReference(lhs) {
			continue
		}

		rhsStart := i + 1
		for rhsStart < len(query) && cyphertext.IsWhitespaceAt(query, rhsStart) {
			rhsStart++
		}
		if rhsStart+len(variable) > len(query) || !strings.EqualFold(query[rhsStart:rhsStart+len(variable)], variable) {
			continue
		}
		rhsEnd := rhsStart + len(variable)
		if rhsEnd < len(query) && cyphertext.NamePartAt(query, rhsEnd) {
			continue
		}
		matches = append(matches, equalityMatch{start: lhsStart, end: rhsEnd, lhsEnd: lhsEnd})
		i = rhsEnd - 1
	}
	if len(matches) == 0 {
		return braceView{}, false
	}
	firstExpr := input.window(matches[0].start, matches[0].lhsEnd)

	var pieces []braceView
	cursor := 0
	for i, m := range matches {
		pieces = append(pieces, input.window(cursor, m.start))
		lhs := input.window(m.start, m.lhsEnd)
		if i == 0 {
			pieces = append(pieces, lhs.wrap("", " IN $"+paramName))
		} else {
			pieces = append(pieces, lhs.wrap("", " = "), firstExpr)
		}
		cursor = m.end
	}
	pieces = append(pieces, input.window(cursor, len(query)))
	return input.assembleCode(pieces, ""), true
}

func isSimplePropertyReference(expr string) bool {
	parts := strings.Split(expr, ".")
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		if !isSimpleIdentifier(strings.TrimSpace(part)) {
			return false
		}
	}
	return true
}

func rewriteTopLevelMultiMatchToCartesianMatch(query braceView) braceView {
	input := query
	query = query.trim()
	trimmed := query.text()
	if !strings.HasPrefix(upperASCII(trimmed), "MATCH ") {
		return input
	}
	returnIdx := topLevelKeywordIndex(query, "RETURN")
	if returnIdx <= 0 {
		return input
	}
	body := query.window(0, returnIdx).trim()
	tail := query.window(returnIdx, len(trimmed)).trim()
	whereIdx := findKeywordIndex(body, "WHERE")
	if whereIdx <= 0 {
		return input
	}
	patterns := body.window(0, whereIdx).trim()
	whereAndMutations := body.window(whereIdx+len("WHERE"), len(body.text())).trim()
	whereClause := whereAndMutations
	for _, keyword := range []string{"CREATE", "MERGE", "SET", "DELETE", "REMOVE"} {
		if index := findKeywordIndexInContext(whereAndMutations, keyword); index >= 0 {
			whereClause = whereAndMutations.window(0, index).trim()
			tail = query.assembleCode([]braceView{whereAndMutations.window(index, len(whereAndMutations.text())).trim(), tail}, " ")
			break
		}
	}
	if patterns.empty() || whereClause.empty() {
		return input
	}

	upperPatterns := upperASCII(patterns.text())
	if strings.Count(upperPatterns, "MATCH ") != 2 {
		return input
	}
	first := patterns.window(len("MATCH "), len(patterns.text())).trim()
	secondIdx := findKeywordIndex(first, "MATCH")
	if secondIdx <= 0 {
		return input
	}
	left := first.window(0, secondIdx).trim()
	right := first.window(secondIdx+len("MATCH"), len(first.text())).trim()
	if left.empty() || right.empty() {
		return input
	}
	return query.assembleCode([]braceView{left.wrap("MATCH ", ", "), right.wrap("", " WHERE "), whereClause.wrap("", " "), tail}, "")
}

func canApplySetBasedUnwindRewrite(view braceView, items []interface{}) bool {
	query := view.text()
	if strings.TrimSpace(query) == "" || len(items) == 0 {
		return false
	}
	upper := upperASCII(query)
	// Keep rewrite on read-only MATCH ... RETURN count(...) pipelines.
	// Mutation clauses with correlated values (SET += row.props, MERGE/CREATE/DELETE/REMOVE)
	// must execute per-row to preserve semantics.
	if findKeywordIndex(view, "CREATE") >= 0 ||
		findKeywordIndex(view, "MERGE") >= 0 ||
		findKeywordIndex(view, "SET") >= 0 ||
		findKeywordIndex(view, "DELETE") >= 0 ||
		findKeywordIndex(view, "REMOVE") >= 0 {
		return false
	}
	if !strings.Contains(upper, "RETURN") || !strings.Contains(upper, "COUNT(") {
		return false
	}
	// Rewrites should preserve semantics. We only apply when unwind items are
	// distinct comparable values so IN-list matching does not collapse duplicates.
	return unwindItemsAreDistinctComparable(items)
}

func unwindItemsAreDistinctComparable(items []interface{}) bool {
	seen := map[interface{}]struct{}{}
	for _, it := range items {
		if it == nil {
			if _, exists := seen[nil]; exists {
				return false
			}
			seen[nil] = struct{}{}
			continue
		}
		rv := reflect.ValueOf(it)
		if !rv.IsValid() || !rv.Type().Comparable() {
			return false
		}
		if _, exists := seen[it]; exists {
			return false
		}
		seen[it] = struct{}{}
	}
	return true
}

// normalizeMultiMatchWhereClauses rewrites a chain of required MATCH
// clauses whose WHEREs sit between MATCH clauses into one terminal WHERE:
//  1. MATCH A WHERE wa MATCH B RETURN ...
//     -> MATCH A MATCH B WHERE wa RETURN ...
//  2. MATCH A WHERE wa MATCH B WHERE wb MATCH C RETURN ...
//     -> MATCH A MATCH B MATCH C WHERE wa AND wb RETURN ...
//
// For required MATCH clauses the predicates filter the same rows wherever
// they stand, so conjoining them after the last MATCH is equivalent. A
// predicate with a top-level OR or XOR is parenthesized so AND doesn't bind
// into it. Anything but MATCH clauses before RETURN (OPTIONAL MATCH, WITH,
// UNWIND, writes) leaves the query unchanged.
func normalizeMultiMatchWhereClauses(query braceView) braceView {
	input := query
	query = query.trim()
	trimmed := query.text()
	if !strings.HasPrefix(upperASCII(trimmed), "MATCH ") {
		return input
	}
	// OPTIONAL MATCH has left-join semantics: its WHERE can't move.
	if findKeywordIndex(query, "OPTIONAL MATCH") >= 0 {
		return input
	}
	returnIdx := topLevelKeywordIndex(query, "RETURN")
	if returnIdx <= 0 {
		return input
	}
	mainPart := query.window(0, returnIdx).trim()
	tailPart := query.window(returnIdx, len(trimmed)).trim()
	for _, keyword := range []string{"WITH", "UNWIND", "CREATE", "MERGE", "SET", "DELETE", "REMOVE", "CALL", "FOREACH"} {
		if len(findAllTopLevelPipelineKeywordPositions(mainPart, keyword)) > 0 {
			return input
		}
	}
	starts := findAllTopLevelPipelineKeywordPositions(mainPart, "MATCH")
	if len(starts) < 2 || starts[0] != 0 {
		return input
	}
	patterns := make([]braceView, 0, len(starts))
	predicates := make([]braceView, 0, len(starts))
	movedWhere := false
	for index, start := range starts {
		end := len(mainPart.text())
		if index+1 < len(starts) {
			end = starts[index+1]
		}
		clause := mainPart.window(start+len("MATCH"), end).trim()
		pattern := clause
		if whereIdx := topLevelKeywordIndex(clause, "WHERE"); whereIdx >= 0 {
			pattern = clause.window(0, whereIdx).trim()
			predicate := clause.window(whereIdx+len("WHERE"), len(clause.text())).trim()
			if predicate.empty() {
				return input
			}
			if topLevelKeywordIndex(predicate, "OR") >= 0 || topLevelKeywordIndex(predicate, "XOR") >= 0 {
				predicate = predicate.wrap("(", ")")
			}
			predicates = append(predicates, predicate)
			movedWhere = movedWhere || index+1 < len(starts)
		}
		if pattern.empty() {
			return input
		}
		patterns = append(patterns, pattern)
	}
	if !movedWhere {
		return input
	}

	var pieces []braceView
	for _, pattern := range patterns {
		pieces = append(pieces, pattern.wrap("MATCH ", " "))
	}
	pieces = append(pieces, query.assembleCode(predicates, " AND ").wrap("WHERE ", " "), tailPart)
	return query.assembleCode(pieces, "")
}

// ========================================
// UNION Clause
// ========================================

// executeUnion handles UNION / UNION ALL
// Supports both single UNION (query1 UNION query2) and chained UNIONs (query1 UNION query2 UNION query3 ...)
// Handles UNION with flexible whitespace (spaces, newlines, tabs)
func (e *StorageExecutor) executeUnion(ctx context.Context, view braceView, unionAll bool, runBranch func(context.Context, string) (*ExecuteResult, error)) (*ExecuteResult, error) {
	cypher := view.text()
	ctx = withBraceView(ctx, view)
	queries, splitAll, mixed, ok := parseTopLevelUnionBranchSpans(ctx, view)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !ok || len(queries) < 2 {
		// A failed product is diagnostic framing only: no branch executes.
		if syntax, parseErr := cypherantlr.ParseSyntax(ctx, cypher, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource); parseErr != nil && syntax.Valid() {
			body := queryBodyFromSyntax(view, statementSyntaxQuery(syntax))
			if len(body.branches) > 1 {
				for index, branch := range body.branches {
					text := cypher[branch.start:branch.end]
					if _, err := cypherantlr.ParseSyntax(ctx, text, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource); err != nil {
						return nil, localizedError(localization.CypherResidualUnionBranchFailed(index+1, truncateQuery(text, 50), err), err)
					}
				}
			}
		}
		return nil, localizedError(localization.CypherResidualUnionClauseNotFound(truncateQuery(cypher, 80)), nil)
	}
	if mixed {
		return nil, unionMixedError()
	}
	if len(queries) < 2 {
		return nil, localizedError(localization.CypherResidualUnionClauseNotFound(truncateQuery(cypher, 80)), nil)
	}
	if unionAll != splitAll {
		if unionAll {
			return nil, localizedError(localization.CypherResidualUnionAllClauseNotFound(truncateQuery(cypher, 80)), nil)
		}
		return nil, localizedError(localization.CypherResidualUnionClauseNotFound(truncateQuery(cypher, 80)), nil)
	}

	// Execute all queries and combine results
	var combinedResult *ExecuteResult
	var seen cypherValueIndex // Shared equivalence for UNION and DISTINCT.

	for i, span := range queries {
		branch := view.window(span.Start, span.End)
		query := branch.text()
		branchCtx := withBraceView(ctx, branch)
		var result *ExecuteResult
		var err error
		if runBranch == nil {
			result, err = e.executeInternal(branchCtx, query, nil)
		} else {
			result, err = runBranch(branchCtx, query)
		}
		if err != nil {
			return nil, localizedError(localization.CypherResidualUnionBranchFailed(i+1, truncateQuery(query, 50), err), err)
		}
		// Some execution branches can return empty column metadata when no rows are produced,
		// even though the query has an explicit RETURN/YIELD projection. For UNION semantics we
		// must validate/provide branch column shapes deterministically.
		if len(result.Columns) == 0 {
			columns, err := e.resultColumns(branchCtx, statementBraceView(branchCtx, query), e.resultIncomingScope(), cypherGrammarVersion(query) == "25")
			if err != nil {
				return nil, err
			}
			result.Columns = columns
		}

		if combinedResult == nil {
			// First query - initialize result
			combinedResult = &ExecuteResult{
				Columns: result.Columns,
				Rows:    make([][]interface{}, 0),
				Stats:   &QueryStats{},
			}
		} else if order, sameNames := unionColumnOrder(combinedResult.Columns, result.Columns); sameNames {
			// The same names in another order: the branch's rows are
			// reordered into the first branch's columns, as in Neo4j 5.26.
			if order != nil {
				result.Rows = reorderUnionRows(result.Rows, order)
			}
		} else {
			return nil, unionColumnsError(combinedResult.Columns, result.Columns)
		}

		addQueryStats(combinedResult.Stats, result.Stats)
		// Add rows from this query
		if unionAll {
			// UNION ALL - include all rows
			combinedResult.Rows = append(combinedResult.Rows, result.Rows...)
		} else {
			// UNION (distinct) - deduplicate rows
			for _, row := range result.Rows {
				if _, exists := seen.addTupleByContract(row, cypherEquivalenceContract); !exists {
					combinedResult.Rows = append(combinedResult.Rows, row)
				}
			}
		}
	}

	if combinedResult == nil {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return combinedResult, nil
}

// unionMixedError is the error for a query that combines UNION and UNION ALL.
func unionMixedError() error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidClauseComposition",
		"UNION and UNION ALL cannot be combined in the same query",
	)
}

// unionColumnsError is the error for a UNION branch whose column names differ
// from the first branch's.
func unionColumnsError(columns, branch []string) error {
	message := fmt.Sprintf("UNION queries must return the same columns (got %v and %v)", columns, branch)
	if len(columns) != len(branch) {
		message = localization.CypherResidualUnionColumnCountMismatch(len(columns), len(branch)).Fallback
	}
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "DifferentColumnsInUnion", message)
}

// unionColumnOrder reports whether a UNION branch returns the same column
// names as the first branch (columns), in any order, and when the order
// differs, for each of columns the index of that column in branch. Neo4j
// 5.26 matches UNION columns by name (#907).
func unionColumnOrder(columns, branch []string) (order []int, sameNames bool) {
	if len(columns) != len(branch) {
		return nil, false
	}
	inOrder := true
	for index := range columns {
		if columns[index] != branch[index] {
			inOrder = false
			break
		}
	}
	if inOrder {
		return nil, true
	}
	positions := make(map[string]int, len(branch))
	for index, name := range branch {
		positions[name] = index
	}
	order = make([]int, len(columns))
	for index, name := range columns {
		position, found := positions[name]
		if !found {
			return nil, false
		}
		order[index] = position
	}
	return order, true
}

// reorderUnionRows rewrites rows into the column order unionColumnOrder gave.
func reorderUnionRows(rows [][]interface{}, order []int) [][]interface{} {
	reordered := make([][]interface{}, len(rows))
	for index, row := range rows {
		values := make([]interface{}, len(order))
		for column, position := range order {
			if position < len(row) {
				values[column] = row[position]
			}
		}
		reordered[index] = values
	}
	return reordered
}

// ========================================
// OPTIONAL MATCH Clause
// ========================================

// joinedRow represents a row from a left outer join between MATCH and OPTIONAL MATCH
type joinedRow struct {
	initialNode  *storage.Node
	relatedNode  *storage.Node
	relationship *storage.Edge
}

// optionalRelPattern holds parsed relationship info for OPTIONAL MATCH
type optionalRelPattern struct {
	sourceVar    string
	relType      string
	relVar       string
	targetVar    string
	targetLabels []string
	targetProps  map[string]interface{}
	direction    string // "out", "in", "both"
}

// optionalRelResult holds a node and its connecting edge for OPTIONAL MATCH
type optionalRelResult struct {
	node *storage.Node
	edge *storage.Edge
}

// resolveReturnExprFromVarMap resolves a RETURN expression against a variable
// map built from a multi-variable MATCH result row and optional OPTIONAL MATCH results.
func (e *StorageExecutor) resolveReturnExprFromVarMap(
	ctx context.Context,
	expr braceView,
	varMap map[string]interface{},
	targetVar, relVar string,
	targetNode *storage.Node,
	targetEdge *storage.Edge,
) interface{} {
	row := e.mergeBindingRow(ctx,
		map[string]*storage.Node{targetVar: targetNode},
		map[string]*storage.Edge{relVar: targetEdge})
	for name, value := range varMap {
		if name != targetVar && name != relVar {
			row[name] = value
		}
	}
	projected, err := e.projectMergeReturn(ctx, []pipelineRow{row}, expr.trim().wrap("RETURN ", ""))
	if err != nil || len(projected.Rows) == 0 || len(projected.Rows[0]) == 0 {
		return nil
	}
	return projected.Rows[0][0]
}

// parseOptionalRelPattern parses patterns like (a)-[r:TYPE]->(b:Label)
func (e *StorageExecutor) parseOptionalRelPattern(ctx context.Context, view braceView) optionalRelPattern {
	result := optionalRelPattern{
		direction:   "out",
		targetProps: make(map[string]interface{}),
	}
	view = normalizeAnonymousTraversalView(view.trim())
	pattern := view.text()

	// Check direction
	if strings.Contains(pattern, "<-") {
		result.direction = "in"
	} else if strings.Contains(pattern, "->") {
		result.direction = "out"
	} else if strings.Contains(pattern, "-") {
		result.direction = "both"
	}

	// Extract source variable
	if idx := strings.Index(pattern, "("); idx >= 0 {
		endIdx := strings.Index(pattern[idx:], ")")
		if endIdx > 0 {
			sourceStr := pattern[idx+1 : idx+endIdx]
			if colonIdx := strings.Index(sourceStr, ":"); colonIdx > 0 {
				result.sourceVar = strings.TrimSpace(sourceStr[:colonIdx])
			} else {
				result.sourceVar = strings.TrimSpace(sourceStr)
			}
		}
	}

	// Extract relationship type and variable
	if idx := strings.Index(pattern, "["); idx >= 0 {
		endIdx := strings.Index(pattern[idx:], "]")
		if endIdx > 0 {
			relStr := pattern[idx+1 : idx+endIdx]
			if colonIdx := strings.Index(relStr, ":"); colonIdx >= 0 {
				result.relVar = strings.TrimSpace(relStr[:colonIdx])
				result.relType = strings.TrimSpace(relStr[colonIdx+1:])
			} else {
				result.relVar = strings.TrimSpace(relStr)
			}
		}
	}

	// Extract target
	relEnd := strings.Index(pattern, "]")
	if relEnd > 0 {
		remaining := pattern[relEnd+1:]
		if idx := strings.Index(remaining, "("); idx >= 0 {
			endIdx := strings.Index(remaining[idx:], ")")
			if endIdx > 0 {
				targetInfo, parseErr := e.parseNodePattern(ctx, view.window(relEnd+1+idx, relEnd+1+idx+endIdx+1))
				if parseErr != nil {
					recordExpressionFailure(ctx, parseErr)
					return optionalRelPattern{}
				}
				if targetInfo.variable != "" {
					result.targetVar = targetInfo.variable
				}
				if len(targetInfo.labels) > 0 {
					result.targetLabels = append([]string(nil), targetInfo.labels...)
				}
				if len(targetInfo.properties) > 0 {
					result.targetProps = targetInfo.properties
				}
			}
		}
	}

	return result
}

func optionalRelationshipTypeMatches(filter, actual string) bool {
	if filter == "" {
		return true
	}
	if !strings.Contains(filter, "|") {
		return filter == actual
	}
	for _, candidate := range strings.Split(filter, "|") {
		if strings.TrimSpace(candidate) == actual {
			return true
		}
	}
	return false
}

// ========================================
// FOREACH Clause
// ========================================

// ========================================
// LOAD CSV Clause
// ========================================

// executeLoadCSV handles LOAD CSV clause
func (e *StorageExecutor) executeLoadCSV(ctx context.Context, cypher string) (*ExecuteResult, error) {
	return nil, localizedError(localization.CypherResidualLoadCSVUnsupported(), nil)
}

// ========================================
// Helper Functions
// ========================================

func replaceIdentifierOutsideQuotes(input string, ident string, replacement string) string {
	return replaceIdentifierOutsideQuotesUnless(input, ident, replacement, nil)
}

// replaceIdentifierOutsideQuotesUnless is replaceIdentifierOutsideQuotes
// that leaves the occurrences that read a local variable named ident
// (locals.keeps).
func replaceIdentifierOutsideQuotesUnless(input string, ident string, replacement string, locals *localOccurrences) string {
	if ident == "" {
		return input
	}
	if locals != nil && !locals.binds(ident) {
		locals = nil
	}
	var b strings.Builder
	b.Grow(len(input) + 16)

	for i := 0; i < len(input); {
		if end, quoted := skipCypherLexicalToken(input, i); quoted {
			b.WriteString(input[i:end])
			i = end
			continue
		}
		ch := input[i]

		if !cyphertext.NamePartAt(input, i) {
			b.WriteByte(ch)
			i++
			continue
		}
		start := i
		i = cyphertext.NamePartEnd(input, i)
		token := input[start:i]
		if token == ident && shouldReplaceIdentifierToken(input, start, i) && (locals == nil || !locals.keeps(input, ident, start)) {
			b.WriteString(replacement)
		} else {
			b.WriteString(token)
		}
	}
	return b.String()
}

func shouldReplaceIdentifierToken(input string, tokenStart int, tokenEnd int) bool {
	// Do not replace property tokens (n.name) or map keys ({name: ...}).
	prev := cyphertext.NeighborByte(input, tokenStart, cyphertext.Before)
	if prev == '.' {
		return false
	}
	next := cyphertext.NeighborByte(input, tokenEnd, cyphertext.AtOrAfter)
	if next == ':' {
		return false
	}
	return true
}

func toStringAnyMap(value interface{}) (map[string]interface{}, bool) {
	if m, ok := value.(map[string]interface{}); ok {
		return storage.PublicRemoteMap(m), true
	}
	if m, ok := value.(map[interface{}]interface{}); ok {
		out := make(map[string]interface{}, len(m))
		for k, v := range m {
			ks, ok := k.(string)
			if !ok {
				return nil, false
			}
			out[ks] = v
		}
		return storage.PublicRemoteMap(out), true
	}
	return nil, false
}
