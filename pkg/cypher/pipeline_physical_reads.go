package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// tryExecutePipelineSimpleRelationshipCountPlan is the fused physical form of
// the pipeline's MATCH -> count reduction for relationship patterns (issue
// #638). Without it, the row pipeline binds the relationship variable for
// every match and counts the materialized rows — so
//
//	MATCH ()-[r:T]->() RETURN count(r)
//
// costs O(store size) instead of an O(1) counter read, even though the
// traversal-layer fast path exists. It delegates to relationshipCountFromStore,
// the single owner of counts answered from the counters (count_store.go).
func (e *StorageExecutor) tryExecutePipelineSimpleRelationshipCountPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) pipelineDispatchOutcome {
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	matchBody := pipelineClauseBody(clauses[0].text, "MATCH")
	if topLevelKeywordIndex(ctx, matchBody, "WHERE") >= 0 {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	// This counter plan owns a relationship pattern, never a path assignment
	// or shortestPath call. Decline their syntax before reading any values.
	if !strings.HasPrefix(matchBody, "(") || !containsRelationshipPattern(matchBody) {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	if len(splitTopLevelComma(matchBody)) != 1 {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	// A counter owns one projected item. Decline other projections before
	// reading a traversal that this plan cannot consume.
	items := e.parseReturnItems(ctx, strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if len(items) != 1 {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	if !patternMapsAreConstant(matchBody) {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	matches := e.parseTraversalPatternStructure(ctx, matchBody)
	if matches == nil {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	count, ok, err := e.relationshipCountFromStore(ctx, matches, items[0])
	if err != nil {
		return newPipelineDispatchOutcome(nil, true, err)
	}
	if !ok.available() {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	column := items[0].column()
	return newCountReductionOutcome(count, column, ok)
}

// tryExecutePipelineSimpleNodeReadPlan applies cardinality and property-index
// operators before row materialization for a single node MATCH. The result is
// still projected by the pipeline, so indexed and scanned inputs share the
// same expression semantics.
func (e *StorageExecutor) tryExecutePipelineSimpleNodeReadPlan(ctx context.Context, clauses []pipelineClause, params map[string]interface{}) pipelineDispatchOutcome {
	stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream)
	if len(clauses) != 2 || clauses[0].kind != pipelineClauseMatch || clauses[1].kind != pipelineClauseReturn {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	matchBody := pipelineClauseBody(clauses[0].text, "MATCH")
	whereClause := ""
	if whereIndex := topLevelKeywordIndex(ctx, matchBody, "WHERE"); whereIndex >= 0 {
		whereClause = strings.TrimSpace(matchBody[whereIndex+len("WHERE"):])
		matchBody = strings.TrimSpace(matchBody[:whereIndex])
	}
	// A path assignment (p = (n:L)) is the general route's, which binds the
	// path: read as a node, its variable would be "p = (n".
	if !e.isSingleNodePattern(matchBody) || extractPathAssignmentVariable(matchBody) != "" {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	if !patternMapsAreConstant(matchBody) || !patternMapsAreConstant(whereClause) || stream.forClause(ctx, &clauses[1]) != nil {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	nodePattern, parseErr := e.parseNodePattern(ctx, matchBody)
	if parseErr != nil {
		return newPipelineDispatchOutcome(nil, true, parseErr)
	}
	if nodePattern.variable == "" {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}

	items := e.parseReturnItems(ctx, strings.TrimSpace(clauses[1].text[len("RETURN"):]))
	if len(items) > 1 && items[0].expr == "*" {
		// RETURN *, items: the general projection writes the * out (#883).
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	hint := e.pipelineMatchHint(ctx, clauses[1:])
	boundedSimpleProjection := whereClause == "" && hint.earlyLimit > 0 && pipelineSimpleNodeProjections(items, nodePattern.variable)
	countColumn, filteredCount := pipelineSingleNodeCountProjection(items, nodePattern.variable)
	if whereClause == "" && len(items) == 1 {
		if count, ok, err := e.nodeCountFromStore(ctx, nodePattern, items[0]); ok.available() {
			if err != nil {
				return newPipelineDispatchOutcome(nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err))
			}
			return newCountReductionOutcome(count, items[0].column(), ok)
		}
	}

	// A WHERE that is an OR of indexed IN lists is read here, as the shortcut
	// for batched key lookups. Its shape admits the statement to this read;
	// which index seeds it is the one node-pattern access decision's
	// (collectPipelineIndexedNodeCandidates, NOR-414), which may prefer the
	// pattern's own unique lookup. Every candidate is held to the whole
	// pattern and to the WHERE.
	if _, orIn, err := e.planPropertyIndexInOrParam(ctx, nodePattern, whereClause, params); err != nil {
		return newPipelineDispatchOutcome(nil, true, err)
	} else if orIn {
		candidates, whereApplied, used, err := e.collectPipelineIndexedNodeCandidates(ctx, nodePattern, whereClause, pipelineMatchPhysicalHint{})
		if err != nil {
			return newPipelineDispatchOutcome(nil, true, err)
		}
		if used {
			var projection *profileRuntimeRecord
			ctx, projection = profileRuntimeInvocation(ctx, "NodeProjection", clauses[1].text, 0)
			rows := make([]pipelineRow, 0, len(candidates))
			for _, node := range candidates {
				if node == nil || !e.pipelineNodeMatchesPattern(ctx, node, nodePattern) {
					continue
				}
				row := pipelineNodeRow(ctx, nodePattern.variable, node)
				if whereApplied || e.evaluateWithWhereCondition(ctx, whereClause, map[string]interface{}(row)) {
					rows = append(rows, row)
				}
			}
			result, projected := e.pipelineApplyReturn(ctx, rows, clauses[1].text)
			if !projected {
				return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
			}
			projection.invocationRows(len(result.Rows))
			return pipelineDispatchOutcome{state: pipelineDispatchHandledObserved, result: result}
		}
	}
	// The store counters answer only the reads they describe, so this
	// reduction also serves a plain labeled count: it counts the label's
	// stream through the same admission and WHERE test as the rows, and
	// never materializes them. It seeds from the same access decision.
	if filteredCount {
		if outcome := e.tryStreamPipelineFilteredNodeCount(ctx, nodePattern, whereClause, countColumn); outcome.terminal() {
			return outcome
		}
	}
	if !boundedSimpleProjection {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	// boundedSimpleProjection requires an empty WHERE, so no seek can
	// have applied one here.
	var projection *profileRuntimeRecord
	ctx, projection = profileRuntimeInvocation(ctx, "NodeProjection", clauses[1].text, 0)
	candidates, _, err := e.collectPipelineInitialNodeCandidates(ctx, nodePattern, whereClause, hint)
	if err != nil {
		return newPipelineDispatchOutcome(nil, true, err)
	}
	if len(candidates) > hint.earlyLimit {
		candidates = candidates[:hint.earlyLimit]
	}
	result := e.projectVisiblePipelineNodeRead(candidates, items, nodePattern.variable)
	projection.invocationRows(len(result.Rows))
	return pipelineDispatchOutcome{state: pipelineDispatchHandledObserved, result: result}
}

// pipelineNodeRow is the binding row of a single-node pipeline read: the node
// under its variable and every parameter under "$name", as row values
// (parameterRowValues). The row loop and the fused filtered count evaluate
// WHERE on it.
func pipelineNodeRow(ctx context.Context, variable string, node *storage.Node) pipelineRow {
	paramRows := parameterRowValues(ctx)
	row := make(pipelineRow, len(paramRows)+1)
	for name, value := range paramRows {
		row[name] = value
	}
	row[variable] = node
	return row
}

func pipelineSingleNodeCountProjection(items []returnItem, variable string) (string, bool) {
	if len(items) != 1 || variable == "" || !countsRows(items[0], variable) {
		return "", false
	}
	return items[0].column(), true
}

// tryStreamPipelineFilteredNodeCount is the fused physical form of the
// pipeline's MATCH -> filter -> count reduction. It consumes the same
// snapshot-aware label stream as the general row operator but reduces each
// qualifying binding immediately, avoiding a node slice and one map-backed
// pipelineRow per match.
func (e *StorageExecutor) tryStreamPipelineFilteredNodeCount(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	column string,
) pipelineDispatchOutcome {
	if len(nodePattern.labels) == 0 {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}
	store := e.getStorage(ctx)
	reader, ok := store.(storage.ProjectedLabelNodeReader)
	if !ok {
		return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
	}

	var reduction *profileRuntimeRecord
	ctx, reduction = profileRuntimeInvocation(ctx, "FilteredNodeCount", whereClause, 0)
	predicateRow := pipelineNodeRow(ctx, nodePattern.variable, nil)
	predicatePlan := planRowPredicate(whereClause)
	hasWhere := strings.TrimSpace(whereClause) != ""
	whereFilter := func(node *storage.Node) bool {
		predicateRow[nodePattern.variable] = node
		if !hasWhere {
			return true
		}
		if predicatePlan != nil && predicatePlan.complete {
			return e.evaluateRowPredicatePlan(ctx, predicatePlan, predicateRow)
		}
		return e.evaluateRowPredicate(ctx, whereClause, predicateRow)
	}
	hideSystemNodes := shouldHideSystemNodes(store)
	viewport, hasViewport := TemporalViewportFromContext(ctx)
	checker, canCheckViewport := store.(temporalCurrentNodeChecker)
	var count int64
	countNode := func(node *storage.Node, whereApplied bool) error {
		if node == nil || (hideSystemNodes && isSystemNode(node)) {
			return nil
		}
		if !mergeNodeHasLabels(node, nodePattern.labels) || !e.nodeMatchesProps(node, nodePattern.properties) || (!whereApplied && !whereFilter(node)) {
			return nil
		}
		if hasViewport && canCheckViewport {
			visible, visibleErr := checker.IsCurrentTemporalNode(node, viewport.AsOf)
			if visibleErr != nil {
				return visibleErr
			}
			if !visible {
				return nil
			}
		}
		count++
		return nil
	}
	// A lookup an index can narrow is counted from the same seed the row
	// read uses (#820); only otherwise is the label streamed.
	candidates, whereApplied, indexed, err := e.collectPipelineIndexedNodeCandidates(ctx, nodePattern, whereClause, pipelineMatchPhysicalHint{})
	if err != nil {
		return newPipelineDispatchOutcome(nil, true, err)
	}
	if indexed {
		for _, node := range candidates {
			if err := countNode(node, whereApplied); err != nil {
				return newPipelineDispatchOutcome(nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err))
			}
		}
		reduction.invocationRows(1)
		return newCountReductionOutcome(count, column, countReductionObservedScan)
	}
	projectedProperties := pipelineNodePredicateProperties(nodePattern.variable, whereClause, nodePattern.properties)
	label := nodeScanLabels(ctx, store, nodePattern.labels)[0]
	visit := queryprofile.EntityVisitor(ctx, profileKindNodeByLabelScan, label, func(node *storage.Node) error {
		return countNode(node, false)
	})
	err = reader.StreamNodesByLabelProjected(ctx, label, projectedProperties, visit)
	if err != nil {
		return newPipelineDispatchOutcome(nil, true, localizedError(localization.CypherMatchingStorageFailed(err), err))
	}
	reduction.invocationRows(1)
	return newCountReductionOutcome(count, column, countReductionObservedScan)
}

func pipelineNodePredicateProperties(variable, expression string, inline map[string]interface{}) []string {
	properties := make([]string, 0, len(inline)+2)
	seen := make(map[string]struct{}, len(inline)+2)
	for property := range inline {
		seen[property] = struct{}{}
		properties = append(properties, property)
	}
	variable = normalizeProjectionColumnName(variable)
	wholeNode := false
	forEachSemanticExpressionReference(expression, func(base, property string, hasProperty bool, _ int) bool {
		if base != variable {
			return true
		}
		if !hasProperty || property == "" {
			wholeNode = true
			return false
		}
		if _, exists := seen[property]; !exists {
			seen[property] = struct{}{}
			properties = append(properties, property)
		}
		return true
	})
	if wholeNode {
		return nil
	}
	sort.Strings(properties)
	return properties
}

func projectPipelineSimpleNodeRead(nodes []*storage.Node, items []returnItem, variable string) *ExecuteResult {
	result := &ExecuteResult{
		Columns: make([]string, len(items)),
		Rows:    make([][]interface{}, 0, len(nodes)),
		Stats:   &QueryStats{},
	}
	for index, item := range items {
		result.Columns[index] = item.column()
	}
	prefix := variable + "."
	for _, node := range nodes {
		row := make([]interface{}, len(items))
		for index, item := range items {
			expression := strings.TrimSpace(item.expr)
			if expression == variable {
				row[index] = node
				continue
			}
			property := normalizeProjectionColumnName(strings.TrimSpace(expression[len(prefix):]))
			row[index] = node.Properties[property]
		}
		result.Rows = append(result.Rows, row)
	}
	return result
}

func pipelineSimpleNodeProjections(items []returnItem, variable string) bool {
	if len(items) == 0 || variable == "" {
		return false
	}
	for _, item := range items {
		expression := strings.TrimSpace(item.expr)
		if expression == variable {
			continue
		}
		prefix := variable + "."
		if !strings.HasPrefix(expression, prefix) {
			return false
		}
		property := strings.TrimSpace(expression[len(prefix):])
		if property == "" || strings.ContainsAny(property, ".()[]{}+-*/%^<>=, ") {
			return false
		}
	}
	return true
}
