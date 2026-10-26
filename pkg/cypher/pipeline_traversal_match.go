package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// pipelineApplyInitialTraversalMatch adapts the shared streaming traversal
// operators to pipeline rows. It returns graph entities as bindings so every
// later WITH, mutation, and RETURN clause continues through the same executor.
func (e *StorageExecutor) pipelineApplyInitialTraversalMatch(ctx context.Context, rows []pipelineRow, clause braceView, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if source, supported, err := e.pipelineTraversalSource(ctx, pipelineRowsSource(rows), clause, hint); supported || err != nil {
		if err != nil {
			return nil, true, err
		}
		out, _ := materializePipelineSource(ctx, source)
		return out, true, getExpressionFailure(ctx)
	}
	return e.pipelineApplySeededTraversalMatch(ctx, rows, clause, hint)
}

// pipelineApplySeededTraversalMatch retains the existing selective access
// owners when a streaming input reveals a bound endpoint at execution time.
func (e *StorageExecutor) pipelineApplySeededTraversalMatch(ctx context.Context, rows []pipelineRow, clause braceView, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	ctx = withBraceView(ctx, clause)
	if len(rows) == 0 {
		return rows, true, nil
	}
	patternView := clause.window(len("MATCH"), len(clause.text())).trim()
	pattern := patternView.text()
	whereClause := patternView.window(0, 0)
	if whereIndex := topLevelKeywordIndex(patternView, "WHERE"); whereIndex >= 0 {
		whereClause = normalizePipelineWhitespace(patternView.window(whereIndex+len("WHERE"), len(pattern)))
		patternView = patternView.window(0, whereIndex).trim()
		pattern = patternView.text()
	}
	if !containsRelationshipPattern(pattern) {
		return nil, false, nil
	}
	// A mixed comma-separated MATCH is a product of independent pattern
	// components. The single traversal operator cannot consume only one
	// component without losing rows; leave the complete product to the shared
	// multi-pattern operator.
	if len(splitTopLevelComma(pattern)) != 1 {
		return nil, false, nil
	}

	variables := make([]string, 0)
	for _, variable := range extractNodeVariables(matchPatternStructure(patternView)) {
		variables = appendUniquePipelineBinding(variables, variable)
	}
	for _, variable := range extractRelationshipVariables(matchPatternStructure(patternView)) {
		variables = appendUniquePipelineBinding(variables, variable)
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	if pathVariable != "" {
		variables = appendUniquePipelineBinding(variables, pathVariable)
	}
	const anonymousBinding = "__nornic_pipeline_traversal"
	returnItems := make([]returnItem, 0, len(variables)+1)
	for _, variable := range variables {
		returnItems = append(returnItems, returnItem{expr: variable, alias: variable, view: clause.generated(variable)})
	}
	if len(returnItems) == 0 {
		returnItems = append(returnItems, returnItem{expr: "1", alias: anonymousBinding, view: clause.generated("1")})
	}

	store := e.getStorage(ctx)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		rowCtx, materializedPattern, prepareErr := e.bindPatternProperties(withValueBindings(ctx, row), patternView, row)
		if prepareErr != nil {
			return nil, true, prepareErr
		}
		materializedWhere := whereClause
		physicalWhere, residualWhere := pipelineTraversalPredicates(materializedWhere, row, variables)
		rowHint := hint
		if physicalWhere.text() != materializedWhere.text() || pipelinePatternJoinsOuterBinding(row, variables) {
			rowHint = pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1}
		}
		var result *ExecuteResult
		var handled bool
		var err error
		if rowHint.limit > 0 && !rowHint.orderExpr.empty() {
			result, handled, err = e.tryExecuteTraversalSeedOrderLimit(rowCtx, materializedPattern, physicalWhere, returnItems, pathVariable, rowHint.orderExpr, rowHint.limit, traversalStartSeed)
			if err == nil && !handled {
				result, handled, err = e.tryExecuteTraversalSeedOrderLimit(rowCtx, materializedPattern, physicalWhere, returnItems, pathVariable, rowHint.orderExpr, rowHint.limit, traversalEndSeed)
			}
		}
		if err != nil {
			return nil, true, err
		}
		if !handled {
			startSeedNodes, endSeedNodes, seedRejected := e.pipelineTraversalSeedNodes(rowCtx, materializedPattern, row)
			if seedRejected {
				// The bound node fails the pattern's own labels or inline
				// properties on that endpoint, so this row matches nothing.
				continue
			}
			if len(rows) == 1 && whereClause.empty() && len(startSeedNodes) == 1 && len(endSeedNodes) == 1 && hint.earlyLimit >= 0 && e.boundTraversalLimitSafe(rowCtx, materializedPattern, row, variables) {
				rowHint.earlyLimit = hint.earlyLimit
				rowHint.readTail = hint.readTail
			}
			if startSeedNodes == nil && endSeedNodes == nil {
				// A relationship pinned by id / elementId starts from its
				// endpoints, not from every node.
				if seeds, none, pinned := e.relationshipIDSeedNodes(rowCtx, materializedPattern, physicalWhere); pinned {
					if none {
						continue
					}
					startSeedNodes = seeds
				}
			}
			result, err = e.executeMatchWithRelationshipsWithPathSeeded(rowCtx, materializedPattern, physicalWhere, returnItems, startSeedNodes, endSeedNodes, pathVariable, rowHint.readTail, rowHint.earlyLimit)
			if err != nil {
				return nil, true, err
			}
		}
		e.normalizeSetMatchRowsToNodes(result, store)
		e.normalizeSetMatchRowsToEdges(result, store)
		var rowWhere *preparedRowPredicate
		if !residualWhere.empty() {
			rowWhere = prepareRowPredicate(residualWhere)
		}
		for _, resultRow := range result.Rows {
			joined := make(pipelineRow, util.SafePreallocSum(len(row), len(result.Columns)))
			for name, value := range row {
				joined[name] = value
			}
			compatible := true
			for index, column := range result.Columns {
				if column == anonymousBinding || index >= len(resultRow) {
					continue
				}
				value := resultRow[index]
				if existing, bound := joined[column]; bound && !pipelineBindingValuesEqual(existing, value) {
					compatible = false
					break
				}
				joined[column] = value
			}
			if compatible && (rowWhere == nil || rowWhere.holds(e, ctx, map[string]interface{}(joined))) {
				out = append(out, joined)
			}
		}
	}
	return out, true, nil
}

func pipelineTraversalPushdownPredicate(whereClause braceView, row pipelineRow, localVariables []string) braceView {
	pushdown, _ := pipelineTraversalPredicates(whereClause, row, localVariables)
	return pushdown
}

// Each WHERE conjunct runs once: at traversal or after the outer-row join.
func pipelineTraversalPredicates(whereClause braceView, row pipelineRow, localVariables []string) (braceView, braceView) {
	if whereClause.trim().empty() {
		return whereClause, whereClause
	}
	local := make(map[string]struct{}, len(localVariables))
	for _, variable := range localVariables {
		local[variable] = struct{}{}
	}
	terms := splitTopLevelAndConjuncts(whereClause)
	pushable := make([]braceView, 0, len(terms))
	residual := make([]braceView, 0, len(terms))
	for _, term := range terms {
		term = term.trim()
		if term.empty() {
			continue
		}
		dependsOnOuterBinding := false
		for name := range row {
			if _, isLocal := local[name]; isLocal || strings.HasPrefix(name, "$") {
				continue
			}
			if referencesVariable(term.text(), name) {
				dependsOnOuterBinding = true
				break
			}
		}
		if !dependsOnOuterBinding {
			pushable = append(pushable, term)
		} else {
			residual = append(residual, term)
		}
	}
	return whereClause.assembleCode(pushable, " AND "), whereClause.assembleCode(residual, " AND ")
}

func pipelinePatternJoinsOuterBinding(row pipelineRow, variables []string) bool {
	for _, variable := range variables {
		if _, bound := row[variable]; bound {
			return true
		}
	}
	return false
}

// pipelineTraversalSeedNodes looks up whether the traversal pattern's
// start-node or end-node variable is already bound to a node by an earlier
// clause in this row. When it is, the caller can seed
// executeMatchWithRelationshipsWithPathSeeded from that single node instead
// of expanding the whole pattern over the store and joining the bound value
// afterward. Both bound endpoints are retained; end-node binding is only
// honored for non-chained patterns, since reversing a multi-segment chain
// is not supported.
//
// The seeded endpoint skips the scan that would otherwise apply the pattern's
// labels and inline properties for that endpoint, so they are checked here:
// every label must be present and every inline property must match. When the
// bound node fails them, rejected is true and the row matches nothing.
func (e *StorageExecutor) pipelineTraversalSeedNodes(ctx context.Context, pattern braceView, row pipelineRow) (startSeedNodes, endSeedNodes []*storage.Node, rejected bool) {
	matches := e.parseTraversalPattern(ctx, pattern)
	if matches == nil {
		return nil, nil, false
	}
	if node, ok := pipelineBoundNode(row, matches.StartNode.variable); ok {
		if !e.matchesEndPattern(node, &matches.StartNode) {
			return nil, nil, true
		}
		startSeedNodes = []*storage.Node{node}
	}
	if !matches.IsChained {
		if node, ok := pipelineBoundNode(row, matches.EndNode.variable); ok {
			if !e.matchesEndPattern(node, &matches.EndNode) {
				return nil, nil, true
			}
			endSeedNodes = []*storage.Node{node}
		}
	}
	return startSeedNodes, endSeedNodes, false
}

func pipelineBoundNode(row pipelineRow, variable string) (*storage.Node, bool) {
	if variable == "" {
		return nil, false
	}
	value, bound := row[variable]
	if !bound {
		return nil, false
	}
	node, ok := value.(*storage.Node)
	if !ok || node == nil {
		return nil, false
	}
	return node, true
}

// boundTraversalLimitSafe admits a cap only when both endpoint identities are
// already checked by the traversal and no other outer binding can reject a path.
func (e *StorageExecutor) boundTraversalLimitSafe(ctx context.Context, pattern braceView, row pipelineRow, variables []string) bool {
	match := e.parseTraversalPatternStructure(ctx, pattern)
	if match == nil || match.IsChained {
		return false
	}
	for _, name := range variables {
		if _, bound := row[name]; bound && name != match.StartNode.variable && name != match.EndNode.variable {
			return false
		}
	}
	return true
}

func (e *StorageExecutor) pipelineMatchHint(ctx context.Context, remaining []pipelineClause) pipelineMatchPhysicalHint {
	hint := pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1, readTail: pipelineReadOnlyTail(remaining)}
	if len(remaining) > 0 && remaining[0].kind == pipelineClauseWith {
		clause := remaining[0].text
		clauseView := remaining[0].input(ctx)
		if topLevelKeywordIndex(clauseView, "WHERE") >= 0 {
			return hint
		}
		end := len(clause)
		for _, keyword := range []string{"SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(clauseView, keyword); index >= 0 && index < end {
				end = index
			}
		}
		if _, local := parsePipelineRowWith(ctx, clauseView.window(0, end)); !local {
			return hint
		}
		limit, hasLimit := e.parseIntModifier(ctx, clauseView, "LIMIT")
		skip, _ := e.parseIntModifier(ctx, clauseView, "SKIP")
		if !hasLimit || limit <= 0 || skip < 0 || skip > int(^uint(0)>>1)-limit {
			return hint
		}
		hint.limit, hint.earlyLimit = skip+limit, skip+limit
		return hint
	}
	var terminalReturn braceView
	for index, clause := range remaining {
		if clause.kind != pipelineClauseReturn || index != len(remaining)-1 {
			return hint
		}
		terminalReturn = clause.view
	}
	if terminalReturn.empty() {
		return hint
	}
	bodyView := terminalReturn.window(len("RETURN"), len(terminalReturn.text())).trim()
	body := bodyView.text()
	if _, distinct := cutDistinct(bodyView); distinct {
		return hint
	}
	for _, item := range e.parseReturnItems(ctx, bodyView) {
		if pipelineExpressionContainsAggregate(item.view) {
			return hint
		}
	}
	skip, hasSkip := e.parseIntModifier(ctx, bodyView, "SKIP")
	if hasSkip && skip != 0 {
		return hint
	}
	limit, hasLimit := e.parseIntModifier(ctx, bodyView, "LIMIT")
	if !hasLimit || limit < 0 {
		return hint
	}
	hint.limit = limit
	if orderIndex := topLevelKeywordIndex(bodyView, "ORDER BY"); orderIndex >= 0 {
		orderExpr := bodyView.window(orderIndex+len("ORDER BY"), len(body)).trim()
		end := len(orderExpr.text())
		for _, keyword := range []string{"SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(orderExpr, keyword); index >= 0 && index < end {
				end = index
			}
		}
		hint.orderExpr = orderExpr.window(0, end).trim()
	} else {
		hint.earlyLimit = limit
	}
	return hint
}
