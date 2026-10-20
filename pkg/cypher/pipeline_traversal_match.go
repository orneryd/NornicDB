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
func (e *StorageExecutor) pipelineApplyInitialTraversalMatch(ctx context.Context, rows []pipelineRow, clause string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if len(rows) == 0 {
		return rows, true, nil
	}
	split := pipelineMatchWhereFor(ctx, clause)
	pattern, whereClause := split.pattern, split.where
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
	structure := matchPatternStructureFor(ctx, pattern)
	for _, variable := range extractNodeVariables(structure) {
		variables = appendUniquePipelineBinding(variables, variable)
	}
	for _, variable := range extractRelationshipVariables(structure) {
		variables = appendUniquePipelineBinding(variables, variable)
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	if pathVariable != "" {
		variables = appendUniquePipelineBinding(variables, pathVariable)
	}
	const anonymousBinding = "__nornic_pipeline_traversal"
	returnItems := make([]returnItem, 0, len(variables)+1)
	for _, variable := range variables {
		returnItems = append(returnItems, returnItem{expr: variable, alias: variable})
	}
	if len(returnItems) == 0 {
		returnItems = append(returnItems, returnItem{expr: "1", alias: anonymousBinding})
	}

	store := e.getStorage(ctx)
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		rowCtx, materializedPattern, prepareErr := e.preparePatternRow(withValueBindings(ctx, row), pattern, row)
		if prepareErr != nil {
			return nil, true, prepareErr
		}
		materializedWhere := whereClause
		physicalWhere, residualWhere := pipelineTraversalPredicates(ctx, materializedWhere, row, variables)
		rowHint := hint
		if physicalWhere != materializedWhere || pipelinePatternJoinsOuterBinding(row, variables) {
			rowHint = pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1}
		}
		var result *ExecuteResult
		var handled bool
		var err error
		if rowHint.limit > 0 && rowHint.orderExpr != "" {
			result, handled, err = e.tryExecuteTraversalStartSeedOrderLimit(rowCtx, materializedPattern, physicalWhere, returnItems, pathVariable, rowHint.orderExpr, rowHint.limit)
			if err == nil && !handled {
				result, handled, err = e.tryExecuteTraversalEndSeedOrderLimit(rowCtx, materializedPattern, physicalWhere, returnItems, pathVariable, rowHint.orderExpr, rowHint.limit)
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
			if len(rows) == 1 && whereClause == "" && len(startSeedNodes) == 1 && len(endSeedNodes) == 1 && hint.earlyLimit >= 0 && e.boundTraversalLimitSafe(rowCtx, materializedPattern, row, variables) {
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
		if residualWhere != "" {
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

// pipelineTraversalPushdownPredicate returns the conjuncts of whereClause
// that read no outer binding of row, which the traversal can apply itself.
// The conjuncts, and which variables each reads, come from the statement's
// clause plan: only the row's names are checked per row.
func pipelineTraversalPushdownPredicate(ctx context.Context, whereClause string, row pipelineRow, localVariables []string) string {
	pushdown, _ := pipelineTraversalPredicates(ctx, whereClause, row, localVariables)
	return pushdown
}

// Each WHERE conjunct runs once: at traversal or after the outer-row join.
func pipelineTraversalPredicates(ctx context.Context, whereClause string, row pipelineRow, localVariables []string) (string, string) {
	if strings.TrimSpace(whereClause) == "" {
		return "", ""
	}
	local := make(map[string]struct{}, len(localVariables))
	for _, variable := range localVariables {
		local[variable] = struct{}{}
	}
	terms := pipelinePushdownTermsFor(ctx, whereClause)
	pushable := make([]string, 0, len(terms.terms))
	residual := make([]string, 0, len(terms.terms))
	for index, term := range terms.terms {
		dependsOnOuterBinding := false
		for name := range row {
			if _, isLocal := local[name]; isLocal || strings.HasPrefix(name, "$") {
				continue
			}
			if terms.referencesVariable(index, name) {
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
	return strings.Join(pushable, " AND "), strings.Join(residual, " AND ")
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
func (e *StorageExecutor) pipelineTraversalSeedNodes(ctx context.Context, pattern string, row pipelineRow) (startSeedNodes, endSeedNodes []*storage.Node, rejected bool) {
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
func (e *StorageExecutor) boundTraversalLimitSafe(ctx context.Context, pattern string, row pipelineRow, variables []string) bool {
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
		if topLevelKeywordIndex(clause, "WHERE") >= 0 {
			return hint
		}
		end := len(clause)
		for _, keyword := range []string{"SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(clause, keyword); index >= 0 && index < end {
				end = index
			}
		}
		if _, local := parsePipelineRowWith(clause[:end]); !local {
			return hint
		}
		limit, hasLimit := e.parseIntModifier(ctx, clause, "LIMIT")
		skip, _ := e.parseIntModifier(ctx, clause, "SKIP")
		if !hasLimit || limit <= 0 || skip < 0 || skip > int(^uint(0)>>1)-limit {
			return hint
		}
		hint.limit, hint.earlyLimit = skip+limit, skip+limit
		return hint
	}
	var terminalReturn string
	for index, clause := range remaining {
		if clause.kind != pipelineClauseReturn || index != len(remaining)-1 {
			return hint
		}
		terminalReturn = clause.text
	}
	if terminalReturn == "" {
		return hint
	}
	body := strings.TrimSpace(terminalReturn[len("RETURN"):])
	if _, distinct := cutDistinct(body); distinct {
		return hint
	}
	for _, item := range e.parseReturnItems(body) {
		if pipelineExpressionContainsAggregate(item.expr) {
			return hint
		}
	}
	skip, hasSkip := e.parseIntModifier(ctx, body, "SKIP")
	if hasSkip && skip != 0 {
		return hint
	}
	limit, hasLimit := e.parseIntModifier(ctx, body, "LIMIT")
	if !hasLimit || limit < 0 {
		return hint
	}
	hint.limit = limit
	if orderIndex := topLevelKeywordIndex(body, "ORDER BY"); orderIndex >= 0 {
		orderExpr := strings.TrimSpace(body[orderIndex+len("ORDER BY"):])
		end := len(orderExpr)
		for _, keyword := range []string{"SKIP", "LIMIT"} {
			if index := topLevelKeywordIndex(orderExpr, keyword); index >= 0 && index < end {
				end = index
			}
		}
		hint.orderExpr = strings.TrimSpace(orderExpr[:end])
	} else {
		hint.earlyLimit = limit
	}
	return hint
}
