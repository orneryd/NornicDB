package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// matchPredicateFrontier owns whether a complete-row predicate can be moved
// to a node source. Binding dependencies and repeatability are separate proofs:
// a volatile predicate must stay on the complete row even if it reads only the
// seed, because a speculative prefix may be discarded or a seed may fan out.
// Function volatility comes from the execution policy's canonical catalog.
type matchPredicateFrontier struct {
	where      string
	repeatable bool
}

// Dependencies belong to the expression, while visibility, bindings and
// volatility belong to the current statement. Both projection and window
// certification reuse the semantic reader's immutable result instead of
// rebuilding local scopes and normalizing identifiers on each access probe.
var matchPredicateVariableCache = newBoundedCache[string, []string](4096, boundedCacheStringShardIndex)

func matchPredicateVariables(expression string) []string {
	if expression == "" {
		return nil
	}
	if variables, cached := matchPredicateVariableCache.get(expression); cached {
		return variables
	}
	variables := expressionFreeVariables(expression)
	matchPredicateVariableCache.put(expression, variables)
	return variables
}

func compileMatchPredicateFrontier(ctx context.Context, where string) matchPredicateFrontier {
	return matchPredicateFrontier{where: where, repeatable: strings.TrimSpace(where) == "" || deterministicExecution(ctx, where, 0)}
}

func (frontier matchPredicateFrontier) certifiesNodeWindow(variable string) bool {
	if !frontier.repeatable || !traversalSeedWhereOnlyReferences(frontier.where, variable) {
		return false
	}
	if strings.TrimSpace(frontier.where) != "" {
		// The complete predicate plan is the existing admission owner for
		// side-effect-free row operands. Graph predicates and producers must
		// remain at the complete-row frontier, even with seed-only references.
		plan := planRowPredicate(frontier.where)
		if plan == nil || !plan.complete {
			return false
		}
	}
	return true
}

func (hint pipelineMatchPhysicalHint) withoutSeedLimit() pipelineMatchPhysicalHint {
	hint.orderExpr, hint.limit, hint.earlyLimit = "", -1, -1
	return hint
}

// LIMIT belongs to a complete-row frontier, never to a traversal's seed list.
// The compiled-clause proof below admits only tails that cannot reject/reorder
// those rows before their own LIMIT. Every pattern condition executes in the
// canonical matcher before a speculative ordered index prefix can be final.
func (e *StorageExecutor) executeOrderedTraversalPrefix(ctx context.Context, pattern, whereClause string, returnItems []returnItem, pathVariable, orderExpr string, limit int, fromEnd bool) (*ExecuteResult, bool, error) {
	if limit <= 0 || strings.TrimSpace(orderExpr) == "" {
		return nil, false, nil
	}
	frontier := compileMatchPredicateFrontier(ctx, whereClause)
	if !frontier.repeatable {
		return nil, false, nil
	}
	for _, item := range returnItems {
		if pipelineExpressionContainsAggregate(item.expr) {
			return nil, false, nil
		}
	}
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || (fromEnd && match.IsChained) {
		return nil, false, nil
	}
	seed := match.StartNode
	if fromEnd {
		seed = match.EndNode
	}
	if strings.TrimSpace(seed.variable) == "" || len(seed.labels) == 0 {
		return nil, false, nil
	}
	if len(e.parseNodeOrderSpecs(orderExpr, seed.variable)) == 0 {
		return nil, false, nil
	}
	seedWhere := frontier.traversalSeed(seed.variable)
	// This bound is an access probe, not final seed truncation. Relationship
	// existence, types, labels, direction, length, maps and WHERE can still
	// reject its seeds, so only complete rows can certify a sufficient prefix.
	nodes, used, err := e.tryCollectTraversalStartSeedOrderNodes(ctx, seed, seedWhere, orderExpr, limit)
	if err != nil || !used {
		return nil, used, err
	}
	windowFull := len(nodes) >= limit
	if len(nodes) == 0 {
		if windowFull {
			return nil, false, nil
		}
		result := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
		for _, item := range returnItems {
			result.Columns = append(result.Columns, item.column())
		}
		e.markOrderedTraversalPrefix(fromEnd)
		return result, true, nil
	}
	var starts, ends []*storage.Node
	if fromEnd {
		ends = nodes
	} else {
		starts = nodes
	}
	result, err := e.executeMatchWithRelationshipsWithPathSeeded(ctx, pattern, whereClause, returnItems, starts, ends, pathVariable, nil, -1)
	if err != nil {
		return nil, true, err
	}
	if len(result.Rows) < limit && windowFull {
		// An incomplete prefix can hide qualifying later seeds, even when its
		// result is empty. Discard it and use the unbounded canonical path.
		return nil, false, nil
	}
	// Seeded sequential traversal and chained expansion retain seed order;
	// every ORDER term was proven to belong to that seed. Only complete rows
	// reach this boundary, after every segment and the complete WHERE.
	if len(result.Rows) > limit {
		result.Rows = result.Rows[:limit]
	}
	e.markOrderedTraversalPrefix(fromEnd)
	return result, true, nil
}

func (e *StorageExecutor) markOrderedTraversalPrefix(fromEnd bool) {
	if fromEnd {
		e.markTraversalEndSeedTopKUsed()
	} else {
		e.markTraversalStartSeedTopKUsed()
	}
}

// Seed predicates use the same semantic dependency grammar as bound paths.
// The complete WHERE still executes on the complete match.
func (frontier matchPredicateFrontier) traversalSeed(variable string) string {
	if !frontier.repeatable {
		return ""
	}
	parts := splitTopLevelAndConjuncts(unwrapOuterParens(strings.TrimSpace(frontier.where)))
	selected := parts[:0]
	for _, part := range parts {
		term := unwrapOuterParens(strings.TrimSpace(part))
		candidate := matchPredicateFrontier{where: term, repeatable: frontier.repeatable}
		if term != "" && candidate.certifiesNodeWindow(variable) {
			selected = append(selected, term)
		}
	}
	return strings.Join(selected, " AND ")
}

// findPaths checks complete non-chained patterns before counting a path.
// WHERE runs afterward and cannot share that earlier traversal budget.
func traversalCompletePathLimit(match *TraversalMatch, where string, requested int) int {
	if requested > 0 && match != nil && !match.IsChained && strings.TrimSpace(where) == "" &&
		(match.StartNode.variable == "" || match.StartNode.variable != match.EndNode.variable) {
		return requested
	}
	return 0
}

// Shared compiled-clause proof for node-index, streaming, bound traversal and
// ordered-prefix consumers. Later MATCH/OPTIONAL/WITH filters, DISTINCT and
// aggregation cannot inherit a final RETURN cap. A local WITH may own an
// earlier semantic LIMIT boundary of its own.
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
