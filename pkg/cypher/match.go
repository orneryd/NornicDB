// MATCH clause implementation for NornicDB.
// This file contains MATCH execution, aggregation, ordering, and filtering.

package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// containsAggregateFunc checks if expression contains any aggregate function
// (handles expressions like SUM(a) + SUM(b))
func containsAggregateFunc(expr string) bool {
	return len(findAggregateSpans(expr)) > 0
}

// isAggregateFuncName checks if expr starts with a specific aggregate function (whitespace-tolerant)
func isAggregateFuncName(expr, funcName string) bool {
	return isFunctionCallWS(expr, funcName)
}

// extractFuncInner extracts the inner expression from a function call (whitespace-tolerant)
// e.g., "COUNT(n)" -> "n", "SUM (x.val)" -> "x.val", "collect({a:1})[..10]" -> "{a:1}"
func extractFuncInner(expr string) string {
	open := strings.IndexByte(expr, '(')
	if open < 0 {
		return ""
	}
	// The matching closing paren, not the last one: collect({…})[..10].
	close := findMatchingParen(expr, open)
	if close < 0 {
		return ""
	}
	return strings.TrimSpace(expr[open+1 : close])
}

// compareForSort compares two values for sorting, returns true if a < b
func compareForSort(a, b interface{}) bool {
	if a == nil && b == nil {
		return false
	}
	if a == nil {
		return true
	}
	if b == nil {
		return false
	}
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			return av < bv
		}
		if bv, ok := b.(float64); ok {
			return float64(av) < bv
		}
	case int:
		if bv, ok := b.(int); ok {
			return av < bv
		}
		if bv, ok := b.(int64); ok {
			return int64(av) < bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av < bv
		}
		if bv, ok := b.(int64); ok {
			return av < float64(bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			return av < bv
		}
	}
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

func storageHasDecayFiltering(engine storage.Engine) bool {
	visited := make(map[storage.Engine]bool)
	for engine != nil && !visited[engine] {
		visited[engine] = true
		if decay, ok := engine.(interface{ IsDecayEnabled() bool }); ok && decay.IsDecayEnabled() {
			return true
		}
		if manual, ok := engine.(interface{ HasManualSuppressions() bool }); ok && manual.HasManualSuppressions() {
			return true
		}
		switch wrapper := engine.(type) {
		case storage.EngineUnwrapper:
			engine = wrapper.GetInnerEngine()
		default:
			engine = nil
		}
	}
	return false
}

func extractMatchWhereClause(cypher string, whereIdx, returnIdx int) string {
	if whereIdx <= 0 || returnIdx <= whereIdx+5 || returnIdx > len(cypher) {
		return ""
	}
	segment := cypher[whereIdx+5 : returnIdx]
	end := len(segment)
	// Clause keywords inside a subquery expression's braces (COUNT { CALL (i)
	// { … } RETURN o }) belong to it, not to the WHERE (#652).
	for _, kw := range []string{
		"OPTIONAL MATCH",
		"UNWIND",
		"CALL",
		"CREATE",
		"MERGE",
		"DELETE",
		"DETACH DELETE",
		"SET",
		"REMOVE",
		"ORDER BY",
		"SKIP",
		"LIMIT",
	} {
		if idx := topLevelKeywordIndex(segment, kw); idx >= 0 && idx < end {
			end = idx
		}
	}
	return strings.TrimSpace(segment[:end])
}

func hasStandaloneWithClause(cypher string) bool {
	searchStart := 0
	for {
		idx := topLevelKeywordIndex(cypher[searchStart:], "WITH")
		if idx < 0 {
			return false
		}
		absIdx := searchStart + idx
		if keywordsBefore(cypher, absIdx, "STARTS") < 0 && keywordsBefore(cypher, absIdx, "ENDS") < 0 {
			return true
		}
		searchStart = absIdx + len("WITH")
		if searchStart >= len(cypher) {
			return false
		}
	}
}

func extractMatchOrderByClause(cypher string, returnIdx int) string {
	if returnIdx < 0 || returnIdx+6 > len(cypher) {
		return ""
	}
	returnScope := strings.TrimSpace(cypher[returnIdx+6:])
	orderIdx := findKeywordIndexInContext(returnScope, "ORDER")
	if orderIdx == -1 {
		return ""
	}
	orderPart := strings.TrimSpace(returnScope[orderIdx:])
	if !strings.HasPrefix(upperASCII(orderPart), "ORDER BY") {
		return ""
	}
	orderExpr := strings.TrimSpace(orderPart[len("ORDER BY"):])
	if orderExpr == "" {
		return ""
	}
	end := len(orderExpr)
	for _, kw := range []string{"SKIP", "LIMIT"} {
		if idx := findKeywordIndexInContext(orderExpr, kw); idx != -1 && idx < end {
			end = idx
		}
	}
	return strings.TrimSpace(orderExpr[:end])
}

func (e *StorageExecutor) compareNodeOrderSpecs(a, b *storage.Node, specs []nodeOrderSpec) int {
	for _, spec := range specs {
		av, _ := a.Properties[spec.propName]
		bv, _ := b.Properties[spec.propName]
		cmp := e.compareOrderValues(av, bv)
		if cmp == 0 {
			continue
		}
		if spec.descending {
			cmp = -cmp
		}
		return cmp
	}
	return 0
}

// selectTopKNodesByOrder returns the first k nodes for simple ORDER BY expressions
// without sorting the full node set. It supports any number of ORDER BY terms.
func (e *StorageExecutor) selectTopKNodesByOrder(ctx context.Context, nodes []*storage.Node, variable, orderExpr string, k int) ([]*storage.Node, bool, error) {
	if k <= 0 || len(nodes) <= k {
		return nodes, false, nil
	}
	specs := e.parseNodeOrderSpecs(orderExpr, variable)
	if len(specs) == 0 {
		return nil, false, nil
	}

	top := make([]*storage.Node, 0, min(k, len(nodes)))
	less := func(a, b *storage.Node) bool {
		return e.compareNodeOrderSpecs(a, b, specs) < 0
	}
	worse := func(a, b *storage.Node) bool {
		return e.compareNodeOrderSpecs(a, b, specs) > 0
	}

	for batch, batchErr := range materializedBatches(ctx, len(nodes)) {
		if batchErr != nil {
			return nil, true, batchErr
		}
		for _, n := range nodes[batch.start:batch.end] {
			if len(top) < k {
				top = append(top, n)
				continue
			}
			// Find current worst in top-K (k is small in this workload, linear scan is faster than heap overhead).
			worstIdx := 0
			for innerBatch, innerErr := range materializedBatches(ctx, len(top)) {
				if innerErr != nil {
					return nil, true, innerErr
				}
				for i := max(innerBatch.start, 1); i < innerBatch.end; i++ {
					if worse(top[i], top[worstIdx]) {
						worstIdx = i
					}
				}
			}
			if less(n, top[worstIdx]) {
				top[worstIdx] = n
			}
		}
	}
	if err := stableSortRows(ctx, top, func(a, b *storage.Node) int { return e.compareNodeOrderSpecs(a, b, specs) }); err != nil {
		return nil, true, err
	}
	return top, true, nil
}

// executeAggregation handles aggregate functions (COUNT, SUM, AVG, etc.)
// with implicit GROUP BY for non-aggregated columns (Neo4j compatible)
