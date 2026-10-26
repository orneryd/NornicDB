// MATCH clause implementation for NornicDB.
// This file contains MATCH execution, aggregation, ordering, and filtering.

package cypher

import (
	"fmt"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// isAggregateFunc checks if expression is an aggregate function (whitespace-tolerant)
func isAggregateFunc(expr string) bool {
	return inspectFunctionCall(expr, namedFunctionCall, "count", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "sum", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "avg", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "min", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "max", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "collect", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "stdev", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "stdevp", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "percentilecont", nil) ||
		inspectFunctionCall(expr, namedFunctionCall, "percentiledisc", nil)
}

// containsAggregateFunc checks if expression contains any aggregate function
// (handles expressions like SUM(a) + SUM(b))
func containsAggregateFunc(view braceView) bool {
	return len(findAggregateSpans(view)) > 0
}

// isAggregateFuncName checks if expr starts with a specific aggregate function (whitespace-tolerant)
func isAggregateFuncName(expr, funcName string) bool {
	return inspectFunctionCall(expr, namedFunctionCall, funcName, nil)
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
	if a == nil {
		return b != nil
	}
	if b == nil {
		return false
	}
	if comparison, exact := compareCypherNumbersExactly(a, b); exact {
		return comparison < 0
	}
	if left, numeric := strictNumericValue(a); numeric {
		if right, numeric := strictNumericValue(b); numeric {
			return left < right
		}
	}
	if left, text := a.(string); text {
		if right, text := b.(string); text {
			return left < right
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

func extractMatchWhereClause(view braceView, whereIdx, returnIdx int) string {
	cypher := view.text()
	if whereIdx <= 0 || returnIdx <= whereIdx+5 || returnIdx > len(cypher) {
		return ""
	}
	segmentView := view.window(whereIdx+5, returnIdx)
	segment := segmentView.text()
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
		if idx := topLevelKeywordIndex(segmentView, kw); idx >= 0 && idx < end {
			end = idx
		}
	}
	return strings.TrimSpace(segment[:end])
}

func hasStandaloneWithClause(view braceView) bool {
	cypher := view.text()
	searchStart := 0
	for {
		idx := topLevelKeywordIndex(view.window(searchStart, len(cypher)), "WITH")
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

func extractMatchOrderByClause(view braceView, returnIdx int) string {
	cypher := view.text()
	if returnIdx < 0 || returnIdx+6 > len(cypher) {
		return ""
	}
	returnScope := view.window(returnIdx+6, len(cypher)).trim()
	orderIdx := findKeywordIndexInContext(returnScope, "ORDER")
	if orderIdx == -1 {
		return ""
	}
	orderPart := returnScope.window(orderIdx, len(returnScope.text())).trim()
	if !strings.HasPrefix(upperASCII(orderPart.text()), "ORDER BY") {
		return ""
	}
	orderView := afterWord(orderPart, "ORDER BY")
	orderExpr := orderView.text()
	if orderExpr == "" {
		return ""
	}
	end := len(orderExpr)
	for _, kw := range []string{"SKIP", "LIMIT"} {
		if idx := findKeywordIndexInContext(orderView, kw); idx != -1 && idx < end {
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
func (e *StorageExecutor) selectTopKNodesByOrder(nodes []*storage.Node, variable string, orderExpr braceView, k int) ([]*storage.Node, bool) {
	if k <= 0 || len(nodes) <= k {
		return nodes, false
	}
	specs, _ := e.parseNodeOrderSpecs(orderExpr, variable)
	if len(specs) == 0 {
		return nil, false
	}

	top := make([]*storage.Node, 0, k)
	less := func(a, b *storage.Node) bool {
		return e.compareNodeOrderSpecs(a, b, specs) < 0
	}
	worse := func(a, b *storage.Node) bool {
		return e.compareNodeOrderSpecs(a, b, specs) > 0
	}

	for _, n := range nodes {
		if len(top) < k {
			top = append(top, n)
			continue
		}
		// Find current worst in top-K (k is small in this workload, linear scan is faster than heap overhead).
		worstIdx := 0
		for i := 1; i < len(top); i++ {
			if worse(top[i], top[worstIdx]) {
				worstIdx = i
			}
		}
		if less(n, top[worstIdx]) {
			top[worstIdx] = n
		}
	}
	sort.Slice(top, func(i, j int) bool { return less(top[i], top[j]) })
	return top, true
}

// executeAggregation handles aggregate functions (COUNT, SUM, AVG, etc.)
// with implicit GROUP BY for non-aggregated columns (Neo4j compatible)
