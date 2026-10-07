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
	return isFunctionCallWS(expr, "count") ||
		isFunctionCallWS(expr, "sum") ||
		isFunctionCallWS(expr, "avg") ||
		isFunctionCallWS(expr, "min") ||
		isFunctionCallWS(expr, "max") ||
		isFunctionCallWS(expr, "collect") ||
		isFunctionCallWS(expr, "stdev") ||
		isFunctionCallWS(expr, "stdevp") ||
		isFunctionCallWS(expr, "percentilecont") ||
		isFunctionCallWS(expr, "percentiledisc")
}

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
	// Find opening paren (may have whitespace before it)
	openIdx := strings.Index(expr, "(")
	if openIdx < 0 {
		return ""
	}

	// Find the MATCHING closing paren, not just the last one
	// This properly handles cases like collect({...})[..10]
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i := openIdx; i < len(expr); i++ {
		ch := rune(expr[i])
		switch {
		case (ch == '\'' || ch == '"') && !inQuote:
			inQuote = true
			quoteChar = ch
		case ch == quoteChar && inQuote:
			inQuote = false
			quoteChar = 0
		case ch == '(' && !inQuote:
			depth++
		case ch == ')' && !inQuote:
			depth--
			if depth == 0 {
				// Found the matching closing parenthesis
				return strings.TrimSpace(expr[openIdx+1 : i])
			}
		}
	}
	return ""
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
		preceding := upperASCII(strings.TrimSpace(cypher[:absIdx]))
		if !strings.HasSuffix(preceding, "STARTS") && !strings.HasSuffix(preceding, "ENDS") {
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
func (e *StorageExecutor) selectTopKNodesByOrder(nodes []*storage.Node, variable, orderExpr string, k int) ([]*storage.Node, bool) {
	if k <= 0 || len(nodes) <= k {
		return nodes, false
	}
	specs := e.parseNodeOrderSpecs(orderExpr, variable)
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
