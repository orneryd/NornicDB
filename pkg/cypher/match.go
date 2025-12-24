// MATCH clause implementation for NornicDB.
// This file contains MATCH execution, aggregation, ordering, and filtering.

package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// isAggregateFunc checks if expression is an aggregate function (whitespace-tolerant)
func isAggregateFunc(expr string) bool {
	return isFunctionCallWS(expr, "count") ||
		isFunctionCallWS(expr, "sum") ||
		isFunctionCallWS(expr, "avg") ||
		isFunctionCallWS(expr, "min") ||
		isFunctionCallWS(expr, "max") ||
		isFunctionCallWS(expr, "collect")
}

// containsAggregateFunc checks if expression contains any aggregate function
// (handles expressions like SUM(a) + SUM(b))
func containsAggregateFunc(expr string) bool {
	upper := strings.ToUpper(expr)
	// Check for aggregate function names followed by opening paren (with optional whitespace)
	for _, fn := range []string{"COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT"} {
		idx := strings.Index(upper, fn)
		if idx >= 0 {
			// Check if followed by ( with optional whitespace
			rest := strings.TrimSpace(upper[idx+len(fn):])
			if len(rest) > 0 && rest[0] == '(' {
				return true
			}
		}
	}
	return false
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

func (e *StorageExecutor) executeMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Validate MATCH syntax
	trimmed := strings.TrimSpace(cypher)
	upper := strings.ToUpper(trimmed)

	// Check for empty MATCH pattern
	if strings.TrimSpace(strings.TrimPrefix(upper, "MATCH")) == "" ||
		strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(upper, "MATCH")), "RETURN") {
		// MATCH with no pattern or MATCH followed immediately by RETURN
		if !strings.Contains(upper, "(") {
			return nil, fmt.Errorf("MATCH clause requires a pattern")
		}
	}

	// Check for bracket syntax without node pattern: MATCH [r] RETURN r
	if strings.Contains(trimmed, "MATCH") {
		afterMatch := strings.TrimSpace(trimmed[5:]) // Skip "MATCH"
		if strings.HasPrefix(afterMatch, "[") && !strings.Contains(strings.Split(afterMatch, "]")[0], "(") {
			return nil, fmt.Errorf("MATCH clause requires a node pattern, not just a relationship pattern")
		}
	}

	// Check for empty RETURN items
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(cypher[returnIdx+6:])
		// Remove trailing clauses
		for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
			if idx := findKeywordIndex(returnPart, kw); idx >= 0 {
				returnPart = strings.TrimSpace(returnPart[:idx])
			}
		}
		if returnPart == "" {
			return nil, fmt.Errorf("RETURN clause requires at least one expression")
		}
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Check for multiple MATCH clauses (excluding OPTIONAL MATCH, UNION, EXISTS, COLLECT subqueries)
	// This handles: MATCH (a)-[:REL]->(b) MATCH (c)-[:REL]->(b) WHERE a <> c RETURN a, b, c
	// And also: MATCH (a)-[:REL]->(b) MATCH (a)-[:REL2]->(c) RETURN count(a), b.name (with aggregation)
	// But NOT: MATCH (a) RETURN a UNION MATCH (b) RETURN b
	// And NOT: MATCH (n) WHERE EXISTS { MATCH (m) ... } RETURN n
	// And NOT: MATCH (p) RETURN p.name, collect { MATCH (p)-[:KNOWS]->(friend) RETURN friend.name }
	hasUnion := strings.Contains(upper, "UNION")
	hasExists := hasSubqueryPattern(cypher, existsSubqueryRe)
	hasCountSubquery := hasSubqueryPattern(cypher, countSubqueryRe)
	hasCollectSubquery := hasSubqueryPattern(cypher, collectSubqueryRe)
	hasWith := findKeywordIndex(cypher, "WITH") > 0

	if !hasUnion && !hasExists && !hasCountSubquery && !hasCollectSubquery && !hasWith {
		matchCount := countKeywordOccurrences(upper, "MATCH")
		optionalMatchCount := countKeywordOccurrences(upper, "OPTIONAL MATCH")
		if matchCount-optionalMatchCount > 1 {
			return e.executeMultiMatch(ctx, cypher)
		}
	}

	// Check for WITH clause between MATCH and RETURN
	// This handles MATCH ... WITH (CASE WHEN) ... RETURN queries
	// But we must avoid false positives from "STARTS WITH" or "ENDS WITH" in WHERE clauses
	withIdx := findKeywordIndex(cypher, "WITH")
	returnIdx = findKeywordIndex(cypher, "RETURN")

	// Check if WITH is actually a standalone clause (not part of "STARTS WITH" or "ENDS WITH")
	isStandaloneWith := false
	if withIdx > 0 && returnIdx > withIdx {
		// Check what precedes WITH - if it's "STARTS" or "ENDS", it's not a standalone WITH
		precedingText := strings.ToUpper(cypher[:withIdx])
		isStandaloneWith = !strings.HasSuffix(strings.TrimSpace(precedingText), "STARTS") &&
			!strings.HasSuffix(strings.TrimSpace(precedingText), "ENDS")
	}

	if isStandaloneWith {
		// Has standalone WITH clause - delegate to special handler
		return e.executeMatchWithClause(ctx, cypher)
	}

	// Check for UNWIND clause between MATCH and RETURN
	unwindIdx := findKeywordIndex(cypher, "UNWIND")
	if unwindIdx > 0 && (returnIdx == -1 || unwindIdx < returnIdx) {
		// Has UNWIND clause - delegate to special handler
		return e.executeMatchUnwind(ctx, cypher)
	}

	if returnIdx == -1 {
		// No RETURN clause - just match and return count
		result.Columns = []string{"matched"}
		result.Rows = [][]interface{}{{true}}
		return result, nil
	}

	// Parse RETURN part (everything after RETURN, before ORDER BY/SKIP/LIMIT)
	returnPart := cypher[returnIdx+6:]

	// Find end of RETURN clause
	returnEndIdx := len(returnPart)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnPart, keyword); idx >= 0 && idx < returnEndIdx {
			returnEndIdx = idx
		}
	}
	returnClause := strings.TrimSpace(returnPart[:returnEndIdx])

	// Check for DISTINCT
	distinct := false
	if strings.HasPrefix(strings.ToUpper(returnClause), "DISTINCT ") {
		distinct = true
		returnClause = strings.TrimSpace(returnClause[9:])
	}

	// Parse RETURN items
	returnItems := e.parseReturnItems(returnClause)
	result.Columns = make([]string, len(returnItems))
	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	// Check if this is an aggregation query
	hasAggregation := false
	for _, item := range returnItems {
		// Use whitespace-tolerant aggregation check
		// containsAggregateFunc handles both standalone (SUM(x)) and arithmetic (SUM(a) + SUM(b))
		if containsAggregateFunc(item.expr) {
			hasAggregation = true
			break
		}
	}

	// Extract pattern between MATCH and WHERE/RETURN
	whereIdx := findKeywordNotInBrackets(upper, " WHERE ")
	// Use findKeywordNotInBrackets to avoid matching WHERE inside list comprehensions like [x WHERE ...]
	matchPart := cypher[5:] // Skip "MATCH"
	// Note: whereIdx already defined above for fast-path count optimization
	if whereIdx > 0 {
		matchPart = cypher[5:whereIdx]
	} else if returnIdx > 0 {
		matchPart = cypher[5:returnIdx]
	}
	matchPart = strings.TrimSpace(matchPart)

	// Check for relationship pattern: (a)-[r:TYPE]->(b) or (a)<-[r]-(b)
	if strings.Contains(matchPart, "-[") || strings.Contains(matchPart, "]-") {
		// Extract WHERE clause if present
		var whereClause string
		if whereIdx > 0 {
			whereClause = strings.TrimSpace(cypher[whereIdx+5 : returnIdx])
		}

		// Extract path variable if pattern has assignment: path = (a)-[r]-(b)
		pathVariable := ""
		patternForParsing := matchPart
		if eqIdx := strings.Index(matchPart, "="); eqIdx > 0 {
			// Check if this is a path assignment (not a property comparison)
			beforeEq := strings.TrimSpace(matchPart[:eqIdx])
			afterEq := strings.TrimSpace(matchPart[eqIdx+1:])
			// Path variable should be a simple identifier, and after = should start with (
			if !strings.Contains(beforeEq, " ") && !strings.Contains(beforeEq, "(") && strings.HasPrefix(afterEq, "(") {
				pathVariable = beforeEq
				patternForParsing = afterEq
			}
		}

		result, err := e.executeMatchWithRelationshipsWithPath(patternForParsing, whereClause, returnItems, pathVariable)
		if err != nil {
			return nil, err
		}

		// Apply ORDER BY (whitespace-tolerant) - ORDER BY is NOT handled inside executeMatchWithRelationships
		orderByIdx := findKeywordIndex(cypher, "ORDER")
		if orderByIdx > 0 {
			orderStart := orderByIdx + 5 // skip "ORDER"
			// Skip whitespace
			for orderStart < len(cypher) && isWhitespace(cypher[orderStart]) {
				orderStart++
			}
			// Skip "BY"
			if orderStart+2 <= len(cypher) && strings.ToUpper(cypher[orderStart:orderStart+2]) == "BY" {
				orderStart += 2
				// Skip whitespace after BY
				for orderStart < len(cypher) && isWhitespace(cypher[orderStart]) {
					orderStart++
				}
			}
			// Find end of ORDER BY clause (before SKIP/LIMIT or end)
			orderEnd := len(cypher)
			for _, kw := range []string{"SKIP", "LIMIT"} {
				if idx := findKeywordIndex(cypher[orderStart:], kw); idx >= 0 {
					if orderStart+idx < orderEnd {
						orderEnd = orderStart + idx
					}
				}
			}
			orderExpr := strings.TrimSpace(cypher[orderStart:orderEnd])
			if orderExpr != "" {
				result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
			}
		}

		// Apply SKIP
		skipIdx := findKeywordIndex(cypher, "SKIP")
		if skipIdx > 0 {
			skipPart := strings.TrimSpace(cypher[skipIdx+4:])
			if fields := strings.Fields(skipPart); len(fields) > 0 {
				if s, err := strconv.Atoi(fields[0]); err == nil && s > 0 {
					if s < len(result.Rows) {
						result.Rows = result.Rows[s:]
					} else {
						result.Rows = [][]interface{}{}
					}
				}
			}
		}

		// Apply LIMIT
		limitIdx := findKeywordIndex(cypher, "LIMIT")
		if limitIdx > 0 {
			limitPart := strings.TrimSpace(cypher[limitIdx+5:])
			if fields := strings.Fields(limitPart); len(fields) > 0 {
				if l, err := strconv.Atoi(fields[0]); err == nil && l >= 0 {
					if l < len(result.Rows) {
						result.Rows = result.Rows[:l]
					}
				}
			}
		}

		return result, nil
	}

	// Check for comma-separated node patterns (cartesian product): (a:Label), (b:Label2)
	// This is different from relationship patterns which contain -[ or ]-
	nodePatterns := e.splitNodePatterns(matchPart)
	if len(nodePatterns) > 1 {
		return e.executeCartesianProductMatch(ctx, cypher, matchPart, nodePatterns, whereIdx, returnIdx, returnItems, hasAggregation, distinct, result)
	}

	// Parse single node pattern
	nodePattern := e.parseNodePattern(matchPart)

	// FAST PATH: For simple node count queries like "MATCH (n) RETURN count(n)" or "MATCH (n:Label) RETURN count(n)"
	// Use O(1) NodeCount() instead of loading all nodes into memory.
	// This optimization ONLY applies to simple node patterns (not relationships - those are handled above)
	if hasAggregation && whereIdx == -1 && len(returnItems) == 1 {
		upperExpr := strings.ToUpper(strings.TrimSpace(returnItems[0].expr))
		// Check for COUNT(*) or COUNT(variable) - not COUNT(n.property)
		if strings.HasPrefix(upperExpr, "COUNT(") && strings.HasSuffix(upperExpr, ")") {
			inner := strings.TrimSpace(upperExpr[6 : len(upperExpr)-1])
			// COUNT(*) or COUNT(n) where n is any variable - just count all nodes
			if inner == "*" || !strings.Contains(inner, ".") {
				var count int64
				var err error
				if len(nodePattern.labels) > 0 {
					// Count nodes with specific label
					nodes, err := e.storage.GetNodesByLabel(nodePattern.labels[0])
					if err != nil {
						return nil, fmt.Errorf("storage error: %w", err)
					}
					count = int64(len(nodes))
				} else {
					// Count all nodes - use O(1) NodeCount()
					count, err = e.storage.NodeCount()
					if err != nil {
						return nil, fmt.Errorf("storage error: %w", err)
					}
				}

				// Return result directly
				result.Rows = [][]interface{}{{count}}
				return result, nil
			}
		}
	}

	// Parse SKIP and LIMIT early for streaming optimization
	// Note: We can only use early termination when there's NO WHERE clause
	// because WHERE filtering happens after loading nodes
	skipIdx := findKeywordIndex(cypher, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(cypher[skipIdx+4:])
		if fields := strings.Fields(skipPart); len(fields) > 0 {
			if s, err := strconv.Atoi(fields[0]); err == nil {
				skip = s
			}
		}
	}

	limitIdx := findKeywordIndex(cypher, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(cypher[limitIdx+5:])
		if fields := strings.Fields(limitPart); len(fields) > 0 {
			if l, err := strconv.Atoi(fields[0]); err == nil {
				limit = l
			}
		}
	}

	// Calculate streaming limit: need to load enough nodes for SKIP + LIMIT
	// Only use streaming optimization when there's NO WHERE clause, NO ORDER BY, and NO aggregation
	// (filtering and sorting invalidate early termination since they need all nodes)
	hasOrderBy := findKeywordIndex(cypher, "ORDER") > 0
	streamingLimit := -1
	if whereIdx == -1 && !hasOrderBy && !hasAggregation && limit > 0 {
		streamingLimit = skip + limit
	}

	// Get matching nodes using streaming optimization when possible
	nodes, err := e.collectNodesWithStreaming(ctx, nodePattern.labels, nodePattern.properties, streamingLimit)
	if err != nil {
		return nil, fmt.Errorf("storage error: %w", err)
	}

	// Apply WHERE filter if present
	if whereIdx > 0 {
		// Find end of WHERE clause (before RETURN)
		wherePart := cypher[whereIdx+5 : returnIdx]
		nodes = e.filterNodes(nodes, nodePattern.variable, strings.TrimSpace(wherePart))
	}

	// Handle aggregation queries
	if hasAggregation {
		aggResult, err := e.executeAggregation(nodes, nodePattern.variable, returnItems, result)
		if err != nil {
			return nil, err
		}
		// Apply ORDER BY to aggregated results (whitespace-tolerant)
		orderByIdx := findKeywordIndex(cypher, "ORDER")
		if orderByIdx > 0 {
			orderStart := orderByIdx + 5
			for orderStart < len(cypher) && isWhitespace(cypher[orderStart]) {
				orderStart++
			}
			if orderStart+2 <= len(cypher) && strings.EqualFold(cypher[orderStart:orderStart+2], "BY") {
				orderStart += 2
			}
			orderPart := cypher[orderStart:]
			endIdx := len(orderPart)
			for _, kw := range []string{"SKIP", "LIMIT"} {
				if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
					endIdx = idx
				}
			}
			orderExpr := strings.TrimSpace(orderPart[:endIdx])
			aggResult.Rows = e.orderResultRows(aggResult.Rows, aggResult.Columns, orderExpr)
		}

		// Apply SKIP to aggregated results (whitespace-tolerant)
		skipIdx := findKeywordIndex(cypher, "SKIP")
		skip := 0
		if skipIdx > 0 {
			skipPart := strings.TrimSpace(cypher[skipIdx+4:])
			if fields := strings.Fields(skipPart); len(fields) > 0 {
				if s, err := strconv.Atoi(fields[0]); err == nil {
					skip = s
				}
			}
		}

		// Apply LIMIT to aggregated results (whitespace-tolerant)
		limitIdx := findKeywordIndex(cypher, "LIMIT")
		limit := -1
		if limitIdx > 0 {
			limitPart := strings.TrimSpace(cypher[limitIdx+5:])
			if fields := strings.Fields(limitPart); len(fields) > 0 {
				if l, err := strconv.Atoi(fields[0]); err == nil {
					limit = l
				}
			}
		}

		// Apply SKIP and LIMIT
		if skip > 0 || limit >= 0 {
			startIdx := skip
			if startIdx > len(aggResult.Rows) {
				startIdx = len(aggResult.Rows)
			}
			endIdx := len(aggResult.Rows)
			if limit >= 0 && startIdx+limit < endIdx {
				endIdx = startIdx + limit
			}
			aggResult.Rows = aggResult.Rows[startIdx:endIdx]
		}

		return aggResult, nil
	}

	// Parse ORDER BY (whitespace-tolerant)
	orderByIdx := findKeywordIndex(cypher, "ORDER")
	if orderByIdx > 0 {
		orderStart := orderByIdx + 5
		for orderStart < len(cypher) && isWhitespace(cypher[orderStart]) {
			orderStart++
		}
		if orderStart+2 <= len(cypher) && strings.EqualFold(cypher[orderStart:orderStart+2], "BY") {
			orderStart += 2
		}
		orderPart := cypher[orderStart:]
		endIdx := len(orderPart)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(orderPart[:endIdx])
		nodes = e.orderNodes(nodes, nodePattern.variable, orderExpr)
	}

	// Note: skipIdx, skip, limitIdx and limit are already parsed earlier for streaming optimization

	// Build result rows with SKIP and LIMIT
	seen := make(map[string]bool) // For DISTINCT
	rowCount := 0
	for i, node := range nodes {
		// Apply SKIP
		if i < skip {
			continue
		}

		// Apply LIMIT
		if limit >= 0 && rowCount >= limit {
			break
		}

		row := make([]interface{}, len(returnItems))
		for j, item := range returnItems {
			// Check for COLLECT { } subquery
			if hasSubqueryPattern(item.expr, collectSubqueryRe) {
				// Execute the subquery with the current node as context
				collected, err := e.evaluateCollectSubquery(ctx, node, nodePattern.variable, item.expr)
				if err != nil {
					return nil, fmt.Errorf("COLLECT subquery failed: %w", err)
				}
				row[j] = collected
			} else {
				row[j] = e.resolveReturnItem(item, nodePattern.variable, node)
			}
		}

		// Handle DISTINCT
		if distinct {
			key := fmt.Sprintf("%v", row)
			if seen[key] {
				continue
			}
			seen[key] = true
		}

		result.Rows = append(result.Rows, row)
		rowCount++
	}

	return result, nil
}

// executeAggregation handles aggregate functions (COUNT, SUM, AVG, etc.)
// with implicit GROUP BY for non-aggregated columns (Neo4j compatible)
