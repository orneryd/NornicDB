package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeMatchWithClause(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Find clause boundaries
	withIdx := findKeywordIndex(cypher, "WITH")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if withIdx == -1 || returnIdx == -1 {
		return nil, fmt.Errorf("WITH and RETURN clauses required")
	}

	// Check for UNWIND between WITH and RETURN - delegate to specialized handler
	unwindIdx := findKeywordNotInBrackets(upper[withIdx:], " UNWIND ")
	if unwindIdx > 0 {
		return e.executeMatchWithUnwind(ctx, cypher)
	}

	// Check for OPTIONAL MATCH between WITH and RETURN - delegate to specialized handler
	// This handles patterns like: MATCH (n) WITH n, x WHERE ... OPTIONAL MATCH (n)-[:REL]-(m) RETURN ...
	optMatchIdx := findKeywordIndex(cypher[withIdx:], "OPTIONAL MATCH")
	if optMatchIdx > 0 {
		return e.executeMatchWithOptionalMatch(ctx, cypher)
	}

	// Extract MATCH part (before WITH)
	matchPart := strings.TrimSpace(cypher[5:withIdx]) // Skip "MATCH"

	// Check for WHERE clause between MATCH and WITH
	whereIdx := findKeywordIndex(matchPart, "WHERE")
	var whereClause string
	var nodePatternPart string

	if whereIdx > 0 {
		nodePatternPart = strings.TrimSpace(matchPart[:whereIdx])
		whereClause = strings.TrimSpace(matchPart[whereIdx+5:]) // Skip "WHERE"
	} else {
		nodePatternPart = matchPart
	}

	// Check for relationship pattern: (a)-[r:TYPE]->(b) or (a)<-[r]-(b)
	if strings.Contains(nodePatternPart, "-[") || strings.Contains(nodePatternPart, "]-") {
		// Delegate to relationship pattern handler with WITH clause
		return e.executeMatchRelationshipsWithClause(ctx, nodePatternPart, whereClause, cypher[withIdx:])
	}

	// Parse node pattern
	nodePattern := e.parseNodePattern(nodePatternPart)

	// Get matching nodes
	var nodes []*storage.Node
	var err error

	if len(nodePattern.labels) > 0 {
		nodes, err = e.storage.GetNodesByLabel(nodePattern.labels[0])
	} else {
		nodes, err = e.storage.AllNodes()
	}
	if err != nil {
		return nil, fmt.Errorf("storage error: %w", err)
	}

	// Apply property filter from MATCH pattern (e.g., {name: 'Alice'})
	if len(nodePattern.properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
	}

	// Apply WHERE clause filter if present
	if whereClause != "" {
		nodes = e.filterNodesByWhereClause(nodes, whereClause, nodePattern.variable)
	}

	// Extract WITH clause expressions
	// Check for WHERE between WITH and RETURN (filters aggregated results, like SQL HAVING)
	withSection := strings.TrimSpace(cypher[withIdx+4 : returnIdx])
	var withClause string
	var postWithWhere string

	// Check for multiple WITH clauses (chained WITH)
	// e.g., WITH a AS x WHERE x > 5 WITH x, x * x AS squared
	secondWithIdx := findKeywordIndex(withSection, "WITH")
	if secondWithIdx > 0 {
		// Extract first WITH clause (may contain WHERE)
		firstWithSection := strings.TrimSpace(withSection[:secondWithIdx])
		// Check for WHERE in the FIRST WITH section (between first WITH and second WITH)
		firstWhereIdx := findKeywordIndex(firstWithSection, "WHERE")
		if firstWhereIdx > 0 {
			withClause = strings.TrimSpace(firstWithSection[:firstWhereIdx])
			postWithWhere = strings.TrimSpace(firstWithSection[firstWhereIdx+5:])
		} else {
			withClause = firstWithSection
		}
		// Get the second WITH clause and check for WHERE there too
		secondWithSection := strings.TrimSpace(withSection[secondWithIdx+4:])
		secondWhereIdx := findKeywordIndex(secondWithSection, "WHERE")
		if secondWhereIdx > 0 && postWithWhere == "" {
			// Only use second WHERE if first didn't have one
			postWithWhere = strings.TrimSpace(secondWithSection[secondWhereIdx+5:])
		}
	} else {
		// Find WHERE in the section between WITH and RETURN
		// Use findKeywordIndex which handles all whitespace (spaces, tabs, newlines)
		postWhereIdx := findKeywordIndex(withSection, "WHERE")
		if postWhereIdx > 0 {
			withClause = strings.TrimSpace(withSection[:postWhereIdx])
			// Skip "WHERE" (5 chars) + any trailing whitespace
			postWithWhere = strings.TrimSpace(withSection[postWhereIdx+5:])
		} else {
			withClause = withSection
		}
	}

	// Remove ORDER BY, SKIP, LIMIT from withClause (these apply after WITH processing)
	// Use findKeywordIndex which handles all whitespace (spaces, tabs, newlines)
	for _, keyword := range []string{"ORDER", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(withClause, keyword); idx >= 0 {
			withClause = strings.TrimSpace(withClause[:idx])
		}
	}
	withItems := e.splitWithItems(withClause)

	// Extract RETURN clause
	returnClause := strings.TrimSpace(cypher[returnIdx+6:])
	// Remove ORDER BY, SKIP, LIMIT
	returnEnd := len(returnClause)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnClause, keyword); idx >= 0 && idx < returnEnd {
			returnEnd = idx
		}
	}
	returnClause = strings.TrimSpace(returnClause[:returnEnd])
	returnItems := e.parseReturnItems(returnClause)

	// Parse WITH items to detect aggregations
	type withItem struct {
		expr        string
		alias       string
		isAggregate bool
	}
	var parsedWithItems []withItem
	hasWithAggregation := false

	for _, item := range withItems {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		upperItem := strings.ToUpper(item)
		asIdx := strings.Index(upperItem, " AS ")
		var alias string
		var expr string
		if asIdx > 0 {
			expr = strings.TrimSpace(item[:asIdx])
			alias = strings.TrimSpace(item[asIdx+4:])
		} else {
			expr = item
			alias = item
		}

		// Use whitespace-tolerant aggregation check
		isAgg := isAggregateFunc(expr)

		if isAgg {
			hasWithAggregation = true
		}

		parsedWithItems = append(parsedWithItems, withItem{
			expr:        expr,
			alias:       alias,
			isAggregate: isAgg,
		})
	}

	// Build computed values for each node
	type computedRow struct {
		node   *storage.Node
		values map[string]interface{}
	}
	var computedRows []computedRow

	if hasWithAggregation {
		// WITH clause has aggregation - need to GROUP BY non-aggregated columns
		// First, identify grouping keys (non-aggregated WITH items)
		var groupByExprs []withItem
		var aggregateExprs []withItem
		for _, wi := range parsedWithItems {
			if wi.isAggregate {
				aggregateExprs = append(aggregateExprs, wi)
			} else {
				groupByExprs = append(groupByExprs, wi)
			}
		}

		// Group nodes by their grouping column values
		groups := make(map[string][]*storage.Node)
		groupKeys := make(map[string]map[string]interface{}) // Store the key values for each group

		for _, node := range nodes {
			nodeMap := map[string]*storage.Node{nodePattern.variable: node}

			// Build the group key from non-aggregated expressions
			keyParts := make([]string, len(groupByExprs))
			keyValues := make(map[string]interface{})

			for i, ge := range groupByExprs {
				var val interface{}
				if strings.HasPrefix(ge.expr, nodePattern.variable+".") {
					propName := ge.expr[len(nodePattern.variable)+1:]
					val = node.Properties[propName]
				} else if ge.expr == nodePattern.variable {
					val = node
				} else {
					val = e.evaluateExpressionWithContext(ge.expr, nodeMap, nil)
				}
				keyParts[i] = fmt.Sprintf("%v", val)
				keyValues[ge.alias] = val
			}

			key := strings.Join(keyParts, "|")
			groups[key] = append(groups[key], node)
			if _, exists := groupKeys[key]; !exists {
				groupKeys[key] = keyValues
			}
		}

		// Now calculate aggregates for each group
		for key, groupNodes := range groups {
			values := make(map[string]interface{})

			// Copy non-aggregated values
			for k, v := range groupKeys[key] {
				values[k] = v
			}

			// Calculate aggregates (using whitespace-tolerant helpers)
			for _, ae := range aggregateExprs {
				inner := extractFuncInner(ae.expr)
				switch {
				case isAggregateFuncName(ae.expr, "count") && strings.Contains(strings.ToUpper(inner), "DISTINCT"):
					// COUNT(DISTINCT ...) - extract after DISTINCT
					distinctInner := strings.TrimSpace(inner[8:]) // skip "DISTINCT"
					seen := make(map[string]bool)
					for _, n := range groupNodes {
						nodeMap := map[string]*storage.Node{nodePattern.variable: n}
						var val interface{}
						if strings.HasPrefix(distinctInner, nodePattern.variable+".") {
							propName := distinctInner[len(nodePattern.variable)+1:]
							val = n.Properties[propName]
						} else if distinctInner == nodePattern.variable {
							val = string(n.ID)
						} else {
							val = e.evaluateExpressionWithContext(distinctInner, nodeMap, nil)
						}
						if val != nil {
							seen[fmt.Sprintf("%v", val)] = true
						}
					}
					values[ae.alias] = int64(len(seen))

				case isAggregateFuncName(ae.expr, "count"):
					if inner == "*" {
						values[ae.alias] = int64(len(groupNodes))
					} else {
						count := int64(0)
						for _, n := range groupNodes {
							nodeMap := map[string]*storage.Node{nodePattern.variable: n}
							var val interface{}
							if strings.HasPrefix(inner, nodePattern.variable+".") {
								propName := inner[len(nodePattern.variable)+1:]
								val = n.Properties[propName]
							} else if inner == nodePattern.variable {
								count++ // Node itself is not null
								continue
							} else {
								val = e.evaluateExpressionWithContext(inner, nodeMap, nil)
							}
							if val != nil {
								count++
							}
						}
						values[ae.alias] = count
					}

				case isAggregateFuncName(ae.expr, "sum"):
					var sumInt int64
					var sumFloat float64
					hasFloat := false
					for _, n := range groupNodes {
						nodeMap := map[string]*storage.Node{nodePattern.variable: n}
						var val interface{}
						if strings.HasPrefix(inner, nodePattern.variable+".") {
							propName := inner[len(nodePattern.variable)+1:]
							val = n.Properties[propName]
						} else {
							val = e.evaluateExpressionWithContext(inner, nodeMap, nil)
						}
						switch v := val.(type) {
						case int64:
							sumInt += v
							sumFloat += float64(v)
						case int:
							sumInt += int64(v)
							sumFloat += float64(v)
						case float64:
							hasFloat = true
							sumFloat += v
							if v == float64(int64(v)) {
								sumInt += int64(v)
							}
						}
					}
					// Return float64 if any input was float, otherwise int64
					if hasFloat {
						values[ae.alias] = sumFloat
					} else {
						values[ae.alias] = sumInt
					}

				case isAggregateFuncName(ae.expr, "collect") && strings.Contains(strings.ToUpper(inner), "DISTINCT"):
					// COLLECT(DISTINCT ...) - extract after DISTINCT
					distinctInner := strings.TrimSpace(inner[8:]) // skip "DISTINCT"
					seen := make(map[string]bool)
					var collected []interface{}
					for _, n := range groupNodes {
						nodeMap := map[string]*storage.Node{nodePattern.variable: n}
						var val interface{}
						if strings.HasPrefix(distinctInner, nodePattern.variable+".") {
							propName := distinctInner[len(nodePattern.variable)+1:]
							val = n.Properties[propName]
						} else if distinctInner == nodePattern.variable {
							val = string(n.ID)
						} else {
							val = e.evaluateExpressionWithContext(distinctInner, nodeMap, nil)
						}
						key := fmt.Sprintf("%v", val)
						if !seen[key] {
							seen[key] = true
							collected = append(collected, val)
						}
					}
					values[ae.alias] = collected

				case isAggregateFuncName(ae.expr, "collect"):
					var collected []interface{}
					for _, n := range groupNodes {
						nodeMap := map[string]*storage.Node{nodePattern.variable: n}
						var val interface{}
						if strings.HasPrefix(inner, nodePattern.variable+".") {
							propName := inner[len(nodePattern.variable)+1:]
							val = n.Properties[propName]
						} else if inner == nodePattern.variable {
							val = n
						} else {
							val = e.evaluateExpressionWithContext(inner, nodeMap, nil)
						}
						collected = append(collected, val)
					}
					values[ae.alias] = collected
				}
			}

			computedRows = append(computedRows, computedRow{node: groupNodes[0], values: values})
		}
	} else {
		// No aggregation in WITH - process each node individually
		for _, node := range nodes {
			nodeMap := map[string]*storage.Node{nodePattern.variable: node}
			values := make(map[string]interface{})

			for _, wi := range parsedWithItems {
				// Check if this is a CASE expression
				if isCaseExpression(wi.expr) {
					values[wi.alias] = e.evaluateCaseExpression(wi.expr, nodeMap, nil)
				} else if strings.HasPrefix(wi.expr, nodePattern.variable+".") {
					// Property access
					propName := wi.expr[len(nodePattern.variable)+1:]
					values[wi.alias] = node.Properties[propName]
				} else if wi.expr == nodePattern.variable {
					// Just the node variable
					values[wi.alias] = node
				} else {
					// Try to evaluate as expression
					values[wi.alias] = e.evaluateExpressionWithContext(wi.expr, nodeMap, nil)
				}
			}

			computedRows = append(computedRows, computedRow{node: node, values: values})
		}
	}

	// Apply WHERE filter after WITH (like SQL HAVING)
	if postWithWhere != "" {
		var filteredRows []computedRow
		for _, cr := range computedRows {
			// Evaluate the WHERE condition against the computed values
			if e.evaluateWithWhereCondition(postWithWhere, cr.values) {
				filteredRows = append(filteredRows, cr)
			}
		}
		computedRows = filteredRows
	}

	// Apply ORDER BY to computedRows (before building result)
	if orderByIdx := findKeywordIndex(cypher, "ORDER BY"); orderByIdx > 0 {
		ks, ke := trimKeywordWSBounds("ORDER BY")
		orderByEnd, ok := keywordMatchAt(cypher, orderByIdx, "ORDER BY", ks, ke)
		if !ok {
			return nil, fmt.Errorf("failed to parse ORDER BY clause")
		}

		orderPart := cypher[orderByEnd:]
		endIdx := len(orderPart)
		// Use findKeywordIndex which handles whitespace/newlines properly
		for _, kw := range []string{"SKIP", "LIMIT", "RETURN"} {
			if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(orderPart[:endIdx])
		isDesc := strings.HasSuffix(strings.ToUpper(orderExpr), " DESC")
		isAsc := strings.HasSuffix(strings.ToUpper(orderExpr), " ASC")
		if isDesc {
			orderExpr = strings.TrimSuffix(strings.TrimSuffix(orderExpr, " DESC"), " desc")
			orderExpr = strings.TrimSpace(orderExpr)
		} else if isAsc {
			orderExpr = strings.TrimSuffix(strings.TrimSuffix(orderExpr, " ASC"), " asc")
			orderExpr = strings.TrimSpace(orderExpr)
		}

		// Sort computedRows by the order expression
		sort.SliceStable(computedRows, func(i, j int) bool {
			var valI, valJ interface{}

			// Check if order expression is a property access
			if strings.Contains(orderExpr, ".") {
				parts := strings.SplitN(orderExpr, ".", 2)
				varName := parts[0]
				propName := parts[1]
				if nodeI, ok := computedRows[i].values[varName].(*storage.Node); ok {
					valI = nodeI.Properties[propName]
				}
				if nodeJ, ok := computedRows[j].values[varName].(*storage.Node); ok {
					valJ = nodeJ.Properties[propName]
				}
			} else {
				valI = computedRows[i].values[orderExpr]
				valJ = computedRows[j].values[orderExpr]
			}

			less := compareForSort(valI, valJ)
			if isDesc {
				return !less
			}
			return less
		})
	}

	// Now process aggregations in RETURN clause
	result := &ExecuteResult{
		Columns: make([]string, len(returnItems)),
		Rows:    [][]interface{}{},
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	// Check for aggregation functions
	hasAggregation := false
	for _, item := range returnItems {
		upperExpr := strings.ToUpper(item.expr)
		if strings.HasPrefix(upperExpr, "COUNT(") ||
			strings.HasPrefix(upperExpr, "SUM(") ||
			strings.HasPrefix(upperExpr, "AVG(") ||
			strings.HasPrefix(upperExpr, "COLLECT(") {
			hasAggregation = true
			break
		}
	}

	if hasAggregation {
		// Single aggregated row
		row := make([]interface{}, len(returnItems))

		for i, item := range returnItems {
			inner := extractFuncInner(item.expr)

			switch {
			case isAggregateFuncName(item.expr, "count") && strings.Contains(strings.ToUpper(inner), "DISTINCT"):
				// COUNT(DISTINCT variable) - extract after DISTINCT
				distinctInner := strings.TrimSpace(inner[8:]) // skip "DISTINCT"
				seen := make(map[interface{}]bool)
				for _, cr := range computedRows {
					if val, ok := cr.values[distinctInner]; ok && val != nil {
						seen[fmt.Sprintf("%v", val)] = true
					} else if cr.node != nil && distinctInner == nodePattern.variable {
						seen[string(cr.node.ID)] = true
					}
				}
				row[i] = int64(len(seen))

			case isAggregateFuncName(item.expr, "count"):
				if inner == "*" {
					row[i] = int64(len(computedRows))
				} else {
					count := int64(0)
					for _, cr := range computedRows {
						if val, ok := cr.values[inner]; ok && val != nil {
							count++
						} else if cr.node != nil {
							count++
						}
					}
					row[i] = count
				}

			case isAggregateFuncName(item.expr, "sum"):
				var sumInt int64
				var sumFloat float64
				hasFloat := false
				for _, cr := range computedRows {
					if val, ok := cr.values[inner]; ok {
						switch v := val.(type) {
						case int64:
							sumInt += v
							sumFloat += float64(v)
						case int:
							sumInt += int64(v)
							sumFloat += float64(v)
						case float64:
							hasFloat = true
							sumFloat += v
						}
					}
				}
				if hasFloat {
					row[i] = sumFloat
				} else {
					row[i] = sumInt
				}

			case isAggregateFuncName(item.expr, "collect"):
				var collected []interface{}
				for _, cr := range computedRows {
					if val, ok := cr.values[inner]; ok {
						collected = append(collected, val)
					}
				}
				row[i] = collected

			default:
				// Non-aggregate - use value from first row or pass through
				if len(computedRows) > 0 {
					if val, ok := computedRows[0].values[item.expr]; ok {
						row[i] = val
					}
				}
			}
		}

		result.Rows = append(result.Rows, row)
	} else {
		// Non-aggregated - return all rows
		for _, cr := range computedRows {
			row := make([]interface{}, len(returnItems))
			for i, item := range returnItems {
				// First try to find by expression (e.g., "cnt")
				// Trim the expression to handle any whitespace issues
				exprKey := strings.TrimSpace(item.expr)
				var val interface{}
				var found bool
				if val, found = cr.values[exprKey]; !found && item.alias != "" {
					// If not found and there's an alias, try the alias
					aliasKey := strings.TrimSpace(item.alias)
					val, found = cr.values[aliasKey]
				}
				// If still not found, try case-insensitive lookup (fallback)
				if !found {
					for k, v := range cr.values {
						if strings.EqualFold(k, exprKey) {
							val = v
							found = true
							break
						}
					}
				}
				if found {
					row[i] = val
				} else {
					// Build node map for evaluation
					nodeMap := make(map[string]*storage.Node)
					for varName, varVal := range cr.values {
						if node, ok := varVal.(*storage.Node); ok {
							nodeMap[varName] = node
						}
					}

					// Check if this is a property access on a node variable (e.g., n.name)
					if strings.Contains(item.expr, ".") && !strings.Contains(item.expr, "(") {
						parts := strings.SplitN(item.expr, ".", 2)
						varName := strings.TrimSpace(parts[0])
						propName := strings.TrimSpace(parts[1])
						if node, ok := nodeMap[varName]; ok {
							if val, ok := node.Properties[propName]; ok {
								row[i] = val
								continue
							}
							// Property doesn't exist - return nil explicitly
							row[i] = nil
							continue
						}
					}

					// Try to evaluate with nodes in context
					if len(nodeMap) > 0 {
						evalResult := e.evaluateExpressionWithContext(item.expr, nodeMap, nil)
						if evalResult != nil {
							if strResult, ok := evalResult.(string); !ok || strResult != item.expr {
								row[i] = evalResult
								continue
							}
						}
					}

					// Fall back to string substitution
					expr := item.expr
					hasSubstitution := false
					for varName, varVal := range cr.values {
						if strings.Contains(expr, varName) {
							var replacement string
							switch v := varVal.(type) {
							case []interface{}:
								parts := make([]string, len(v))
								for j, elem := range v {
									switch e := elem.(type) {
									case string:
										parts[j] = fmt.Sprintf("'%s'", e)
									default:
										parts[j] = fmt.Sprintf("%v", e)
									}
								}
								replacement = "[" + strings.Join(parts, ", ") + "]"
							case string:
								replacement = fmt.Sprintf("'%s'", v)
							case *storage.Node:
								continue
							default:
								replacement = fmt.Sprintf("%v", v)
							}
							expr = strings.ReplaceAll(expr, varName, replacement)
							hasSubstitution = true
						}
					}
					if hasSubstitution {
						row[i] = e.evaluateExpressionWithContext(expr, nodeMap, nil)
					}
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	// Apply ORDER BY, SKIP, LIMIT to results (using findKeywordIndex for whitespace tolerance)

	// Apply ORDER BY
	orderByIdx := findKeywordIndex(cypher, "ORDER")
	if orderByIdx > 0 {
		// Find start after "ORDER BY" (skip "ORDER" + whitespace + "BY")
		orderStart := orderByIdx + 5 // skip "ORDER"
		for orderStart < len(cypher) && isWhitespace(cypher[orderStart]) {
			orderStart++
		}
		if orderStart+2 <= len(cypher) && strings.EqualFold(cypher[orderStart:orderStart+2], "BY") {
			orderStart += 2
		}
		orderPart := cypher[orderStart:]
		endIdx := len(orderPart)
		// Find SKIP or LIMIT
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(orderPart[:endIdx])
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	// Apply SKIP
	skipIdx := findKeywordIndex(cypher, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(cypher[skipIdx+4:])
		skipPart = strings.Fields(skipPart)[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	// Apply LIMIT
	limitIdx := findKeywordIndex(cypher, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(cypher[limitIdx+5:])
		limitPart = strings.Fields(limitPart)[0]
		if l, err := strconv.Atoi(limitPart); err == nil {
			limit = l
		}
	}

	// Apply SKIP and LIMIT
	if skip > 0 || limit >= 0 {
		startIdx := skip
		if startIdx > len(result.Rows) {
			startIdx = len(result.Rows)
		}
		endIdx := len(result.Rows)
		if limit >= 0 && startIdx+limit < endIdx {
			endIdx = startIdx + limit
		}
		result.Rows = result.Rows[startIdx:endIdx]
	}

	return result, nil
}

// executeMatchWithOptionalMatch handles MATCH ... WITH ... WHERE ... OPTIONAL MATCH ... RETURN queries
// This is a Neo4j compatibility feature that processes WITH clause filtering before OPTIONAL MATCH
func (e *StorageExecutor) executeMatchWithOptionalMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Find clause boundaries
	withIdx := findKeywordIndex(cypher, "WITH")
	optMatchIdx := findKeywordIndex(cypher, "OPTIONAL MATCH")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if withIdx == -1 || optMatchIdx == -1 || returnIdx == -1 {
		return nil, fmt.Errorf("WITH, OPTIONAL MATCH, and RETURN clauses required")
	}

	// Extract MATCH part (before WITH)
	matchPart := strings.TrimSpace(cypher[5:withIdx]) // Skip "MATCH"

	// Check for WHERE clause between MATCH and WITH
	matchWhereIdx := findKeywordIndex(matchPart, "WHERE")
	var matchWhereClause string
	var nodePatternPart string

	if matchWhereIdx > 0 {
		nodePatternPart = strings.TrimSpace(matchPart[:matchWhereIdx])
		matchWhereClause = strings.TrimSpace(matchPart[matchWhereIdx+5:])
	} else {
		nodePatternPart = matchPart
	}

	// Parse node pattern
	nodePattern := e.parseNodePattern(nodePatternPart)

	// Get matching nodes
	var nodes []*storage.Node
	var err error
	if len(nodePattern.labels) > 0 {
		nodes, err = e.storage.GetNodesByLabel(nodePattern.labels[0])
	} else {
		nodes, err = e.storage.AllNodes()
	}
	if err != nil {
		return nil, fmt.Errorf("storage error: %w", err)
	}

	// Apply property filter from MATCH pattern
	if len(nodePattern.properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
	}

	// Apply WHERE clause filter from MATCH if present
	if matchWhereClause != "" {
		nodes = e.filterNodesByWhereClause(nodes, matchWhereClause, nodePattern.variable)
	}

	// Extract WITH clause section (between WITH and OPTIONAL MATCH)
	withSection := strings.TrimSpace(cypher[withIdx+4 : optMatchIdx])

	// Check for WHERE after WITH (filters WITH results)
	var withClause string
	var postWithWhere string

	postWhereIdx := findKeywordIndex(withSection, "WHERE")
	if postWhereIdx > 0 {
		withClause = strings.TrimSpace(withSection[:postWhereIdx])
		postWithWhere = strings.TrimSpace(withSection[postWhereIdx+5:])
	} else {
		withClause = withSection
	}

	// Parse WITH items
	withItems := e.splitWithItems(withClause)

	// Build computed values for each node from WITH clause
	type computedRow struct {
		node   *storage.Node
		values map[string]interface{}
	}
	var computedRows []computedRow

	for _, node := range nodes {
		nodeMap := map[string]*storage.Node{nodePattern.variable: node}
		values := make(map[string]interface{})

		for _, item := range withItems {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}

			upperItem := strings.ToUpper(item)
			asIdx := strings.Index(upperItem, " AS ")
			var alias string
			var expr string
			if asIdx > 0 {
				expr = strings.TrimSpace(item[:asIdx])
				alias = strings.TrimSpace(item[asIdx+4:])
			} else {
				expr = item
				alias = item
			}

			// Evaluate the expression
			if expr == nodePattern.variable {
				values[alias] = node
			} else if strings.HasPrefix(expr, nodePattern.variable+".") {
				propName := expr[len(nodePattern.variable)+1:]
				values[alias] = node.Properties[propName]
			} else {
				// Try to evaluate as numeric or expression
				values[alias] = e.evaluateExpressionWithContext(expr, nodeMap, nil)
			}
		}

		computedRows = append(computedRows, computedRow{node: node, values: values})
	}

	// Apply WHERE filter after WITH (filters computed rows)
	if postWithWhere != "" {
		var filteredRows []computedRow
		for _, cr := range computedRows {
			if e.evaluateWithWhereCondition(postWithWhere, cr.values) {
				filteredRows = append(filteredRows, cr)
			}
		}
		computedRows = filteredRows
	}

	// Extract OPTIONAL MATCH pattern (between OPTIONAL MATCH and RETURN)
	optMatchPattern := strings.TrimSpace(cypher[optMatchIdx+14 : returnIdx])

	// Check for WHERE in OPTIONAL MATCH section
	optMatchWhereIdx := findKeywordIndex(optMatchPattern, "WHERE")
	var optMatchWhereClause string
	if optMatchWhereIdx > 0 {
		optMatchWhereClause = strings.TrimSpace(optMatchPattern[optMatchWhereIdx+5:])
		optMatchPattern = strings.TrimSpace(optMatchPattern[:optMatchWhereIdx])
	}

	// Parse OPTIONAL MATCH relationship pattern
	relPattern := e.parseOptionalRelPattern(optMatchPattern)

	// Build result with left outer join semantics
	type joinedRow struct {
		computedValues map[string]interface{}
		relatedNode    *storage.Node
		relationship   *storage.Edge
	}
	var joinedRows []joinedRow

	for idx, cr := range computedRows {
		// Find the node in the computed values (the one we're joining from)
		var sourceNode *storage.Node
		for _, v := range cr.values {
			if node, ok := v.(*storage.Node); ok {
				sourceNode = node
				break
			}
		}

		if sourceNode == nil {
			// No node to join from - add row with nils
			joinedRows = append(joinedRows, joinedRow{
				computedValues: cr.values,
				relatedNode:    nil,
				relationship:   nil,
			})
			continue
		}

		// Try to find related nodes via the relationship
		relatedNodes := e.findRelatedNodes(sourceNode, relPattern)

		if len(relatedNodes) == 0 {
			// No match - add row with null for the optional part (left outer join)
			joinedRows = append(joinedRows, joinedRow{
				computedValues: cr.values,
				relatedNode:    nil,
				relationship:   nil,
			})
		} else {
			// Add a row for each match
			addedAny := false
			for _, related := range relatedNodes {
				// Apply optional WHERE filter on related node
				if optMatchWhereClause != "" {
					// Simple property filter
					if related.node != nil && !e.nodeMatchesWhereClause(related.node, optMatchWhereClause, relPattern.targetVar) {
						continue
					}
				}
				joinedRows = append(joinedRows, joinedRow{
					computedValues: cr.values,
					relatedNode:    related.node,
					relationship:   related.edge,
				})
				addedAny = true
			}
			// If all related nodes were filtered out, add a null row
			if !addedAny {
				joinedRows = append(joinedRows, joinedRow{
					computedValues: cr.values,
					relatedNode:    nil,
					relationship:   nil,
				})
			}
		}
		_ = idx // Suppress unused variable warning
	}

	// Extract RETURN clause
	returnClause := strings.TrimSpace(cypher[returnIdx+6:])

	// Remove ORDER BY, SKIP, LIMIT from return clause
	returnEnd := len(returnClause)
	for _, keyword := range []string{"ORDER", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnClause, keyword); idx >= 0 && idx < returnEnd {
			returnEnd = idx
		}
	}
	returnExpr := strings.TrimSpace(returnClause[:returnEnd])

	// Parse RETURN items
	returnItems := e.parseReturnItems(returnExpr)

	// Build result
	result := &ExecuteResult{
		Columns: make([]string, len(returnItems)),
		Rows:    make([][]interface{}, 0),
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	// Build result rows
	for _, jr := range joinedRows {
		row := make([]interface{}, len(returnItems))

		// Build context for expression evaluation
		nodeMap := make(map[string]*storage.Node)
		edgeMap := make(map[string]*storage.Edge)

		// Add nodes from computed values
		for varName, varVal := range jr.computedValues {
			if node, ok := varVal.(*storage.Node); ok {
				nodeMap[varName] = node
			}
		}

		// Add related node if present
		if jr.relatedNode != nil && relPattern.targetVar != "" {
			nodeMap[relPattern.targetVar] = jr.relatedNode
		}

		// Add relationship if present
		if jr.relationship != nil && relPattern.relVar != "" {
			edgeMap[relPattern.relVar] = jr.relationship
		}

		for i, item := range returnItems {
			expr := item.expr

			// Handle CASE expressions
			if isCaseExpression(expr) {
				row[i] = e.evaluateCaseExpression(expr, nodeMap, edgeMap)
				continue
			}

			// Handle COALESCE
			if strings.HasPrefix(strings.ToUpper(expr), "COALESCE(") {
				row[i] = e.evaluateCoalesceInContext(expr, nodeMap, edgeMap, jr.computedValues)
				continue
			}

			// Check computed values first
			if val, ok := jr.computedValues[expr]; ok {
				row[i] = val
				continue
			}

			// Handle property access
			if strings.Contains(expr, ".") && !strings.Contains(expr, "(") {
				parts := strings.SplitN(expr, ".", 2)
				varName := parts[0]
				propName := parts[1]

				if node, ok := nodeMap[varName]; ok && node != nil {
					row[i] = node.Properties[propName]
					continue
				}
				// Node is nil (from OPTIONAL MATCH)
				row[i] = nil
				continue
			}

			// Check node map
			if node, ok := nodeMap[expr]; ok {
				row[i] = node
				continue
			}

			// Check edge map
			if edge, ok := edgeMap[expr]; ok {
				row[i] = edge
				continue
			}

			// Try expression evaluation
			row[i] = e.evaluateExpressionWithContext(expr, nodeMap, edgeMap)
		}

		result.Rows = append(result.Rows, row)
	}

	// Apply ORDER BY, SKIP, LIMIT
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
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	skipIdx := findKeywordIndex(cypher, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(cypher[skipIdx+4:])
		skipPart = strings.Fields(skipPart)[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	limitIdx := findKeywordIndex(cypher, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(cypher[limitIdx+5:])
		limitPart = strings.Fields(limitPart)[0]
		if l, err := strconv.Atoi(limitPart); err == nil {
			limit = l
		}
	}

	if skip > 0 || limit >= 0 {
		startIdx := skip
		if startIdx > len(result.Rows) {
			startIdx = len(result.Rows)
		}
		endIdx := len(result.Rows)
		if limit >= 0 && startIdx+limit < endIdx {
			endIdx = startIdx + limit
		}
		result.Rows = result.Rows[startIdx:endIdx]
	}

	return result, nil
}

// evaluateCoalesceInContext evaluates COALESCE function with node and computed value context
