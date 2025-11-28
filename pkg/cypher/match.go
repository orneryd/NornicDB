// MATCH clause implementation for NornicDB.
// This file contains MATCH execution, aggregation, ordering, and filtering.

package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	upper := strings.ToUpper(cypher)

	// Check for WITH clause between MATCH and RETURN
	// This handles MATCH ... WITH (CASE WHEN) ... RETURN queries
	// But we must avoid false positives from "STARTS WITH" or "ENDS WITH" in WHERE clauses
	withIdx := findKeywordIndex(cypher, "WITH")
	returnIdx := findKeywordIndex(cypher, "RETURN")

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
	for _, keyword := range []string{" ORDER BY ", " SKIP ", " LIMIT "} {
		idx := strings.Index(strings.ToUpper(returnPart), keyword)
		if idx >= 0 && idx < returnEndIdx {
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
		upperExpr := strings.ToUpper(item.expr)
		if strings.HasPrefix(upperExpr, "COUNT(") ||
			strings.HasPrefix(upperExpr, "SUM(") ||
			strings.HasPrefix(upperExpr, "AVG(") ||
			strings.HasPrefix(upperExpr, "MIN(") ||
			strings.HasPrefix(upperExpr, "MAX(") ||
			strings.HasPrefix(upperExpr, "COLLECT(") {
			hasAggregation = true
			break
		}
	}

	// Extract pattern between MATCH and WHERE/RETURN
	matchPart := cypher[5:] // Skip "MATCH"
	whereIdx := findKeywordIndex(cypher, "WHERE")
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
		return e.executeMatchWithRelationships(matchPart, whereClause, returnItems)
	}

	// Parse node pattern
	nodePattern := e.parseNodePattern(matchPart)

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

	// Apply WHERE filter if present
	if whereIdx > 0 {
		// Find end of WHERE clause (before RETURN)
		wherePart := cypher[whereIdx+5 : returnIdx]
		nodes = e.filterNodes(nodes, nodePattern.variable, strings.TrimSpace(wherePart))
	}

	// Handle aggregation queries
	if hasAggregation {
		return e.executeAggregation(nodes, nodePattern.variable, returnItems, result)
	}

	// Parse ORDER BY
	orderByIdx := strings.Index(upper, "ORDER BY")
	if orderByIdx > 0 {
		orderPart := upper[orderByIdx+8:]
		// Find end
		endIdx := len(orderPart)
		for _, kw := range []string{" SKIP ", " LIMIT "} {
			if idx := strings.Index(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(cypher[orderByIdx+8 : orderByIdx+8+endIdx])
		nodes = e.orderNodes(nodes, nodePattern.variable, orderExpr)
	}

	// Parse SKIP
	skipIdx := strings.Index(upper, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(cypher[skipIdx+4:])
		skipPart = strings.Split(skipPart, " ")[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	// Parse LIMIT
	limitIdx := strings.Index(upper, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(cypher[limitIdx+5:])
		limitPart = strings.Split(limitPart, " ")[0]
		if l, err := strconv.Atoi(limitPart); err == nil {
			limit = l
		}
	}

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
			row[j] = e.resolveReturnItem(item, nodePattern.variable, node)
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
func (e *StorageExecutor) executeAggregation(nodes []*storage.Node, variable string, items []returnItem, result *ExecuteResult) (*ExecuteResult, error) {
	// Use pre-compiled case-insensitive regex patterns for aggregation functions

	// Pre-compute upper-case expressions ONCE for all subsequent use
	upperExprs := make([]string, len(items))
	for i, item := range items {
		upperExprs[i] = strings.ToUpper(item.expr)
	}
	upperVariable := strings.ToUpper(variable)

	// Identify which columns are aggregations and which are grouping keys
	type colInfo struct {
		isAggregation bool
		propName      string // For grouping columns: the property being accessed
	}
	colInfos := make([]colInfo, len(items))

	for i, item := range items {
		upperExpr := upperExprs[i] // Use pre-computed upper-case
		if strings.HasPrefix(upperExpr, "COUNT(") ||
			strings.HasPrefix(upperExpr, "SUM(") ||
			strings.HasPrefix(upperExpr, "AVG(") ||
			strings.HasPrefix(upperExpr, "MIN(") ||
			strings.HasPrefix(upperExpr, "MAX(") ||
			strings.HasPrefix(upperExpr, "COLLECT(") {
			colInfos[i] = colInfo{isAggregation: true}
		} else {
			// Non-aggregation - this becomes an implicit GROUP BY key
			propName := ""
			if strings.HasPrefix(item.expr, variable+".") {
				propName = item.expr[len(variable)+1:]
			}
			colInfos[i] = colInfo{isAggregation: false, propName: propName}
		}
	}

	// Check if there are any grouping columns
	hasGrouping := false
	for _, ci := range colInfos {
		if !ci.isAggregation && ci.propName != "" {
			hasGrouping = true
			break
		}
	}

	// If no grouping columns OR no nodes, return single aggregated row (old behavior)
	if !hasGrouping || len(nodes) == 0 {
		return e.executeAggregationSingleGroup(nodes, variable, items, result)
	}

	// Group nodes by the non-aggregated column values
	groups := make(map[string][]*storage.Node)
	groupKeys := make(map[string][]interface{}) // Store the actual key values

	for _, node := range nodes {
		// Build group key from all non-aggregated columns
		keyParts := make([]interface{}, 0)
		for i, ci := range colInfos {
			if !ci.isAggregation {
				var val interface{}
				if ci.propName != "" {
					val = node.Properties[ci.propName]
				} else {
					val = e.resolveReturnItem(items[i], variable, node)
				}
				keyParts = append(keyParts, val)
			}
		}
		key := fmt.Sprintf("%v", keyParts)
		groups[key] = append(groups[key], node)
		if _, exists := groupKeys[key]; !exists {
			groupKeys[key] = keyParts
		}
	}

	// Build result rows - one per group
	for key, groupNodes := range groups {
		row := make([]interface{}, len(items))
		keyIdx := 0 // Track position in keyParts

		for i, item := range items {
			upperExpr := upperExprs[i] // Use pre-computed upper-case expression

			if !colInfos[i].isAggregation {
				// Non-aggregated column - use the group key value
				row[i] = groupKeys[key][keyIdx]
				keyIdx++
				continue
			}

			switch {
			case strings.HasPrefix(upperExpr, "COUNT("):
				// COUNT(*) or COUNT(n)
				if strings.Contains(upperExpr, "*") || strings.Contains(upperExpr, "("+upperVariable+")") {
					row[i] = int64(len(groupNodes))
				} else {
					// COUNT(n.property) - count non-null values
					propMatch := countPropPattern.FindStringSubmatch(item.expr)
					if len(propMatch) == 3 {
						count := int64(0)
						for _, node := range groupNodes {
							if _, exists := node.Properties[propMatch[2]]; exists {
								count++
							}
						}
						row[i] = count
					} else {
						row[i] = int64(len(groupNodes))
					}
				}

			case strings.HasPrefix(upperExpr, "SUM("):
				propMatch := sumPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					sum := float64(0)
					for _, node := range groupNodes {
						if val, exists := node.Properties[propMatch[2]]; exists {
							if num, ok := toFloat64(val); ok {
								sum += num
							}
						}
					}
					row[i] = sum
				} else {
					row[i] = float64(0)
				}

			case strings.HasPrefix(upperExpr, "AVG("):
				propMatch := avgPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					sum := float64(0)
					count := 0
					for _, node := range groupNodes {
						if val, exists := node.Properties[propMatch[2]]; exists {
							if num, ok := toFloat64(val); ok {
								sum += num
								count++
							}
						}
					}
					if count > 0 {
						row[i] = sum / float64(count)
					} else {
						row[i] = nil
					}
				} else {
					row[i] = nil
				}

			case strings.HasPrefix(upperExpr, "MIN("):
				propMatch := minPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					var min *float64
					for _, node := range groupNodes {
						if val, exists := node.Properties[propMatch[2]]; exists {
							if num, ok := toFloat64(val); ok {
								if min == nil || num < *min {
									minVal := num
									min = &minVal
								}
							}
						}
					}
					if min != nil {
						row[i] = *min
					} else {
						row[i] = nil
					}
				} else {
					row[i] = nil
				}

			case strings.HasPrefix(upperExpr, "MAX("):
				propMatch := maxPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					var max *float64
					for _, node := range groupNodes {
						if val, exists := node.Properties[propMatch[2]]; exists {
							if num, ok := toFloat64(val); ok {
								if max == nil || num > *max {
									maxVal := num
									max = &maxVal
								}
							}
						}
					}
					if max != nil {
						row[i] = *max
					} else {
						row[i] = nil
					}
				} else {
					row[i] = nil
				}

			case strings.HasPrefix(upperExpr, "COLLECT("):
				propMatch := collectPropPattern.FindStringSubmatch(item.expr)
				collected := make([]interface{}, 0)
				if len(propMatch) >= 2 {
					for _, node := range groupNodes {
						if len(propMatch) == 3 && propMatch[2] != "" {
							// COLLECT(n.property)
							if val, exists := node.Properties[propMatch[2]]; exists {
								collected = append(collected, val)
							}
						} else {
							// COLLECT(n)
							collected = append(collected, map[string]interface{}{
								"id":         string(node.ID),
								"labels":     node.Labels,
								"properties": node.Properties,
							})
						}
					}
				}
				row[i] = collected
			}
		}

		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// executeAggregationSingleGroup handles aggregation without grouping (original behavior)
func (e *StorageExecutor) executeAggregationSingleGroup(nodes []*storage.Node, variable string, items []returnItem, result *ExecuteResult) (*ExecuteResult, error) {
	row := make([]interface{}, len(items))

	// Pre-compute upper-case expressions ONCE to avoid repeated ToUpper calls in loop
	upperExprs := make([]string, len(items))
	for i, item := range items {
		upperExprs[i] = strings.ToUpper(item.expr)
	}
	upperVariable := strings.ToUpper(variable)

	// Use pre-compiled regex patterns from regex_patterns.go

	for i, item := range items {
		upperExpr := upperExprs[i]

		switch {
		// Handle SUM() + SUM() arithmetic expressions first
		case strings.Contains(upperExpr, "+") && strings.Contains(upperExpr, "SUM("):
			row[i] = e.evaluateSumArithmetic(item.expr, nodes, variable)

		// Handle COUNT(DISTINCT n.property)
		case strings.HasPrefix(upperExpr, "COUNT(") && strings.Contains(upperExpr, "DISTINCT"):
			propMatch := countDistinctPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				seen := make(map[interface{}]bool)
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists && val != nil {
						seen[val] = true
					}
				}
				row[i] = int64(len(seen))
			} else {
				// COUNT(DISTINCT n) - count distinct nodes
				row[i] = int64(len(nodes))
			}

		case strings.HasPrefix(upperExpr, "COUNT("):
			if strings.Contains(upperExpr, "*") || strings.Contains(upperExpr, "("+upperVariable+")") {
				row[i] = int64(len(nodes))
			} else {
				propMatch := countPropPattern.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					count := int64(0)
					for _, node := range nodes {
						if _, exists := node.Properties[propMatch[2]]; exists {
							count++
						}
					}
					row[i] = count
				} else {
					row[i] = int64(len(nodes))
				}
			}

		case strings.HasPrefix(upperExpr, "SUM("):
			propMatch := sumPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				sum := float64(0)
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
				}
				row[i] = sum
			} else {
				row[i] = float64(0)
			}

		case strings.HasPrefix(upperExpr, "AVG("):
			propMatch := avgPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				sum := float64(0)
				count := 0
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							sum += num
							count++
						}
					}
				}
				if count > 0 {
					row[i] = sum / float64(count)
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "MIN("):
			propMatch := minPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				var min *float64
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							if min == nil || num < *min {
								minVal := num
								min = &minVal
							}
						}
					}
				}
				if min != nil {
					row[i] = *min
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "MAX("):
			propMatch := maxPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				var max *float64
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							if max == nil || num > *max {
								maxVal := num
								max = &maxVal
							}
						}
					}
				}
				if max != nil {
					row[i] = *max
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		// Handle COLLECT(DISTINCT n.property)
		case strings.HasPrefix(upperExpr, "COLLECT(") && strings.Contains(upperExpr, "DISTINCT"):
			propMatch := collectDistinctPropPattern.FindStringSubmatch(item.expr)
			seen := make(map[interface{}]bool)
			collected := make([]interface{}, 0)
			if len(propMatch) == 3 {
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists && val != nil {
						if !seen[val] {
							seen[val] = true
							collected = append(collected, val)
						}
					}
				}
			}
			row[i] = collected

		case strings.HasPrefix(upperExpr, "COLLECT("):
			propMatch := collectPropPattern.FindStringSubmatch(item.expr)
			collected := make([]interface{}, 0)
			if len(propMatch) >= 2 {
				for _, node := range nodes {
					if len(propMatch) == 3 && propMatch[2] != "" {
						if val, exists := node.Properties[propMatch[2]]; exists {
							collected = append(collected, val)
						}
					} else {
						collected = append(collected, map[string]interface{}{
							"id":         string(node.ID),
							"labels":     node.Labels,
							"properties": node.Properties,
						})
					}
				}
			}
			row[i] = collected

		default:
			// Non-aggregate in aggregation query - return first value
			if len(nodes) > 0 {
				row[i] = e.resolveReturnItem(item, variable, nodes[0])
			} else {
				row[i] = nil
			}
		}
	}

	result.Rows = [][]interface{}{row}
	return result, nil
}

// orderNodes sorts nodes by the given expression
func (e *StorageExecutor) orderNodes(nodes []*storage.Node, variable, orderExpr string) []*storage.Node {
	// Parse: n.property [ASC|DESC]
	desc := strings.HasSuffix(strings.ToUpper(orderExpr), " DESC")
	orderExpr = strings.TrimSuffix(strings.TrimSuffix(orderExpr, " DESC"), " ASC")
	orderExpr = strings.TrimSpace(orderExpr)

	// Extract property name
	var propName string
	if strings.HasPrefix(orderExpr, variable+".") {
		propName = orderExpr[len(variable)+1:]
	} else {
		propName = orderExpr
	}

	// Sort using a simple bubble sort (could use sort.Slice for efficiency)
	sorted := make([]*storage.Node, len(nodes))
	copy(sorted, nodes)

	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			val1, _ := sorted[j].Properties[propName]
			val2, _ := sorted[j+1].Properties[propName]

			shouldSwap := false
			num1, ok1 := toFloat64(val1)
			num2, ok2 := toFloat64(val2)

			if ok1 && ok2 {
				if desc {
					shouldSwap = num1 < num2
				} else {
					shouldSwap = num1 > num2
				}
			} else {
				str1 := fmt.Sprintf("%v", val1)
				str2 := fmt.Sprintf("%v", val2)
				if desc {
					shouldSwap = str1 < str2
				} else {
					shouldSwap = str1 > str2
				}
			}

			if shouldSwap {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	return sorted
}

// executeMatchWithClause handles MATCH ... WITH ... RETURN queries
// This processes computed values (like CASE WHEN) in the WITH clause
func (e *StorageExecutor) executeMatchWithClause(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Find clause boundaries
	withIdx := findKeywordIndex(cypher, "WITH")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if withIdx == -1 || returnIdx == -1 {
		return nil, fmt.Errorf("WITH and RETURN clauses required")
	}

	// Extract MATCH part (before WITH)
	matchPart := strings.TrimSpace(cypher[5:withIdx]) // Skip "MATCH"

	// Parse node pattern
	nodePattern := e.parseNodePattern(matchPart)

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

	// Extract WITH clause expressions
	withClause := strings.TrimSpace(cypher[withIdx+4 : returnIdx])
	withItems := e.splitWithItems(withClause)

	// Extract RETURN clause
	returnClause := strings.TrimSpace(cypher[returnIdx+6:])
	// Remove ORDER BY, SKIP, LIMIT
	for _, keyword := range []string{" ORDER BY ", " SKIP ", " LIMIT "} {
		if idx := strings.Index(strings.ToUpper(returnClause), keyword); idx >= 0 {
			returnClause = returnClause[:idx]
		}
	}
	returnItems := e.parseReturnItems(returnClause)

	// Build computed values for each node
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

			// Check if this is a CASE expression
			if isCaseExpression(expr) {
				values[alias] = e.evaluateCaseExpression(expr, nodeMap, nil)
			} else if strings.HasPrefix(expr, nodePattern.variable+".") {
				// Property access
				propName := expr[len(nodePattern.variable)+1:]
				values[alias] = node.Properties[propName]
			} else if expr == nodePattern.variable {
				// Just the node variable
				values[alias] = node
			} else {
				// Try to evaluate as expression
				values[alias] = e.evaluateExpressionWithContext(expr, nodeMap, nil)
			}
		}

		computedRows = append(computedRows, computedRow{node: node, values: values})
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
			upperExpr := strings.ToUpper(item.expr)

			switch {
			case strings.HasPrefix(upperExpr, "COUNT(DISTINCT "):
				// COUNT(DISTINCT variable)
				inner := item.expr[15 : len(item.expr)-1]
				seen := make(map[interface{}]bool)
				for _, cr := range computedRows {
					if val, ok := cr.values[inner]; ok && val != nil {
						seen[fmt.Sprintf("%v", val)] = true
					} else if cr.node != nil && inner == nodePattern.variable {
						seen[string(cr.node.ID)] = true
					}
				}
				row[i] = int64(len(seen))

			case strings.HasPrefix(upperExpr, "COUNT("):
				inner := item.expr[6 : len(item.expr)-1]
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

			case strings.HasPrefix(upperExpr, "SUM("):
				inner := item.expr[4 : len(item.expr)-1]
				sum := float64(0)
				for _, cr := range computedRows {
					if val, ok := cr.values[inner]; ok {
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
				}
				row[i] = sum

			case strings.HasPrefix(upperExpr, "COLLECT("):
				inner := item.expr[8 : len(item.expr)-1]
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
				if val, ok := cr.values[item.expr]; ok {
					row[i] = val
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// evaluateSumArithmetic handles expressions like SUM(n.a) + SUM(n.b)
// Uses pre-compiled sumPropPattern from regex_patterns.go
func (e *StorageExecutor) evaluateSumArithmetic(expr string, nodes []*storage.Node, variable string) float64 {
	// Split by + and - operators (respecting parentheses)
	parts := splitArithmeticExpression(expr)
	result := float64(0)
	currentOp := "+"

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "+" {
			currentOp = "+"
			continue
		}
		if part == "-" {
			currentOp = "-"
			continue
		}

		// Evaluate this part
		var value float64
		upperPart := strings.ToUpper(part)

		if strings.HasPrefix(upperPart, "SUM(") {
			propMatch := sumPropPattern.FindStringSubmatch(part)
			if len(propMatch) == 3 {
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							value += num
						}
					}
				}
			}
		} else if num, err := strconv.ParseFloat(part, 64); err == nil {
			value = num
		}

		// Apply operator
		if currentOp == "+" {
			result += value
		} else {
			result -= value
		}
	}

	return result
}

// splitArithmeticExpression splits an arithmetic expression by + and - operators
// while respecting parentheses
func splitArithmeticExpression(expr string) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for i, ch := range expr {
		if ch == '(' {
			depth++
			current.WriteRune(ch)
		} else if ch == ')' {
			depth--
			current.WriteRune(ch)
		} else if depth == 0 && (ch == '+' || ch == '-') {
			// Check if this is a unary minus (at start or after operator)
			isUnary := i == 0 || (i > 0 && (expr[i-1] == '+' || expr[i-1] == '-' || expr[i-1] == '('))
			if !isUnary {
				if current.Len() > 0 {
					parts = append(parts, current.String())
					current.Reset()
				}
				parts = append(parts, string(ch))
			} else {
				current.WriteRune(ch)
			}
		} else {
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// filterNodesByProperties filters nodes to only include those matching ALL specified properties.
// This is used for MATCH pattern property filtering like MATCH (n:Label {prop: value}).
// Uses parallel execution for large datasets (>1000 nodes) for improved performance.
func (e *StorageExecutor) filterNodesByProperties(nodes []*storage.Node, props map[string]interface{}) []*storage.Node {
	if len(props) == 0 {
		return nodes
	}

	// Create filter function that checks all properties
	filterFn := func(node *storage.Node) bool {
		for key, expectedVal := range props {
			actualVal, exists := node.Properties[key]
			if !exists {
				return false
			}
			if !e.compareEqual(actualVal, expectedVal) {
				return false
			}
		}
		return true
	}

	// Use parallel filtering for large datasets
	return parallelFilterNodes(nodes, filterFn)
}

// executeCreate handles CREATE queries.
