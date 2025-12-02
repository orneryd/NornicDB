// MATCH clause implementation for NornicDB.
// This file contains MATCH execution, aggregation, ordering, and filtering.

package cypher

import (
	"context"
	"fmt"
	"sort"
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

	// Check for multiple MATCH clauses (excluding OPTIONAL MATCH, UNION, EXISTS)
	// This handles: MATCH (a)-[:REL]->(b) MATCH (c)-[:REL]->(b) WHERE a <> c RETURN a, b, c
	// And also: MATCH (a)-[:REL]->(b) MATCH (a)-[:REL2]->(c) RETURN count(a), b.name (with aggregation)
	// But NOT: MATCH (a) RETURN a UNION MATCH (b) RETURN b
	// And NOT: MATCH (n) WHERE EXISTS { MATCH (m) ... } RETURN n
	hasUnion := strings.Contains(upper, "UNION")
	hasExists := strings.Contains(upper, "EXISTS")
	hasCountSubquery := strings.Contains(upper, "COUNT {")
	hasWith := findKeywordIndex(cypher, "WITH") > 0

	if !hasUnion && !hasExists && !hasCountSubquery && !hasWith {
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
	// Use findKeywordNotInBrackets to avoid matching WHERE inside list comprehensions like [x WHERE ...]
	matchPart := cypher[5:] // Skip "MATCH"
	whereIdx := findKeywordNotInBrackets(upper, " WHERE ")
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
		aggResult, err := e.executeAggregation(nodes, nodePattern.variable, returnItems, result)
		if err != nil {
			return nil, err
		}
		// Apply ORDER BY to aggregated results
		orderByIdx := strings.Index(upper, "ORDER BY")
		if orderByIdx > 0 {
			orderPart := upper[orderByIdx+8:]
			endIdx := len(orderPart)
			for _, kw := range []string{" SKIP ", " LIMIT "} {
				if idx := strings.Index(orderPart, kw); idx >= 0 && idx < endIdx {
					endIdx = idx
				}
			}
			orderExpr := strings.TrimSpace(cypher[orderByIdx+8 : orderByIdx+8+endIdx])
			aggResult.Rows = e.orderResultRows(aggResult.Rows, aggResult.Columns, orderExpr)
		}

		// Apply SKIP to aggregated results
		skipIdx := strings.Index(upper, "SKIP")
		skip := 0
		if skipIdx > 0 {
			skipPart := strings.TrimSpace(cypher[skipIdx+4:])
			skipPart = strings.Split(skipPart, " ")[0]
			if s, err := strconv.Atoi(skipPart); err == nil {
				skip = s
			}
		}

		// Apply LIMIT to aggregated results
		limitIdx := strings.Index(upper, "LIMIT")
		limit := -1
		if limitIdx > 0 {
			limitPart := strings.TrimSpace(cypher[limitIdx+5:])
			limitPart = strings.Split(limitPart, " ")[0]
			if l, err := strconv.Atoi(limitPart); err == nil {
				limit = l
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
			inner := item.expr[4 : len(item.expr)-1] // Extract inner expression
			propMatch := sumPropPattern.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				// SUM(n.property)
				sum := float64(0)
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
				}
				row[i] = sum
			} else if isCaseExpression(inner) {
				// SUM(CASE WHEN ... END)
				sum := float64(0)
				for _, node := range nodes {
					nodeMap := map[string]*storage.Node{variable: node}
					val := e.evaluateCaseExpression(inner, nodeMap, nil)
					if num, ok := toFloat64(val); ok {
						sum += num
					}
				}
				row[i] = sum
			} else if num, ok := toFloat64(e.parseValue(inner)); ok {
				// SUM(literal) like SUM(1)
				row[i] = num * float64(len(nodes))
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

// nodeOrderSpec represents a single ORDER BY specification for nodes
type nodeOrderSpec struct {
	propName   string
	descending bool
}

// orderNodes sorts nodes by the given expression, supporting multiple columns
func (e *StorageExecutor) orderNodes(nodes []*storage.Node, variable, orderExpr string) []*storage.Node {
	if len(nodes) <= 1 {
		return nodes
	}

	// Parse multiple ORDER BY columns: "n.value ASC, n.name DESC"
	specs := e.parseNodeOrderSpecs(orderExpr, variable)
	if len(specs) == 0 {
		return nodes
	}

	sorted := make([]*storage.Node, len(nodes))
	copy(sorted, nodes)

	sort.Slice(sorted, func(i, j int) bool {
		for _, spec := range specs {
			val1, _ := sorted[i].Properties[spec.propName]
			val2, _ := sorted[j].Properties[spec.propName]

			cmp := e.compareOrderValues(val1, val2)
			if cmp != 0 {
				if spec.descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false // All equal
	})

	return sorted
}

// parseNodeOrderSpecs parses "n.value ASC, n.name DESC" for node sorting
func (e *StorageExecutor) parseNodeOrderSpecs(orderExpr, variable string) []nodeOrderSpec {
	var specs []nodeOrderSpec

	// Split by comma
	parts := splitOutsideParens(orderExpr, ',')

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Parse: "n.property [ASC|DESC]"
		tokens := strings.Fields(part)
		if len(tokens) == 0 {
			continue
		}

		expr := tokens[0]
		descending := len(tokens) > 1 && strings.ToUpper(tokens[1]) == "DESC"

		// Extract property name
		var propName string
		if strings.HasPrefix(expr, variable+".") {
			propName = expr[len(variable)+1:]
		} else {
			propName = expr
		}

		specs = append(specs, nodeOrderSpec{propName: propName, descending: descending})
	}

	return specs
}

// executeMatchRelationshipsWithClause handles MATCH (a)-[r:TYPE]->(b) WITH ... RETURN queries
// This combines relationship traversal with WITH clause aggregation
func (e *StorageExecutor) executeMatchRelationshipsWithClause(ctx context.Context, pattern string, preWithWhere string, withAndReturn string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Parse the traversal pattern
	matches := e.parseTraversalPattern(pattern)
	if matches == nil {
		return result, fmt.Errorf("invalid traversal pattern: %s", pattern)
	}

	// Execute traversal to get all paths
	paths := e.traverseGraph(matches)

	// Apply pre-WITH WHERE clause filter if present
	if preWithWhere != "" {
		paths = e.filterPathsByWhere(paths, matches, preWithWhere)
	}

	// Parse WITH and RETURN clauses from withAndReturn string
	// withAndReturn starts with "WITH ..."
	upper := strings.ToUpper(withAndReturn)
	returnIdx := findKeywordIndex(withAndReturn, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required after WITH")
	}

	// Extract WITH clause section
	withSection := strings.TrimSpace(withAndReturn[4:returnIdx]) // Skip "WITH"

	// Check for WHERE between WITH and RETURN (post-aggregation filter, like SQL HAVING)
	var withClause string
	var postWithWhere string
	postWhereIdx := findKeywordNotInBrackets(strings.ToUpper(withSection), " WHERE ")
	if postWhereIdx > 0 {
		withClause = strings.TrimSpace(withSection[:postWhereIdx])
		postWithWhere = strings.TrimSpace(withSection[postWhereIdx+7:]) // Skip " WHERE "
	} else {
		withClause = withSection
	}

	// Extract ORDER BY, SKIP, LIMIT from after RETURN
	returnPart := strings.TrimSpace(withAndReturn[returnIdx+6:])
	var orderByClause string
	var skipVal, limitVal int

	orderByIdx := strings.Index(strings.ToUpper(returnPart), " ORDER BY ")
	if orderByIdx >= 0 {
		afterReturn := returnPart[orderByIdx+10:]
		endIdx := len(afterReturn)
		for _, kw := range []string{" SKIP ", " LIMIT "} {
			if idx := strings.Index(strings.ToUpper(afterReturn), kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderByClause = strings.TrimSpace(afterReturn[:endIdx])
		returnPart = returnPart[:orderByIdx]
	}

	// Parse SKIP
	if idx := strings.Index(upper[returnIdx:], " SKIP "); idx >= 0 {
		skipPart := withAndReturn[returnIdx+idx+6:]
		endIdx := len(skipPart)
		for _, kw := range []string{" LIMIT ", " ORDER BY "} {
			if i := strings.Index(strings.ToUpper(skipPart), kw); i >= 0 && i < endIdx {
				endIdx = i
			}
		}
		skipVal, _ = strconv.Atoi(strings.TrimSpace(skipPart[:endIdx]))
	}

	// Parse LIMIT
	if idx := strings.Index(upper[returnIdx:], " LIMIT "); idx >= 0 {
		limitPart := withAndReturn[returnIdx+idx+7:]
		endIdx := len(limitPart)
		for _, kw := range []string{" SKIP ", " ORDER BY "} {
			if i := strings.Index(strings.ToUpper(limitPart), kw); i >= 0 && i < endIdx {
				endIdx = i
			}
		}
		limitVal, _ = strconv.Atoi(strings.TrimSpace(limitPart[:endIdx]))
	}

	returnClause := strings.TrimSpace(returnPart)

	// Parse WITH items
	withItems := e.splitWithItems(withClause)
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

		upperExpr := strings.ToUpper(expr)
		isAgg := strings.HasPrefix(upperExpr, "COUNT(") ||
			strings.HasPrefix(upperExpr, "SUM(") ||
			strings.HasPrefix(upperExpr, "AVG(") ||
			strings.HasPrefix(upperExpr, "COLLECT(") ||
			strings.HasPrefix(upperExpr, "MIN(") ||
			strings.HasPrefix(upperExpr, "MAX(")

		if isAgg {
			hasWithAggregation = true
		}

		parsedWithItems = append(parsedWithItems, withItem{
			expr:        expr,
			alias:       alias,
			isAggregate: isAgg,
		})
	}

	// Build computed values for each path (or group of paths if aggregating)
	type computedRow struct {
		values map[string]interface{}
	}
	var computedRows []computedRow

	if hasWithAggregation {
		// WITH clause has aggregation - need to GROUP BY non-aggregated columns
		var groupByExprs []withItem
		var aggregateExprs []withItem
		for _, wi := range parsedWithItems {
			if wi.isAggregate {
				aggregateExprs = append(aggregateExprs, wi)
			} else {
				groupByExprs = append(groupByExprs, wi)
			}
		}

		// Group paths by their grouping column values
		groups := make(map[string][]PathResult)
		groupKeys := make(map[string]map[string]interface{})

		for _, path := range paths {
			pathCtx := e.buildPathContext(path, matches)

			// Build the group key from non-aggregated expressions
			keyParts := make([]string, len(groupByExprs))
			keyValues := make(map[string]interface{})

			for i, ge := range groupByExprs {
				val := e.evaluateExpressionWithContext(ge.expr, pathCtx.nodes, pathCtx.rels)
				keyParts[i] = fmt.Sprintf("%v", val)
				keyValues[ge.alias] = val
			}

			key := strings.Join(keyParts, "|")
			groups[key] = append(groups[key], path)
			if _, exists := groupKeys[key]; !exists {
				groupKeys[key] = keyValues
			}
		}

		// Calculate aggregates for each group
		for key, groupPaths := range groups {
			values := make(map[string]interface{})

			// Copy non-aggregated values
			for k, v := range groupKeys[key] {
				values[k] = v
			}

			// Calculate aggregates
			for _, ae := range aggregateExprs {
				upperExpr := strings.ToUpper(ae.expr)
				switch {
				case strings.HasPrefix(upperExpr, "COUNT(DISTINCT "):
					inner := ae.expr[15 : len(ae.expr)-1]
					seen := make(map[string]bool)
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithContext(inner, pCtx.nodes, pCtx.rels)
						if val != nil {
							seen[fmt.Sprintf("%v", val)] = true
						}
					}
					values[ae.alias] = int64(len(seen))

				case strings.HasPrefix(upperExpr, "COUNT("):
					inner := ae.expr[6 : len(ae.expr)-1]
					if inner == "*" {
						values[ae.alias] = int64(len(groupPaths))
					} else {
						count := int64(0)
						for _, p := range groupPaths {
							pCtx := e.buildPathContext(p, matches)
							val := e.evaluateExpressionWithContext(inner, pCtx.nodes, pCtx.rels)
							if val != nil {
								count++
							}
						}
						values[ae.alias] = count
					}

				case strings.HasPrefix(upperExpr, "SUM("):
					inner := ae.expr[4 : len(ae.expr)-1]
					sum := float64(0)
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithContext(inner, pCtx.nodes, pCtx.rels)
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
					values[ae.alias] = sum

				case strings.HasPrefix(upperExpr, "AVG("):
					inner := ae.expr[4 : len(ae.expr)-1]
					sum := float64(0)
					count := 0
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithContext(inner, pCtx.nodes, pCtx.rels)
						if num, ok := toFloat64(val); ok {
							sum += num
							count++
						}
					}
					if count > 0 {
						values[ae.alias] = sum / float64(count)
					} else {
						values[ae.alias] = nil
					}

				case strings.HasPrefix(upperExpr, "MIN("):
					inner := ae.expr[4 : len(ae.expr)-1]
					var minVal interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithContext(inner, pCtx.nodes, pCtx.rels)
						if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
							minVal = val
						}
					}
					values[ae.alias] = minVal

				case strings.HasPrefix(upperExpr, "MAX("):
					inner := ae.expr[4 : len(ae.expr)-1]
					var maxVal interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithContext(inner, pCtx.nodes, pCtx.rels)
						if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
							maxVal = val
						}
					}
					values[ae.alias] = maxVal

				case strings.HasPrefix(upperExpr, "COLLECT(DISTINCT "):
					inner := ae.expr[17 : len(ae.expr)-1]
					seen := make(map[string]bool)
					var collected []interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithContext(inner, pCtx.nodes, pCtx.rels)
						key := fmt.Sprintf("%v", val)
						if !seen[key] {
							seen[key] = true
							collected = append(collected, val)
						}
					}
					values[ae.alias] = collected

				case strings.HasPrefix(upperExpr, "COLLECT("):
					inner := ae.expr[8 : len(ae.expr)-1]
					var collected []interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithContext(inner, pCtx.nodes, pCtx.rels)
						collected = append(collected, val)
					}
					values[ae.alias] = collected
				}
			}

			computedRows = append(computedRows, computedRow{values: values})
		}
	} else {
		// No aggregation - process each path individually
		for _, path := range paths {
			pathCtx := e.buildPathContext(path, matches)
			values := make(map[string]interface{})

			for _, wi := range parsedWithItems {
				values[wi.alias] = e.evaluateExpressionWithContext(wi.expr, pathCtx.nodes, pathCtx.rels)
			}

			computedRows = append(computedRows, computedRow{values: values})
		}
	}

	// Apply post-WITH WHERE clause filter
	if postWithWhere != "" {
		var filtered []computedRow
		for _, row := range computedRows {
			if e.evaluateWhereOnComputedRow(postWithWhere, row.values) {
				filtered = append(filtered, row)
			}
		}
		computedRows = filtered
	}

	// Parse RETURN items and build final result
	returnItems := e.parseReturnItems(returnClause)
	result.Columns = make([]string, len(returnItems))
	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	// Build result rows
	for _, row := range computedRows {
		resultRow := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			// Try alias first, then expression
			if val, ok := row.values[item.expr]; ok {
				resultRow[i] = val
			} else if val, ok := row.values[item.alias]; ok {
				resultRow[i] = val
			} else {
				// Evaluate expression using computed values as context
				resultRow[i] = e.evaluateExpressionFromValues(item.expr, row.values)
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	// Apply ORDER BY
	if orderByClause != "" {
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderByClause)
	}

	// Apply SKIP
	if skipVal > 0 && skipVal < len(result.Rows) {
		result.Rows = result.Rows[skipVal:]
	} else if skipVal >= len(result.Rows) {
		result.Rows = [][]interface{}{}
	}

	// Apply LIMIT
	if limitVal > 0 && limitVal < len(result.Rows) {
		result.Rows = result.Rows[:limitVal]
	}

	return result, nil
}

// evaluateWhereOnComputedRow evaluates a WHERE condition on computed values
func (e *StorageExecutor) evaluateWhereOnComputedRow(whereClause string, values map[string]interface{}) bool {
	whereClause = strings.TrimSpace(whereClause)

	// Handle AND
	if idx := strings.Index(strings.ToUpper(whereClause), " AND "); idx > 0 {
		left := whereClause[:idx]
		right := whereClause[idx+5:]
		return e.evaluateWhereOnComputedRow(left, values) && e.evaluateWhereOnComputedRow(right, values)
	}

	// Handle OR
	if idx := strings.Index(strings.ToUpper(whereClause), " OR "); idx > 0 {
		left := whereClause[:idx]
		right := whereClause[idx+4:]
		return e.evaluateWhereOnComputedRow(left, values) || e.evaluateWhereOnComputedRow(right, values)
	}

	// Handle comparison operators
	for _, op := range []string{">=", "<=", "<>", "!=", "=", ">", "<"} {
		if idx := strings.Index(whereClause, op); idx > 0 {
			left := strings.TrimSpace(whereClause[:idx])
			right := strings.TrimSpace(whereClause[idx+len(op):])

			leftVal := e.evaluateExpressionFromValues(left, values)
			rightVal := e.parseValue(right)

			switch op {
			case "=":
				return fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", rightVal)
			case "<>", "!=":
				return fmt.Sprintf("%v", leftVal) != fmt.Sprintf("%v", rightVal)
			case ">":
				lf, lok := toFloat64(leftVal)
				rf, rok := toFloat64(rightVal)
				return lok && rok && lf > rf
			case "<":
				lf, lok := toFloat64(leftVal)
				rf, rok := toFloat64(rightVal)
				return lok && rok && lf < rf
			case ">=":
				lf, lok := toFloat64(leftVal)
				rf, rok := toFloat64(rightVal)
				return lok && rok && lf >= rf
			case "<=":
				lf, lok := toFloat64(leftVal)
				rf, rok := toFloat64(rightVal)
				return lok && rok && lf <= rf
			}
		}
	}

	return true
}

// evaluateExpressionFromValues evaluates an expression using computed values map
func (e *StorageExecutor) evaluateExpressionFromValues(expr string, values map[string]interface{}) interface{} {
	expr = strings.TrimSpace(expr)

	// Direct lookup
	if val, ok := values[expr]; ok {
		return val
	}

	// Handle property access on computed values (e.g., x.property where x is a node)
	if idx := strings.Index(expr, "."); idx > 0 {
		varName := expr[:idx]
		propName := expr[idx+1:]
		if val, ok := values[varName]; ok {
			if node, ok := val.(*storage.Node); ok {
				return node.Properties[propName]
			}
		}
	}

	return expr // Return as literal if not found
}

// executeMatchWithClause handles MATCH ... WHERE ... WITH ... RETURN queries
// This processes computed values (like CASE WHEN) in the WITH clause
// and handles aggregation with implicit GROUP BY
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

	// Find WHERE in the section between WITH and RETURN
	// Use findKeywordNotInBrackets to avoid matching WHERE inside list comprehensions like [x WHERE ...]
	postWhereIdx := findKeywordNotInBrackets(strings.ToUpper(withSection), " WHERE ")
	if postWhereIdx > 0 {
		withClause = strings.TrimSpace(withSection[:postWhereIdx])
		// Skip "WHERE" (5 chars) + any trailing whitespace
		postWithWhere = strings.TrimSpace(withSection[postWhereIdx+5:])
	} else {
		withClause = withSection
	}
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

		upperExpr := strings.ToUpper(expr)
		isAgg := strings.HasPrefix(upperExpr, "COUNT(") ||
			strings.HasPrefix(upperExpr, "SUM(") ||
			strings.HasPrefix(upperExpr, "AVG(") ||
			strings.HasPrefix(upperExpr, "COLLECT(") ||
			strings.HasPrefix(upperExpr, "MIN(") ||
			strings.HasPrefix(upperExpr, "MAX(")

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

			// Calculate aggregates
			for _, ae := range aggregateExprs {
				upperExpr := strings.ToUpper(ae.expr)
				switch {
				case strings.HasPrefix(upperExpr, "COUNT(DISTINCT "):
					inner := ae.expr[15 : len(ae.expr)-1]
					seen := make(map[string]bool)
					for _, n := range groupNodes {
						nodeMap := map[string]*storage.Node{nodePattern.variable: n}
						var val interface{}
						if strings.HasPrefix(inner, nodePattern.variable+".") {
							propName := inner[len(nodePattern.variable)+1:]
							val = n.Properties[propName]
						} else if inner == nodePattern.variable {
							val = string(n.ID)
						} else {
							val = e.evaluateExpressionWithContext(inner, nodeMap, nil)
						}
						if val != nil {
							seen[fmt.Sprintf("%v", val)] = true
						}
					}
					values[ae.alias] = int64(len(seen))

				case strings.HasPrefix(upperExpr, "COUNT("):
					inner := ae.expr[6 : len(ae.expr)-1]
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

				case strings.HasPrefix(upperExpr, "SUM("):
					inner := ae.expr[4 : len(ae.expr)-1]
					sum := float64(0)
					for _, n := range groupNodes {
						nodeMap := map[string]*storage.Node{nodePattern.variable: n}
						var val interface{}
						if strings.HasPrefix(inner, nodePattern.variable+".") {
							propName := inner[len(nodePattern.variable)+1:]
							val = n.Properties[propName]
						} else {
							val = e.evaluateExpressionWithContext(inner, nodeMap, nil)
						}
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
					values[ae.alias] = sum

				case strings.HasPrefix(upperExpr, "COLLECT(DISTINCT "):
					inner := ae.expr[17 : len(ae.expr)-1] // Skip "COLLECT(DISTINCT "
					seen := make(map[string]bool)
					var collected []interface{}
					for _, n := range groupNodes {
						nodeMap := map[string]*storage.Node{nodePattern.variable: n}
						var val interface{}
						if strings.HasPrefix(inner, nodePattern.variable+".") {
							propName := inner[len(nodePattern.variable)+1:]
							val = n.Properties[propName]
						} else if inner == nodePattern.variable {
							val = string(n.ID)
						} else {
							val = e.evaluateExpressionWithContext(inner, nodeMap, nil)
						}
						key := fmt.Sprintf("%v", val)
						if !seen[key] {
							seen[key] = true
							collected = append(collected, val)
						}
					}
					values[ae.alias] = collected

				case strings.HasPrefix(upperExpr, "COLLECT("):
					inner := ae.expr[8 : len(ae.expr)-1]
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
				} else {
					// Try to evaluate expression by substituting bound variables
					expr := item.expr
					hasSubstitution := false
					for varName, varVal := range cr.values {
						if strings.Contains(expr, varName) {
							// Convert value to string representation for list comprehension
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
								// Skip node values - can't substitute directly
								continue
							default:
								replacement = fmt.Sprintf("%v", v)
							}
							expr = strings.ReplaceAll(expr, varName, replacement)
							hasSubstitution = true
						}
					}
					if hasSubstitution {
						// Build node map for evaluation (for labels() etc)
						nodeMap := make(map[string]*storage.Node)
						for varName, varVal := range cr.values {
							if node, ok := varVal.(*storage.Node); ok {
								nodeMap[varName] = node
							}
						}
						row[i] = e.evaluateExpressionWithContext(expr, nodeMap, nil)
					}
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	// Apply ORDER BY, SKIP, LIMIT to results
	upper = strings.ToUpper(cypher)

	// Apply ORDER BY
	orderByIdx := strings.Index(upper, "ORDER BY")
	if orderByIdx > 0 {
		orderPart := upper[orderByIdx+8:]
		endIdx := len(orderPart)
		for _, kw := range []string{" SKIP ", " LIMIT "} {
			if idx := strings.Index(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(cypher[orderByIdx+8 : orderByIdx+8+endIdx])
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	// Apply SKIP
	skipIdx := strings.Index(upper, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(cypher[skipIdx+4:])
		skipPart = strings.Split(skipPart, " ")[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	// Apply LIMIT
	limitIdx := strings.Index(upper, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(cypher[limitIdx+5:])
		limitPart = strings.Split(limitPart, " ")[0]
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

// evaluateWithWhereCondition evaluates a WHERE condition against computed WITH values.
// This is for filtering after WITH aggregation (like SQL HAVING).
func (e *StorageExecutor) evaluateWithWhereCondition(whereClause string, values map[string]interface{}) bool {
	upperClause := strings.ToUpper(whereClause)

	// Handle IS NULL / IS NOT NULL
	if strings.Contains(upperClause, " IS NOT NULL") {
		idx := strings.Index(upperClause, " IS NOT NULL")
		varName := strings.TrimSpace(whereClause[:idx])
		val, exists := values[varName]
		return exists && val != nil
	}
	if strings.Contains(upperClause, " IS NULL") {
		idx := strings.Index(upperClause, " IS NULL")
		varName := strings.TrimSpace(whereClause[:idx])
		val, exists := values[varName]
		return !exists || val == nil
	}

	// Handle comparison operators
	operators := []string{">=", "<=", "<>", "!=", ">", "<", "="}
	for _, op := range operators {
		if idx := strings.Index(whereClause, op); idx > 0 {
			left := strings.TrimSpace(whereClause[:idx])
			right := strings.TrimSpace(whereClause[idx+len(op):])

			leftVal, exists := values[left]
			if !exists {
				leftVal = e.parseValue(left)
			}
			rightVal, exists := values[right]
			if !exists {
				rightVal = e.parseValue(right)
			}

			switch op {
			case "=":
				return e.compareEqual(leftVal, rightVal)
			case "<>", "!=":
				return !e.compareEqual(leftVal, rightVal)
			case ">":
				return e.compareGreater(leftVal, rightVal)
			case "<":
				return e.compareLess(leftVal, rightVal)
			case ">=":
				return e.compareEqual(leftVal, rightVal) || e.compareGreater(leftVal, rightVal)
			case "<=":
				return e.compareEqual(leftVal, rightVal) || e.compareLess(leftVal, rightVal)
			}
		}
	}

	return true // No recognized condition, include all
}

// filterNodesByWhereClause filters nodes based on a WHERE clause condition.
// Uses evaluateWhere for consistent condition evaluation.
func (e *StorageExecutor) filterNodesByWhereClause(nodes []*storage.Node, whereClause, variable string) []*storage.Node {
	if whereClause == "" {
		return nodes
	}

	filterFn := func(node *storage.Node) bool {
		return e.evaluateWhere(node, variable, whereClause)
	}

	return parallelFilterNodes(nodes, filterFn)
}

// orderSpec represents a single ORDER BY column specification
type orderSpec struct {
	colIdx     int
	descending bool
}

// orderResultRows sorts result rows by the specified ORDER BY expression.
// Supports multiple columns: "col1 ASC, col2 DESC"
func (e *StorageExecutor) orderResultRows(rows [][]interface{}, columns []string, orderExpr string) [][]interface{} {
	if len(rows) <= 1 {
		return rows
	}

	// Parse multiple ORDER BY columns separated by comma
	orderSpecs := e.parseOrderBySpecs(orderExpr, columns)
	if len(orderSpecs) == 0 {
		return rows
	}

	// Sort rows using all order specifications
	sort.Slice(rows, func(i, j int) bool {
		for _, spec := range orderSpecs {
			cmp := e.compareOrderValues(rows[i][spec.colIdx], rows[j][spec.colIdx])
			if cmp != 0 {
				if spec.descending {
					return cmp > 0
				}
				return cmp < 0
			}
			// Values are equal, try next ORDER BY column
		}
		return false // All columns equal, maintain order
	})

	return rows
}

// parseOrderBySpecs parses "col1 ASC, col2 DESC" into orderSpec slice
func (e *StorageExecutor) parseOrderBySpecs(orderExpr string, columns []string) []orderSpec {
	var specs []orderSpec

	// Split by comma (but not inside parentheses)
	parts := splitOutsideParens(orderExpr, ',')

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Parse: "column [ASC|DESC]"
		tokens := strings.Fields(part)
		if len(tokens) == 0 {
			continue
		}

		colName := tokens[0]
		descending := len(tokens) > 1 && strings.ToUpper(tokens[1]) == "DESC"

		// Find column index
		colIdx := -1
		for i, col := range columns {
			if strings.EqualFold(col, colName) {
				colIdx = i
				break
			}
		}
		if colIdx == -1 {
			continue // Column not found, skip
		}

		specs = append(specs, orderSpec{colIdx: colIdx, descending: descending})
	}

	return specs
}

// compareOrderValues compares two values for ordering
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func (e *StorageExecutor) compareOrderValues(a, b interface{}) int {
	// Handle nil values (nulls last)
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // nil goes last
	}
	if b == nil {
		return -1 // non-nil before nil
	}

	// Try numeric comparison
	numA, okA := toFloat64(a)
	numB, okB := toFloat64(b)
	if okA && okB {
		if numA < numB {
			return -1
		}
		if numA > numB {
			return 1
		}
		return 0
	}

	// String comparison
	strA := fmt.Sprintf("%v", a)
	strB := fmt.Sprintf("%v", b)
	if strA < strB {
		return -1
	}
	if strA > strB {
		return 1
	}
	return 0
}

// splitOutsideParens splits a string by delimiter, respecting parentheses
func splitOutsideParens(s string, delim rune) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for _, ch := range s {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}

		if ch == delim && depth == 0 {
			parts = append(parts, current.String())
			current.Reset()
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

// executeMatchUnwind handles MATCH ... UNWIND ... RETURN queries
// This allows UNWIND to access variables defined in MATCH
func (e *StorageExecutor) executeMatchUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Find clause boundaries
	matchIdx := findKeywordIndex(cypher, "MATCH")
	unwindIdx := findKeywordIndex(cypher, "UNWIND")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if matchIdx == -1 || unwindIdx == -1 {
		return nil, fmt.Errorf("MATCH and UNWIND clauses required")
	}

	// Parse MATCH clause
	matchPart := strings.TrimSpace(cypher[matchIdx+5 : unwindIdx])

	// Check for WHERE clause in MATCH part
	whereIdx := findKeywordIndex(matchPart, "WHERE")
	var whereClause string
	var nodePatternPart string

	if whereIdx > 0 {
		nodePatternPart = strings.TrimSpace(matchPart[:whereIdx])
		whereClause = strings.TrimSpace(matchPart[whereIdx+5:])
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

	// Apply WHERE clause filter if present
	if whereClause != "" {
		nodes = e.filterNodesByWhereClause(nodes, whereClause, nodePattern.variable)
	}

	// Parse UNWIND clause: UNWIND expr AS variable
	unwindPart := strings.TrimSpace(cypher[unwindIdx+6:])
	var unwindExpr, unwindVar string

	// Find AS keyword
	asIdx := strings.Index(strings.ToUpper(unwindPart), " AS ")
	if asIdx == -1 {
		return nil, fmt.Errorf("UNWIND requires AS clause")
	}

	unwindExpr = strings.TrimSpace(unwindPart[:asIdx])

	// Find the end of the variable name (next clause)
	remainder := strings.TrimSpace(unwindPart[asIdx+4:])
	spaceIdx := strings.IndexAny(remainder, " \t\n")
	if spaceIdx > 0 {
		unwindVar = remainder[:spaceIdx]
	} else {
		unwindVar = remainder
	}

	// Find WHERE clause after UNWIND (if any)
	postUnwindWhere := ""
	unwindUpperRemainder := strings.ToUpper(unwindPart[asIdx+4:])
	postWhereIdx := strings.Index(unwindUpperRemainder, " WHERE ")
	if postWhereIdx > 0 {
		// Find WHERE and RETURN boundaries
		postWhereStart := asIdx + 4 + postWhereIdx + 7
		postWhereEnd := len(unwindPart)
		if returnIdx > unwindIdx {
			relativeReturnIdx := returnIdx - unwindIdx - 6
			if relativeReturnIdx > 0 && relativeReturnIdx < postWhereEnd {
				postWhereEnd = relativeReturnIdx
			}
		}
		postUnwindWhere = strings.TrimSpace(unwindPart[postWhereStart:postWhereEnd])
	}

	// Parse RETURN clause
	var returnItems []returnItem
	var returnColumns []string

	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		// Remove ORDER BY, SKIP, LIMIT
		for _, keyword := range []string{" ORDER BY ", " SKIP ", " LIMIT "} {
			if idx := strings.Index(strings.ToUpper(returnClause), keyword); idx >= 0 {
				returnClause = returnClause[:idx]
			}
		}
		returnItems = e.parseReturnItems(returnClause)
		returnColumns = make([]string, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				returnColumns[i] = item.alias
			} else {
				returnColumns[i] = item.expr
			}
		}
	}

	// Build result by unwinding for each matched node
	type unwoundRow struct {
		nodeVar   string
		node      *storage.Node
		unwindVar string
		unwindVal interface{}
	}
	var unwoundRows []unwoundRow

	for _, node := range nodes {
		// Evaluate the UNWIND expression in the context of this node
		nodeMap := map[string]*storage.Node{nodePattern.variable: node}
		listVal := e.evaluateExpressionWithContext(unwindExpr, nodeMap, nil)

		// Convert to list
		var items []interface{}
		switch v := listVal.(type) {
		case nil:
			continue // null produces no rows
		case []interface{}:
			items = v
		case []string:
			items = make([]interface{}, len(v))
			for i, s := range v {
				items[i] = s
			}
		default:
			items = []interface{}{listVal}
		}

		// Create a row for each item
		for _, item := range items {
			// Apply WHERE filter after UNWIND
			if postUnwindWhere != "" {
				// Simple filter: variable <> 'value' or variable = 'value'
				if strings.Contains(postUnwindWhere, "<>") {
					parts := strings.SplitN(postUnwindWhere, "<>", 2)
					varName := strings.TrimSpace(parts[0])
					valStr := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
					if varName == unwindVar && fmt.Sprintf("%v", item) == valStr {
						continue // Skip this row
					}
				} else if strings.Contains(postUnwindWhere, "=") {
					parts := strings.SplitN(postUnwindWhere, "=", 2)
					varName := strings.TrimSpace(parts[0])
					valStr := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
					if varName == unwindVar && fmt.Sprintf("%v", item) != valStr {
						continue // Skip this row
					}
				}
			}

			unwoundRows = append(unwoundRows, unwoundRow{
				nodeVar:   nodePattern.variable,
				node:      node,
				unwindVar: unwindVar,
				unwindVal: item,
			})
		}
	}

	// Check for aggregation in RETURN
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

	result := &ExecuteResult{
		Columns: returnColumns,
		Rows:    [][]interface{}{},
	}

	if hasAggregation {
		// Group by non-aggregated columns
		type groupKey struct {
			key    string
			values map[string]interface{}
		}
		groups := make(map[string]*groupKey)
		groupOrder := []string{}

		for _, ur := range unwoundRows {
			keyParts := []string{}
			keyValues := make(map[string]interface{})

			for _, item := range returnItems {
				upperExpr := strings.ToUpper(item.expr)
				isAgg := strings.HasPrefix(upperExpr, "COUNT(") ||
					strings.HasPrefix(upperExpr, "SUM(") ||
					strings.HasPrefix(upperExpr, "AVG(") ||
					strings.HasPrefix(upperExpr, "COLLECT(")

				if !isAgg {
					var val interface{}
					if item.expr == unwindVar {
						val = ur.unwindVal
					} else if strings.HasPrefix(item.expr, ur.nodeVar+".") {
						propName := item.expr[len(ur.nodeVar)+1:]
						val = ur.node.Properties[propName]
					}
					keyParts = append(keyParts, fmt.Sprintf("%v", val))
					alias := item.alias
					if alias == "" {
						alias = item.expr
					}
					keyValues[alias] = val
				}
			}

			key := strings.Join(keyParts, "|")
			if _, exists := groups[key]; !exists {
				groups[key] = &groupKey{key: key, values: keyValues}
				groupOrder = append(groupOrder, key)
			}
		}

		// Calculate aggregates for each group
		for _, key := range groupOrder {
			group := groups[key]
			row := make([]interface{}, len(returnItems))

			// Count rows in this group
			groupRows := []unwoundRow{}
			for _, ur := range unwoundRows {
				keyParts := []string{}
				for _, item := range returnItems {
					upperExpr := strings.ToUpper(item.expr)
					isAgg := strings.HasPrefix(upperExpr, "COUNT(") ||
						strings.HasPrefix(upperExpr, "SUM(") ||
						strings.HasPrefix(upperExpr, "AVG(") ||
						strings.HasPrefix(upperExpr, "COLLECT(")

					if !isAgg {
						var val interface{}
						if item.expr == unwindVar {
							val = ur.unwindVal
						} else if strings.HasPrefix(item.expr, ur.nodeVar+".") {
							propName := item.expr[len(ur.nodeVar)+1:]
							val = ur.node.Properties[propName]
						}
						keyParts = append(keyParts, fmt.Sprintf("%v", val))
					}
				}
				if strings.Join(keyParts, "|") == key {
					groupRows = append(groupRows, ur)
				}
			}

			for i, item := range returnItems {
				upperExpr := strings.ToUpper(item.expr)
				alias := item.alias
				if alias == "" {
					alias = item.expr
				}

				switch {
				case strings.HasPrefix(upperExpr, "COUNT("):
					row[i] = int64(len(groupRows))
				case strings.HasPrefix(upperExpr, "COLLECT("):
					inner := item.expr[8 : len(item.expr)-1]
					collected := make([]interface{}, 0, len(groupRows))
					for _, ur := range groupRows {
						if inner == unwindVar {
							collected = append(collected, ur.unwindVal)
						}
					}
					row[i] = collected
				default:
					// Non-aggregate - use group value
					if val, ok := group.values[alias]; ok {
						row[i] = val
					}
				}
			}

			result.Rows = append(result.Rows, row)
		}
	} else {
		// Non-aggregated - return all unwound rows
		for _, ur := range unwoundRows {
			row := make([]interface{}, len(returnItems))
			for i, item := range returnItems {
				if item.expr == unwindVar {
					row[i] = ur.unwindVal
				} else if strings.HasPrefix(item.expr, ur.nodeVar+".") {
					propName := item.expr[len(ur.nodeVar)+1:]
					row[i] = ur.node.Properties[propName]
				} else if item.expr == ur.nodeVar {
					row[i] = e.nodeToMap(ur.node)
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	// Apply ORDER BY, SKIP, LIMIT
	orderByIdx := strings.Index(upper, "ORDER BY")
	if orderByIdx > 0 {
		orderPart := upper[orderByIdx+8:]
		endIdx := len(orderPart)
		for _, kw := range []string{" SKIP ", " LIMIT "} {
			if idx := strings.Index(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(cypher[orderByIdx+8 : orderByIdx+8+endIdx])
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	// Apply SKIP
	skipIdx := strings.Index(upper, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(cypher[skipIdx+4:])
		skipPart = strings.Split(skipPart, " ")[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	// Apply LIMIT
	limitIdx := strings.Index(upper, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(cypher[limitIdx+5:])
		limitPart = strings.Split(limitPart, " ")[0]
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

// executeMatchWithUnwind handles MATCH ... WITH ... UNWIND ... RETURN queries
// This is the complex pattern used by Mimir's byType query:
// MATCH (f:File) WITH f, [...] as list UNWIND list as item WITH item, COUNT(*) RETURN item
func (e *StorageExecutor) executeMatchWithUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Find all clause boundaries
	matchIdx := findKeywordIndex(cypher, "MATCH")
	withIdx := findKeywordIndex(cypher, "WITH")
	unwindIdx := findKeywordNotInBrackets(upper, " UNWIND ")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if matchIdx == -1 || withIdx == -1 || unwindIdx == -1 || returnIdx == -1 {
		return nil, fmt.Errorf("MATCH, WITH, UNWIND, and RETURN clauses required")
	}

	// Step 1: Parse MATCH clause
	matchPart := strings.TrimSpace(cypher[matchIdx+5 : withIdx])

	// Check for WHERE clause in MATCH part
	matchWhereIdx := findKeywordNotInBrackets(strings.ToUpper(matchPart), " WHERE ")
	var matchWhere string
	var nodePatternPart string

	if matchWhereIdx > 0 {
		nodePatternPart = strings.TrimSpace(matchPart[:matchWhereIdx])
		matchWhere = strings.TrimSpace(matchPart[matchWhereIdx+7:])
	} else {
		nodePatternPart = matchPart
	}

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

	if len(nodePattern.properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
	}

	if matchWhere != "" {
		nodes = e.filterNodesByWhereClause(nodes, matchWhere, nodePattern.variable)
	}

	// Step 2: Process first WITH clause - compute filteredLabels for each node
	withSection := strings.TrimSpace(cypher[withIdx+4 : unwindIdx])
	withItems := e.splitWithItems(withSection)

	type nodeWithValues struct {
		node   *storage.Node
		values map[string]interface{}
	}
	var nodeRows []nodeWithValues

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
			var alias, expr string
			if asIdx > 0 {
				expr = strings.TrimSpace(item[:asIdx])
				alias = strings.TrimSpace(item[asIdx+4:])
			} else {
				expr = item
				alias = item
			}

			if expr == nodePattern.variable {
				values[alias] = node
			} else if strings.HasPrefix(expr, nodePattern.variable+".") {
				propName := expr[len(nodePattern.variable)+1:]
				values[alias] = node.Properties[propName]
			} else {
				values[alias] = e.evaluateExpressionWithContext(expr, nodeMap, nil)
			}
		}

		nodeRows = append(nodeRows, nodeWithValues{node: node, values: values})
	}

	// Step 3: Parse UNWIND clause
	unwindSection := strings.TrimSpace(cypher[unwindIdx+7:]) // Skip " UNWIND "
	asIdx := strings.Index(strings.ToUpper(unwindSection), " AS ")
	if asIdx == -1 {
		return nil, fmt.Errorf("UNWIND requires AS clause")
	}

	unwindExpr := strings.TrimSpace(unwindSection[:asIdx])

	// Find end of unwind var (next clause)
	remainder := strings.TrimSpace(unwindSection[asIdx+4:])
	spaceIdx := strings.IndexAny(remainder, " \t\n")
	var unwindVar string
	if spaceIdx > 0 {
		unwindVar = remainder[:spaceIdx]
	} else {
		unwindVar = remainder
	}

	// Step 4: Expand UNWIND - create rows for each item in the list
	type unwoundRow struct {
		origNode   *storage.Node
		origValues map[string]interface{}
		unwindVar  string
		unwindVal  interface{}
	}
	var unwoundRows []unwoundRow

	for _, nr := range nodeRows {
		// Get the list to unwind
		var listToUnwind []interface{}

		if val, ok := nr.values[unwindExpr]; ok {
			switch v := val.(type) {
			case []interface{}:
				listToUnwind = v
			case []string:
				listToUnwind = make([]interface{}, len(v))
				for i, s := range v {
					listToUnwind[i] = s
				}
			}
		}

		// Empty list = no rows (skip)
		if len(listToUnwind) == 0 {
			continue
		}

		// Create a row for each item
		for _, item := range listToUnwind {
			unwoundRows = append(unwoundRows, unwoundRow{
				origNode:   nr.node,
				origValues: nr.values,
				unwindVar:  unwindVar,
				unwindVal:  item,
			})
		}
	}

	// Step 5: Find second WITH clause (between UNWIND and RETURN) for aggregation
	secondWithIdx := findKeywordNotInBrackets(upper[unwindIdx:], " WITH ")
	hasSecondWith := secondWithIdx > 0 && unwindIdx+secondWithIdx < returnIdx

	// Parse RETURN clause
	returnClause := strings.TrimSpace(cypher[returnIdx+6:])
	for _, keyword := range []string{" ORDER BY ", " SKIP ", " LIMIT "} {
		if idx := strings.Index(strings.ToUpper(returnClause), keyword); idx >= 0 {
			returnClause = returnClause[:idx]
		}
	}
	returnItems := e.parseReturnItems(returnClause)

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

	if hasSecondWith {
		// Second WITH clause with aggregation - GROUP BY unwind value
		secondWithSection := strings.TrimSpace(cypher[unwindIdx+secondWithIdx+5 : returnIdx])
		secondWithItems := e.splitWithItems(secondWithSection)

		// Group by unwind value
		groups := make(map[interface{}][]unwoundRow)
		groupOrder := []interface{}{}

		for _, ur := range unwoundRows {
			key := ur.unwindVal
			if _, exists := groups[key]; !exists {
				groupOrder = append(groupOrder, key)
			}
			groups[key] = append(groups[key], ur)
		}

		// Process each group
		for _, key := range groupOrder {
			groupRows := groups[key]
			row := make([]interface{}, len(returnItems))

			for i, item := range returnItems {
				upperExpr := strings.ToUpper(item.expr)

				switch {
				case strings.HasPrefix(upperExpr, "COUNT("):
					row[i] = int64(len(groupRows))
				case item.expr == unwindVar || item.expr == "type":
					// Return the unwind value (group key)
					row[i] = key
				default:
					// Check if it matches a second WITH alias
					for _, swi := range secondWithItems {
						swi = strings.TrimSpace(swi)
						swiUpper := strings.ToUpper(swi)
						swiAsIdx := strings.Index(swiUpper, " AS ")
						if swiAsIdx > 0 {
							swiAlias := strings.TrimSpace(swi[swiAsIdx+4:])
							if swiAlias == item.expr || item.alias == swiAlias {
								swiExpr := strings.TrimSpace(swi[:swiAsIdx])
								if swiExpr == unwindVar {
									row[i] = key
								} else if strings.HasPrefix(strings.ToUpper(swiExpr), "COUNT(") {
									row[i] = int64(len(groupRows))
								}
							}
						}
					}
				}
			}

			result.Rows = append(result.Rows, row)
		}
	} else {
		// No second WITH - just return unwound rows
		for _, ur := range unwoundRows {
			row := make([]interface{}, len(returnItems))
			for i, item := range returnItems {
				if item.expr == unwindVar {
					row[i] = ur.unwindVal
				} else if strings.HasPrefix(item.expr, nodePattern.variable+".") {
					propName := item.expr[len(nodePattern.variable)+1:]
					row[i] = ur.origNode.Properties[propName]
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	// Apply ORDER BY
	orderByIdx := strings.Index(upper, "ORDER BY")
	if orderByIdx > 0 {
		orderPart := upper[orderByIdx+8:]
		endIdx := len(orderPart)
		for _, kw := range []string{" SKIP ", " LIMIT "} {
			if idx := strings.Index(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(cypher[orderByIdx+8 : orderByIdx+8+endIdx])
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	return result, nil
}

// countKeywordOccurrences counts how many times a keyword appears in the query
// using word boundary detection. Excludes occurrences inside labels (after ':')
func countKeywordOccurrences(upper, keyword string) int {
	count := 0
	idx := 0
	for {
		found := strings.Index(upper[idx:], keyword)
		if found == -1 {
			break
		}
		// Check word boundary before
		pos := idx + found
		// Must have space/newline/tab before, NOT ':' (which would indicate a label)
		beforeOk := pos == 0 || (upper[pos-1] == ' ' || upper[pos-1] == '\n' || upper[pos-1] == '\t')
		// Check word boundary after
		afterPos := pos + len(keyword)
		afterOk := afterPos >= len(upper) || (upper[afterPos] == ' ' || upper[afterPos] == '(' || upper[afterPos] == '\n' || upper[afterPos] == '\t')

		if beforeOk && afterOk {
			count++
		}
		idx = pos + len(keyword)
	}
	return count
}

// executeMultiMatch handles queries with multiple MATCH clauses
// Example: MATCH (p1:Person)-[:WORKS_AT]->(c:Company) MATCH (p2:Person)-[:WORKS_AT]->(c) WHERE p1 <> p2 RETURN p1, p2, c
func (e *StorageExecutor) executeMultiMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Find RETURN and WHERE positions
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("multi-MATCH query requires RETURN clause")
	}

	// Extract WHERE clause if present (between last MATCH pattern and RETURN)
	var whereClause string
	whereIdx := findKeywordIndex(cypher, "WHERE")
	if whereIdx > 0 && whereIdx < returnIdx {
		whereClause = strings.TrimSpace(cypher[whereIdx+5 : returnIdx])
	}

	// Parse RETURN clause
	returnPart := cypher[returnIdx+6:]
	returnEndIdx := len(returnPart)
	for _, kw := range []string{" ORDER BY ", " SKIP ", " LIMIT "} {
		if idx := strings.Index(strings.ToUpper(returnPart), kw); idx >= 0 && idx < returnEndIdx {
			returnEndIdx = idx
		}
	}
	returnClause := strings.TrimSpace(returnPart[:returnEndIdx])
	returnItems := e.parseReturnItems(returnClause)

	// Split MATCH clauses
	matchClauses := splitMatchClauses(cypher, whereIdx, returnIdx)
	if len(matchClauses) < 2 {
		return nil, fmt.Errorf("expected multiple MATCH clauses")
	}

	// Execute first MATCH and get initial bindings
	bindings := e.executeFirstMatch(matchClauses[0])

	// Execute subsequent MATCH clauses with bindings
	for i := 1; i < len(matchClauses); i++ {
		bindings = e.executeChainedMatch(matchClauses[i], bindings)
	}

	// Apply WHERE filter if present
	if whereClause != "" {
		bindings = e.filterBindingsByWhere(bindings, whereClause)
	}

	// Build result from bindings
	result := &ExecuteResult{
		Columns: make([]string, len(returnItems)),
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	// Check if this is an aggregation query
	hasAggregation := false
	isAggFlags := make([]bool, len(returnItems))
	for i, item := range returnItems {
		upperExpr := strings.ToUpper(item.expr)
		isAggFlags[i] = strings.HasPrefix(upperExpr, "COUNT(") ||
			strings.HasPrefix(upperExpr, "SUM(") ||
			strings.HasPrefix(upperExpr, "AVG(") ||
			strings.HasPrefix(upperExpr, "MIN(") ||
			strings.HasPrefix(upperExpr, "MAX(") ||
			strings.HasPrefix(upperExpr, "COLLECT(")
		if isAggFlags[i] {
			hasAggregation = true
		}
	}

	if hasAggregation {
		// Group bindings by non-aggregated columns
		groups := make(map[string][]binding)
		groupKeys := make(map[string][]interface{})

		for _, b := range bindings {
			// Build group key from non-aggregated columns
			keyParts := make([]interface{}, 0)
			for i, item := range returnItems {
				if !isAggFlags[i] {
					val := e.resolveBindingItem(item, b)
					keyParts = append(keyParts, val)
				}
			}
			key := fmt.Sprintf("%v", keyParts)
			groups[key] = append(groups[key], b)
			if _, exists := groupKeys[key]; !exists {
				groupKeys[key] = keyParts
			}
		}

		// Build result rows with aggregations
		for key, groupBindings := range groups {
			row := make([]interface{}, len(returnItems))
			keyIdx := 0

			for i, item := range returnItems {
				if !isAggFlags[i] {
					// Non-aggregated column - use group key value
					row[i] = groupKeys[key][keyIdx]
					keyIdx++
					continue
				}

				// Aggregation function
				upperExpr := strings.ToUpper(item.expr)
				switch {
				case strings.HasPrefix(upperExpr, "COUNT("):
					inner := item.expr[6 : len(item.expr)-1]
					if inner == "*" {
						row[i] = int64(len(groupBindings))
					} else {
						count := int64(0)
						for _, b := range groupBindings {
							val := e.resolveBindingItem(returnItem{expr: inner}, b)
							if val != nil {
								count++
							}
						}
						row[i] = count
					}

				case strings.HasPrefix(upperExpr, "SUM("):
					inner := item.expr[4 : len(item.expr)-1]
					sum := float64(0)
					for _, b := range groupBindings {
						val := e.resolveBindingItem(returnItem{expr: inner}, b)
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
					row[i] = sum

				case strings.HasPrefix(upperExpr, "AVG("):
					inner := item.expr[4 : len(item.expr)-1]
					sum := float64(0)
					count := 0
					for _, b := range groupBindings {
						val := e.resolveBindingItem(returnItem{expr: inner}, b)
						if num, ok := toFloat64(val); ok {
							sum += num
							count++
						}
					}
					if count > 0 {
						row[i] = sum / float64(count)
					} else {
						row[i] = nil
					}

				case strings.HasPrefix(upperExpr, "MIN("):
					inner := item.expr[4 : len(item.expr)-1]
					var minVal interface{}
					for _, b := range groupBindings {
						val := e.resolveBindingItem(returnItem{expr: inner}, b)
						if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
							minVal = val
						}
					}
					row[i] = minVal

				case strings.HasPrefix(upperExpr, "MAX("):
					inner := item.expr[4 : len(item.expr)-1]
					var maxVal interface{}
					for _, b := range groupBindings {
						val := e.resolveBindingItem(returnItem{expr: inner}, b)
						if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
							maxVal = val
						}
					}
					row[i] = maxVal

				case strings.HasPrefix(upperExpr, "COLLECT("):
					inner := item.expr[8 : len(item.expr)-1]
					var collected []interface{}
					for _, b := range groupBindings {
						val := e.resolveBindingItem(returnItem{expr: inner}, b)
						collected = append(collected, val)
					}
					row[i] = collected
				}
			}
			result.Rows = append(result.Rows, row)
		}
	} else {
		// Non-aggregation - process each binding directly
		for _, b := range bindings {
			row := make([]interface{}, len(returnItems))
			for i, item := range returnItems {
				row[i] = e.resolveBindingItem(item, b)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	// Apply ORDER BY, SKIP, LIMIT
	orderByIdx := strings.Index(upper, "ORDER BY")
	if orderByIdx > 0 {
		orderPart := upper[orderByIdx+8:]
		endIdx := len(orderPart)
		for _, kw := range []string{" SKIP ", " LIMIT "} {
			if idx := strings.Index(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(cypher[orderByIdx+8 : orderByIdx+8+endIdx])
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	return result, nil
}

// splitMatchClauses splits the query into individual MATCH clause patterns
func splitMatchClauses(cypher string, whereIdx, returnIdx int) []string {
	upper := strings.ToUpper(cypher)
	var clauses []string

	// Find the end of MATCH patterns (before WHERE or RETURN)
	endIdx := returnIdx
	if whereIdx > 0 && whereIdx < returnIdx {
		endIdx = whereIdx
	}

	matchPart := cypher[5:endIdx] // Skip first "MATCH"

	// Split by subsequent MATCH keywords
	parts := strings.Split(strings.ToUpper(matchPart), "MATCH")
	offset := 5 // Start after first MATCH

	for i, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		// Find the actual length in original case
		pattern := strings.TrimSpace(cypher[offset : offset+len(p)])
		clauses = append(clauses, pattern)
		offset += len(p)
		if i < len(parts)-1 {
			offset += 5 // Skip "MATCH"
		}
	}

	// Fix: Re-split using findKeywordIndex for accuracy
	clauses = clauses[:0]
	start := 5 // After first MATCH
	searchStart := start
	for {
		nextMatch := strings.Index(upper[searchStart:], "MATCH")
		if nextMatch == -1 || searchStart+nextMatch >= endIdx {
			// No more MATCH - take everything to end
			clauses = append(clauses, strings.TrimSpace(cypher[start:endIdx]))
			break
		}
		// Check if it's a real MATCH (word boundary)
		pos := searchStart + nextMatch
		beforeOk := pos == 0 || upper[pos-1] == ' ' || upper[pos-1] == '\n' || upper[pos-1] == '\t'
		afterOk := pos+5 >= len(upper) || upper[pos+5] == ' ' || upper[pos+5] == '('

		if beforeOk && afterOk {
			clauses = append(clauses, strings.TrimSpace(cypher[start:pos]))
			start = pos + 5 // Skip "MATCH"
		}
		searchStart = pos + 5
	}

	return clauses
}

// binding represents variable bindings from multiple MATCH clauses
type binding map[string]*storage.Node

// executeFirstMatch executes the first MATCH and returns initial bindings
func (e *StorageExecutor) executeFirstMatch(pattern string) []binding {
	var bindings []binding

	// Check for relationship pattern
	if strings.Contains(pattern, "-[") || strings.Contains(pattern, "]-") {
		matches := e.parseTraversalPattern(pattern)
		if matches == nil {
			return bindings
		}

		paths := e.traverseGraph(matches)
		for _, path := range paths {
			if len(path.Nodes) < 2 {
				continue
			}
			b := make(binding)
			if matches.StartNode.variable != "" {
				b[matches.StartNode.variable] = path.Nodes[0]
			}
			if matches.EndNode.variable != "" {
				b[matches.EndNode.variable] = path.Nodes[len(path.Nodes)-1]
			}
			bindings = append(bindings, b)
		}
	} else {
		// Simple node pattern
		nodePattern := e.parseNodePattern(pattern)
		var nodes []*storage.Node
		if len(nodePattern.labels) > 0 {
			nodes, _ = e.storage.GetNodesByLabel(nodePattern.labels[0])
		} else {
			nodes, _ = e.storage.AllNodes()
		}

		if len(nodePattern.properties) > 0 {
			nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
		}

		for _, node := range nodes {
			b := make(binding)
			b[nodePattern.variable] = node
			bindings = append(bindings, b)
		}
	}

	return bindings
}

// executeChainedMatch executes a subsequent MATCH against existing bindings
func (e *StorageExecutor) executeChainedMatch(pattern string, existingBindings []binding) []binding {
	var newBindings []binding

	for _, existing := range existingBindings {
		// Check for relationship pattern
		if strings.Contains(pattern, "-[") || strings.Contains(pattern, "]-") {
			matches := e.parseTraversalPattern(pattern)
			if matches == nil {
				continue
			}

			// Check if any bound variables are referenced
			boundStartNode := existing[matches.StartNode.variable]
			boundEndNode := existing[matches.EndNode.variable]

			paths := e.traverseGraph(matches)
			for _, path := range paths {
				if len(path.Nodes) < 2 {
					continue
				}
				startNode := path.Nodes[0]
				endNode := path.Nodes[len(path.Nodes)-1]

				// Check if path matches any bound variables
				startMatches := boundStartNode == nil || startNode.ID == boundStartNode.ID
				endMatches := boundEndNode == nil || endNode.ID == boundEndNode.ID

				if startMatches && endMatches {
					// Create new binding combining existing and new
					b := make(binding)
					for k, v := range existing {
						b[k] = v
					}
					if matches.StartNode.variable != "" {
						b[matches.StartNode.variable] = startNode
					}
					if matches.EndNode.variable != "" {
						b[matches.EndNode.variable] = endNode
					}
					newBindings = append(newBindings, b)
				}
			}
		} else {
			// Simple node pattern
			nodePattern := e.parseNodePattern(pattern)

			// Check if variable is already bound
			if boundNode := existing[nodePattern.variable]; boundNode != nil {
				// Variable is bound, just propagate
				newBindings = append(newBindings, existing)
				continue
			}

			var nodes []*storage.Node
			if len(nodePattern.labels) > 0 {
				nodes, _ = e.storage.GetNodesByLabel(nodePattern.labels[0])
			} else {
				nodes, _ = e.storage.AllNodes()
			}

			if len(nodePattern.properties) > 0 {
				nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
			}

			for _, node := range nodes {
				b := make(binding)
				for k, v := range existing {
					b[k] = v
				}
				b[nodePattern.variable] = node
				newBindings = append(newBindings, b)
			}
		}
	}

	return newBindings
}

// filterBindingsByWhere filters bindings based on WHERE clause
func (e *StorageExecutor) filterBindingsByWhere(bindings []binding, whereClause string) []binding {
	var result []binding

	for _, b := range bindings {
		if e.evaluateBindingWhere(b, whereClause) {
			result = append(result, b)
		}
	}

	return result
}

// evaluateBindingWhere evaluates WHERE clause against a binding
func (e *StorageExecutor) evaluateBindingWhere(b binding, whereClause string) bool {
	whereClause = strings.TrimSpace(whereClause)
	upper := strings.ToUpper(whereClause)

	// Handle AND
	if andIdx := findTopLevelKeyword(whereClause, " AND "); andIdx > 0 {
		left := strings.TrimSpace(whereClause[:andIdx])
		right := strings.TrimSpace(whereClause[andIdx+5:])
		return e.evaluateBindingWhere(b, left) && e.evaluateBindingWhere(b, right)
	}

	// Handle OR
	if orIdx := findTopLevelKeyword(whereClause, " OR "); orIdx > 0 {
		left := strings.TrimSpace(whereClause[:orIdx])
		right := strings.TrimSpace(whereClause[orIdx+4:])
		return e.evaluateBindingWhere(b, left) || e.evaluateBindingWhere(b, right)
	}

	// Handle NOT
	if strings.HasPrefix(upper, "NOT ") {
		return !e.evaluateBindingWhere(b, whereClause[4:])
	}

	// Handle variable comparison: p1 <> p2 (comparing node IDs)
	if strings.Contains(whereClause, "<>") || strings.Contains(whereClause, "!=") {
		op := "<>"
		opIdx := strings.Index(whereClause, "<>")
		if opIdx == -1 {
			op = "!="
			opIdx = strings.Index(whereClause, "!=")
		}

		left := strings.TrimSpace(whereClause[:opIdx])
		right := strings.TrimSpace(whereClause[opIdx+len(op):])

		// Check if comparing node variables (not properties)
		if !strings.Contains(left, ".") && !strings.Contains(right, ".") {
			leftNode := b[left]
			rightNode := b[right]
			if leftNode != nil && rightNode != nil {
				return leftNode.ID != rightNode.ID
			}
		}
	}

	// Handle property comparison: n.prop = value
	for _, op := range []string{"<>", "!=", ">=", "<=", "=", ">", "<"} {
		if idx := strings.Index(whereClause, op); idx > 0 {
			left := strings.TrimSpace(whereClause[:idx])
			right := strings.TrimSpace(whereClause[idx+len(op):])

			if dotIdx := strings.Index(left, "."); dotIdx > 0 {
				varName := left[:dotIdx]
				propName := left[dotIdx+1:]

				if node := b[varName]; node != nil {
					actualVal := node.Properties[propName]
					expectedVal := e.parseValue(right)

					switch op {
					case "=":
						return e.compareEqual(actualVal, expectedVal)
					case "<>", "!=":
						return !e.compareEqual(actualVal, expectedVal)
					case ">":
						return e.compareGreater(actualVal, expectedVal)
					case ">=":
						return e.compareGreater(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
					case "<":
						return e.compareLess(actualVal, expectedVal)
					case "<=":
						return e.compareLess(actualVal, expectedVal) || e.compareEqual(actualVal, expectedVal)
					}
				}
			}
			break
		}
	}

	return true
}

// resolveBindingItem resolves a return item against a binding
func (e *StorageExecutor) resolveBindingItem(item returnItem, b binding) interface{} {
	expr := item.expr

	// Check for property access: var.prop
	if dotIdx := strings.Index(expr, "."); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]

		if node := b[varName]; node != nil {
			return node.Properties[propName]
		}
		return nil
	}

	// Check for node variable
	if node := b[expr]; node != nil {
		return e.nodeToMap(node)
	}

	return nil
}

// executeCreate handles CREATE queries.
