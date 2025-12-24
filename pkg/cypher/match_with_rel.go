package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

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

	// Extract path variable if pattern has assignment: path = (a)-[r]-(b)
	pathVariable := ""
	patternForParsing := pattern
	if eqIdx := strings.Index(pattern, "="); eqIdx > 0 {
		beforeEq := strings.TrimSpace(pattern[:eqIdx])
		afterEq := strings.TrimSpace(pattern[eqIdx+1:])
		// Path variable should be a simple identifier, and after = should start with (
		if !strings.Contains(beforeEq, " ") && !strings.Contains(beforeEq, "(") && strings.HasPrefix(afterEq, "(") {
			pathVariable = beforeEq
			patternForParsing = afterEq
		}
	}

	// Parse the traversal pattern
	matches := e.parseTraversalPattern(patternForParsing)
	if matches == nil {
		return result, fmt.Errorf("invalid traversal pattern: %s", patternForParsing)
	}

	// Set the path variable in matches for buildPathContext to use
	if pathVariable != "" {
		matches.PathVariable = pathVariable
	}

	// Execute traversal to get all paths
	paths := e.traverseGraph(matches)

	// Apply pre-WITH WHERE clause filter if present
	if preWithWhere != "" {
		paths = e.filterPathsByWhere(paths, matches, preWithWhere)
	}

	// Parse WITH and RETURN clauses from withAndReturn string
	// withAndReturn starts with "WITH ..."
	returnIdx := findKeywordIndex(withAndReturn, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required after WITH")
	}

	// Extract WITH clause section
	withSection := strings.TrimSpace(withAndReturn[4:returnIdx]) // Skip "WITH"

	// Extract LIMIT/SKIP from WITH section (e.g., "WITH path, connected LIMIT 10")
	var withLimitVal, withSkipVal int
	upperWithSection := strings.ToUpper(withSection)
	if idx := findKeywordNotInBrackets(upperWithSection, " LIMIT "); idx > 0 {
		limitPart := strings.TrimSpace(withSection[idx+7:])
		// Find end of LIMIT value (at SKIP or end)
		endIdx := len(limitPart)
		if skipIdx := findKeywordNotInBrackets(strings.ToUpper(limitPart), " SKIP "); skipIdx >= 0 && skipIdx < endIdx {
			endIdx = skipIdx
		}
		withLimitVal, _ = strconv.Atoi(strings.TrimSpace(limitPart[:endIdx]))
		withSection = strings.TrimSpace(withSection[:idx])
		upperWithSection = strings.ToUpper(withSection)
	}
	if idx := findKeywordNotInBrackets(upperWithSection, " SKIP "); idx > 0 {
		skipPart := strings.TrimSpace(withSection[idx+6:])
		endIdx := len(skipPart)
		if limIdx := findKeywordNotInBrackets(strings.ToUpper(skipPart), " LIMIT "); limIdx >= 0 && limIdx < endIdx {
			endIdx = limIdx
		}
		withSkipVal, _ = strconv.Atoi(strings.TrimSpace(skipPart[:endIdx]))
		withSection = strings.TrimSpace(withSection[:idx])
	}

	// Check for WHERE between WITH and RETURN (post-aggregation filter, like SQL HAVING)
	var withClause string
	var postWithWhere string
	postWhereIdx := findKeywordIndex(withSection, "WHERE")
	if postWhereIdx > 0 {
		withClause = strings.TrimSpace(withSection[:postWhereIdx])
		postWithWhere = strings.TrimSpace(withSection[postWhereIdx+5:]) // Skip "WHERE"
	} else {
		withClause = withSection
	}

	// Extract ORDER BY, SKIP, LIMIT from after RETURN
	returnPart := strings.TrimSpace(withAndReturn[returnIdx+6:])
	var orderByClause string
	var skipVal, limitVal int

	orderByIdx := findKeywordIndex(returnPart, "ORDER BY")
	if orderByIdx >= 0 {
		ks, ke := trimKeywordWSBounds("ORDER BY")
		orderByEnd, ok := keywordMatchAt(returnPart, orderByIdx, "ORDER BY", ks, ke)
		if !ok {
			return nil, fmt.Errorf("failed to parse ORDER BY clause")
		}

		afterReturn := returnPart[orderByEnd:]
		endIdx := len(afterReturn)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(afterReturn, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderByClause = strings.TrimSpace(afterReturn[:endIdx])
		returnPart = returnPart[:orderByIdx]
	}

	// Parse SKIP
	if idx := findKeywordIndex(withAndReturn[returnIdx:], "SKIP"); idx >= 0 {
		ks, ke := trimKeywordWSBounds("SKIP")
		skipEnd, ok := keywordMatchAt(withAndReturn[returnIdx:], idx, "SKIP", ks, ke)
		if !ok {
			return nil, fmt.Errorf("failed to parse SKIP clause")
		}
		skipPart := withAndReturn[returnIdx+skipEnd:]
		endIdx := len(skipPart)
		for _, kw := range []string{"LIMIT", "ORDER BY"} {
			if i := findKeywordIndex(skipPart, kw); i >= 0 && i < endIdx {
				endIdx = i
			}
		}
		skipVal, _ = strconv.Atoi(strings.TrimSpace(skipPart[:endIdx]))
	}

	// Parse LIMIT
	if idx := findKeywordIndex(withAndReturn[returnIdx:], "LIMIT"); idx >= 0 {
		ks, ke := trimKeywordWSBounds("LIMIT")
		limitEnd, ok := keywordMatchAt(withAndReturn[returnIdx:], idx, "LIMIT", ks, ke)
		if !ok {
			return nil, fmt.Errorf("failed to parse LIMIT clause")
		}
		limitPart := withAndReturn[returnIdx+limitEnd:]
		endIdx := len(limitPart)
		for _, kw := range []string{"SKIP", "ORDER BY"} {
			if i := findKeywordIndex(limitPart, kw); i >= 0 && i < endIdx {
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
				val := e.evaluateExpressionWithPathContext(ge.expr, pathCtx)
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

			// Calculate aggregates (using whitespace-tolerant helpers)
			for _, ae := range aggregateExprs {
				inner := extractFuncInner(ae.expr)
				switch {
				case isAggregateFuncName(ae.expr, "count") && strings.Contains(strings.ToUpper(inner), "DISTINCT"):
					// COUNT(DISTINCT ...) - extract after DISTINCT
					distinctInner := strings.TrimSpace(inner[8:]) // skip "DISTINCT"
					seen := make(map[string]bool)
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(distinctInner, pCtx)
						if val != nil {
							seen[fmt.Sprintf("%v", val)] = true
						}
					}
					values[ae.alias] = int64(len(seen))

				case isAggregateFuncName(ae.expr, "count"):
					if inner == "*" {
						values[ae.alias] = int64(len(groupPaths))
					} else {
						count := int64(0)
						for _, p := range groupPaths {
							pCtx := e.buildPathContext(p, matches)
							val := e.evaluateExpressionWithPathContext(inner, pCtx)
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
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(inner, pCtx)
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
							// Check if it's a whole number
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

				case isAggregateFuncName(ae.expr, "avg"):
					sum := float64(0)
					count := 0
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(inner, pCtx)
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

				case isAggregateFuncName(ae.expr, "min"):
					var minVal interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(inner, pCtx)
						if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
							minVal = val
						}
					}
					values[ae.alias] = minVal

				case isAggregateFuncName(ae.expr, "max"):
					var maxVal interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(inner, pCtx)
						if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
							maxVal = val
						}
					}
					values[ae.alias] = maxVal

				case isAggregateFuncName(ae.expr, "collect") && strings.Contains(strings.ToUpper(inner), "DISTINCT"):
					// COLLECT(DISTINCT ...) - extract after DISTINCT
					distinctInner := strings.TrimSpace(inner[8:]) // skip "DISTINCT"
					seen := make(map[string]bool)
					var collected []interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(distinctInner, pCtx)
						key := fmt.Sprintf("%v", val)
						if !seen[key] {
							seen[key] = true
							collected = append(collected, val)
						}
					}
					values[ae.alias] = collected

				case isAggregateFuncName(ae.expr, "collect"):
					var collected []interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(inner, pCtx)
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
				values[wi.alias] = e.evaluateExpressionWithPathContext(wi.expr, pathCtx)
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

	// Apply WITH SKIP
	if withSkipVal > 0 && withSkipVal < len(computedRows) {
		computedRows = computedRows[withSkipVal:]
	} else if withSkipVal >= len(computedRows) {
		computedRows = []computedRow{}
	}

	// Apply WITH LIMIT
	if withLimitVal > 0 && withLimitVal < len(computedRows) {
		computedRows = computedRows[:withLimitVal]
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

	// Check if RETURN clause has aggregation functions
	hasReturnAggregation := false
	for _, item := range returnItems {
		if containsAggregateFunc(item.expr) {
			hasReturnAggregation = true
			break
		}
	}

	if hasReturnAggregation {
		// RETURN clause has aggregation - need to aggregate all rows into one
		// Identify group-by columns (non-aggregated) and aggregation expressions
		resultRow := make([]interface{}, len(returnItems))

		for i, item := range returnItems {
			if containsAggregateFunc(item.expr) {
				// Handle aggregation functions
				inner := extractFuncInner(item.expr)

				if isAggregateFuncName(item.expr, "collect") {
					// Handle COLLECT (with or without DISTINCT)
					upperInner := strings.ToUpper(inner)
					isDistinct := strings.HasPrefix(upperInner, "DISTINCT ")
					collectExpr := inner
					if isDistinct {
						collectExpr = strings.TrimSpace(inner[9:])
					}

					seen := make(map[string]bool)
					var collected []interface{}
					for _, row := range computedRows {
						val := e.evaluateExpressionFromValues(collectExpr, row.values)
						if isDistinct {
							key := fmt.Sprintf("%v", val)
							if !seen[key] {
								seen[key] = true
								collected = append(collected, val)
							}
						} else {
							collected = append(collected, val)
						}
					}
					resultRow[i] = collected
				} else if isAggregateFuncName(item.expr, "count") {
					if inner == "*" {
						resultRow[i] = int64(len(computedRows))
					} else {
						count := int64(0)
						for _, row := range computedRows {
							val := e.evaluateExpressionFromValues(inner, row.values)
							if val != nil {
								count++
							}
						}
						resultRow[i] = count
					}
				} else if isAggregateFuncName(item.expr, "sum") {
					sum := float64(0)
					for _, row := range computedRows {
						val := e.evaluateExpressionFromValues(inner, row.values)
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
					resultRow[i] = sum
				} else if isAggregateFuncName(item.expr, "avg") {
					sum := float64(0)
					count := 0
					for _, row := range computedRows {
						val := e.evaluateExpressionFromValues(inner, row.values)
						if num, ok := toFloat64(val); ok {
							sum += num
							count++
						}
					}
					if count > 0 {
						resultRow[i] = sum / float64(count)
					}
				} else if isAggregateFuncName(item.expr, "min") {
					var minVal interface{}
					for _, row := range computedRows {
						val := e.evaluateExpressionFromValues(inner, row.values)
						if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
							minVal = val
						}
					}
					resultRow[i] = minVal
				} else if isAggregateFuncName(item.expr, "max") {
					var maxVal interface{}
					for _, row := range computedRows {
						val := e.evaluateExpressionFromValues(inner, row.values)
						if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
							maxVal = val
						}
					}
					resultRow[i] = maxVal
				}
			} else {
				// Non-aggregated column - use value from first row
				if len(computedRows) > 0 {
					if val, ok := computedRows[0].values[item.expr]; ok {
						resultRow[i] = val
					} else if val, ok := computedRows[0].values[item.alias]; ok {
						resultRow[i] = val
					} else {
						resultRow[i] = e.evaluateExpressionFromValues(item.expr, computedRows[0].values)
					}
				}
			}
		}
		result.Rows = append(result.Rows, resultRow)
	} else {
		// No aggregation - Build result rows individually
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
			// Handle *storage.Node (direct node reference)
			if node, ok := val.(*storage.Node); ok {
				return node.Properties[propName]
			}
			// Handle map[string]interface{} (node converted to map)
			if nodeMap, ok := val.(map[string]interface{}); ok {
				// Check properties sub-map first
				if props, ok := nodeMap["properties"].(map[string]interface{}); ok {
					if propVal, ok := props[propName]; ok {
						return propVal
					}
				}
				// Check top-level map for the property
				if propVal, ok := nodeMap[propName]; ok {
					return propVal
				}
			}
		}
	}

	// Handle map literal expressions {...}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return e.evaluateMapLiteralFromValues(expr, values)
	}

	// Handle function calls
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		// For labels(connected), we need to extract the node and get labels
		if matchFuncStartAndSuffix(expr, "labels") {
			inner := extractFuncArgs(expr, "labels")
			if val, ok := values[inner]; ok {
				if nodeMap, ok := val.(map[string]interface{}); ok {
					if labels, ok := nodeMap["labels"]; ok {
						return labels
					}
				}
				if node, ok := val.(*storage.Node); ok {
					result := make([]interface{}, len(node.Labels))
					for i, label := range node.Labels {
						result[i] = label
					}
					return result
				}
			}
		}

		// For length(path), extract the path length from a path map
		if matchFuncStartAndSuffix(expr, "length") {
			inner := extractFuncArgs(expr, "length")
			if val, ok := values[inner]; ok {
				if pathMap, ok := val.(map[string]interface{}); ok {
					if length, ok := pathMap["length"]; ok {
						return length
					}
				}
			}
		}

		// For relationships(path), extract the relationships from a path map
		if matchFuncStartAndSuffix(expr, "relationships") {
			inner := extractFuncArgs(expr, "relationships")
			if val, ok := values[inner]; ok {
				if pathMap, ok := val.(map[string]interface{}); ok {
					if rels, ok := pathMap["rels"]; ok {
						// Convert []*storage.Edge to []interface{} of maps
						if edges, ok := rels.([]*storage.Edge); ok {
							result := make([]interface{}, len(edges))
							for i, edge := range edges {
								result[i] = map[string]interface{}{
									"_edgeId":    string(edge.ID),
									"type":       edge.Type,
									"properties": edge.Properties,
								}
							}
							return result
						}
					}
				}
			}
		}

		// For size(list), get the count of elements in a list or variable
		if matchFuncStartAndSuffix(expr, "size") {
			inner := extractFuncArgs(expr, "size")
			if val, ok := values[inner]; ok {
				switch v := val.(type) {
				case []interface{}:
					return int64(len(v))
				case []*storage.Node:
					return int64(len(v))
				case []*storage.Edge:
					return int64(len(v))
				case []string:
					return int64(len(v))
				case string:
					return int64(len(v))
				}
			}
			// Recursively evaluate the inner expression first
			innerVal := e.evaluateExpressionFromValues(inner, values)
			switch v := innerVal.(type) {
			case []interface{}:
				return int64(len(v))
			case []*storage.Node:
				return int64(len(v))
			case []*storage.Edge:
				return int64(len(v))
			case []string:
				return int64(len(v))
			case string:
				return int64(len(v))
			}
			return int64(0)
		}
	}

	// Handle list comprehension [r IN relationships(path) | type(r)]
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") && strings.Contains(expr, " IN ") && strings.Contains(expr, " | ") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		inIdx := strings.Index(strings.ToUpper(inner), " IN ")
		if inIdx > 0 {
			// varName is the iterator variable (e.g., "r" in "[r IN ... | ...]")
			_ = strings.TrimSpace(inner[:inIdx]) // varName - used for context if needed
			rest := inner[inIdx+4:]
			pipeIdx := strings.Index(rest, " | ")
			if pipeIdx > 0 {
				listExpr := strings.TrimSpace(rest[:pipeIdx])
				transform := strings.TrimSpace(rest[pipeIdx+3:])

				// Evaluate the list expression
				list := e.evaluateExpressionFromValues(listExpr, values)
				listVal, ok := list.([]interface{})
				if !ok {
					return []interface{}{}
				}

				result := make([]interface{}, len(listVal))
				for i, item := range listVal {
					// For type(r), extract the type from the relationship map
					if matchFuncStartAndSuffix(transform, "type") {
						if mapItem, ok := item.(map[string]interface{}); ok {
							if relType, ok := mapItem["type"]; ok {
								result[i] = relType
								continue
							}
						}
					}
					// Fallback
					result[i] = nil
				}
				return result
			}
		}
	}

	return expr // Return as literal if not found
}

// evaluateMapLiteralFromValues evaluates a map literal using computed values
func (e *StorageExecutor) evaluateMapLiteralFromValues(expr string, values map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "{") || !strings.HasSuffix(expr, "}") {
		return result
	}

	inner := strings.TrimSpace(expr[1 : len(expr)-1])
	if inner == "" {
		return result
	}

	// Split by commas, respecting nesting
	pairs := e.splitMapPairsRespectingNesting(inner)

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Find the first colon (key: value)
		colonIdx := strings.Index(pair, ":")
		if colonIdx == -1 {
			continue
		}

		key := strings.TrimSpace(pair[:colonIdx])
		valueExpr := strings.TrimSpace(pair[colonIdx+1:])

		// Evaluate the value expression using the values map
		value := e.evaluateExpressionFromValues(valueExpr, values)
		result[key] = value
	}

	return result
}

// executeMatchWithClause handles MATCH ... WHERE ... WITH ... RETURN queries
// This processes computed values (like CASE WHEN) in the WITH clause
// and handles aggregation with implicit GROUP BY
