package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
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

		tokens := strings.Fields(part)
		if len(tokens) == 0 {
			continue
		}

		expr := tokens[0]
		descending := len(tokens) > 1 && strings.ToUpper(tokens[1]) == "DESC"

		// Node sorting is safe only for direct properties of the matched
		// variable. Aliases and computed expressions must be sorted after
		// projection, when their values actually exist.
		prefix := variable + "."
		if !strings.HasPrefix(expr, prefix) {
			return nil
		}
		propName := expr[len(prefix):]
		if propName == "" || strings.ContainsAny(propName, ".()[]") {
			return nil
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
	matches := e.parseTraversalPattern(ctx, patternForParsing)
	if matches == nil {
		return result, localizedError(localization.CypherMatchingTraversalPatternInvalid(patternForParsing), nil)
	}

	// Set the path variable in matches for buildPathContext to use
	if pathVariable != "" {
		matches.PathVariable = pathVariable
	}

	// Parse WITH and RETURN clauses from withAndReturn string
	// withAndReturn starts with "WITH ..."
	returnIdx := findKeywordIndex(withAndReturn, "RETURN")
	if returnIdx == -1 {
		return nil, localizedError(localization.CypherMatchingReturnAfterWithRequired(), nil)
	}

	// Extract WITH clause section
	withSection := strings.TrimSpace(withAndReturn[4:returnIdx]) // Skip "WITH"
	callSection := ""
	if callIdx := findKeywordIndex(withSection, "CALL"); callIdx > 0 {
		callSection = strings.TrimSpace(withSection[callIdx:])
		withSection = strings.TrimSpace(withSection[:callIdx])
	}

	// Extract LIMIT/SKIP from WITH section (e.g., "WITH path, connected LIMIT 10")
	var withLimitVal, withSkipVal int
	upperWithSection := strings.ToUpper(withSection)
	if idx := findKeywordNotInBrackets(upperWithSection, "LIMIT"); idx >= 0 {
		limitPart := strings.TrimSpace(withSection[idx+len("LIMIT"):])
		// Find end of LIMIT value (at SKIP or end)
		endIdx := len(limitPart)
		if skipIdx := findKeywordNotInBrackets(strings.ToUpper(limitPart), " SKIP "); skipIdx >= 0 && skipIdx < endIdx {
			endIdx = skipIdx
		}
		withLimitVal, _ = strconv.Atoi(strings.TrimSpace(limitPart[:endIdx]))
		withSection = strings.TrimSpace(withSection[:idx])
		upperWithSection = strings.ToUpper(withSection)
	}
	if idx := findKeywordNotInBrackets(upperWithSection, "SKIP"); idx >= 0 {
		skipPart := strings.TrimSpace(withSection[idx+len("SKIP"):])
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
			return nil, localizedError(localization.CypherMatchingOrderByParseFailed(), nil)
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
			return nil, localizedError(localization.CypherMatchingSkipParseFailed(), nil)
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
			return nil, localizedError(localization.CypherMatchingLimitParseFailed(), nil)
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

	// Fast path: revenue aggregation by product (Northwind-style WITH).
	// Avoid full traversal/path materialization for MATCH (p)<-[r:ORDERS]-(:Order) + sum(p.unitPrice * r.quantity).
	if preWithWhere == "" && pathVariable == "" {
		if fastRes, ok, err := e.tryFastRevenueByProduct(matches, withClause, returnClause, orderByClause, skipVal, limitVal); ok || err != nil {
			if err != nil {
				return nil, err
			}
			return fastRes, nil
		}
	}

	// Execute traversal to get all paths (generic path).
	paths := e.traverseGraph(ctx, matches)

	// Apply pre-WITH WHERE clause filter if present
	if preWithWhere != "" {
		paths = e.filterPathsByWhere(ctx, paths, matches, preWithWhere)
	}

	// Parse WITH items
	withClause, withDistinct := cutDistinct(withClause)
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

		asIdx := projectionAliasIndex(item)
		var alias string
		var expr string
		if asIdx > 0 {
			expr = strings.TrimSpace(item[:asIdx])
			alias = strings.TrimSpace(item[asIdx+len("AS"):])
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
				val := e.evaluateExpressionWithPathContext(ctx, ge.expr, pathCtx)
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
				case isAggregateFuncName(ae.expr, "count") && startsWithDistinct(inner):
					// COUNT(DISTINCT ...) - extract after DISTINCT
					distinctInner, _ := cutDistinct(inner)
					seen := make(map[string]bool)
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(ctx, distinctInner, pCtx)
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
							val := e.evaluateExpressionWithPathContext(ctx, inner, pCtx)
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
						val := e.evaluateExpressionWithPathContext(ctx, inner, pCtx)
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
						val := e.evaluateExpressionWithPathContext(ctx, inner, pCtx)
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
						val := e.evaluateExpressionWithPathContext(ctx, inner, pCtx)
						if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
							minVal = val
						}
					}
					values[ae.alias] = minVal

				case isAggregateFuncName(ae.expr, "max"):
					var maxVal interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(ctx, inner, pCtx)
						if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
							maxVal = val
						}
					}
					values[ae.alias] = maxVal

				case isAggregateFuncName(ae.expr, "collect") && startsWithDistinct(inner):
					// COLLECT(DISTINCT ...) - extract after DISTINCT
					distinctInner, _ := cutDistinct(inner)
					seen := make(map[string]bool)
					var collected []interface{}
					for _, p := range groupPaths {
						pCtx := e.buildPathContext(p, matches)
						val := e.evaluateExpressionWithPathContext(ctx, distinctInner, pCtx)
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
						val := e.evaluateExpressionWithPathContext(ctx, inner, pCtx)
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
				values[wi.alias] = e.evaluateExpressionWithPathContext(ctx, wi.expr, pathCtx)
			}

			computedRows = append(computedRows, computedRow{values: values})
		}
	}

	// Apply post-WITH WHERE clause filter
	if postWithWhere != "" {
		var filtered []computedRow
		for _, row := range computedRows {
			if e.evaluateWhereOnComputedRow(ctx, postWithWhere, row.values) {
				filtered = append(filtered, row)
			}
		}
		computedRows = filtered
	}
	if withDistinct {
		withAliases := make([]string, 0, len(parsedWithItems))
		for _, wi := range parsedWithItems {
			withAliases = append(withAliases, wi.alias)
		}
		seen := make(map[string]bool)
		distinctRows := make([]computedRow, 0, len(computedRows))
		for _, row := range computedRows {
			parts := make([]string, 0, len(withAliases))
			for _, alias := range withAliases {
				parts = append(parts, alias+"="+joinedValueKey(row.values[alias]))
			}
			key := strings.Join(parts, "|")
			if !seen[key] {
				seen[key] = true
				distinctRows = append(distinctRows, row)
			}
		}
		computedRows = distinctRows
	}

	if callSection != "" {
		for _, row := range computedRows {
			nodeScope := make(map[string]*storage.Node)
			relScope := make(map[string]*storage.Edge)
			for alias, value := range row.values {
				switch v := value.(type) {
				case *storage.Node:
					if v != nil {
						nodeScope[alias] = v
					}
				case *storage.Edge:
					if v != nil {
						relScope[alias] = v
					}
				}
			}
			evaluatedCall := e.substituteBoundVariablesInCall(callSection, nodeScope, relScope)
			if _, err := e.executeProcedureCall(ctx, evaluatedCall, true); err != nil {
				return nil, err
			}
		}
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
					collectExpr, isDistinct := cutDistinct(inner)

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
func (e *StorageExecutor) evaluateWhereOnComputedRow(ctx context.Context, whereClause string, values map[string]interface{}) bool {
	whereClause = strings.TrimSpace(whereClause)
	upperClause := strings.ToUpper(whereClause)

	// Handle AND
	if idx := strings.Index(strings.ToUpper(whereClause), " AND "); idx > 0 {
		left := whereClause[:idx]
		right := whereClause[idx+5:]
		return e.evaluateWhereOnComputedRow(ctx, left, values) && e.evaluateWhereOnComputedRow(ctx, right, values)
	}

	// Handle OR
	if idx := strings.Index(strings.ToUpper(whereClause), " OR "); idx > 0 {
		left := whereClause[:idx]
		right := whereClause[idx+4:]
		return e.evaluateWhereOnComputedRow(ctx, left, values) || e.evaluateWhereOnComputedRow(ctx, right, values)
	}

	if strings.HasSuffix(upperClause, " IS NOT NULL") {
		expr := strings.TrimSpace(whereClause[:len(whereClause)-len(" IS NOT NULL")])
		return e.evaluateExpressionFromValues(expr, values) != nil
	}
	if strings.HasSuffix(upperClause, " IS NULL") {
		expr := strings.TrimSpace(whereClause[:len(whereClause)-len(" IS NULL")])
		return e.evaluateExpressionFromValues(expr, values) == nil
	}

	// Handle a bare label test such as `n:Workload` or `n:A:B`. It carries no
	// comparison operator, so without this branch it falls through to the
	// pass-through at the end of this function and admits every row -- the
	// filter is silently not applied.
	if variable, labels, ok := parseWithWhereLabelTest(whereClause); ok {
		return entityHasAllLabelsOrTypesPredicate(values[variable], labels)
	}

	// Handle comparison operators
	for _, op := range []string{">=", "<=", "<>", "!=", "=", ">", "<"} {
		if idx := strings.Index(whereClause, op); idx > 0 {
			left := strings.TrimSpace(whereClause[:idx])
			right := strings.TrimSpace(whereClause[idx+len(op):])

			leftVal := e.evaluateExpressionFromValues(left, values)
			rightVal := e.parseValue(ctx, right)
			return compareCypherPredicateValues(leftVal, rightVal, op)
		}
	}

	return true
}

// evaluateExpressionFromValues evaluates an expression over a computed values
// map (CALL projections, UNWIND rows, YIELD values, SET expressions, collect
// transforms). The values scope is carried as context bindings and evaluation
// runs through the shared evaluator, so property access, keys(), properties(),
// labels(), type checks, temporal constructors and function dispatch see the
// real values. Node/relationship values are additionally mirrored into the
// entity scopes so legacy handlers that resolve variables there keep working.
//
// The terminal contract is unchanged from the legacy evaluator: expression
// text the shared evaluator does not recognize round-trips to the caller, so
// callers can tell "unrecognized" apart from "evaluated to null" and raise the
// proper statement error (or route to the EXISTS-subquery machinery).
func (e *StorageExecutor) evaluateExpressionFromValues(expr string, values map[string]interface{}) interface{} {
	return e.evaluateExpressionFromValuesContext(context.Background(), expr, values)
}

// evaluateRowFallback is the row evaluator's fallback to the shared
// evaluator. err is the error a function raised there (a registry function's
// argument error), which the shared evaluator records as the statement error
// rather than returning it; the row evaluator reports the expression as
// unresolved and evaluateRowExpressionWithContext records err.
func (e *StorageExecutor) evaluateRowFallback(expr string, values map[string]interface{}) (interface{}, error) {
	ctx := context.WithValue(context.Background(), expressionFailureKey{}, &expressionFailure{})
	value := e.evaluateExpressionFromValuesContext(ctx, expr, values)
	return value, getExpressionFailure(ctx)
}

// evaluateExpressionFromValuesContext is evaluateExpressionFromValues with
// the evaluation context: expression failures are recorded on ctx.
func (e *StorageExecutor) evaluateExpressionFromValuesContext(ctx context.Context, expr string, values map[string]interface{}) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	if isCaseExpression(expr) {
		return e.evaluateCaseExpressionFromValues(expr, values)
	}

	// Direct lookup: callers store pre-computed values under the expression
	// text itself (call-tail plans set values["max(length(p))"] per row), and
	// plain variable references are the hot path.
	if val, ok := values[expr]; ok {
		return val
	}

	// Legacy FromValues precedence for a node projected as a map: a
	// "properties" sub-map wins over a same-named top-level key.
	if dot := strings.IndexByte(expr, '.'); dot > 0 {
		if base, ok := values[expr[:dot]].(map[string]interface{}); ok {
			if props, propsOK := base["properties"].(map[string]interface{}); propsOK {
				if property, propertyOK := props[expr[dot+1:]]; propertyOK {
					return property
				}
			}
		}
	}

	// Relationship-pattern fragments are not scalar expressions: leave them
	// for the pattern machinery (WHERE exists-patterns and friends). The
	// shared recognition heuristics would otherwise misread "--" as
	// arithmetic operators and a lone ">()" arrow fragment as a comparison,
	// evaluating the fragment to null instead of round-tripping it.
	if looksLikeRowRelationshipPattern(expr) && !strings.ContainsAny(expr, "'\"") {
		return expr
	}
	if len(expr) > 0 && (expr[0] == '>' || expr[0] == '<') {
		return expr
	}

	ctx = withValueBindings(ctx, values)
	nodes, rels := entityScopesFromValues(values)
	if value, recognized := e.evaluateExpressionWithContextDefined(ctx, expr, nodes, rels); recognized {
		return value
	}
	// The shared recognition heuristics do not know about value bindings; a
	// bound-map property access is evaluable even when the plain node/rel
	// scopes are empty.
	if bindings := valueBindingsFromContext(ctx); bindings != nil {
		if dot := strings.IndexByte(expr, '.'); dot > 0 {
			if _, ok := bindings[expr[:dot]]; ok {
				return e.evaluateExpressionWithContextFull(ctx, expr, nodes, rels, nil, nil, nil, 0)
			}
		}
	}
	return expr
}

// entityScopesFromValues extracts the *storage.Node and *storage.Edge entries of
// a computed values map into node/relationship scopes for the shared evaluator.
// Rows without entities keep nil scopes, so the common scalar row path stays
// allocation-free.
func entityScopesFromValues(values map[string]interface{}) (map[string]*storage.Node, map[string]*storage.Edge) {
	var nodes map[string]*storage.Node
	var rels map[string]*storage.Edge
	for name, val := range values {
		switch entity := val.(type) {
		case *storage.Node:
			if entity == nil {
				continue
			}
			if nodes == nil {
				nodes = make(map[string]*storage.Node, 1)
			}
			nodes[name] = entity
		case *storage.Edge:
			if entity == nil {
				continue
			}
			if rels == nil {
				rels = make(map[string]*storage.Edge, 1)
			}
			rels[name] = entity
		}
	}
	return nodes, rels
}

func (e *StorageExecutor) evaluateCaseExpressionFromValues(expr string, values map[string]interface{}) interface{} {
	// The shared CASE evaluator resolves non-entity variables through the
	// context value bindings, so the values scope is passed the same way as
	// every other scope instead of using a parallel evaluator.
	return e.evaluateCaseExpression(withValueBindings(context.Background(), values), expr, nil, nil, nil, nil, nil, 0)
}

// evaluateConditionFromValues evaluates a boolean condition over a computed
// values scope. It delegates to the shared condition evaluator with the values
// carried as context bindings, mirroring how the main expression evaluator
// resolves non-entity variables.
func (e *StorageExecutor) evaluateConditionFromValues(condition string, values map[string]interface{}) bool {
	return e.evaluateCondition(withValueBindings(context.Background(), values), condition, nil, nil)
}

func parseLiteralValueFromComputedRow(expr string) (interface{}, bool) {
	return parseLiteralScalarForPipeline(expr)
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
	pairs := splitTopLevelComma(inner)

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
