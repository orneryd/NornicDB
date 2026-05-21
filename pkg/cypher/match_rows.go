package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateCoalesceInContext(expr string, nodeMap map[string]*storage.Node, edgeMap map[string]*storage.Edge, computedValues map[string]interface{}) interface{} {
	// Extract arguments from COALESCE(arg1, arg2, ...)
	innerStart := strings.Index(expr, "(")
	innerEnd := strings.LastIndex(expr, ")")
	if innerStart == -1 || innerEnd == -1 {
		return nil
	}
	inner := expr[innerStart+1 : innerEnd]

	// Split arguments
	args := e.splitFunctionArgs(inner)

	for _, arg := range args {
		arg = strings.TrimSpace(arg)

		// Check computed values
		if val, ok := computedValues[arg]; ok && val != nil {
			return val
		}

		// Handle property access
		if strings.Contains(arg, ".") {
			parts := strings.SplitN(arg, ".", 2)
			varName := parts[0]
			propName := parts[1]

			if node, ok := nodeMap[varName]; ok && node != nil {
				if propVal := node.Properties[propName]; propVal != nil {
					return propVal
				}
			}
			if edge, ok := edgeMap[varName]; ok && edge != nil {
				if propVal := edge.Properties[propName]; propVal != nil {
					return propVal
				}
			}
			if computedValues != nil {
				if raw, ok := computedValues[varName]; ok && raw != nil {
					if valueMap, ok := raw.(map[string]interface{}); ok {
						if propVal, ok := valueMap[propName]; ok && propVal != nil {
							return propVal
						}
						if props, ok := valueMap["properties"].(map[string]interface{}); ok {
							if propVal, ok := props[propName]; ok && propVal != nil {
								return propVal
							}
						}
					}
				}
			}
			continue
		}

		// Check node map
		if node, ok := nodeMap[arg]; ok && node != nil {
			return node
		}

		// Literal value
		if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
			return arg[1 : len(arg)-1]
		}
	}

	return nil
}

// nodeMatchesWhereClause checks if a node matches a simple WHERE clause
func (e *StorageExecutor) nodeMatchesWhereClause(ctx context.Context, node *storage.Node, whereClause string, varName string) bool {
	// Use the standard WHERE evaluation with node and variable name
	return e.evaluateWhere(ctx, node, varName, whereClause)
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
func (e *StorageExecutor) evaluateWithWhereCondition(ctx context.Context, whereClause string, values map[string]interface{}) bool {
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
				leftVal = e.parseValue(ctx, left)
			}
			rightVal, exists := values[right]
			if !exists {
				rightVal = e.parseValue(ctx, right)
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
func (e *StorageExecutor) filterNodesByWhereClause(ctx context.Context, nodes []*storage.Node, whereClause, variable string) []*storage.Node {
	if whereClause == "" {
		return nodes
	}

	filterFn := func(node *storage.Node) bool {
		return e.evaluateWhere(ctx, node, variable, whereClause)
	}

	return parallelFilterNodes(nodes, filterFn)
}

// orderSpec represents a single ORDER BY column specification
type orderSpec struct {
	colIdx     int
	descending bool
	propPath   []string
}

// orderResultRows sorts result rows by the specified ORDER BY expression.
// Supports multiple columns: "col1 ASC, col2 DESC"
func (e *StorageExecutor) orderResultRows(rows [][]interface{}, columns []string, orderExpr string) [][]interface{} {
	if len(rows) <= 1 {
		return rows
	}

	return e.orderRowsBySpecs(rows, e.parseOrderBySpecs(orderExpr, columns))
}

func (e *StorageExecutor) orderResultRowsForReturnItems(rows [][]interface{}, columns []string, returnItems []returnItem, orderExpr string) [][]interface{} {
	if len(rows) <= 1 {
		return rows
	}

	return e.orderRowsBySpecs(rows, e.parseOrderBySpecsWithResolver(orderExpr, columns, func(colName string) int {
		for _, item := range returnItems {
			if item.alias == "" || !strings.EqualFold(strings.TrimSpace(item.expr), colName) {
				continue
			}
			return findOrderByColumnIndex(columns, item.alias)
		}
		return -1
	}))
}

func (e *StorageExecutor) orderRowsBySpecs(rows [][]interface{}, orderSpecs []orderSpec) [][]interface{} {
	if len(orderSpecs) == 0 {
		return rows
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, spec := range orderSpecs {
			left := rows[i][spec.colIdx]
			right := rows[j][spec.colIdx]
			if len(spec.propPath) > 0 {
				left = extractOrderByPropertyValue(left, spec.propPath)
				right = extractOrderByPropertyValue(right, spec.propPath)
			}
			cmp := e.compareOrderValues(left, right)
			if cmp != 0 {
				if spec.descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return rows
}

// parseOrderBySpecs parses "col1 ASC, col2 DESC" into orderSpec slice
func (e *StorageExecutor) parseOrderBySpecs(orderExpr string, columns []string) []orderSpec {
	return e.parseOrderBySpecsWithResolver(orderExpr, columns, nil)
}

func (e *StorageExecutor) parseOrderBySpecsWithResolver(orderExpr string, columns []string, resolveExpression func(colName string) int) []orderSpec {
	var specs []orderSpec

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

		colName := tokens[0]
		descending := len(tokens) > 1 && strings.ToUpper(tokens[1]) == "DESC"

		colIdx := findOrderByColumnIndex(columns, colName)

		if colIdx == -1 && resolveExpression != nil {
			colIdx = resolveExpression(colName)
		}

		propPath := make([]string, 0)
		if colIdx == -1 {
			dot := strings.Index(colName, ".")
			if dot <= 0 || dot >= len(colName)-1 {
				continue
			}
			varName := strings.TrimSpace(colName[:dot])
			propExpr := strings.TrimSpace(colName[dot+1:])
			if varName == "" || propExpr == "" {
				continue
			}
			for i, col := range columns {
				if strings.EqualFold(col, varName) {
					colIdx = i
					break
				}
			}
			if colIdx == -1 {
				continue
			}
			for _, segment := range strings.Split(propExpr, ".") {
				segment = strings.TrimSpace(segment)
				if segment == "" {
					propPath = nil
					break
				}
				propPath = append(propPath, segment)
			}
			if len(propPath) == 0 {
				continue
			}
		}

		specs = append(specs, orderSpec{colIdx: colIdx, descending: descending, propPath: propPath})
	}

	return specs
}

func findOrderByColumnIndex(columns []string, colName string) int {
	for i, col := range columns {
		if strings.EqualFold(col, colName) {
			return i
		}
	}
	return -1
}

func extractOrderByPropertyValue(value interface{}, propPath []string) interface{} {
	current := value
	for _, part := range propPath {
		switch v := current.(type) {
		case *storage.Node:
			switch {
			case strings.EqualFold(part, "id"):
				current = string(v.ID)
			case strings.EqualFold(part, "createdAt"):
				// Cypher property access should prefer explicit node properties.
				// Fall back to metadata field for nodes without a createdAt property.
				if v.Properties != nil {
					if propVal, ok := mapLookupCaseInsensitive(v.Properties, "createdAt"); ok {
						current = propVal
						break
					}
				}
				current = v.CreatedAt
			case strings.EqualFold(part, "updatedAt"):
				// Cypher property access should prefer explicit node properties.
				// Fall back to metadata field for nodes without an updatedAt property.
				if v.Properties != nil {
					if propVal, ok := mapLookupCaseInsensitive(v.Properties, "updatedAt"); ok {
						current = propVal
						break
					}
				}
				current = v.UpdatedAt
			default:
				if v.Properties == nil {
					return nil
				}
				if propVal, ok := mapLookupCaseInsensitive(v.Properties, part); ok {
					current = propVal
				} else {
					current = nil
				}
			}
		case map[string]interface{}:
			// Match evaluateExpressionFromValues semantics:
			// properties sub-map first, then top-level keys.
			if propsRaw, ok := mapLookupCaseInsensitive(v, "properties"); ok {
				if props, ok := propsRaw.(map[string]interface{}); ok {
					if propVal, ok := mapLookupCaseInsensitive(props, part); ok {
						current = propVal
						break
					}
				}
			}
			if propVal, ok := mapLookupCaseInsensitive(v, part); ok {
				current = propVal
			} else {
				return nil
			}
		default:
			return nil
		}
		if current == nil {
			return nil
		}
	}
	return current
}

func mapLookupCaseInsensitive(m map[string]interface{}, key string) (interface{}, bool) {
	if val, ok := m[key]; ok {
		return val, true
	}
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return nil, false
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
	// Find clause boundaries
	matchIdx := findKeywordIndex(cypher, "MATCH")
	unwindIdx := findKeywordIndex(cypher, "UNWIND")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if matchIdx == -1 || unwindIdx == -1 {
		return nil, fmt.Errorf("MATCH and UNWIND clauses required (e.g., MATCH (n) UNWIND n.items AS item RETURN item)")
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
	nodePattern := e.parseNodePattern(ctx, nodePatternPart)

	// Get matching nodes
	var nodes []*storage.Node
	var err error

	if len(nodePattern.labels) > 0 {
		nodes, err = e.loadNodesWithTemporalViewport(ctx, nodePattern.labels)
	} else {
		nodes, err = e.loadNodesWithTemporalViewport(ctx, nil)
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
		nodes = e.filterNodesByWhereClause(ctx, nodes, whereClause, nodePattern.variable)
	}

	// Parse UNWIND clause: UNWIND expr AS variable
	unwindPart := strings.TrimSpace(cypher[unwindIdx+6:])
	var unwindExpr, unwindVar string

	// Find AS keyword
	asIdx := strings.Index(strings.ToUpper(unwindPart), " AS ")
	if asIdx == -1 {
		return nil, fmt.Errorf("UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)")
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
		returnEnd := len(returnClause)
		for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
			if idx := findKeywordIndex(returnClause, keyword); idx >= 0 && idx < returnEnd {
				returnEnd = idx
			}
		}
		returnClause = strings.TrimSpace(returnClause[:returnEnd])
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
		listVal := e.resolveUnwindValueFromExpr(ctx, unwindExpr, nodeMap)
		items := coerceToUnwindItems(listVal)
		if len(items) == 0 {
			continue // null produces no rows
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
				// Use whitespace-tolerant aggregation check
				isAgg := isAggregateFunc(item.expr)

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
					// Use whitespace-tolerant aggregation check
					isAgg := isAggregateFunc(item.expr)

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
				alias := item.alias
				if alias == "" {
					alias = item.expr
				}

				switch {
				case isAggregateFuncName(item.expr, "count"):
					row[i] = int64(len(groupRows))
				case isAggregateFuncName(item.expr, "collect"):
					inner := extractFuncInner(item.expr)
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
					row[i] = ur.node
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	// Apply ORDER BY, SKIP, LIMIT
	orderByIdx := findKeywordIndex(cypher, "ORDER BY")
	if orderByIdx > 0 {
		orderPart := cypher[orderByIdx+8:]
		endIdx := len(orderPart)
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
		skipPart = strings.Split(skipPart, " ")[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	// Apply LIMIT
	limitIdx := findKeywordIndex(cypher, "LIMIT")
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
// Complex UNWIND + CALL pattern used by stats queries:
// MATCH (f:File) WITH f, [...] as list UNWIND list as item WITH item, COUNT(*) RETURN item
