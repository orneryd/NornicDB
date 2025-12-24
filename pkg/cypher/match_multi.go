package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeMatchWithUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Find all clause boundaries
	matchIdx := findKeywordIndex(cypher, "MATCH")
	withIdx := findKeywordIndex(cypher, "WITH")
	unwindIdx := findKeywordNotInBrackets(upper, " UNWIND ")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if matchIdx == -1 || withIdx == -1 || unwindIdx == -1 || returnIdx == -1 {
		return nil, fmt.Errorf("MATCH, WITH, UNWIND, and RETURN clauses required (e.g., MATCH (n) WITH n UNWIND n.items AS item RETURN item)")
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
		return nil, fmt.Errorf("UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)")
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
	returnEnd := len(returnClause)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnClause, keyword); idx >= 0 && idx < returnEnd {
			returnEnd = idx
		}
	}
	returnClause = strings.TrimSpace(returnClause[:returnEnd])
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
	orderByIdx := findKeywordIndex(cypher, "ORDER BY")
	if orderByIdx > 0 {
		ks, ke := trimKeywordWSBounds("ORDER BY")
		orderByEnd, ok := keywordMatchAt(cypher, orderByIdx, "ORDER BY", ks, ke)
		if !ok {
			return nil, fmt.Errorf("failed to parse ORDER BY clause")
		}

		orderPart := cypher[orderByEnd:]
		endIdx := len(orderPart)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(orderPart[:endIdx])
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
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnPart, kw); idx >= 0 && idx < returnEndIdx {
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

	// Check if this is an aggregation query (whitespace-tolerant)
	hasAggregation := false
	isAggFlags := make([]bool, len(returnItems))
	for i, item := range returnItems {
		isAggFlags[i] = isAggregateFunc(item.expr)
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

				// Aggregation function (whitespace-tolerant)
				inner := extractFuncInner(item.expr)
				switch {
				case isAggregateFuncName(item.expr, "count"):
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

				case isAggregateFuncName(item.expr, "sum"):
					sum := float64(0)
					for _, b := range groupBindings {
						val := e.resolveBindingItem(returnItem{expr: inner}, b)
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
					row[i] = sum

				case isAggregateFuncName(item.expr, "avg"):
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

				case isAggregateFuncName(item.expr, "min"):
					var minVal interface{}
					for _, b := range groupBindings {
						val := e.resolveBindingItem(returnItem{expr: inner}, b)
						if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
							minVal = val
						}
					}
					row[i] = minVal

				case isAggregateFuncName(item.expr, "max"):
					var maxVal interface{}
					for _, b := range groupBindings {
						val := e.resolveBindingItem(returnItem{expr: inner}, b)
						if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
							maxVal = val
						}
					}
					row[i] = maxVal

				case isAggregateFuncName(item.expr, "collect"):
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

	// Apply ORDER BY, SKIP, LIMIT (whitespace-tolerant)
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
		return node
	}

	return nil
}

// collectNodesWithStreaming efficiently collects nodes from storage using streaming when possible.
// This avoids loading all nodes into memory, which is critical for performance with large datasets.
//
// Parameters:
//   - ctx: Context for cancellation
//   - labels: Optional label filter (only nodes with this label)
//   - properties: Optional property filters
//   - limit: Maximum number of nodes to collect (-1 for unlimited)
//
// Returns collected nodes or error.
func (e *StorageExecutor) collectNodesWithStreaming(
	ctx context.Context,
	labels []string,
	properties map[string]interface{},
	limit int,
) ([]*storage.Node, error) {
	// Determine if we can use streaming optimization
	canStream := len(properties) == 0 // Can't filter properties inline yet

	var nodes []*storage.Node
	var err error

	if canStream && limit > 0 {
		// Use streaming with early termination for LIMIT queries
		nodes = make([]*storage.Node, 0, limit)
		if streamer, ok := e.storage.(storage.StreamingEngine); ok {
			hideSystemNodes := shouldHideSystemNodes(e.storage)
			err = streamer.StreamNodes(ctx, func(node *storage.Node) error {
				// Skip system nodes (labels starting with _)
				if hideSystemNodes && isSystemNode(node) {
					return nil
				}

				// Check label filter
				if len(labels) > 0 {
					hasLabel := false
					for _, nodeLabel := range node.Labels {
						if nodeLabel == labels[0] {
							hasLabel = true
							break
						}
					}
					if !hasLabel {
						return nil // Skip this node
					}
				}

				nodes = append(nodes, node)
				if len(nodes) >= limit {
					return storage.ErrIterationStopped // Early termination
				}
				return nil
			})
			// ErrIterationStopped is expected
			if err == storage.ErrIterationStopped {
				err = nil
			}
			if err != nil {
				return nil, err
			}
			return nodes, nil
		}
		// Fall through to standard path if streaming not supported
	}

	// Standard path: load all nodes then filter
	if len(labels) > 0 {
		nodes, err = e.storage.GetNodesByLabel(labels[0])
	} else {
		nodes, err = e.storage.AllNodes()
	}
	if err != nil {
		return nil, err
	}

	// Filter out system nodes (labels starting with _)
	hideSystemNodes := shouldHideSystemNodes(e.storage)
	filteredNodes := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if !hideSystemNodes || !isSystemNode(node) {
			filteredNodes = append(filteredNodes, node)
		}
	}
	nodes = filteredNodes

	// Apply property filters
	if len(properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, properties)
	}

	return nodes, nil
}

func shouldHideSystemNodes(engine storage.Engine) bool {
	// Allow system nodes to be queried when the active database is system.
	// For all other databases, hide internal nodes (labels starting with "_")
	// to avoid leaking metadata into normal user queries.
	if namespaced, ok := engine.(*storage.NamespacedEngine); ok {
		return namespaced.Namespace() != "system"
	}
	return true
}

func isSystemNode(node *storage.Node) bool {
	if node == nil {
		return false
	}
	for _, label := range node.Labels {
		if strings.HasPrefix(label, "_") {
			return true
		}
	}
	return false
}

// executeCartesianProductMatch handles MATCH queries with multiple comma-separated node patterns.
// For example: MATCH (p:Person), (a:Area) RETURN p.name, a.code
// This creates a cartesian product of all matching nodes.
func (e *StorageExecutor) executeCartesianProductMatch(
	ctx context.Context,
	cypher string,
	matchPart string,
	nodePatterns []string,
	whereIdx int,
	returnIdx int,
	returnItems []returnItem,
	hasAggregation bool,
	distinct bool,
	result *ExecuteResult,
) (*ExecuteResult, error) {
	// For each node pattern, find matching nodes
	patternMatches := make([]struct {
		variable string
		nodes    []*storage.Node
	}, 0, len(nodePatterns))

	for _, pattern := range nodePatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}

		nodeInfo := e.parseNodePattern(pattern)

		var nodes []*storage.Node
		var err error

		if len(nodeInfo.labels) > 0 {
			nodes, err = e.storage.GetNodesByLabel(nodeInfo.labels[0])
			if err != nil {
				return nil, fmt.Errorf("storage error: %w", err)
			}
			// Filter by additional labels if present
			if len(nodeInfo.labels) > 1 {
				var filtered []*storage.Node
				for _, node := range nodes {
					hasAll := true
					for _, reqLabel := range nodeInfo.labels[1:] {
						found := false
						for _, nodeLabel := range node.Labels {
							if nodeLabel == reqLabel {
								found = true
								break
							}
						}
						if !found {
							hasAll = false
							break
						}
					}
					if hasAll {
						filtered = append(filtered, node)
					}
				}
				nodes = filtered
			}
		} else {
			nodes, err = e.storage.AllNodes()
			if err != nil {
				return nil, fmt.Errorf("storage error: %w", err)
			}
		}

		// Filter by properties if specified in pattern
		if len(nodeInfo.properties) > 0 {
			nodes = e.filterNodesByProperties(nodes, nodeInfo.properties)
		}

		if nodeInfo.variable != "" {
			patternMatches = append(patternMatches, struct {
				variable string
				nodes    []*storage.Node
			}{
				variable: nodeInfo.variable,
				nodes:    nodes,
			})
		}
	}

	// Build cartesian product
	allMatches := e.buildCartesianProduct(patternMatches)

	// Apply WHERE clause to filter combinations
	if whereIdx > 0 {
		wherePart := strings.TrimSpace(cypher[whereIdx+5 : returnIdx])
		var filtered []map[string]*storage.Node
		for _, match := range allMatches {
			if e.evaluateWhereForContext(wherePart, match) {
				filtered = append(filtered, match)
			}
		}
		allMatches = filtered
	}

	// Handle aggregation queries
	if hasAggregation {
		return e.executeCartesianAggregation(allMatches, returnItems, result)
	}

	// Build result rows from cartesian product
	for _, match := range allMatches {
		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			row[i] = e.evaluateExpressionWithContext(item.expr, match, nil)
		}
		result.Rows = append(result.Rows, row)
	}

	// Apply DISTINCT if needed
	if distinct {
		seen := make(map[string]bool)
		var uniqueRows [][]interface{}
		for _, row := range result.Rows {
			key := fmt.Sprintf("%v", row)
			if !seen[key] {
				seen[key] = true
				uniqueRows = append(uniqueRows, row)
			}
		}
		result.Rows = uniqueRows
	}

	// Apply ORDER BY
	orderByIdx := findKeywordIndex(cypher, "ORDER")
	if orderByIdx > 0 {
		orderStart := orderByIdx + 5
		for orderStart < len(cypher) && isWhitespace(cypher[orderStart]) {
			orderStart++
		}
		if orderStart+2 <= len(cypher) && strings.ToUpper(cypher[orderStart:orderStart+2]) == "BY" {
			orderStart += 2
			for orderStart < len(cypher) && isWhitespace(cypher[orderStart]) {
				orderStart++
			}
		}
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

// executeCartesianAggregation handles aggregation over cartesian product results
func (e *StorageExecutor) executeCartesianAggregation(
	allMatches []map[string]*storage.Node,
	returnItems []returnItem,
	result *ExecuteResult,
) (*ExecuteResult, error) {
	// Check if we have grouping columns (non-aggregated expressions)
	hasGrouping := false
	for _, item := range returnItems {
		upper := strings.ToUpper(item.expr)
		if !strings.HasPrefix(upper, "COUNT(") &&
			!strings.HasPrefix(upper, "SUM(") &&
			!strings.HasPrefix(upper, "AVG(") &&
			!strings.HasPrefix(upper, "MIN(") &&
			!strings.HasPrefix(upper, "MAX(") &&
			!strings.HasPrefix(upper, "COLLECT(") {
			hasGrouping = true
			break
		}
	}

	if !hasGrouping {
		// Simple aggregation without grouping
		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			upper := strings.ToUpper(item.expr)
			switch {
			case strings.HasPrefix(upper, "COUNT("):
				row[i] = int64(len(allMatches))
			case strings.HasPrefix(upper, "COLLECT("):
				inner := item.expr[8 : len(item.expr)-1]
				collected := make([]interface{}, 0, len(allMatches))
				for _, match := range allMatches {
					val := e.evaluateExpressionWithContext(inner, match, nil)
					collected = append(collected, val)
				}
				row[i] = collected
			default:
				if len(allMatches) > 0 {
					row[i] = e.evaluateExpressionWithContext(item.expr, allMatches[0], nil)
				}
			}
		}
		result.Rows = append(result.Rows, row)
		return result, nil
	}

	// GROUP BY: group by non-aggregation columns
	groups := make(map[string][]map[string]*storage.Node)
	groupKeys := make(map[string][]interface{})

	for _, match := range allMatches {
		keyParts := make([]interface{}, 0)
		for _, item := range returnItems {
			upper := strings.ToUpper(item.expr)
			if !strings.HasPrefix(upper, "COUNT(") &&
				!strings.HasPrefix(upper, "SUM(") &&
				!strings.HasPrefix(upper, "AVG(") &&
				!strings.HasPrefix(upper, "MIN(") &&
				!strings.HasPrefix(upper, "MAX(") &&
				!strings.HasPrefix(upper, "COLLECT(") {
				val := e.evaluateExpressionWithContext(item.expr, match, nil)
				keyParts = append(keyParts, val)
			}
		}
		key := fmt.Sprintf("%v", keyParts)
		groups[key] = append(groups[key], match)
		if _, exists := groupKeys[key]; !exists {
			groupKeys[key] = keyParts
		}
	}

	// Build result rows for each group
	for key, groupMatches := range groups {
		row := make([]interface{}, len(returnItems))
		keyIdx := 0

		for i, item := range returnItems {
			upper := strings.ToUpper(item.expr)
			if !strings.HasPrefix(upper, "COUNT(") &&
				!strings.HasPrefix(upper, "SUM(") &&
				!strings.HasPrefix(upper, "AVG(") &&
				!strings.HasPrefix(upper, "MIN(") &&
				!strings.HasPrefix(upper, "MAX(") &&
				!strings.HasPrefix(upper, "COLLECT(") {
				// Non-aggregated column
				row[i] = groupKeys[key][keyIdx]
				keyIdx++
				continue
			}

			// Aggregation
			switch {
			case strings.HasPrefix(upper, "COUNT("):
				row[i] = int64(len(groupMatches))
			case strings.HasPrefix(upper, "COLLECT("):
				inner := item.expr[8 : len(item.expr)-1]
				collected := make([]interface{}, 0, len(groupMatches))
				for _, match := range groupMatches {
					val := e.evaluateExpressionWithContext(inner, match, nil)
					collected = append(collected, val)
				}
				row[i] = collected
			default:
				if len(groupMatches) > 0 {
					row[i] = e.evaluateExpressionWithContext(item.expr, groupMatches[0], nil)
				}
			}
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// evaluateWhereForContext evaluates a WHERE clause against a node context
func (e *StorageExecutor) evaluateWhereForContext(whereClause string, nodes map[string]*storage.Node) bool {
	// Parse the WHERE clause and evaluate against the node context
	result := e.evaluateExpressionWithContext(whereClause, nodes, nil)
	if b, ok := result.(bool); ok {
		return b
	}
	return false
}

// executeCreate handles CREATE queries.
