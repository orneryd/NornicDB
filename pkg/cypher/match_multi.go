package cypher

import (
	"context"
	"fmt"
	"regexp"
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

	if len(nodePattern.properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
	}

	if matchWhere != "" {
		nodes = e.filterNodesByWhereClause(ctx, nodes, matchWhere, nodePattern.variable)
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
				values[alias] = e.evaluateExpressionWithContext(ctx, expr, nodeMap, nil)
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
	// Normalize two-MATCH forms where WHERE appears between MATCH clauses:
	// MATCH A WHERE wa MATCH B RETURN ...
	// -> MATCH A MATCH B WHERE wa RETURN ...
	cypher = normalizeMultiMatchWhereClauses(cypher)

	// Find RETURN and WHERE positions
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("multi-MATCH query requires RETURN clause")
	}

	// Extract WHERE clause if present (between last MATCH pattern and RETURN).
	// Use the last WHERE before RETURN so queries like:
	// MATCH A WHERE wa MATCH B RETURN ...
	// (after normalization) do not accidentally pick an earlier WHERE position.
	var whereClause string
	whereIdx := lastKeywordIndexBefore(cypher, "WHERE", returnIdx)
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
	bindings := e.executeFirstMatch(ctx, matchClauses[0])

	// Execute subsequent MATCH clauses with bindings
	for i := 1; i < len(matchClauses); i++ {
		bindings = e.executeChainedMatch(ctx, matchClauses[i], bindings)
	}

	// Apply WHERE filter if present
	if whereClause != "" {
		bindings = e.filterBindingsByWhere(ctx, bindings, whereClause, getParamsFromContext(ctx))
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
					val := e.resolveBindingItem(ctx, item, b)
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
							val := e.resolveBindingItem(ctx, returnItem{expr: inner}, b)
							if val != nil {
								count++
							}
						}
						row[i] = count
					}

				case isAggregateFuncName(item.expr, "sum"):
					sum := float64(0)
					for _, b := range groupBindings {
						val := e.resolveBindingItem(ctx, returnItem{expr: inner}, b)
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
					row[i] = sum

				case isAggregateFuncName(item.expr, "avg"):
					sum := float64(0)
					count := 0
					for _, b := range groupBindings {
						val := e.resolveBindingItem(ctx, returnItem{expr: inner}, b)
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
						val := e.resolveBindingItem(ctx, returnItem{expr: inner}, b)
						if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
							minVal = val
						}
					}
					row[i] = minVal

				case isAggregateFuncName(item.expr, "max"):
					var maxVal interface{}
					for _, b := range groupBindings {
						val := e.resolveBindingItem(ctx, returnItem{expr: inner}, b)
						if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
							maxVal = val
						}
					}
					row[i] = maxVal

				case isAggregateFuncName(item.expr, "collect"):
					var collected []interface{}
					for _, b := range groupBindings {
						val := e.resolveBindingItem(ctx, returnItem{expr: inner}, b)
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
				row[i] = e.resolveBindingItem(ctx, item, b)
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

// lastKeywordIndexBefore returns the last occurrence of keyword before endIdx.
// It uses keyword-aware scanning and returns -1 when not found.
func lastKeywordIndexBefore(query, keyword string, endIdx int) int {
	if endIdx <= 0 {
		return -1
	}
	if endIdx > len(query) {
		endIdx = len(query)
	}
	segment := query[:endIdx]
	pos := -1
	search := 0
	for {
		rel := findKeywordIndex(segment[search:], keyword)
		if rel < 0 {
			break
		}
		found := search + rel
		pos = found
		search = found + len(keyword)
		if search >= len(segment) {
			break
		}
	}
	return pos
}

// splitMatchClauses splits the query into individual MATCH clause patterns
func splitMatchClauses(cypher string, whereIdx, returnIdx int) []string {
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
		nextMatch := findKeywordIndex(cypher[searchStart:], "MATCH")
		if nextMatch == -1 || searchStart+nextMatch >= endIdx {
			// No more MATCH - take everything to end
			clauses = append(clauses, strings.TrimSpace(cypher[start:endIdx]))
			break
		}
		pos := searchStart + nextMatch
		clauses = append(clauses, strings.TrimSpace(cypher[start:pos]))
		start = pos + 5 // Skip "MATCH"
		searchStart = pos + 5
	}

	return clauses
}

// binding represents variable bindings from multiple MATCH clauses
type binding map[string]*storage.Node

// executeFirstMatch executes the first MATCH and returns initial bindings
func (e *StorageExecutor) executeFirstMatch(ctx context.Context, pattern string) []binding {
	var bindings []binding

	// Check for relationship pattern
	if strings.Contains(pattern, "-[") || strings.Contains(pattern, "]-") {
		matches := e.parseTraversalPattern(ctx, pattern)
		if matches == nil {
			return bindings
		}

		paths := e.traverseGraph(ctx, matches)
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
		nodePattern := e.parseNodePattern(ctx, pattern)
		nodes, _ := e.collectNodesWithStreaming(ctx, nodePattern.labels, nodePattern.properties, nodePattern.variable, "", -1)

		for _, node := range nodes {
			b := make(binding)
			b[nodePattern.variable] = node
			bindings = append(bindings, b)
		}
	}

	return bindings
}

// executeChainedMatch executes a subsequent MATCH against existing bindings
func (e *StorageExecutor) executeChainedMatch(ctx context.Context, pattern string, existingBindings []binding) []binding {
	var newBindings []binding

	for _, existing := range existingBindings {
		// Check for relationship pattern
		if strings.Contains(pattern, "-[") || strings.Contains(pattern, "]-") {
			matches := e.parseTraversalPattern(ctx, pattern)
			if matches == nil {
				continue
			}

			// Check if any bound variables are referenced
			boundStartNode := existing[matches.StartNode.variable]
			boundEndNode := existing[matches.EndNode.variable]

			var paths []PathResult
			if boundStartNode != nil {
				paths = e.traverseFromNode(ctx, boundStartNode, matches)
			} else {
				paths = e.traverseGraph(ctx, matches)
			}
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
			nodePattern := e.parseNodePattern(ctx, pattern)

			// Check if variable is already bound
			if boundNode := existing[nodePattern.variable]; boundNode != nil {
				// Variable is bound, just propagate
				newBindings = append(newBindings, existing)
				continue
			}

			var nodes []*storage.Node
			if len(nodePattern.labels) > 0 {
				nodes, _ = e.loadNodesWithTemporalViewport(ctx, nodePattern.labels)
			} else {
				nodes, _ = e.loadNodesWithTemporalViewport(ctx, nil)
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
func (e *StorageExecutor) filterBindingsByWhere(ctx context.Context, bindings []binding, whereClause string, params map[string]interface{}) []binding {
	compiled := e.getCompiledBindingWhere(ctx, whereClause)
	result := make([]binding, 0, len(bindings))

	for _, b := range bindings {
		if compiled(b, params) {
			result = append(result, b)
		}
	}

	return result
}

// evaluateBindingWhere evaluates WHERE clause against a binding
func (e *StorageExecutor) evaluateBindingWhere(ctx context.Context, b binding, whereClause string, params map[string]interface{}) bool {
	// Thread the explicit params map onto ctx so the recursive expression
	// evaluator can resolve $param refs to their typed values via
	// resolveDirectParamRef. Without this, the evaluator only sees what's
	// already in ctx — and callers (tests, internal binding compiles) often
	// provide params as an explicit argument instead of via ctx.
	if params != nil {
		ctx = withParams(ctx, params)
	}
	return e.evaluateBindingWhereGeneric(ctx, b, whereClause, params)
}

func (e *StorageExecutor) resolveWhereValue(ctx context.Context, raw string, params map[string]interface{}) interface{} {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "$") {
		name := strings.TrimSpace(strings.TrimPrefix(s, "$"))
		if params != nil {
			if v, ok := params[name]; ok {
				return v
			}
		}
	}
	return e.parseValue(ctx, s)
}

// resolveBindingItem resolves a return item against a binding
func (e *StorageExecutor) resolveBindingItem(ctx context.Context, item returnItem, b binding) interface{} {
	expr := strings.TrimSpace(item.expr)
	if expr == "" {
		return nil
	}
	return e.resolveBindingExpr(ctx, expr, b)
}

func (e *StorageExecutor) resolveBindingExpr(ctx context.Context, expr string, b binding) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}

	// elementId(var)
	if strings.HasPrefix(strings.ToLower(expr), "elementid(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[len("elementId(") : len(expr)-1])
		if node := b[inner]; node != nil {
			return string(node.ID)
		}
		return nil
	}

	// Reuse shared COALESCE evaluator used in MATCH row projection paths.
	if strings.HasPrefix(strings.ToUpper(expr), "COALESCE(") && strings.HasSuffix(expr, ")") {
		return e.evaluateCoalesceInContext(expr, b, nil, nil)
	}

	// Literal value
	if strings.HasPrefix(expr, "'") || strings.HasPrefix(expr, "\"") ||
		strings.EqualFold(expr, "true") || strings.EqualFold(expr, "false") ||
		strings.EqualFold(expr, "null") || isNumericLiteral(expr) {
		return e.parseValue(ctx, expr)
	}

	// Property access: var.prop
	if dotIdx := strings.Index(expr, "."); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]
		if node := b[varName]; node != nil {
			if val, ok := getBindingNodeValue(node, propName); ok {
				return val
			}
			return nil
		}
		return nil
	}

	// Node variable
	if node := b[expr]; node != nil {
		return node
	}

	// Fallback to the common expression evaluator used in other MATCH/RETURN paths.
	if val := e.evaluateExpressionWithContext(ctx, expr, b, nil); val != nil {
		return val
	}

	return nil
}

func isNumericLiteral(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
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
	whereVariable string,
	whereClause string,
	limit int,
) ([]*storage.Node, error) {
	store := e.getStorage(ctx)
	viewport, hasViewport := TemporalViewportFromContext(ctx)
	checker, canCheckViewport := store.(temporalCurrentNodeChecker)

	// Pattern-inline property fast-path: when the pattern carries inline
	// equality on one or more indexed properties (labelled or labelless), we
	// can probe the property index instead of streaming or scanning the full
	// label population. This is the generic, shape-agnostic counterpart of
	// the WHERE-clause index fast-paths in executeMatch — it covers patterns
	// like `MATCH (n:Code {id:$x})`, `MATCH (n {id:$x})` (graphify's labelless
	// edge MATCH), and N-ary `MATCH (n:Code {id:$x, sku:$y})`. The residual
	// property filter and viewport / system-node filters below are still
	// applied so semantics match the scan path exactly.
	if len(properties) > 0 && strings.TrimSpace(whereClause) == "" {
		nodeInfo := nodePatternInfo{labels: labels, properties: properties}
		if indexed, ok := e.lookupPatternCandidatesUsingPropertyIndex(nodeInfo, store); ok {
			out := indexed
			// Residual property / viewport / system-node filters.
			hideSystemNodes := shouldHideSystemNodes(store)
			filtered := out[:0]
			for _, n := range out {
				if hideSystemNodes && isSystemNode(n) {
					continue
				}
				if len(labels) > 0 && !mergeNodeHasLabels(n, labels) {
					continue
				}
				if !e.nodeMatchesProps(n, properties) {
					continue
				}
				if hasViewport && canCheckViewport {
					visible, err := checker.IsCurrentTemporalNode(n, viewport.AsOf)
					if err != nil {
						return nil, err
					}
					if !visible {
						continue
					}
				}
				filtered = append(filtered, n)
				if limit > 0 && len(filtered) >= limit {
					break
				}
			}
			return filtered, nil
		}
	}

	// For label-constrained LIMIT queries, prefer direct label lookup over full
	// graph streaming. This avoids scanning unrelated labels until LIMIT is met
	// (a major latency issue for sparse labels like SystemPrompt).
	if limit > 0 && len(labels) == 1 && len(properties) == 0 && strings.TrimSpace(whereClause) == "" {
		ids, err := storage.NodeIDsByLabel(store, labels[0], limit)
		if err != nil {
			return nil, err
		}
		hideSystemNodes := shouldHideSystemNodes(store)
		filtered := make([]*storage.Node, 0, min(limit, len(ids)))
		for _, id := range ids {
			node, getErr := store.GetNode(id)
			if getErr != nil || node == nil {
				continue
			}
			if hideSystemNodes && isSystemNode(node) {
				continue
			}
			if hasViewport && canCheckViewport {
				visible, err := checker.IsCurrentTemporalNode(node, viewport.AsOf)
				if err != nil {
					return nil, err
				}
				if !visible {
					continue
				}
			}
			filtered = append(filtered, node)
			if len(filtered) >= limit {
				break
			}
		}
		return filtered, nil
	}

	// Determine if we can use streaming optimization
	canStream := len(properties) == 0 // Can't filter properties inline yet

	var nodes []*storage.Node
	var err error

	if canStream && limit > 0 {
		// Use streaming with early termination for LIMIT queries
		nodes = make([]*storage.Node, 0, limit)
		var whereFilter FilterFunc
		if strings.TrimSpace(whereClause) != "" {
			if fastIN, ok := e.buildBoundInFastFilter(whereVariable, whereClause); ok {
				whereFilter = fastIN
			} else if compiled, ok := e.getCompiledSimpleWhere(ctx, whereVariable, whereClause); ok {
				whereFilter = compiled
			}
		}
		if streamer, ok := store.(storage.StreamingEngine); ok {
			hideSystemNodes := shouldHideSystemNodes(store)
			err = streamer.StreamNodes(ctx, func(node *storage.Node) error {
				// Skip system nodes (labels starting with _)
				if hideSystemNodes && isSystemNode(node) {
					return nil
				}

				// Check label filter.
				if len(labels) > 0 && !mergeNodeHasLabels(node, labels) {
					return nil // Skip this node
				}
				if whereFilter != nil && !whereFilter(node) {
					return nil
				}
				if hasViewport && canCheckViewport {
					visible, err := checker.IsCurrentTemporalNode(node, viewport.AsOf)
					if err != nil {
						return err
					}
					if !visible {
						return nil
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

	// Standard path: load all nodes then filter (use same store as CREATE for consistency)
	if len(labels) > 0 {
		nodes, err = store.GetNodesByLabel(labels[0])
	} else {
		nodes, err = store.AllNodes()
	}
	if err != nil {
		return nil, err
	}

	// Filter out system nodes (labels starting with _)
	hideSystemNodes := shouldHideSystemNodes(store)
	filteredNodes := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if len(labels) > 0 && !mergeNodeHasLabels(node, labels) {
			continue
		}
		if !hideSystemNodes || !isSystemNode(node) {
			filteredNodes = append(filteredNodes, node)
		}
	}
	nodes = filteredNodes
	if hasViewport && canCheckViewport {
		nodes, err = filterNodesByTemporalViewport(nodes, viewport, checker)
		if err != nil {
			return nil, err
		}
	}

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
	whereClause := ""
	if whereIdx > 0 {
		whereClause = strings.TrimSpace(cypher[whereIdx+5 : returnIdx])
	}

	for _, pattern := range nodePatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}

		nodeInfo := e.parseNodePattern(ctx, pattern)

		var nodes []*storage.Node
		var err error

		// Pattern-inline property fast-path: when the pattern carries inline
		// equality on an indexed property, probe the property index per
		// cartesian leg instead of loading the full label population. This
		// is the path that fires for graphify's labelless edge MATCH —
		// `MATCH (a {id:$src}),(b {id:$tgt})` — and turns each leg into an
		// O(1) lookup. The residual property filter below is still applied
		// to enforce any non-indexed inline props identically to the scan.
		usedPatternIndex := false
		if len(nodeInfo.properties) > 0 {
			if indexed, ok := e.lookupPatternCandidatesUsingPropertyIndex(nodeInfo, e.getStorage(ctx)); ok {
				nodes = indexed
				usedPatternIndex = true
			}
		}
		if !usedPatternIndex {
			if len(nodeInfo.labels) > 0 {
				nodes, err = e.loadNodesWithTemporalViewport(ctx, nodeInfo.labels)
			} else {
				nodes, err = e.loadNodesWithTemporalViewport(ctx, nil)
			}
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

	// Push down selective WHERE predicates before cartesian expansion.
	// This avoids catastrophic row explosion for shapes like:
	// MATCH (o),(t) WHERE o.k IN [...] AND t.k = o.k
	if whereClause != "" && len(patternMatches) > 1 {
		patternMatches = e.applyCartesianWherePushdown(patternMatches, whereClause)
	}

	// Build cartesian product
	allMatches := e.buildCartesianProduct(patternMatches)

	// Apply WHERE clause to filter combinations
	if whereClause != "" {
		var filtered []map[string]*storage.Node
		for _, match := range allMatches {
			if e.evaluateWhereForContext(ctx, whereClause, match) {
				filtered = append(filtered, match)
			}
		}
		allMatches = filtered
	}

	// Handle aggregation queries
	if hasAggregation {
		return e.executeCartesianAggregation(ctx, allMatches, returnItems, result)
	}

	// Build result rows from cartesian product
	for _, match := range allMatches {
		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			row[i] = e.evaluateExpressionWithContext(ctx, item.expr, match, nil)
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

type cartesianInConstraint struct {
	prop      string
	allowed   map[string]struct{}
	hasValues bool
}

type cartesianEqConstraint struct {
	leftVar   string
	leftProp  string
	rightVar  string
	rightProp string
}

func (e *StorageExecutor) applyCartesianWherePushdown(
	patternMatches []struct {
		variable string
		nodes    []*storage.Node
	},
	whereClause string,
) []struct {
	variable string
	nodes    []*storage.Node
} {
	if len(patternMatches) < 2 || strings.TrimSpace(whereClause) == "" {
		return patternMatches
	}

	varIndex := make(map[string]int, len(patternMatches))
	for i, pm := range patternMatches {
		if pm.variable != "" {
			varIndex[pm.variable] = i
		}
	}

	inConstraints := map[string]cartesianInConstraint{}
	nullConstraints := map[string]cartesianNullConstraint{}
	eqConstraints := make([]cartesianEqConstraint, 0, 2)
	for _, term := range splitTopLevelAndCartesian(whereClause) {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		if v, p, vals, ok := parseCartesianInListTerm(term); ok {
			c := inConstraints[v]
			if c.prop == "" {
				c.prop = p
				c.allowed = make(map[string]struct{}, len(vals))
				for _, raw := range vals {
					c.allowed[cartesianValueKey(raw)] = struct{}{}
				}
				c.hasValues = true
			}
			inConstraints[v] = c
			continue
		}
		if v, p, expectNotNull, ok := parseCartesianNullTerm(term); ok {
			key := v + "|" + p
			c := nullConstraints[key]
			if c.hasValue && c.expectNotNull != expectNotNull {
				c.conflict = true
			} else {
				c.expectNotNull = expectNotNull
				c.hasValue = true
			}
			nullConstraints[key] = c
			continue
		}
		if lv, lp, rv, rp, ok := parseCartesianVarPropEqualityTerm(term); ok {
			eqConstraints = append(eqConstraints, cartesianEqConstraint{
				leftVar:   lv,
				leftProp:  lp,
				rightVar:  rv,
				rightProp: rp,
			})
			continue
		}
	}

	applyCartesianNullConstraints(patternMatches, varIndex, nullConstraints)

	// Apply direct IN constraints.
	for v, c := range inConstraints {
		if !c.hasValues {
			continue
		}
		idx, ok := varIndex[v]
		if !ok {
			continue
		}
		patternMatches[idx].nodes = filterNodesByAllowedPropSet(patternMatches[idx].nodes, c.prop, c.allowed)
	}

	// Propagate equality constraints both directions until stable.
	for changed := true; changed; {
		changed = false
		for _, c := range eqConstraints {
			leftIdx, lok := varIndex[c.leftVar]
			rightIdx, rok := varIndex[c.rightVar]
			if !lok || !rok {
				continue
			}
			leftAllowed := collectPropValues(patternMatches[leftIdx].nodes, c.leftProp)
			rightAllowed := collectPropValues(patternMatches[rightIdx].nodes, c.rightProp)
			if len(leftAllowed) == 0 || len(rightAllowed) == 0 {
				continue
			}
			if filtered := filterNodesByAllowedPropSet(patternMatches[rightIdx].nodes, c.rightProp, leftAllowed); len(filtered) != len(patternMatches[rightIdx].nodes) {
				patternMatches[rightIdx].nodes = filtered
				changed = true
			}
			if filtered := filterNodesByAllowedPropSet(patternMatches[leftIdx].nodes, c.leftProp, rightAllowed); len(filtered) != len(patternMatches[leftIdx].nodes) {
				patternMatches[leftIdx].nodes = filtered
				changed = true
			}
		}
	}
	return patternMatches
}

func applyCartesianNullConstraints(
	patternMatches []struct {
		variable string
		nodes    []*storage.Node
	},
	varIndex map[string]int,
	nullConstraints map[string]cartesianNullConstraint,
) {
	for key, constraint := range nullConstraints {
		parts := strings.SplitN(key, "|", 2)
		if len(parts) != 2 {
			continue
		}
		idx, ok := varIndex[parts[0]]
		if !ok {
			continue
		}
		if constraint.conflict {
			patternMatches[idx].nodes = patternMatches[idx].nodes[:0]
			continue
		}
		patternMatches[idx].nodes = filterNodesByNullConstraint(patternMatches[idx].nodes, parts[1], constraint.expectNotNull)
	}
}

func parseCartesianNullTerm(term string) (string, string, bool, bool) {
	upper := strings.ToUpper(strings.TrimSpace(term))
	if strings.HasSuffix(upper, " IS NOT NULL") {
		expr := strings.TrimSpace(term[:len(term)-len(" IS NOT NULL")])
		v, p, ok := parseCartesianVarProp(expr)
		if !ok {
			return "", "", false, false
		}
		return v, p, true, true
	}
	if strings.HasSuffix(upper, " IS NULL") {
		expr := strings.TrimSpace(term[:len(term)-len(" IS NULL")])
		v, p, ok := parseCartesianVarProp(expr)
		if !ok {
			return "", "", false, false
		}
		return v, p, false, true
	}
	return "", "", false, false
}

func splitTopLevelAndCartesian(whereClause string) []string {
	parts := make([]string, 0, 4)
	start := 0
	paren, bracket, brace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(whereClause); i++ {
		ch := whereClause[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			paren++
		case ')':
			if paren > 0 {
				paren--
			}
		case '[':
			bracket++
		case ']':
			if bracket > 0 {
				bracket--
			}
		case '{':
			brace++
		case '}':
			if brace > 0 {
				brace--
			}
		}
		if paren != 0 || bracket != 0 || brace != 0 {
			continue
		}
		if i+5 <= len(whereClause) && strings.EqualFold(whereClause[i:i+5], " AND ") {
			parts = append(parts, strings.TrimSpace(whereClause[start:i]))
			start = i + 5
			i += 4
		}
	}
	parts = append(parts, strings.TrimSpace(whereClause[start:]))
	return parts
}

func parseCartesianVarProp(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	dot := strings.IndexByte(expr, '.')
	if dot <= 0 || dot >= len(expr)-1 {
		return "", "", false
	}
	v := strings.TrimSpace(expr[:dot])
	p := strings.TrimSpace(expr[dot+1:])
	if !isSimpleIdentifierCartesian(v) || p == "" {
		return "", "", false
	}
	return v, normalizePropertyKey(p), true
}

func parseCartesianInListTerm(term string) (string, string, []interface{}, bool) {
	upper := strings.ToUpper(term)
	idx := strings.Index(upper, " IN ")
	if idx <= 0 || idx+4 >= len(term) {
		return "", "", nil, false
	}
	lhs := strings.TrimSpace(term[:idx])
	rhs := strings.TrimSpace(term[idx+4:])
	v, p, ok := parseCartesianVarProp(lhs)
	if !ok {
		return "", "", nil, false
	}
	if !(strings.HasPrefix(rhs, "[") && strings.HasSuffix(rhs, "]")) {
		return "", "", nil, false
	}
	inner := strings.TrimSpace(rhs[1 : len(rhs)-1])
	if inner == "" {
		return v, p, []interface{}{}, true
	}
	items := splitTopLevelCommaKeepEmpty(inner)
	values := make([]interface{}, 0, len(items))
	for _, raw := range items {
		lit, ok := parseLiteralValue(strings.TrimSpace(raw))
		if !ok {
			return "", "", nil, false
		}
		values = append(values, lit)
	}
	return v, p, values, true
}

func isSimpleIdentifierCartesian(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' {
			continue
		}
		if i > 0 && ch >= '0' && ch <= '9' {
			continue
		}
		return false
	}
	return true
}

func parseCartesianVarPropEqualityTerm(term string) (string, string, string, string, bool) {
	idx := strings.Index(term, "=")
	if idx <= 0 || idx+1 >= len(term) {
		return "", "", "", "", false
	}
	lhs := strings.TrimSpace(term[:idx])
	rhs := strings.TrimSpace(term[idx+1:])
	lv, lp, lok := parseCartesianVarProp(lhs)
	rv, rp, rok := parseCartesianVarProp(rhs)
	if !lok || !rok {
		return "", "", "", "", false
	}
	return lv, lp, rv, rp, true
}

func cartesianValueKey(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case string:
		return "s:" + x
	case int:
		return fmt.Sprintf("i:%d", x)
	case int64:
		return fmt.Sprintf("i64:%d", x)
	case float64:
		return fmt.Sprintf("f:%g", x)
	case bool:
		if x {
			return "b:1"
		}
		return "b:0"
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}

func collectPropValues(nodes []*storage.Node, prop string) map[string]struct{} {
	out := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n == nil || n.Properties == nil {
			continue
		}
		val, ok := n.Properties[prop]
		if !ok {
			continue
		}
		out[cartesianValueKey(val)] = struct{}{}
	}
	return out
}

func filterNodesByAllowedPropSet(nodes []*storage.Node, prop string, allowed map[string]struct{}) []*storage.Node {
	if len(nodes) == 0 || len(allowed) == 0 {
		return nodes
	}
	out := make([]*storage.Node, 0, len(nodes))
	for _, n := range nodes {
		if n == nil || n.Properties == nil {
			continue
		}
		val, ok := n.Properties[prop]
		if !ok {
			continue
		}
		if _, keep := allowed[cartesianValueKey(val)]; keep {
			out = append(out, n)
		}
	}
	return out
}

type cartesianNullConstraint struct {
	expectNotNull bool
	hasValue      bool
	conflict      bool
}

func filterNodesByNullConstraint(nodes []*storage.Node, prop string, expectNotNull bool) []*storage.Node {
	if len(nodes) == 0 {
		return nodes
	}
	out := make([]*storage.Node, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		val, exists := n.Properties[prop]
		isNull := !exists || val == nil
		if expectNotNull {
			if !isNull {
				out = append(out, n)
			}
			continue
		}
		if isNull {
			out = append(out, n)
		}
	}
	return out
}

// executeCartesianAggregation handles aggregation over cartesian product results
func (e *StorageExecutor) executeCartesianAggregation(
	ctx context.Context,
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
					val := e.evaluateExpressionWithContext(ctx, inner, match, nil)
					collected = append(collected, val)
				}
				row[i] = collected
			default:
				if len(allMatches) > 0 {
					row[i] = e.evaluateExpressionWithContext(ctx, item.expr, allMatches[0], nil)
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
				val := e.evaluateExpressionWithContext(ctx, item.expr, match, nil)
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
					val := e.evaluateExpressionWithContext(ctx, inner, match, nil)
					collected = append(collected, val)
				}
				row[i] = collected
			default:
				if len(groupMatches) > 0 {
					row[i] = e.evaluateExpressionWithContext(ctx, item.expr, groupMatches[0], nil)
				}
			}
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// evaluateWhereForContext evaluates a WHERE clause against a node context
func (e *StorageExecutor) evaluateWhereForContext(ctx context.Context, whereClause string, nodes map[string]*storage.Node) bool {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return true
	}
	if hasPrefixFold(clause, "NOT ") {
		return !e.evaluateWhereForContext(ctx, strings.TrimSpace(clause[4:]), nodes)
	}

	// Handle top-level conjunction/disjunction explicitly so each side can use
	// the single-variable WHERE evaluator (supports relationship predicates).
	if andIdx := findTopLevelKeyword(clause, " AND "); andIdx > 0 {
		left := strings.TrimSpace(clause[:andIdx])
		right := strings.TrimSpace(clause[andIdx+5:])
		return e.evaluateWhereForContext(ctx, left, nodes) && e.evaluateWhereForContext(ctx, right, nodes)
	}
	if orIdx := findTopLevelKeyword(clause, " OR "); orIdx > 0 {
		left := strings.TrimSpace(clause[:orIdx])
		right := strings.TrimSpace(clause[orIdx+4:])
		return e.evaluateWhereForContext(ctx, left, nodes) || e.evaluateWhereForContext(ctx, right, nodes)
	}

	// Relationship existence predicate across two bound variables:
	// (a)-[:TYPE]->(b) or (a)<-[:TYPE]-(b)
	relForwardRe := regexp.MustCompile(`^\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*-\s*\[:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*->\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$`)
	if m := relForwardRe.FindStringSubmatch(clause); len(m) == 4 {
		start := nodes[m[1]]
		end := nodes[m[3]]
		if start == nil || end == nil {
			return false
		}
		outEdges, err := e.storage.GetOutgoingEdges(start.ID)
		if err != nil {
			return false
		}
		for _, edge := range outEdges {
			if edge != nil && edge.Type == m[2] && edge.EndNode == end.ID {
				return true
			}
		}
		return false
	}
	relReverseRe := regexp.MustCompile(`^\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*<-\s*\[:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*-\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$`)
	if m := relReverseRe.FindStringSubmatch(clause); len(m) == 4 {
		start := nodes[m[3]]
		end := nodes[m[1]]
		if start == nil || end == nil {
			return false
		}
		outEdges, err := e.storage.GetOutgoingEdges(start.ID)
		if err != nil {
			return false
		}
		for _, edge := range outEdges {
			if edge != nil && edge.Type == m[2] && edge.EndNode == end.ID {
				return true
			}
		}
		return false
	}

	if predicate, ok := e.getCompiledBindingWhereIfSupported(ctx, clause); ok {
		return predicate(binding(nodes), getParamsFromContext(ctx))
	}

	// If this clause references exactly one bound variable, route through
	// evaluateWhere to preserve semantics like NOT (n)-[:TYPE]->().
	referenced := ""
	for varName := range nodes {
		if strings.Contains(clause, "("+varName+")") ||
			strings.Contains(clause, "("+varName+":") ||
			strings.Contains(clause, varName+".") ||
			strings.HasPrefix(clause, varName+")") ||
			strings.HasPrefix(clause, varName+":") {
			if referenced != "" && referenced != varName {
				referenced = "__multi__"
				break
			}
			referenced = varName
		}
	}
	if referenced != "" && referenced != "__multi__" {
		if node := nodes[referenced]; node != nil {
			return e.evaluateWhere(ctx, node, referenced, clause)
		}
	}

	// Fallback: parse/evaluate as expression with full node context.
	result := e.evaluateExpressionWithContext(ctx, clause, nodes, nil)
	if b, ok := result.(bool); ok {
		return b
	}
	return false
}

// executeCreate handles CREATE queries.
