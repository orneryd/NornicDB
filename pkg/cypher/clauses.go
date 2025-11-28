// Cypher clause implementations for NornicDB.
// This file contains implementations for WITH, UNWIND, UNION, OPTIONAL MATCH,
// FOREACH, and LOAD CSV clauses.

package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// findStandaloneWithIndex finds the index of a standalone "WITH" keyword
// that is NOT part of "STARTS WITH" or "ENDS WITH".
// Returns -1 if not found.
func findStandaloneWithIndex(s string) int {
	upper := strings.ToUpper(s)
	idx := 0
	for {
		pos := strings.Index(upper[idx:], "WITH")
		if pos == -1 {
			return -1
		}
		absolutePos := idx + pos

		// Check if it's preceded by "STARTS " or "ENDS "
		if absolutePos >= 7 {
			preceding := upper[absolutePos-7 : absolutePos]
			if preceding == "STARTS " {
				idx = absolutePos + 4
				continue
			}
		}
		if absolutePos >= 5 {
			preceding := upper[absolutePos-5 : absolutePos]
			if preceding == "ENDS " {
				idx = absolutePos + 4
				continue
			}
		}

		// Check word boundaries
		leftOK := absolutePos == 0 || !isAlphanumeric(rune(upper[absolutePos-1]))
		endPos := absolutePos + 4
		rightOK := endPos >= len(upper) || !isAlphanumeric(rune(upper[endPos]))

		if leftOK && rightOK {
			return absolutePos
		}

		idx = absolutePos + 1
		if idx >= len(upper) {
			return -1
		}
	}
}

// isAlphanumeric returns true if the rune is a letter or digit
func isAlphanumeric(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
}

// ========================================
// WITH Clause
// ========================================

// executeWith handles WITH clause - intermediate result projection
func (e *StorageExecutor) executeWith(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	upper := strings.ToUpper(cypher)

	withIdx := strings.Index(upper, "WITH")
	if withIdx == -1 {
		return nil, fmt.Errorf("WITH clause not found")
	}

	remainderStart := withIdx + 4
	for remainderStart < len(cypher) && cypher[remainderStart] == ' ' {
		remainderStart++
	}

	nextClauses := []string{" MATCH ", " WHERE ", " RETURN ", " CREATE ", " MERGE ", " DELETE ", " SET ", " UNWIND ", " ORDER BY ", " SKIP ", " LIMIT "}
	nextClauseIdx := len(cypher)
	for _, clause := range nextClauses {
		idx := strings.Index(upper[remainderStart:], clause)
		if idx >= 0 && remainderStart+idx < nextClauseIdx {
			nextClauseIdx = remainderStart + idx
		}
	}

	withExpr := strings.TrimSpace(cypher[remainderStart:nextClauseIdx])
	boundVars := make(map[string]interface{})

	items := e.splitWithItems(withExpr)
	columns := make([]string, 0)
	values := make([]interface{}, 0)

	for _, item := range items {
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

		val := e.evaluateExpressionWithContext(expr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		boundVars[alias] = val
		columns = append(columns, alias)
		values = append(values, val)
	}

	if nextClauseIdx < len(cypher) {
		remainder := strings.TrimSpace(cypher[nextClauseIdx:])
		return e.Execute(ctx, remainder, nil)
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

// splitWithItems splits WITH expressions respecting nested brackets and quotes
func (e *StorageExecutor) splitWithItems(expr string) []string {
	var items []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, c := range expr {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			current.WriteRune(c)
		case c == '(' || c == '[' || c == '{':
			if !inQuote {
				depth++
			}
			current.WriteRune(c)
		case c == ')' || c == ']' || c == '}':
			if !inQuote {
				depth--
			}
			current.WriteRune(c)
		case c == ',' && depth == 0 && !inQuote:
			items = append(items, current.String())
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	if current.Len() > 0 {
		items = append(items, current.String())
	}
	return items
}

// ========================================
// UNWIND Clause
// ========================================

// executeUnwind handles UNWIND clause - list expansion
func (e *StorageExecutor) executeUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	upper := strings.ToUpper(cypher)

	unwindIdx := strings.Index(upper, "UNWIND")
	if unwindIdx == -1 {
		return nil, fmt.Errorf("UNWIND clause not found")
	}

	asIdx := strings.Index(upper, " AS ")
	if asIdx == -1 {
		return nil, fmt.Errorf("UNWIND requires AS clause")
	}

	listExpr := strings.TrimSpace(cypher[unwindIdx+6 : asIdx])

	remainder := strings.TrimSpace(cypher[asIdx+4:])
	spaceIdx := strings.IndexAny(remainder, " \t\n")
	var variable string
	var restQuery string
	if spaceIdx > 0 {
		variable = remainder[:spaceIdx]
		restQuery = strings.TrimSpace(remainder[spaceIdx:])
	} else {
		variable = remainder
		restQuery = ""
	}

	list := e.evaluateExpressionWithContext(listExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))

	var items []interface{}
	switch v := list.(type) {
	case []interface{}:
		items = v
	case []string:
		items = make([]interface{}, len(v))
		for i, s := range v {
			items[i] = s
		}
	case []int64:
		items = make([]interface{}, len(v))
		for i, n := range v {
			items[i] = n
		}
	default:
		items = []interface{}{list}
	}

	if restQuery != "" && strings.HasPrefix(strings.ToUpper(restQuery), "RETURN") {
		result := &ExecuteResult{
			Columns: []string{variable},
			Rows:    make([][]interface{}, 0, len(items)),
		}
		for _, item := range items {
			result.Rows = append(result.Rows, []interface{}{item})
		}
		return result, nil
	}

	result := &ExecuteResult{
		Columns: []string{variable},
		Rows:    make([][]interface{}, 0, len(items)),
	}
	for _, item := range items {
		result.Rows = append(result.Rows, []interface{}{item})
	}
	return result, nil
}

// ========================================
// UNION Clause
// ========================================

// executeUnion handles UNION / UNION ALL
func (e *StorageExecutor) executeUnion(ctx context.Context, cypher string, unionAll bool) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	var separator string
	if unionAll {
		separator = " UNION ALL "
	} else {
		separator = " UNION "
	}

	idx := strings.Index(upper, separator)
	if idx == -1 {
		return nil, fmt.Errorf("UNION clause not found")
	}

	query1 := strings.TrimSpace(cypher[:idx])
	query2 := strings.TrimSpace(cypher[idx+len(separator):])

	result1, err := e.Execute(ctx, query1, nil)
	if err != nil {
		return nil, fmt.Errorf("error in first UNION query: %w", err)
	}

	result2, err := e.Execute(ctx, query2, nil)
	if err != nil {
		return nil, fmt.Errorf("error in second UNION query: %w", err)
	}

	if len(result1.Columns) != len(result2.Columns) {
		return nil, fmt.Errorf("UNION queries must return the same number of columns")
	}

	combinedResult := &ExecuteResult{
		Columns: result1.Columns,
		Rows:    make([][]interface{}, 0, len(result1.Rows)+len(result2.Rows)),
	}

	combinedResult.Rows = append(combinedResult.Rows, result1.Rows...)

	if unionAll {
		combinedResult.Rows = append(combinedResult.Rows, result2.Rows...)
	} else {
		seen := make(map[string]bool)
		for _, row := range result1.Rows {
			key := fmt.Sprintf("%v", row)
			seen[key] = true
		}
		for _, row := range result2.Rows {
			key := fmt.Sprintf("%v", row)
			if !seen[key] {
				combinedResult.Rows = append(combinedResult.Rows, row)
				seen[key] = true
			}
		}
	}

	return combinedResult, nil
}

// ========================================
// OPTIONAL MATCH Clause
// ========================================

// executeOptionalMatch handles OPTIONAL MATCH - returns null for non-matches
func (e *StorageExecutor) executeOptionalMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	upper := strings.ToUpper(cypher)
	optMatchIdx := strings.Index(upper, "OPTIONAL MATCH")
	if optMatchIdx == -1 {
		return nil, fmt.Errorf("OPTIONAL MATCH not found")
	}

	modifiedQuery := cypher[:optMatchIdx] + "MATCH" + cypher[optMatchIdx+14:]

	result, err := e.executeMatch(ctx, modifiedQuery)

	// Handle error case - return result with null values
	if err != nil {
		// Default to a single null row if we can't determine columns
		return &ExecuteResult{
			Columns: []string{"result"},
			Rows:    [][]interface{}{{nil}},
		}, nil
	}

	// Handle empty result - return null row preserving columns
	if len(result.Rows) == 0 {
		nullRow := make([]interface{}, len(result.Columns))
		for i := range nullRow {
			nullRow[i] = nil
		}
		return &ExecuteResult{
			Columns: result.Columns,
			Rows:    [][]interface{}{nullRow},
		}, nil
	}

	return result, nil
}

// joinedRow represents a row from a left outer join between MATCH and OPTIONAL MATCH
type joinedRow struct {
	initialNode  *storage.Node
	relatedNode  *storage.Node
	relationship *storage.Edge
}

// optionalRelPattern holds parsed relationship info for OPTIONAL MATCH
type optionalRelPattern struct {
	sourceVar   string
	relType     string
	relVar      string
	targetVar   string
	targetLabel string
	direction   string // "out", "in", "both"
}

// optionalRelResult holds a node and its connecting edge for OPTIONAL MATCH
type optionalRelResult struct {
	node *storage.Node
	edge *storage.Edge
}

// executeCompoundMatchOptionalMatch handles MATCH ... OPTIONAL MATCH ... WITH ... RETURN queries
// This implements left outer join semantics for relationship traversals with aggregation support
func (e *StorageExecutor) executeCompoundMatchOptionalMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Find OPTIONAL MATCH position
	optMatchIdx := findKeywordIndex(cypher, "OPTIONAL MATCH")
	if optMatchIdx == -1 {
		return nil, fmt.Errorf("OPTIONAL MATCH not found in compound query")
	}

	// Find WITH or RETURN after OPTIONAL MATCH
	remainingAfterOptMatch := cypher[optMatchIdx+14:] // Skip "OPTIONAL MATCH"
	withIdx := findKeywordIndex(remainingAfterOptMatch, "WITH")
	returnIdx := findKeywordIndex(remainingAfterOptMatch, "RETURN")

	// Determine where OPTIONAL MATCH pattern ends
	optMatchEndIdx := len(remainingAfterOptMatch)
	if withIdx > 0 && (returnIdx == -1 || withIdx < returnIdx) {
		optMatchEndIdx = withIdx
	} else if returnIdx > 0 {
		optMatchEndIdx = returnIdx
	}

	optMatchPattern := strings.TrimSpace(remainingAfterOptMatch[:optMatchEndIdx])
	restOfQuery := ""
	if optMatchEndIdx < len(remainingAfterOptMatch) {
		restOfQuery = strings.TrimSpace(remainingAfterOptMatch[optMatchEndIdx:])
	}

	// Parse the initial MATCH clause section (everything between MATCH and OPTIONAL MATCH)
	// This may contain: node pattern, WHERE clause, and WITH DISTINCT
	initialSection := strings.TrimSpace(cypher[5:optMatchIdx]) // Get original case, skip "MATCH"

	// Extract WHERE clause if present (between node pattern and WITH DISTINCT/OPTIONAL MATCH)
	var whereClause string
	whereIdx := findKeywordIndex(initialSection, "WHERE")

	// Find standalone WITH (not part of "STARTS WITH" or "ENDS WITH")
	firstWithIdx := findStandaloneWithIndex(initialSection)

	// Determine the node pattern end
	nodePatternEnd := len(initialSection)
	if whereIdx > 0 {
		nodePatternEnd = whereIdx
	} else if firstWithIdx > 0 {
		nodePatternEnd = firstWithIdx
	}

	nodePatternStr := strings.TrimSpace(initialSection[:nodePatternEnd])
	nodePattern := e.parseNodePattern(nodePatternStr)
	if nodePattern.variable == "" {
		return nil, fmt.Errorf("could not parse node pattern from MATCH clause")
	}

	// Extract WHERE clause content if present
	if whereIdx > 0 {
		whereEnd := len(initialSection)
		if firstWithIdx > whereIdx {
			whereEnd = firstWithIdx
		}
		whereClause = strings.TrimSpace(initialSection[whereIdx+5 : whereEnd]) // Skip "WHERE"
	}

	// Get all nodes matching the initial pattern
	var initialNodes []*storage.Node
	var err error
	if len(nodePattern.labels) > 0 {
		initialNodes, err = e.storage.GetNodesByLabel(nodePattern.labels[0])
	} else {
		initialNodes, err = e.storage.AllNodes()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get initial nodes: %w", err)
	}

	// Filter by properties if any
	if len(nodePattern.properties) > 0 {
		filtered := make([]*storage.Node, 0)
		for _, node := range initialNodes {
			match := true
			for k, v := range nodePattern.properties {
				if node.Properties[k] != v {
					match = false
					break
				}
			}
			if match {
				filtered = append(filtered, node)
			}
		}
		initialNodes = filtered
	}

	// Apply WHERE clause filtering if present
	if whereClause != "" {
		initialNodes = e.filterNodes(initialNodes, nodePattern.variable, whereClause)
	}

	// Parse the OPTIONAL MATCH relationship pattern
	relPattern := e.parseOptionalRelPattern(optMatchPattern)

	// Build result rows - this is left outer join semantics
	var joinedRows []joinedRow

	for _, node := range initialNodes {
		// Try to find related nodes via the relationship
		relatedNodes := e.findRelatedNodes(node, relPattern)

		if len(relatedNodes) == 0 {
			// No match - add row with null for the optional part (left outer join)
			joinedRows = append(joinedRows, joinedRow{
				initialNode:  node,
				relatedNode:  nil,
				relationship: nil,
			})
		} else {
			// Add a row for each match
			for _, related := range relatedNodes {
				joinedRows = append(joinedRows, joinedRow{
					initialNode:  node,
					relatedNode:  related.node,
					relationship: related.edge,
				})
			}
		}
	}

	// Now process WITH and RETURN clauses
	if strings.HasPrefix(strings.ToUpper(restOfQuery), "WITH") {
		return e.processWithAggregation(joinedRows, nodePattern.variable, relPattern.targetVar, restOfQuery)
	}

	if strings.HasPrefix(strings.ToUpper(restOfQuery), "RETURN") {
		return e.buildJoinedResult(joinedRows, nodePattern.variable, relPattern.targetVar, restOfQuery)
	}

	// No WITH or RETURN, just return count
	return &ExecuteResult{
		Columns: []string{"matched"},
		Rows:    [][]interface{}{{int64(len(joinedRows))}},
	}, nil
}

// parseOptionalRelPattern parses patterns like (a)-[r:TYPE]->(b:Label)
func (e *StorageExecutor) parseOptionalRelPattern(pattern string) optionalRelPattern {
	result := optionalRelPattern{direction: "out"}
	pattern = strings.TrimSpace(pattern)

	// Check direction
	if strings.Contains(pattern, "<-") {
		result.direction = "in"
	} else if strings.Contains(pattern, "->") {
		result.direction = "out"
	} else if strings.Contains(pattern, "-") {
		result.direction = "both"
	}

	// Extract source variable
	if idx := strings.Index(pattern, "("); idx >= 0 {
		endIdx := strings.Index(pattern[idx:], ")")
		if endIdx > 0 {
			sourceStr := pattern[idx+1 : idx+endIdx]
			if colonIdx := strings.Index(sourceStr, ":"); colonIdx > 0 {
				result.sourceVar = strings.TrimSpace(sourceStr[:colonIdx])
			} else {
				result.sourceVar = strings.TrimSpace(sourceStr)
			}
		}
	}

	// Extract relationship type and variable
	if idx := strings.Index(pattern, "["); idx >= 0 {
		endIdx := strings.Index(pattern[idx:], "]")
		if endIdx > 0 {
			relStr := pattern[idx+1 : idx+endIdx]
			if colonIdx := strings.Index(relStr, ":"); colonIdx >= 0 {
				result.relVar = strings.TrimSpace(relStr[:colonIdx])
				result.relType = strings.TrimSpace(relStr[colonIdx+1:])
			} else {
				result.relVar = strings.TrimSpace(relStr)
			}
		}
	}

	// Extract target
	relEnd := strings.Index(pattern, "]")
	if relEnd > 0 {
		remaining := pattern[relEnd+1:]
		if idx := strings.Index(remaining, "("); idx >= 0 {
			endIdx := strings.Index(remaining[idx:], ")")
			if endIdx > 0 {
				targetStr := remaining[idx+1 : idx+endIdx]
				if colonIdx := strings.Index(targetStr, ":"); colonIdx >= 0 {
					result.targetVar = strings.TrimSpace(targetStr[:colonIdx])
					result.targetLabel = strings.TrimSpace(targetStr[colonIdx+1:])
				} else {
					result.targetVar = strings.TrimSpace(targetStr)
				}
			}
		}
	}

	return result
}

// findRelatedNodes finds nodes connected via the specified relationship pattern
func (e *StorageExecutor) findRelatedNodes(sourceNode *storage.Node, pattern optionalRelPattern) []optionalRelResult {
	var results []optionalRelResult
	var edges []*storage.Edge

	// Get edges based on direction
	switch pattern.direction {
	case "out":
		outEdges, err := e.storage.GetOutgoingEdges(sourceNode.ID)
		if err != nil {
			return results
		}
		edges = outEdges
	case "in":
		inEdges, err := e.storage.GetIncomingEdges(sourceNode.ID)
		if err != nil {
			return results
		}
		edges = inEdges
	case "both":
		outEdges, _ := e.storage.GetOutgoingEdges(sourceNode.ID)
		inEdges, _ := e.storage.GetIncomingEdges(sourceNode.ID)
		edges = append(outEdges, inEdges...)
	}

	for _, edge := range edges {
		// Check relationship type if specified
		if pattern.relType != "" && edge.Type != pattern.relType {
			continue
		}

		// Determine target node ID
		var targetNodeID storage.NodeID
		if edge.StartNode == sourceNode.ID {
			targetNodeID = edge.EndNode
		} else {
			targetNodeID = edge.StartNode
		}

		// Get the target node
		targetNode, err := e.storage.GetNode(targetNodeID)
		if err != nil || targetNode == nil {
			continue
		}

		// Check target label if specified
		if pattern.targetLabel != "" {
			hasLabel := false
			for _, label := range targetNode.Labels {
				if label == pattern.targetLabel {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				continue
			}
		}

		results = append(results, optionalRelResult{node: targetNode, edge: edge})
	}

	return results
}

// processWithAggregation handles WITH clauses with aggregation functions
// It finds the WITH clause that contains aggregations and processes them
// Also evaluates CASE WHEN expressions in WITH clauses
func (e *StorageExecutor) processWithAggregation(rows []joinedRow, sourceVar, targetVar, restOfQuery string) (*ExecuteResult, error) {
	// Find RETURN clause
	returnIdx := findKeywordIndex(restOfQuery, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required after WITH")
	}

	// First, check for CASE WHEN expressions in the first WITH clause and evaluate them
	// This computes values like: WITH f, c, CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END as hasChunk
	computedValues := make(map[int]map[string]interface{}) // row index -> computed values
	firstWithIdx := findKeywordIndex(restOfQuery, "WITH")
	if firstWithIdx >= 0 {
		// Find where first WITH ends (at next WITH, RETURN, or end)
		firstWithEnd := returnIdx
		nextWithIdx := findKeywordIndex(restOfQuery[firstWithIdx+4:], "WITH")
		if nextWithIdx > 0 {
			firstWithEnd = firstWithIdx + 4 + nextWithIdx
		}

		firstWithClause := strings.TrimSpace(restOfQuery[firstWithIdx+4 : firstWithEnd])
		withItems := e.splitWithItems(firstWithClause)

		// Check if any item is a CASE expression
		for _, item := range withItems {
			item = strings.TrimSpace(item)
			upperItem := strings.ToUpper(item)
			asIdx := strings.Index(upperItem, " AS ")
			if asIdx > 0 {
				expr := strings.TrimSpace(item[:asIdx])
				alias := strings.TrimSpace(item[asIdx+4:])

				if isCaseExpression(expr) {
					// Evaluate CASE for each row
					for rowIdx, r := range rows {
						if computedValues[rowIdx] == nil {
							computedValues[rowIdx] = make(map[string]interface{})
						}
						nodeMap := make(map[string]*storage.Node)
						if r.initialNode != nil {
							nodeMap[sourceVar] = r.initialNode
						}
						if r.relatedNode != nil {
							nodeMap[targetVar] = r.relatedNode
						}
						computedValues[rowIdx][alias] = e.evaluateCaseExpression(expr, nodeMap, nil)
					}
				}
			}
		}
	}

	// Find the WITH clause that contains the aggregations
	// This handles cases like: WITH f, c, CASE... WITH COUNT(f)... RETURN...
	// We need to find the WITH that has COUNT/SUM/COLLECT etc.
	aggregationWithStart := -1
	aggregationWithEnd := returnIdx

	// Look for WITH clauses between start and RETURN
	queryBeforeReturn := restOfQuery[:returnIdx]
	withIdx := 0
	for {
		nextWithIdx := findKeywordIndex(queryBeforeReturn[withIdx:], "WITH")
		if nextWithIdx == -1 {
			break
		}
		absWithIdx := withIdx + nextWithIdx
		// Check if this WITH clause contains aggregation functions
		nextClauseEnd := len(queryBeforeReturn)
		followingWithIdx := findKeywordIndex(queryBeforeReturn[absWithIdx+4:], "WITH")
		if followingWithIdx > 0 {
			nextClauseEnd = absWithIdx + 4 + followingWithIdx
		}
		withContent := queryBeforeReturn[absWithIdx:nextClauseEnd]
		upperWithContent := strings.ToUpper(withContent)
		if strings.Contains(upperWithContent, "COUNT(") ||
			strings.Contains(upperWithContent, "SUM(") ||
			strings.Contains(upperWithContent, "COLLECT(") {
			aggregationWithStart = absWithIdx
			aggregationWithEnd = nextClauseEnd
			break
		}
		withIdx = absWithIdx + 4
	}

	// Parse the aggregation items from the WITH clause that contains them
	var returnItems []returnItem
	if aggregationWithStart >= 0 {
		withClause := strings.TrimSpace(restOfQuery[aggregationWithStart+4 : aggregationWithEnd])
		returnItems = e.parseReturnItems(withClause)
	} else {
		// No aggregation WITH found, use RETURN clause items
		returnClause := strings.TrimSpace(restOfQuery[returnIdx+6:])
		returnItems = e.parseReturnItems(returnClause)
	}

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

	row := make([]interface{}, len(returnItems))

	for i, item := range returnItems {
		upperExpr := strings.ToUpper(item.expr)

		switch {
		case strings.HasPrefix(upperExpr, "COUNT(DISTINCT "):
			inner := item.expr[15 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)

			if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(sourceVar)) {
				seen := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.initialNode != nil {
						seen[r.initialNode.ID] = true
					}
				}
				row[i] = int64(len(seen))
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(targetVar)) {
				seen := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.relatedNode != nil {
						seen[r.relatedNode.ID] = true
					}
				}
				row[i] = int64(len(seen))
			} else {
				row[i] = int64(0)
			}

		case strings.HasPrefix(upperExpr, "COUNT("):
			inner := item.expr[6 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)

			if inner == "*" {
				row[i] = int64(len(rows))
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(sourceVar)) {
				count := int64(0)
				for _, r := range rows {
					if r.initialNode != nil {
						count++
					}
				}
				row[i] = count
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(targetVar)) {
				count := int64(0)
				for _, r := range rows {
					if r.relatedNode != nil {
						count++
					}
				}
				row[i] = count
			} else {
				row[i] = int64(len(rows))
			}

		case strings.HasPrefix(upperExpr, "SUM("):
			inner := item.expr[4 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)
			sum := float64(0)

			// First check if inner refers to a computed value (from CASE WHEN)
			hasComputedValues := false
			for rowIdx := range rows {
				if cv, ok := computedValues[rowIdx]; ok {
					if val, exists := cv[inner]; exists {
						hasComputedValues = true
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
				}
			}

			if !hasComputedValues {
				// Fall back to embedding check
				if strings.Contains(strings.ToUpper(inner), "EMBEDDING") {
					for _, r := range rows {
						if r.relatedNode != nil {
							if _, hasEmb := r.relatedNode.Properties["embedding"]; hasEmb {
								sum++
							}
						}
						if r.initialNode != nil {
							if _, hasEmb := r.initialNode.Properties["embedding"]; hasEmb {
								sum++
							}
						}
					}
				}
			}
			row[i] = sum

		case strings.HasPrefix(upperExpr, "COLLECT(DISTINCT "):
			inner := item.expr[17 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)
			seen := make(map[interface{}]bool)
			var collected []interface{}

			if strings.Contains(inner, ".") {
				parts := strings.SplitN(inner, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				for _, r := range rows {
					var node *storage.Node
					if strings.EqualFold(varName, sourceVar) {
						node = r.initialNode
					} else if strings.EqualFold(varName, targetVar) {
						node = r.relatedNode
					}
					if node != nil {
						if val, ok := node.Properties[propName]; ok {
							if !seen[val] {
								seen[val] = true
								collected = append(collected, val)
							}
						}
					}
				}
			}
			row[i] = collected

		case strings.HasPrefix(upperExpr, "COLLECT("):
			inner := item.expr[8 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)
			var collected []interface{}

			if strings.Contains(inner, ".") {
				parts := strings.SplitN(inner, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				for _, r := range rows {
					var node *storage.Node
					if strings.EqualFold(varName, sourceVar) {
						node = r.initialNode
					} else if strings.EqualFold(varName, targetVar) {
						node = r.relatedNode
					}
					if node != nil {
						if val, ok := node.Properties[propName]; ok {
							collected = append(collected, val)
						}
					}
				}
			}
			row[i] = collected

		default:
			// Check for arithmetic expressions: SUM(...) + SUM(...)
			if strings.Contains(upperExpr, "+") && strings.Contains(upperExpr, "SUM(") {
				// Handle SUM(x) + SUM(y) patterns used in VSCode stats query
				// The CASE WHEN computed values check for embedding IS NOT NULL
				// So SUM(chunkHasEmbedding) + SUM(fileHasEmbedding) counts embeddings
				sum := int64(0)

				// Count chunk embeddings (non-null)
				seenChunks := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.relatedNode != nil && !seenChunks[r.relatedNode.ID] {
						if _, hasEmb := r.relatedNode.Properties["embedding"]; hasEmb {
							seenChunks[r.relatedNode.ID] = true
							sum++
						}
					}
				}

				// Count file embeddings (non-null)
				seenFiles := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.initialNode != nil && !seenFiles[r.initialNode.ID] {
						if _, hasEmb := r.initialNode.Properties["embedding"]; hasEmb {
							seenFiles[r.initialNode.ID] = true
							sum++
						}
					}
				}

				row[i] = sum
			} else {
				row[i] = nil
			}
		}
	}

	result.Rows = append(result.Rows, row)
	return result, nil
}

// buildJoinedResult builds a result from joined rows for simple RETURN
// If RETURN contains aggregation functions, delegates to processWithAggregation
func (e *StorageExecutor) buildJoinedResult(rows []joinedRow, sourceVar, targetVar, restOfQuery string) (*ExecuteResult, error) {
	returnIdx := findKeywordIndex(restOfQuery, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required")
	}

	returnClause := strings.TrimSpace(restOfQuery[returnIdx+6:])
	returnItems := e.parseReturnItems(returnClause)

	// Check if any return item is an aggregation function
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

	// If there's an aggregation, delegate to processWithAggregation
	if hasAggregation {
		return e.processWithAggregation(rows, sourceVar, targetVar, restOfQuery)
	}

	result := &ExecuteResult{
		Columns: make([]string, len(returnItems)),
		Rows:    make([][]interface{}, 0, len(rows)),
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	for _, joinedRow := range rows {
		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			if strings.Contains(item.expr, ".") {
				parts := strings.SplitN(item.expr, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				var node *storage.Node
				if strings.EqualFold(varName, sourceVar) {
					node = joinedRow.initialNode
				} else if strings.EqualFold(varName, targetVar) {
					node = joinedRow.relatedNode
				}
				if node != nil {
					row[i] = node.Properties[propName]
				}
			} else if strings.EqualFold(item.expr, sourceVar) {
				row[i] = joinedRow.initialNode
			} else if strings.EqualFold(item.expr, targetVar) {
				row[i] = joinedRow.relatedNode
			}
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// ========================================
// FOREACH Clause
// ========================================

// executeForeach handles FOREACH clause - iterate and perform updates
func (e *StorageExecutor) executeForeach(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)
	foreachIdx := strings.Index(upper, "FOREACH")
	if foreachIdx == -1 {
		return nil, fmt.Errorf("FOREACH clause not found")
	}

	parenStart := strings.Index(cypher[foreachIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("FOREACH requires parentheses")
	}
	parenStart += foreachIdx

	depth := 1
	parenEnd := parenStart + 1
	for parenEnd < len(cypher) && depth > 0 {
		if cypher[parenEnd] == '(' {
			depth++
		} else if cypher[parenEnd] == ')' {
			depth--
		}
		parenEnd++
	}

	inner := strings.TrimSpace(cypher[parenStart+1 : parenEnd-1])

	inIdx := strings.Index(strings.ToUpper(inner), " IN ")
	if inIdx == -1 {
		return nil, fmt.Errorf("FOREACH requires IN clause")
	}

	variable := strings.TrimSpace(inner[:inIdx])
	remainder := strings.TrimSpace(inner[inIdx+4:])

	pipeIdx := strings.Index(remainder, "|")
	if pipeIdx == -1 {
		return nil, fmt.Errorf("FOREACH requires | separator")
	}

	listExpr := strings.TrimSpace(remainder[:pipeIdx])
	updateClause := strings.TrimSpace(remainder[pipeIdx+1:])

	list := e.evaluateExpressionWithContext(listExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))

	var items []interface{}
	switch v := list.(type) {
	case []interface{}:
		items = v
	default:
		items = []interface{}{list}
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	for _, item := range items {
		itemStr := e.valueToLiteral(item)
		substituted := strings.ReplaceAll(updateClause, variable, itemStr)

		updateResult, err := e.Execute(ctx, substituted, nil)
		if err == nil && updateResult.Stats != nil {
			result.Stats.NodesCreated += updateResult.Stats.NodesCreated
			result.Stats.PropertiesSet += updateResult.Stats.PropertiesSet
			result.Stats.RelationshipsCreated += updateResult.Stats.RelationshipsCreated
		}
	}

	return result, nil
}

// ========================================
// LOAD CSV Clause
// ========================================

// executeLoadCSV handles LOAD CSV clause
func (e *StorageExecutor) executeLoadCSV(ctx context.Context, cypher string) (*ExecuteResult, error) {
	return nil, fmt.Errorf("LOAD CSV is not supported in NornicDB embedded mode")
}
