// MERGE clause implementation for NornicDB.
// This file contains MERGE execution, compound queries, and context-aware operations.

package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeMerge(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Extract the main MERGE pattern - use word boundary detection
	mergeIdx := findKeywordIndex(cypher, "MERGE")
	if mergeIdx == -1 {
		return nil, fmt.Errorf("MERGE clause not found")
	}

	// Find ON CREATE SET, ON MATCH SET, standalone SET, and RETURN clauses
	// Use word boundary detection to avoid matching substrings
	onCreateIdx := findKeywordIndex(cypher, "ON CREATE SET")
	onMatchIdx := findKeywordIndex(cypher, "ON MATCH SET")
	returnIdx := findKeywordIndex(cypher, "RETURN")
	withIdx := findKeywordIndex(cypher, "WITH")

	// Find standalone SET clause (after ON CREATE SET / ON MATCH SET)
	// Must handle SET preceded by space, tab, or newline
	setIdx := -1
	searchStart := 0
	if onCreateIdx > 0 {
		searchStart = onCreateIdx + 13 // After "ON CREATE SET"
	}
	if onMatchIdx > 0 && onMatchIdx > searchStart {
		searchStart = onMatchIdx + 12 // After "ON MATCH SET"
	}

	// Helper function to find SET with any whitespace before it
	findStandaloneSet := func(s string, start int) int {
		upperS := strings.ToUpper(s)
		for i := start; i <= len(upperS)-3; i++ {
			if strings.HasPrefix(upperS[i:], "SET") {
				// Check for whitespace before SET
				if i > 0 {
					prevChar := upperS[i-1]
					if prevChar != ' ' && prevChar != '\n' && prevChar != '\t' && prevChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Check for whitespace/end after SET
				endPos := i + 3
				if endPos < len(upperS) {
					nextChar := upperS[endPos]
					if nextChar != ' ' && nextChar != '\n' && nextChar != '\t' && nextChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Make sure this isn't part of ON CREATE SET or ON MATCH SET
				if i >= 10 && strings.HasPrefix(upperS[i-10:], "ON CREATE ") {
					continue
				}
				if i >= 9 && strings.HasPrefix(upperS[i-9:], "ON MATCH ") {
					continue
				}
				return i
			}
		}
		return -1
	}

	if searchStart > 0 {
		setIdx = findStandaloneSet(cypher, searchStart)
	} else {
		setIdx = findStandaloneSet(cypher, 0)
	}

	// Determine where the MERGE pattern ends
	patternEnd := len(cypher)
	for _, idx := range []int{onCreateIdx, onMatchIdx, setIdx, returnIdx} {
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}

	// Extract MERGE pattern (e.g., "(n:Label {prop: value})")
	mergePattern := strings.TrimSpace(cypher[mergeIdx+5 : patternEnd])

	// Parse the pattern to extract labels and properties for matching
	// Note: Parameters ($param) should already be substituted by substituteParams()
	varName, labels, matchProps, err := e.parseMergePattern(mergePattern)

	// If pattern contains unsubstituted params (like $path), handle gracefully
	if strings.Contains(mergePattern, "$") {
		// Extract what we can from the pattern
		varName = e.extractVarName(mergePattern)
		labels = e.extractLabels(mergePattern)
		matchProps = make(map[string]interface{})
		err = nil // Continue with partial info
	}

	if err != nil || (len(labels) == 0 && len(matchProps) == 0) {
		// If we truly can't parse, create a basic node
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("node-%d", e.idCounter())),
			Labels:     labels,
			Properties: matchProps,
		}
		e.storage.CreateNode(node)
		result.Stats.NodesCreated = 1

		if varName == "" {
			varName = "n"
		}
		result.Columns = []string{varName}
		result.Rows = append(result.Rows, []interface{}{e.nodeToMap(node)})
		return result, nil
	}

	// Try to find existing node
	var existingNode *storage.Node
	if len(labels) > 0 && len(matchProps) > 0 {
		// Search for node with matching label and properties
		nodes, _ := e.storage.GetNodesByLabel(labels[0])
		for _, n := range nodes {
			matches := true
			for key, val := range matchProps {
				if nodeVal, ok := n.Properties[key]; !ok || nodeVal != val {
					matches = false
					break
				}
			}
			if matches {
				existingNode = n
				break
			}
		}
	}

	var node *storage.Node
	if existingNode != nil {
		// Node exists - apply ON MATCH SET if present
		node = existingNode
		if onMatchIdx > 0 {
			setEnd := len(cypher)
			for _, idx := range []int{onCreateIdx, returnIdx} {
				if idx > onMatchIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onMatchIdx+13 : setEnd])
			e.applySetToNode(node, varName, setClause)
			e.storage.UpdateNode(node)
		}
	} else {
		// Node doesn't exist - create it
		node = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("node-%d", e.idCounter())),
			Labels:     labels,
			Properties: matchProps,
		}
		e.storage.CreateNode(node)
		result.Stats.NodesCreated = 1

		// Apply ON CREATE SET if present
		if onCreateIdx > 0 {
			setEnd := len(cypher)
			// Stop at: standalone SET, ON MATCH SET, WITH, or RETURN
			for _, idx := range []int{setIdx, onMatchIdx, withIdx, returnIdx} {
				if idx > onCreateIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onCreateIdx+13 : setEnd])
			e.applySetToNode(node, varName, setClause)
		}
	}

	// Apply standalone SET clause (runs for both create and match)
	if setIdx > 0 {
		setEnd := len(cypher)
		for _, idx := range []int{withIdx, returnIdx} {
			if idx > setIdx && idx < setEnd {
				setEnd = idx
			}
		}
		setClause := strings.TrimSpace(cypher[setIdx+3 : setEnd]) // +3 to skip "SET"
		e.applySetToNode(node, varName, setClause)
	}

	// Persist updates
	if existingNode != nil || setIdx > 0 || onCreateIdx > 0 {
		e.storage.UpdateNode(node)
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClause(returnClause, varName, node)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

// executeCompoundMatchMerge handles MATCH ... MERGE ... queries where MERGE references matched nodes.
// This is the Neo4j pattern: MATCH (a) ... MERGE (b) ... SET b.prop = a.prop, etc.
func (e *StorageExecutor) executeCompoundMatchMerge(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Use word boundary detection to avoid matching substrings
	matchIdx := findKeywordIndex(cypher, "MATCH")
	mergeIdx := findKeywordIndex(cypher, "MERGE")

	// If MERGE appears at the start, find the second one (after MATCH)
	if mergeIdx <= matchIdx && mergeIdx != -1 {
		// Search for MERGE after MATCH
		afterMatch := cypher[matchIdx+5:]
		secondMergeIdx := findKeywordIndex(afterMatch, "MERGE")
		if secondMergeIdx != -1 {
			mergeIdx = matchIdx + 5 + secondMergeIdx
		}
	}

	if matchIdx == -1 || mergeIdx == -1 {
		return nil, fmt.Errorf("invalid MATCH ... MERGE query")
	}

	// Extract MATCH clause
	matchClause := strings.TrimSpace(cypher[matchIdx:mergeIdx])
	mergeClause := strings.TrimSpace(cypher[mergeIdx:])

	// Execute MATCH to get context
	matchedNodes, matchedRels, err := e.executeMatchForContext(ctx, matchClause)
	if err != nil {
		return nil, fmt.Errorf("failed to execute MATCH: %v", err)
	}

	// If no matches found and not OPTIONAL MATCH, return empty
	if len(matchedNodes) == 0 && findKeywordIndex(cypher, "OPTIONAL MATCH") == -1 {
		return result, nil
	}

	// For each set of matched nodes, execute the MERGE with context
	for _, nodeContext := range matchedNodes {
		mergeResult, err := e.executeMergeWithContext(ctx, mergeClause, nodeContext, matchedRels)
		if err != nil {
			return nil, err
		}

		// Combine results
		if mergeResult.Stats != nil {
			result.Stats.NodesCreated += mergeResult.Stats.NodesCreated
			result.Stats.RelationshipsCreated += mergeResult.Stats.RelationshipsCreated
			result.Stats.PropertiesSet += mergeResult.Stats.PropertiesSet
		}

		// Add rows from merge result
		if len(mergeResult.Columns) > 0 && len(result.Columns) == 0 {
			result.Columns = mergeResult.Columns
		}
		result.Rows = append(result.Rows, mergeResult.Rows...)
	}

	// If no matched nodes but had OPTIONAL MATCH, still try to execute MERGE
	if len(matchedNodes) == 0 {
		mergeResult, err := e.executeMergeWithContext(ctx, mergeClause, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		if err != nil {
			return nil, err
		}
		result = mergeResult
	}

	return result, nil
}

// executeMatchForContext executes a MATCH clause and returns matched nodes by variable name.
// Handles multiple node patterns like (a:Label {prop: val}), (b:Label2 {prop: val2})
func (e *StorageExecutor) executeMatchForContext(ctx context.Context, matchClause string) ([]map[string]*storage.Node, map[string]*storage.Edge, error) {
	relMatches := make(map[string]*storage.Edge)

	upper := strings.ToUpper(matchClause)

	// Find WHERE clause if present
	whereIdx := strings.Index(upper, " WHERE ")
	var wherePart string
	var patternPart string

	if whereIdx > 0 {
		patternPart = matchClause[5:whereIdx]
		wherePart = matchClause[whereIdx+7:]
	} else {
		patternPart = matchClause[5:]
	}

	patternPart = strings.TrimSpace(patternPart)

	// Split multiple node patterns: (a:Label), (b:Label2)
	nodePatterns := e.splitNodePatterns(patternPart)

	// If no patterns found, try parsing as single pattern
	if len(nodePatterns) == 0 {
		nodePatterns = []string{patternPart}
	}

	// For each node pattern, find matching nodes
	patternMatches := make([]struct {
		variable string
		nodes    []*storage.Node
	}, len(nodePatterns))

	for i, np := range nodePatterns {
		nodeInfo := e.parseNodePattern(np)

		var candidates []*storage.Node
		if len(nodeInfo.labels) > 0 {
			candidates, _ = e.storage.GetNodesByLabel(nodeInfo.labels[0])
		} else {
			candidates = e.storage.GetAllNodes()
		}

		// Filter by properties
		var filtered []*storage.Node
		for _, node := range candidates {
			if e.nodeMatchesProps(node, nodeInfo.properties) {
				filtered = append(filtered, node)
			}
		}

		patternMatches[i] = struct {
			variable string
			nodes    []*storage.Node
		}{
			variable: nodeInfo.variable,
			nodes:    filtered,
		}
	}

	// Build cartesian product of all pattern matches
	allMatches := e.buildCartesianProduct(patternMatches)

	// Apply WHERE clause to each combination
	if wherePart != "" {
		var filtered []map[string]*storage.Node
		for _, nodeMap := range allMatches {
			matches := true
			for varName, node := range nodeMap {
				if !e.evaluateWhere(node, varName, wherePart) {
					// Check if WHERE references this variable (property access, function call, or direct reference)
					lowerWhere := strings.ToLower(wherePart)
					refsVar := strings.Contains(wherePart, varName+".") ||
						strings.Contains(wherePart, varName+" ") ||
						strings.Contains(lowerWhere, "id("+varName+")") ||
						strings.Contains(lowerWhere, "elementid("+varName+")")
					if refsVar {
						matches = false
						break
					}
				}
			}
			if matches {
				filtered = append(filtered, nodeMap)
			}
		}
		allMatches = filtered
	}

	return allMatches, relMatches, nil
}

// buildCartesianProduct creates all combinations of node matches
func (e *StorageExecutor) buildCartesianProduct(patternMatches []struct {
	variable string
	nodes    []*storage.Node
}) []map[string]*storage.Node {
	if len(patternMatches) == 0 {
		return nil
	}

	// Start with first pattern's nodes
	var result []map[string]*storage.Node
	for _, node := range patternMatches[0].nodes {
		result = append(result, map[string]*storage.Node{
			patternMatches[0].variable: node,
		})
	}

	// For each subsequent pattern, expand the combinations
	for i := 1; i < len(patternMatches); i++ {
		pm := patternMatches[i]
		var expanded []map[string]*storage.Node

		for _, existing := range result {
			for _, node := range pm.nodes {
				// Copy existing map and add new variable
				newMap := make(map[string]*storage.Node)
				for k, v := range existing {
					newMap[k] = v
				}
				newMap[pm.variable] = node
				expanded = append(expanded, newMap)
			}
		}

		result = expanded
	}

	return result
}

// executeMergeWithContext executes a MERGE clause with context from a prior MATCH.
func (e *StorageExecutor) executeMergeWithContext(ctx context.Context, cypher string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Find clauses - use word boundary detection
	mergeIdx := findKeywordIndex(cypher, "MERGE")
	if mergeIdx == -1 {
		mergeIdx = 0 // Already stripped
	}

	onCreateIdx := findKeywordIndex(cypher, "ON CREATE SET")
	onMatchIdx := findKeywordIndex(cypher, "ON MATCH SET")
	// Use quote-aware search for RETURN and WITH since text content may contain these keywords
	returnIdx := findKeywordIndexInContext(cypher, "RETURN")
	withIdx := findKeywordIndexInContext(cypher, "WITH")

	// Find standalone SET (not ON CREATE/MATCH SET)
	// Must handle SET preceded by space, tab, or newline
	setIdx := -1
	searchStart := 0
	if onCreateIdx > 0 {
		searchStart = onCreateIdx + 13
	}
	if onMatchIdx > 0 && onMatchIdx > searchStart {
		searchStart = onMatchIdx + 12
	}

	// Helper function to find SET with any whitespace before it (reuse from executeMerge)
	findStandaloneSetInContext := func(s string, start int) int {
		upperS := strings.ToUpper(s)
		for i := start; i <= len(upperS)-3; i++ {
			if strings.HasPrefix(upperS[i:], "SET") {
				// Check for whitespace before SET
				if i > 0 {
					prevChar := upperS[i-1]
					if prevChar != ' ' && prevChar != '\n' && prevChar != '\t' && prevChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Check for whitespace/end after SET
				endPos := i + 3
				if endPos < len(upperS) {
					nextChar := upperS[endPos]
					if nextChar != ' ' && nextChar != '\n' && nextChar != '\t' && nextChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Make sure this isn't part of ON CREATE SET or ON MATCH SET
				if i >= 10 && strings.HasPrefix(upperS[i-10:], "ON CREATE ") {
					continue
				}
				if i >= 9 && strings.HasPrefix(upperS[i-9:], "ON MATCH ") {
					continue
				}
				return i
			}
		}
		return -1
	}

	if searchStart > 0 {
		setIdx = findStandaloneSetInContext(cypher, searchStart)
	} else {
		setIdx = findStandaloneSetInContext(cypher, 0)
	}

	// Find MERGE pattern end
	patternEnd := len(cypher)
	for _, idx := range []int{onCreateIdx, onMatchIdx, setIdx, returnIdx, withIdx} {
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}

	// Handle second MERGE in compound query (handle any whitespace before MERGE)
	// Use quote-aware search since text content may contain "MERGE" keyword
	secondMergeIdx := findKeywordIndexInContext(cypher[mergeIdx+5:], "MERGE")
	if secondMergeIdx > 0 {
		// There's a second MERGE clause - this is for relationships
		// Handle the first MERGE, then process second
		firstMergeEnd := mergeIdx + 5 + secondMergeIdx
		if firstMergeEnd < patternEnd {
			patternEnd = firstMergeEnd
		}
	}

	// Extract and parse MERGE pattern
	mergePattern := strings.TrimSpace(cypher[mergeIdx+5 : patternEnd])

	// Check if this is a relationship pattern: (a)-[r:TYPE]->(b)
	if strings.Contains(mergePattern, "->") || strings.Contains(mergePattern, "<-") || strings.Contains(mergePattern, "]-") {
		// Relationship MERGE - need to create relationship between nodes
		return e.executeMergeRelationshipWithContext(ctx, cypher, mergePattern, nodeContext, relContext)
	}

	// Parse node pattern
	varName, labels, matchProps, err := e.parseMergePattern(mergePattern)
	if err != nil || varName == "" {
		varName = e.extractVarName(mergePattern)
		labels = e.extractLabels(mergePattern)
		matchProps = make(map[string]interface{})
	}

	// Try to find existing node
	var existingNode *storage.Node
	if len(labels) > 0 && len(matchProps) > 0 {
		nodes, _ := e.storage.GetNodesByLabel(labels[0])
		for _, n := range nodes {
			matches := true
			for key, val := range matchProps {
				if nodeVal, ok := n.Properties[key]; !ok || !e.compareEqual(nodeVal, val) {
					matches = false
					break
				}
			}
			if matches {
				existingNode = n
				break
			}
		}
	}

	var node *storage.Node
	if existingNode != nil {
		node = existingNode
		if onMatchIdx > 0 {
			setEnd := len(cypher)
			for _, idx := range []int{onCreateIdx, returnIdx, withIdx, setIdx} {
				if idx > onMatchIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onMatchIdx+13 : setEnd])
			e.applySetToNodeWithContext(node, varName, setClause, nodeContext, relContext)
			e.storage.UpdateNode(node)
		}
	} else {
		node = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("node-%d", e.idCounter())),
			Labels:     labels,
			Properties: matchProps,
		}
		e.storage.CreateNode(node)
		result.Stats.NodesCreated = 1

		if onCreateIdx > 0 {
			setEnd := len(cypher)
			for _, idx := range []int{setIdx, onMatchIdx, withIdx, returnIdx} {
				if idx > onCreateIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onCreateIdx+13 : setEnd])
			e.applySetToNodeWithContext(node, varName, setClause, nodeContext, relContext)
		}
	}

	// Apply standalone SET
	if setIdx > 0 {
		setEnd := len(cypher)
		// Also check for second MERGE - SET clause ends there too
		secondMergeAbsIdx := -1
		if secondMergeIdx > 0 {
			secondMergeAbsIdx = mergeIdx + 5 + secondMergeIdx
		}
		for _, idx := range []int{withIdx, returnIdx, secondMergeAbsIdx} {
			if idx > setIdx && idx < setEnd {
				setEnd = idx
			}
		}
		setClause := strings.TrimSpace(cypher[setIdx+3 : setEnd])
		e.applySetToNodeWithContext(node, varName, setClause, nodeContext, relContext)
	}

	// Save updates
	e.storage.UpdateNode(node)

	// Add this node to context for subsequent MERGEs
	nodeContext[varName] = node

	// Handle second MERGE (usually relationship creation)
	if secondMergeIdx > 0 {
		secondMergePart := strings.TrimSpace(cypher[mergeIdx+5+secondMergeIdx+1:])
		_, err := e.executeMergeWithContext(ctx, secondMergePart, nodeContext, relContext)
		if err != nil {
			return nil, err
		}
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClauseWithContext(returnClause, nodeContext, relContext)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

// executeMergeRelationshipWithContext handles MERGE for relationship patterns.
func (e *StorageExecutor) executeMergeRelationshipWithContext(ctx context.Context, cypher string, pattern string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Use word boundary detection
	returnIdx := findKeywordIndex(cypher, "RETURN")

	// Parse relationship pattern: (a)-[r:TYPE {props}]->(b)
	// Extract start node, relationship, end node

	// Find the relationship part
	relStart := strings.Index(pattern, "[")
	relEnd := strings.Index(pattern, "]")

	if relStart == -1 || relEnd == -1 {
		return result, nil // Not a valid relationship pattern
	}

	// Get start and end node variables
	startPart := strings.TrimSpace(pattern[:relStart])
	endPart := strings.TrimSpace(pattern[relEnd+1:])
	relPart := pattern[relStart+1 : relEnd]

	// Remove direction markers and parens
	startPart = strings.Trim(startPart, "()-")
	endPart = strings.Trim(endPart, "()<>-")

	// Extract start/end variable names
	startVar := strings.Split(startPart, ":")[0]
	endVar := strings.Split(endPart, ":")[0]

	// Parse relationship type and variable
	relVar := ""
	relType := ""
	relProps := make(map[string]interface{})

	relPart = strings.TrimSpace(relPart)
	propsStart := strings.Index(relPart, "{")
	if propsStart > 0 {
		propsEnd := strings.LastIndex(relPart, "}")
		if propsEnd > propsStart {
			relProps = e.parseProperties(relPart[propsStart : propsEnd+1])
		}
		relPart = relPart[:propsStart]
	}

	relParts := strings.Split(relPart, ":")
	if len(relParts) > 0 {
		relVar = strings.TrimSpace(relParts[0])
	}
	if len(relParts) > 1 {
		relType = strings.TrimSpace(relParts[1])
	}

	// Get start and end nodes from context
	startNode := nodeContext[startVar]
	endNode := nodeContext[endVar]

	if startNode == nil || endNode == nil {
		// Nodes not in context - can't create relationship
		return result, nil
	}

	// Check if relationship exists
	existingEdge := e.storage.GetEdgeBetween(startNode.ID, endNode.ID, relType)

	var edge *storage.Edge
	if existingEdge != nil {
		edge = existingEdge
	} else {
		// Create new relationship
		edge = &storage.Edge{
			ID:         storage.EdgeID(fmt.Sprintf("edge-%d", e.idCounter())),
			Type:       relType,
			StartNode:  startNode.ID,
			EndNode:    endNode.ID,
			Properties: relProps,
		}
		err := e.storage.CreateEdge(edge)
		if err != nil {
			// If already exists error, ignore it (MERGE semantics)
			if err == storage.ErrAlreadyExists {
				// Try to find the existing edge again
				existingEdge = e.storage.GetEdgeBetween(startNode.ID, endNode.ID, relType)
				if existingEdge != nil {
					edge = existingEdge
				}
			} else {
				return nil, fmt.Errorf("failed to create relationship: %w", err)
			}
		} else {
			result.Stats.RelationshipsCreated = 1
		}
	}

	// Store in context
	if relVar != "" {
		relContext[relVar] = edge
	}

	// Handle RETURN
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClauseWithContext(returnClause, nodeContext, relContext)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

// applySetToNodeWithContext applies SET clauses with access to matched context.
func (e *StorageExecutor) applySetToNodeWithContext(node *storage.Node, varName string, setClause string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) {
	// Add current node to context for self-references
	fullContext := make(map[string]*storage.Node)
	for k, v := range nodeContext {
		fullContext[k] = v
	}
	fullContext[varName] = node

	// Split SET clause into individual assignments
	assignments := e.splitSetAssignments(setClause)

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if !strings.HasPrefix(assignment, varName+".") {
			continue
		}

		eqIdx := strings.Index(assignment, "=")
		if eqIdx <= 0 {
			continue
		}

		propName := strings.TrimSpace(assignment[len(varName)+1 : eqIdx])
		propValue := strings.TrimSpace(assignment[eqIdx+1:])

		// Evaluate expression with full context
		setNodeProperty(node, propName, e.evaluateSetExpressionWithContext(propValue, fullContext, relContext))
	}
}

// evaluateSetExpressionWithContext evaluates SET clause expressions with context.
func (e *StorageExecutor) evaluateSetExpressionWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	return e.evaluateExpressionWithContext(expr, nodes, rels)
}

// parseReturnClauseWithContext parses RETURN with context from MATCH.
func (e *StorageExecutor) parseReturnClauseWithContext(returnClause string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) ([]string, []interface{}) {
	// Handle RETURN *
	if strings.TrimSpace(returnClause) == "*" {
		var columns []string
		var values []interface{}
		for name, node := range nodes {
			columns = append(columns, name)
			values = append(values, e.nodeToMap(node))
		}
		return columns, values
	}

	var columns []string
	var values []interface{}

	parts := e.splitReturnExpressions(returnClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var expr, alias string
		asIdx := strings.LastIndex(strings.ToUpper(part), " AS ")
		if asIdx > 0 {
			expr = strings.TrimSpace(part[:asIdx])
			alias = strings.TrimSpace(part[asIdx+4:])
		} else {
			expr = part
			alias = e.expressionToAlias(expr)
		}

		value := e.evaluateExpressionWithContext(expr, nodes, rels)
		columns = append(columns, alias)
		values = append(values, value)
	}

	return columns, values
}

// parseReturnClause parses RETURN expressions and evaluates them against a node.
// Supports: n.prop, n.prop AS alias, id(n), *, literal values
func (e *StorageExecutor) parseReturnClause(returnClause string, varName string, node *storage.Node) ([]string, []interface{}) {
	// Handle RETURN *
	if strings.TrimSpace(returnClause) == "*" {
		return []string{varName}, []interface{}{e.nodeToMap(node)}
	}

	var columns []string
	var values []interface{}

	// Split by comma, but be careful with nested expressions
	parts := e.splitReturnExpressions(returnClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Check for AS alias
		var expr, alias string
		asIdx := strings.LastIndex(strings.ToUpper(part), " AS ")
		if asIdx > 0 {
			expr = strings.TrimSpace(part[:asIdx])
			alias = strings.TrimSpace(part[asIdx+4:])
		} else {
			expr = part
			// Generate alias from expression
			alias = e.expressionToAlias(expr)
		}

		// Evaluate expression
		value := e.evaluateExpression(expr, varName, node)
		columns = append(columns, alias)
		values = append(values, value)
	}

	return columns, values
}

// splitReturnExpressions splits RETURN clause by commas, respecting parentheses.
func (e *StorageExecutor) splitReturnExpressions(clause string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, ch := range clause {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// expressionToAlias converts an expression to a column alias.
func (e *StorageExecutor) expressionToAlias(expr string) string {
	expr = strings.TrimSpace(expr)

	// Function call: id(n) -> id(n)
	if strings.Contains(expr, "(") {
		return expr
	}

	// Property access: n.prop -> prop
	if dotIdx := strings.LastIndex(expr, "."); dotIdx > 0 {
		return expr[dotIdx+1:]
	}

	return expr
}

// parseMergePattern parses a MERGE pattern like "(n:Label {prop: value})"
