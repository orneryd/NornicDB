// CREATE clause implementation for NornicDB.
// This file contains CREATE execution for nodes and relationships.

package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeCreate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Parse CREATE pattern
	pattern := cypher[6:] // Skip "CREATE"

	// Use word boundary detection to avoid matching substrings
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx > 0 {
		pattern = cypher[6:returnIdx]
	}
	pattern = strings.TrimSpace(pattern)

	// Check for relationship pattern: (a)-[r:TYPE]->(b)
	if strings.Contains(pattern, "->") || strings.Contains(pattern, "<-") || strings.Contains(pattern, "-[") {
		return e.executeCreateRelationship(ctx, cypher, pattern, returnIdx)
	}

	// Handle multiple node patterns: CREATE (a:Person), (b:Company)
	// Split by comma but respect parentheses
	nodePatterns := e.splitNodePatterns(pattern)
	createdNodes := make(map[string]*storage.Node)

	for _, nodePatternStr := range nodePatterns {
		nodePatternStr = strings.TrimSpace(nodePatternStr)
		if nodePatternStr == "" {
			continue
		}

		nodePattern := e.parseNodePattern(nodePatternStr)

		// Create the node
		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     nodePattern.labels,
			Properties: nodePattern.properties,
		}

		if err := e.storage.CreateNode(node); err != nil {
			return nil, fmt.Errorf("failed to create node: %w", err)
		}

		result.Stats.NodesCreated++

		if nodePattern.variable != "" {
			createdNodes[nodePattern.variable] = node
		}
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(cypher[returnIdx+6:])
		returnItems := e.parseReturnItems(returnPart)

		result.Columns = make([]string, len(returnItems))
		row := make([]interface{}, len(returnItems))

		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}

			// Find the matching node for this return item
			for variable, node := range createdNodes {
				if strings.HasPrefix(item.expr, variable) || item.expr == variable {
					row[i] = e.resolveReturnItem(item, variable, node)
					break
				}
			}
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil
}

// splitNodePatterns splits a CREATE pattern into individual node patterns
func (e *StorageExecutor) splitNodePatterns(pattern string) []string {
	var patterns []string
	var current strings.Builder
	depth := 0

	for _, c := range pattern {
		switch c {
		case '(':
			depth++
			current.WriteRune(c)
		case ')':
			depth--
			current.WriteRune(c)
			if depth == 0 {
				patterns = append(patterns, current.String())
				current.Reset()
			}
		case ',':
			if depth == 0 {
				// Skip comma between patterns
				continue
			}
			current.WriteRune(c)
		default:
			if depth > 0 {
				current.WriteRune(c)
			}
		}
	}

	// Handle any remaining content
	if current.Len() > 0 {
		patterns = append(patterns, current.String())
	}

	return patterns
}

// executeCreateRelationship handles CREATE with relationships.
func (e *StorageExecutor) executeCreateRelationship(ctx context.Context, cypher, pattern string, returnIdx int) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Parse relationship pattern: (a:Label {props})-[r:TYPE {props}]->(b:Label {props})
	// Simplified parsing - assumes format (a)-[r:TYPE]->(b)
	relPattern := regexp.MustCompile(`\(([^)]*)\)\s*-\[([^\]]*)\]->\s*\(([^)]*)\)`)
	matches := relPattern.FindStringSubmatch(pattern)

	if len(matches) < 4 {
		// Try other direction
		relPattern = regexp.MustCompile(`\(([^)]*)\)\s*<-\[([^\]]*)\]-\s*\(([^)]*)\)`)
		matches = relPattern.FindStringSubmatch(pattern)
	}

	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid relationship pattern")
	}

	// Parse source node
	sourcePattern := e.parseNodePattern("(" + matches[1] + ")")
	sourceNode := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     sourcePattern.labels,
		Properties: sourcePattern.properties,
	}
	if err := e.storage.CreateNode(sourceNode); err != nil {
		return nil, fmt.Errorf("failed to create source node: %w", err)
	}
	result.Stats.NodesCreated++

	// Parse target node
	targetPattern := e.parseNodePattern("(" + matches[3] + ")")
	targetNode := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     targetPattern.labels,
		Properties: targetPattern.properties,
	}
	if err := e.storage.CreateNode(targetNode); err != nil {
		return nil, fmt.Errorf("failed to create target node: %w", err)
	}
	result.Stats.NodesCreated++

	// Parse relationship
	relPart := matches[2]
	relType := "RELATED_TO"
	if colonIdx := strings.Index(relPart, ":"); colonIdx >= 0 {
		relType = strings.TrimSpace(relPart[colonIdx+1:])
		// Remove any properties from type
		if braceIdx := strings.Index(relType, "{"); braceIdx >= 0 {
			relType = strings.TrimSpace(relType[:braceIdx])
		}
	}

	// Create relationship
	edge := &storage.Edge{
		ID:         storage.EdgeID(e.generateID()),
		StartNode:  sourceNode.ID,
		EndNode:    targetNode.ID,
		Type:       relType,
		Properties: make(map[string]interface{}),
	}
	if err := e.storage.CreateEdge(edge); err != nil {
		return nil, fmt.Errorf("failed to create relationship: %w", err)
	}
	result.Stats.RelationshipsCreated++

	// Handle RETURN
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(cypher[returnIdx+6:])
		returnItems := e.parseReturnItems(returnPart)

		result.Columns = make([]string, len(returnItems))
		row := make([]interface{}, len(returnItems))

		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
			// Resolve based on variable name
			switch {
			case strings.HasPrefix(item.expr, sourcePattern.variable):
				row[i] = e.resolveReturnItem(item, sourcePattern.variable, sourceNode)
			case strings.HasPrefix(item.expr, targetPattern.variable):
				row[i] = e.resolveReturnItem(item, targetPattern.variable, targetNode)
			default:
				row[i] = e.resolveReturnItem(item, sourcePattern.variable, sourceNode)
			}
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil
}

// executeCompoundMatchCreate handles MATCH ... CREATE queries.
// This creates relationships between nodes that were matched by the MATCH clause.
//
// Example:
//
//	MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
//	CREATE (a)-[:KNOWS]->(b)
//
// The key difference from simple CREATE is that (a) and (b) reference
// EXISTING nodes from the MATCH, rather than creating new nodes.
func (e *StorageExecutor) executeCompoundMatchCreate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Split into MATCH and CREATE parts
	// Use findKeywordIndex to handle whitespace (space, newline, tab) before CREATE
	createIdx := findKeywordIndex(cypher, "CREATE")
	if createIdx < 0 {
		return nil, fmt.Errorf("invalid MATCH...CREATE query: no CREATE clause found")
	}

	matchPart := strings.TrimSpace(cypher[:createIdx])
	createPart := strings.TrimSpace(cypher[createIdx+6:]) // Skip "CREATE" (6 chars)

	// Find RETURN clause if present
	returnIdx := strings.Index(strings.ToUpper(createPart), "RETURN")
	var returnPart string
	if returnIdx > 0 {
		returnPart = strings.TrimSpace(createPart[returnIdx+6:])
		createPart = strings.TrimSpace(createPart[:returnIdx])
	}

	// Parse all node patterns from MATCH clauses
	// Handle: MATCH (a), (b)  OR  MATCH (a) MATCH (b)
	nodeVars := make(map[string]*storage.Node)

	// Split by MATCH keyword to handle multiple MATCH clauses
	// e.g., "MATCH (a) MATCH (b)" -> ["(a)", "(b)"]
	matchRe := regexp.MustCompile(`(?i)\bMATCH\s+`)
	matchClauses := matchRe.Split(matchPart, -1)

	var allPatterns []string
	for _, clause := range matchClauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}

		// Handle WHERE clause if present
		if whereIdx := strings.Index(strings.ToUpper(clause), " WHERE "); whereIdx > 0 {
			clause = clause[:whereIdx]
		}

		// Split by comma but respect parentheses
		patterns := e.splitNodePatterns(clause)
		allPatterns = append(allPatterns, patterns...)
	}

	nodePatterns := allPatterns

	for _, pattern := range nodePatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}

		nodeInfo := e.parseNodePattern(pattern)
		if nodeInfo.variable == "" {
			continue
		}

		// Find matching node
		var candidates []*storage.Node
		if len(nodeInfo.labels) > 0 {
			candidates, _ = e.storage.GetNodesByLabel(nodeInfo.labels[0])
		} else {
			candidates, _ = e.storage.AllNodes()
		}

		// Filter by properties - need to match ALL properties
		found := false
		for _, node := range candidates {
			if e.nodeMatchesProps(node, nodeInfo.properties) {
				nodeVars[nodeInfo.variable] = node
				found = true
				break // Take first match
			}
		}

		// If node not found by properties, try matching by id property specifically
		if !found && len(nodeInfo.properties) > 0 {
			if idVal, hasID := nodeInfo.properties["id"]; hasID {
				// Try exact match on id property
				for _, node := range candidates {
					if nodeID, ok := node.Properties["id"]; ok {
						// Compare as strings for reliability
						if fmt.Sprintf("%v", nodeID) == fmt.Sprintf("%v", idVal) {
							nodeVars[nodeInfo.variable] = node
							found = true
							break
						}
					}
				}
			}
		}
	}

	// Parse the CREATE pattern for relationship
	// Pattern: (varA)-[r:TYPE]->(varB) or (varA)-[:TYPE]->(varB)
	relPattern := regexp.MustCompile(`\((\w+)\)\s*-\[(\w*):?(\w+)\]->\s*\((\w+)\)`)
	matches := relPattern.FindStringSubmatch(createPart)

	if matches == nil {
		// Try with left arrow
		relPattern = regexp.MustCompile(`\((\w+)\)\s*<-\[(\w*):?(\w+)\]-\s*\((\w+)\)`)
		matches = relPattern.FindStringSubmatch(createPart)
	}

	if matches == nil {
		// Try undirected
		relPattern = regexp.MustCompile(`\((\w+)\)\s*-\[(\w*):?(\w+)\]-\s*\((\w+)\)`)
		matches = relPattern.FindStringSubmatch(createPart)
	}

	if len(matches) < 5 {
		return nil, fmt.Errorf("invalid relationship pattern in CREATE: %s", createPart)
	}

	sourceVar := matches[1]
	// relVar := matches[2]  // Optional relationship variable
	relType := matches[3]
	targetVar := matches[4]

	// Look up the source and target nodes from matched variables
	sourceNode, sourceExists := nodeVars[sourceVar]
	targetNode, targetExists := nodeVars[targetVar]

	if !sourceExists {
		// Provide detailed error for debugging
		return nil, fmt.Errorf("variable '%s' not found in MATCH results (have: %v). Patterns processed: %v",
			sourceVar, getKeys(nodeVars), allPatterns)
	}
	if !targetExists {
		// Provide detailed error for debugging
		return nil, fmt.Errorf("variable '%s' not found in MATCH results (have: %v). Patterns processed: %v",
			targetVar, getKeys(nodeVars), allPatterns)
	}

	// Create the relationship
	edge := &storage.Edge{
		ID:         storage.EdgeID(e.generateID()),
		StartNode:  sourceNode.ID,
		EndNode:    targetNode.ID,
		Type:       relType,
		Properties: make(map[string]interface{}),
	}

	if err := e.storage.CreateEdge(edge); err != nil {
		return nil, fmt.Errorf("failed to create relationship: %w", err)
	}
	result.Stats.RelationshipsCreated++

	// Handle RETURN clause
	if returnPart != "" {
		returnItems := e.parseReturnItems(returnPart)
		result.Columns = make([]string, len(returnItems))
		row := make([]interface{}, len(returnItems))

		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}

			// Resolve based on variable
			switch {
			case strings.HasPrefix(item.expr, sourceVar):
				row[i] = e.resolveReturnItem(item, sourceVar, sourceNode)
			case strings.HasPrefix(item.expr, targetVar):
				row[i] = e.resolveReturnItem(item, targetVar, targetNode)
			}
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil
}

// getKeys returns the keys of a map as a slice
func getKeys(m map[string]*storage.Node) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// extractVariablesFromMatch extracts variable names from a MATCH pattern
func (e *StorageExecutor) extractVariablesFromMatch(matchPart string) map[string]bool {
	vars := make(map[string]bool)

	// Match node patterns: (varName:Label) or (varName)
	nodePattern := regexp.MustCompile(`\((\w+)(?::\w+)?`)
	matches := nodePattern.FindAllStringSubmatch(matchPart, -1)

	for _, m := range matches {
		if len(m) > 1 && m[1] != "" {
			vars[m[1]] = true
		}
	}

	return vars
}

// executeMerge handles MERGE queries with ON CREATE SET / ON MATCH SET support.
// This implements Neo4j-compatible MERGE semantics:
// 1. Try to find an existing node matching the pattern
// 2. If found, apply ON MATCH SET if present
// 3. If not found, create the node and apply ON CREATE SET if present
