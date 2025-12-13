// CREATE clause implementation for NornicDB.
// This file contains CREATE execution for nodes and relationships.

package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeCreate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

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

	// Split into individual patterns (nodes and relationships)
	allPatterns := e.splitCreatePatterns(pattern)

	// Separate node patterns from relationship patterns
	var nodePatterns []string
	var relPatterns []string
	for _, p := range allPatterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Use string-literal-aware checks to avoid matching arrows inside content strings
		// e.g., 'Data -> Output' should NOT be treated as a relationship
		if containsOutsideStrings(p, "->") || containsOutsideStrings(p, "<-") || containsOutsideStrings(p, "-[") {
			relPatterns = append(relPatterns, p)
		} else {
			nodePatterns = append(nodePatterns, p)
		}
	}

	// First, create all nodes
	createdNodes := make(map[string]*storage.Node)
	for _, nodePatternStr := range nodePatterns {
		nodePatternStr = strings.TrimSpace(nodePatternStr)
		if nodePatternStr == "" {
			continue
		}

		nodePattern := e.parseNodePattern(nodePatternStr)

		// Check for empty label (e.g., "n:" or ":") - only check before properties
		patternBeforeProps := nodePatternStr
		if braceIdx := strings.Index(nodePatternStr, "{"); braceIdx >= 0 {
			patternBeforeProps = nodePatternStr[:braceIdx]
		}
		// Check if there's a colon that doesn't have a label after it
		if strings.Contains(patternBeforeProps, ":") && len(nodePattern.labels) == 0 {
			return nil, fmt.Errorf("empty label name after colon in pattern: %s", nodePatternStr)
		}

		// SECURITY: Validate labels to prevent injection attacks
		for _, label := range nodePattern.labels {
			if !isValidIdentifier(label) {
				return nil, fmt.Errorf("invalid label name: %q (must be alphanumeric starting with letter or underscore)", label)
			}
			if containsReservedKeyword(label) {
				return nil, fmt.Errorf("invalid label name: %q (contains reserved keyword)", label)
			}
		}

		// SECURITY: Validate property keys and values
		for key, val := range nodePattern.properties {
			if !isValidIdentifier(key) {
				return nil, fmt.Errorf("invalid property key: %q (must be alphanumeric starting with letter or underscore)", key)
			}
			// Check for invalid property values (malformed syntax)
			if _, ok := val.(invalidPropertyValue); ok {
				return nil, fmt.Errorf("invalid property value for key %q: malformed syntax", key)
			}
		}

		// Create the node
		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     nodePattern.labels,
			Properties: nodePattern.properties,
		}

		if err := e.storage.CreateNode(node); err != nil {
			return nil, fmt.Errorf("failed to create node: %w", err)
		}
		e.notifyNodeCreated(string(node.ID))

		result.Stats.NodesCreated++

		if nodePattern.variable != "" {
			createdNodes[nodePattern.variable] = node
		}
	}

	// Then, create all relationships using variable references or inline node definitions
	for _, relPatternStr := range relPatterns {
		relPatternStr = strings.TrimSpace(relPatternStr)
		if relPatternStr == "" {
			continue
		}

		// Process relationship chains - keep going until no remainder
		currentPattern := relPatternStr
		for currentPattern != "" {
			// Parse the relationship pattern: (varA)-[:TYPE {props}]->(varB)
			sourceContent, relStr, targetContent, isReverse, remainder, err := e.parseCreateRelPatternWithVars(currentPattern)
			if err != nil {
				return nil, err
			}

			// Parse node patterns first to get variable names for lookup
			sourcePattern := e.parseNodePattern("(" + sourceContent + ")")
			targetPattern := e.parseNodePattern("(" + targetContent + ")")

			// Determine source node - either lookup by variable or create inline
			var sourceNode *storage.Node
			if sourcePattern.variable != "" {
				if node, exists := createdNodes[sourcePattern.variable]; exists {
					sourceNode = node
				}
			}
			if sourceNode == nil {
				// Create new node
				sourceNode = &storage.Node{
					ID:         storage.NodeID(e.generateID()),
					Labels:     sourcePattern.labels,
					Properties: sourcePattern.properties,
				}
				if err := e.storage.CreateNode(sourceNode); err != nil {
					return nil, fmt.Errorf("failed to create source node: %w", err)
				}
				e.notifyNodeCreated(string(sourceNode.ID))
				result.Stats.NodesCreated++
				if sourcePattern.variable != "" {
					createdNodes[sourcePattern.variable] = sourceNode
				}
			}

			// Determine target node - either lookup by variable or create inline
			var targetNode *storage.Node
			if targetPattern.variable != "" {
				if node, exists := createdNodes[targetPattern.variable]; exists {
					targetNode = node
				}
			}
			if targetNode == nil {
				// Create new node
				targetNode = &storage.Node{
					ID:         storage.NodeID(e.generateID()),
					Labels:     targetPattern.labels,
					Properties: targetPattern.properties,
				}
				if err := e.storage.CreateNode(targetNode); err != nil {
					return nil, fmt.Errorf("failed to create target node: %w", err)
				}
				e.notifyNodeCreated(string(targetNode.ID))
				result.Stats.NodesCreated++
				if targetPattern.variable != "" {
					createdNodes[targetPattern.variable] = targetNode
				}
			}

			// Parse relationship type and properties
			relType, relProps := e.parseRelationshipTypeAndProps(relStr)

			// SECURITY: Validate relationship type
			if relType != "" && !isValidIdentifier(relType) {
				return nil, fmt.Errorf("invalid relationship type: %q (must be alphanumeric starting with letter or underscore)", relType)
			}

			// SECURITY: Validate relationship property keys
			for key := range relProps {
				if !isValidIdentifier(key) {
					return nil, fmt.Errorf("invalid relationship property key: %q (must be alphanumeric starting with letter or underscore)", key)
				}
			}

			// Handle reverse direction
			startNode, endNode := sourceNode, targetNode
			if isReverse {
				startNode, endNode = targetNode, sourceNode
			}

			// Create relationship
			edge := &storage.Edge{
				ID:         storage.EdgeID(e.generateID()),
				StartNode:  startNode.ID,
				EndNode:    endNode.ID,
				Type:       relType,
				Properties: relProps,
			}
			if err := e.storage.CreateEdge(edge); err != nil {
				return nil, fmt.Errorf("failed to create relationship: %w", err)
			}
			result.Stats.RelationshipsCreated++

			// If there's more chain to process, continue with target as new source
			if remainder != "" && (strings.HasPrefix(remainder, "-[") || strings.HasPrefix(remainder, "<-[")) {
				// Build the next pattern: (targetContent) + remainder
				currentPattern = "(" + targetContent + ")" + remainder
			} else {
				currentPattern = ""
			}
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

// executeCreateWithRefs is like executeCreate but also returns the created nodes and edges maps.
// This is used by compound queries like CREATE...WITH...DELETE to avoid expensive O(n) scans
// when looking up the created entities.
func (e *StorageExecutor) executeCreateWithRefs(ctx context.Context, cypher string) (*ExecuteResult, map[string]*storage.Node, map[string]*storage.Edge, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

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

	// Split into individual patterns (nodes and relationships)
	allPatterns := e.splitCreatePatterns(pattern)

	// Separate node patterns from relationship patterns
	var nodePatterns []string
	var relPatterns []string
	for _, p := range allPatterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if containsOutsideStrings(p, "->") || containsOutsideStrings(p, "<-") || containsOutsideStrings(p, "-[") {
			relPatterns = append(relPatterns, p)
		} else {
			nodePatterns = append(nodePatterns, p)
		}
	}

	// First, create all nodes
	createdNodes := make(map[string]*storage.Node)
	createdEdges := make(map[string]*storage.Edge)

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
			return nil, nil, nil, fmt.Errorf("failed to create node: %w", err)
		}
		e.notifyNodeCreated(string(node.ID))

		result.Stats.NodesCreated++

		if nodePattern.variable != "" {
			createdNodes[nodePattern.variable] = node
		}
	}

	// Then, create all relationships using variable references or inline node definitions
	for _, relPatternStr := range relPatterns {
		relPatternStr = strings.TrimSpace(relPatternStr)
		if relPatternStr == "" {
			continue
		}

		// Process relationship chains - keep going until no remainder
		currentPattern := relPatternStr
		for currentPattern != "" {
			// Parse the relationship pattern: (varA)-[:TYPE {props}]->(varB)
			sourceContent, relStr, targetContent, isReverse, remainder, err := e.parseCreateRelPatternWithVars(currentPattern)
			if err != nil {
				return nil, nil, nil, err
			}

			// Parse node patterns first to get variable names for lookup
			sourcePattern := e.parseNodePattern("(" + sourceContent + ")")
			targetPattern := e.parseNodePattern("(" + targetContent + ")")

			// Determine source node - either lookup by variable or create inline
			var sourceNode *storage.Node
			if sourcePattern.variable != "" {
				if node, exists := createdNodes[sourcePattern.variable]; exists {
					sourceNode = node
				}
			}
			if sourceNode == nil {
				sourceNode = &storage.Node{
					ID:         storage.NodeID(e.generateID()),
					Labels:     sourcePattern.labels,
					Properties: sourcePattern.properties,
				}
				if err := e.storage.CreateNode(sourceNode); err != nil {
					return nil, nil, nil, fmt.Errorf("failed to create source node: %w", err)
				}
				e.notifyNodeCreated(string(sourceNode.ID))
				result.Stats.NodesCreated++
				if sourcePattern.variable != "" {
					createdNodes[sourcePattern.variable] = sourceNode
				}
			}

			// Determine target node - either lookup by variable or create inline
			var targetNode *storage.Node
			if targetPattern.variable != "" {
				if node, exists := createdNodes[targetPattern.variable]; exists {
					targetNode = node
				}
			}
			if targetNode == nil {
				targetNode = &storage.Node{
					ID:         storage.NodeID(e.generateID()),
					Labels:     targetPattern.labels,
					Properties: targetPattern.properties,
				}
				if err := e.storage.CreateNode(targetNode); err != nil {
					return nil, nil, nil, fmt.Errorf("failed to create target node: %w", err)
				}
				e.notifyNodeCreated(string(targetNode.ID))
				result.Stats.NodesCreated++
				if targetPattern.variable != "" {
					createdNodes[targetPattern.variable] = targetNode
				}
			}

			// Parse relationship type and properties
			relType, relProps := e.parseRelationshipTypeAndProps(relStr)

			// Extract relationship variable if present (e.g., "r:TYPE" -> "r")
			relVar := ""
			if colonIdx := strings.Index(relStr, ":"); colonIdx > 0 {
				relVar = strings.TrimSpace(relStr[:colonIdx])
			} else if !strings.Contains(relStr, "{") {
				// No colon and no props - entire string might be variable
				relVar = strings.TrimSpace(relStr)
			}

			// Handle direction
			var startNode, endNode *storage.Node
			if isReverse {
				startNode, endNode = targetNode, sourceNode
			} else {
				startNode, endNode = sourceNode, targetNode
			}

			// Create the relationship
			edge := &storage.Edge{
				ID:         storage.EdgeID(e.generateID()),
				Type:       relType,
				StartNode:  startNode.ID,
				EndNode:    endNode.ID,
				Properties: relProps,
			}

			if err := e.storage.CreateEdge(edge); err != nil {
				return nil, nil, nil, fmt.Errorf("failed to create relationship: %w", err)
			}

			if relVar != "" {
				createdEdges[relVar] = edge
			}
			result.Stats.RelationshipsCreated++

			// If there's more chain to process, continue with target as new source
			if remainder != "" && (strings.HasPrefix(remainder, "-[") || strings.HasPrefix(remainder, "<-[")) {
				currentPattern = "(" + targetContent + ")" + remainder
			} else {
				currentPattern = ""
			}
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

	return result, createdNodes, createdEdges, nil
}

// splitCreatePatterns splits a CREATE pattern into individual patterns (nodes and relationships)
// respecting parentheses depth, string literals, and handling relationship syntax.
// IMPORTANT: This properly handles content inside string literals (single/double quotes)
// so that Cypher-like content inside strings is not parsed as relationship patterns.
func (e *StorageExecutor) splitCreatePatterns(pattern string) []string {
	var patterns []string
	var current strings.Builder
	depth := 0
	inRelationship := false
	inString := false
	stringChar := byte(0) // Track which quote character started the string

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]

		// Handle string literal boundaries
		if (c == '\'' || c == '"') && (i == 0 || pattern[i-1] != '\\') {
			if !inString {
				// Starting a string literal
				inString = true
				stringChar = c
			} else if c == stringChar {
				// Ending the string literal (same quote type)
				inString = false
				stringChar = 0
			}
			current.WriteByte(c)
			continue
		}

		// If inside a string literal, add character without parsing
		if inString {
			current.WriteByte(c)
			continue
		}

		// Normal parsing outside string literals
		switch c {
		case '(':
			depth++
			current.WriteByte(c)
		case ')':
			depth--
			current.WriteByte(c)
			if depth == 0 {
				// Check if next non-whitespace is a relationship operator
				j := i + 1
				for j < len(pattern) && (pattern[j] == ' ' || pattern[j] == '\t' || pattern[j] == '\n' || pattern[j] == '\r') {
					j++
				}
				if j < len(pattern) && (pattern[j] == '-' || pattern[j] == '<') {
					// This is part of a relationship pattern, continue accumulating
					inRelationship = true
				} else if !inRelationship {
					// End of a standalone node pattern
					patterns = append(patterns, current.String())
					current.Reset()
				} else {
					// End of a relationship pattern
					patterns = append(patterns, current.String())
					current.Reset()
					inRelationship = false
				}
			}
		case ',':
			if depth == 0 && !inRelationship {
				// Skip comma between patterns
				continue
			}
			current.WriteByte(c)
		case ' ', '\t', '\n', '\r':
			if depth > 0 || inRelationship {
				// Only keep whitespace inside patterns
				current.WriteByte(c)
			}
		default:
			if depth > 0 || inRelationship || c == '-' || c == '<' || c == '[' || c == ']' || c == '>' || c == ':' {
				current.WriteByte(c)
				if c == '-' || c == '<' {
					inRelationship = true
				}
			}
		}
	}

	// Handle any remaining content
	if current.Len() > 0 {
		patterns = append(patterns, current.String())
	}

	return patterns
}

// parseCreateRelPatternWithVars parses patterns like (varA)-[r:TYPE {props}]->(varB)
// where varA and varB are variable references (not full node definitions)
// Returns: sourceVar, relContent, targetVar, isReverse, remainder, error
// remainder is any content after the target node (for chained patterns)
func (e *StorageExecutor) parseCreateRelPatternWithVars(pattern string) (string, string, string, bool, string, error) {
	pattern = strings.TrimSpace(pattern)

	// Find the first node: (varA)
	if !strings.HasPrefix(pattern, "(") {
		return "", "", "", false, "", fmt.Errorf("invalid relationship pattern: must start with (")
	}

	// Find end of first node
	depth := 0
	firstNodeEnd := -1
	for i, c := range pattern {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				firstNodeEnd = i
				break
			}
		}
	}
	if firstNodeEnd < 0 {
		return "", "", "", false, "", fmt.Errorf("invalid relationship pattern: unmatched parenthesis")
	}

	sourceVar := strings.TrimSpace(pattern[1:firstNodeEnd])
	rest := pattern[firstNodeEnd+1:]
	rest = strings.TrimSpace(rest) // Remove any whitespace before -[ or <-[

	// Detect direction and find relationship bracket
	isReverse := false
	var relStart int

	if strings.HasPrefix(rest, "-[") {
		relStart = 2 // Skip "-["
	} else if strings.HasPrefix(rest, "<-[") {
		isReverse = true
		relStart = 3 // Skip "<-["
	} else {
		return "", "", "", false, "", fmt.Errorf("invalid relationship pattern: expected -[ or <-[, got: %s", rest[:min(20, len(rest))])
	}

	// Find matching ] considering nested brackets in properties
	depth = 1
	relEnd := -1
	inQuote := false
	quoteChar := rune(0)
	for i := relStart; i < len(rest); i++ {
		c := rune(rest[i])
		if !inQuote {
			if c == '\'' || c == '"' {
				inQuote = true
				quoteChar = c
			} else if c == '[' {
				depth++
			} else if c == ']' {
				depth--
				if depth == 0 {
					relEnd = i
					break
				}
			}
		} else if c == quoteChar {
			if i > 0 && rest[i-1] != '\\' {
				inQuote = false
			}
		}
	}
	if relEnd < 0 {
		return "", "", "", false, "", fmt.Errorf("invalid relationship pattern: unmatched bracket")
	}

	relContent := rest[relStart:relEnd]
	afterRel := strings.TrimSpace(rest[relEnd+1:])

	// Now find the second node
	var secondNodeStart int
	if isReverse {
		if !strings.HasPrefix(afterRel, "-(") {
			return "", "", "", false, "", fmt.Errorf("invalid relationship pattern: expected -( after ]")
		}
		secondNodeStart = 2
	} else {
		if !strings.HasPrefix(afterRel, "->(") {
			return "", "", "", false, "", fmt.Errorf("invalid relationship pattern: expected ->( after ]")
		}
		secondNodeStart = 3
	}

	// Find end of second node
	depth = 1
	secondNodeEnd := -1
	for i := secondNodeStart; i < len(afterRel); i++ {
		c := afterRel[i]
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				secondNodeEnd = i
				break
			}
		}
	}
	if secondNodeEnd < 0 {
		return "", "", "", false, "", fmt.Errorf("invalid relationship pattern: unmatched parenthesis for second node")
	}

	targetVar := strings.TrimSpace(afterRel[secondNodeStart:secondNodeEnd])
	remainder := strings.TrimSpace(afterRel[secondNodeEnd+1:])

	return sourceVar, relContent, targetVar, isReverse, remainder, nil
}

// splitNodePatterns splits a CREATE pattern into individual node patterns
// (Used for simple node-only patterns and by other parts of the system)
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

// parseRelationshipTypeAndProps parses "r:TYPE {props}" or ":TYPE {props}" or just "r" (variable only)
// Returns the type and properties map
func (e *StorageExecutor) parseRelationshipTypeAndProps(relStr string) (string, map[string]interface{}) {
	relStr = strings.TrimSpace(relStr)
	relType := "RELATED_TO"
	var relProps map[string]interface{}

	// Find properties block if present
	propsStart := strings.Index(relStr, "{")
	if propsStart >= 0 {
		// Find matching }
		depth := 0
		propsEnd := -1
		inQuote := false
		quoteChar := rune(0)
		for i := propsStart; i < len(relStr); i++ {
			c := rune(relStr[i])
			if !inQuote {
				if c == '\'' || c == '"' {
					inQuote = true
					quoteChar = c
				} else if c == '{' {
					depth++
				} else if c == '}' {
					depth--
					if depth == 0 {
						propsEnd = i
						break
					}
				}
			} else if c == quoteChar && (i == 0 || relStr[i-1] != '\\') {
				inQuote = false
			}
		}
		if propsEnd > propsStart {
			relProps = e.parseProperties(relStr[propsStart : propsEnd+1])
		}
		relStr = strings.TrimSpace(relStr[:propsStart])
	}

	// Parse type: "r:TYPE" or ":TYPE" - if no colon, it's just a variable (use default type)
	if colonIdx := strings.Index(relStr, ":"); colonIdx >= 0 {
		// Has colon - everything after is the type
		relType = strings.TrimSpace(relStr[colonIdx+1:])
		if relType == "" {
			relType = "RELATED_TO" // Handle case like ":" with no type
		}
	}
	// If no colon, relStr is just a variable name like "r" - keep default RELATED_TO

	if relProps == nil {
		relProps = make(map[string]interface{})
	}

	return relType, relProps
}

// executeCompoundMatchCreate handles MATCH ... CREATE queries.
// This handles multiple scenarios:
// 1. Create relationships between matched nodes
// 2. Create new nodes and relationships referencing matched nodes
// 3. Multiple MATCH...CREATE blocks in a single query
//
// Example 1: Create relationship only
//
//	MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
//	CREATE (a)-[:KNOWS]->(b)
//
// Example 2: Create new node with relationships to matched nodes
//
//	MATCH (s:Supplier {supplierID: 1}), (c:Category {categoryID: 1})
//	CREATE (p:Product {productName: 'Chai'})
//	CREATE (p)-[:PART_OF]->(c)
//	CREATE (s)-[:SUPPLIES]->(p)
//
// Example 3: Multiple MATCH...CREATE blocks
//
//	MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})
//	CREATE (p1:Product {...})
//	MATCH (s2:Supplier {supplierID: 2}), (c2:Category {categoryID: 2})
//	CREATE (p2:Product {...})
func (e *StorageExecutor) executeCompoundMatchCreate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Check if this query has multiple MATCH blocks (each starting a new scope)
	// Split into independent MATCH...CREATE blocks
	blocks := e.splitMatchCreateBlocks(cypher)

	// Track all created nodes across blocks (for cross-block references)
	allNodeVars := make(map[string]*storage.Node)
	allEdgeVars := make(map[string]*storage.Edge)

	for _, block := range blocks {
		blockResult, err := e.executeMatchCreateBlock(ctx, block, allNodeVars, allEdgeVars)
		if err != nil {
			return nil, err
		}
		// Accumulate stats
		result.Stats.NodesCreated += blockResult.Stats.NodesCreated
		result.Stats.RelationshipsCreated += blockResult.Stats.RelationshipsCreated
		result.Stats.NodesDeleted += blockResult.Stats.NodesDeleted
		result.Stats.RelationshipsDeleted += blockResult.Stats.RelationshipsDeleted
	}

	return result, nil
}

// splitMatchCreateBlocks splits a query into independent MATCH...CREATE blocks
// Each block starts with MATCH and contains all following CREATEs until the next MATCH
func (e *StorageExecutor) splitMatchCreateBlocks(cypher string) []string {
	var blocks []string

	// Find all MATCH keyword positions
	matchPositions := findAllKeywordPositions(cypher, "MATCH")

	if len(matchPositions) == 0 {
		return []string{cypher}
	}

	// Split into blocks: each MATCH starts a new block
	for i, pos := range matchPositions {
		var endPos int
		if i+1 < len(matchPositions) {
			endPos = matchPositions[i+1]
		} else {
			endPos = len(cypher)
		}

		block := strings.TrimSpace(cypher[pos:endPos])
		if block != "" {
			blocks = append(blocks, block)
		}
	}

	return blocks
}

// findAllKeywordPositions finds all positions of a keyword in the query
func findAllKeywordPositions(cypher string, keyword string) []int {
	var positions []int
	keywordLen := len(keyword)

	for i := 0; i <= len(cypher)-keywordLen; i++ {
		// Check if keyword matches at this position (case insensitive)
		if strings.EqualFold(cypher[i:i+keywordLen], keyword) {
			// Check word boundary before
			if i > 0 {
				prevChar := cypher[i-1]
				if isAlphaNumericByte(prevChar) {
					continue // Part of another word
				}
			}
			// Check word boundary after
			if i+keywordLen < len(cypher) {
				nextChar := cypher[i+keywordLen]
				if isAlphaNumericByte(nextChar) {
					continue // Part of another word
				}
			}
			positions = append(positions, i)
		}
	}

	// Handle nested MATCH in strings - check if position is inside quotes
	var validPositions []int
	for _, pos := range positions {
		if !isInsideQuotes(cypher, pos) {
			validPositions = append(validPositions, pos)
		}
	}

	return validPositions
}

// isAlphaNumericByte checks if a byte is alphanumeric or underscore
func isAlphaNumericByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// isInsideQuotes checks if a position is inside quotes
func isInsideQuotes(s string, pos int) bool {
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < pos; i++ {
		c := s[i]
		if c == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
		} else if c == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
		}
	}

	return inSingleQuote || inDoubleQuote
}

// executeMatchCreateBlock executes a single MATCH...CREATE block
func (e *StorageExecutor) executeMatchCreateBlock(ctx context.Context, block string, allNodeVars map[string]*storage.Node, allEdgeVars map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Split into MATCH and CREATE parts
	createIdx := findKeywordIndex(block, "CREATE")
	if createIdx < 0 {
		// No CREATE in this block, just MATCH - skip
		return result, nil
	}

	matchPart := strings.TrimSpace(block[:createIdx])
	createPart := strings.TrimSpace(block[createIdx:]) // Keep "CREATE" for splitting

	// Strip WITH clause from matchPart (handles MATCH ... WITH ... LIMIT 1 CREATE ...)
	if withInMatch := findKeywordIndex(matchPart, "WITH"); withInMatch > 0 {
		matchPart = strings.TrimSpace(matchPart[:withInMatch])
	}

	// Find WITH clause (for MATCH...CREATE...WITH...DELETE pattern)
	withIdx := findKeywordIndex(createPart, "WITH")
	deleteIdx := findKeywordIndex(createPart, "DELETE")
	var deleteTarget string
	hasWithDelete := withIdx > 0 && deleteIdx > withIdx
	hasDirectDelete := deleteIdx > 0 && withIdx <= 0 // DELETE without WITH in createPart

	// Find RETURN clause if present (only in last block typically)
	returnIdx := findKeywordIndex(createPart, "RETURN")
	var returnPart string
	if returnIdx > 0 {
		returnPart = strings.TrimSpace(createPart[returnIdx+6:])
		if hasWithDelete {
			// WITH...DELETE...RETURN - extract delete target and strip
			withDeletePart := createPart[withIdx:returnIdx]
			deletePartIdx := findKeywordIndex(withDeletePart, "DELETE")
			if deletePartIdx > 0 {
				deleteTarget = strings.TrimSpace(withDeletePart[deletePartIdx+6:])
			}
			createPart = strings.TrimSpace(createPart[:withIdx])
		} else if hasDirectDelete {
			// CREATE...DELETE...RETURN (DELETE without WITH)
			deleteTarget = strings.TrimSpace(createPart[deleteIdx+6 : returnIdx])
			createPart = strings.TrimSpace(createPart[:deleteIdx])
		} else {
			createPart = strings.TrimSpace(createPart[:returnIdx])
		}
	} else if hasWithDelete {
		// WITH...DELETE without RETURN
		withDeletePart := createPart[withIdx:]
		deletePartIdx := findKeywordIndex(withDeletePart, "DELETE")
		if deletePartIdx > 0 {
			deleteTarget = strings.TrimSpace(withDeletePart[deletePartIdx+6:])
		}
		createPart = strings.TrimSpace(createPart[:withIdx])
	} else if hasDirectDelete {
		// CREATE...DELETE without WITH or RETURN
		deleteTarget = strings.TrimSpace(createPart[deleteIdx+6:])
		createPart = strings.TrimSpace(createPart[:deleteIdx])
	}

	// Parse all node patterns from MATCH clause and find matching nodes
	// Start with existing vars from previous blocks
	nodeVars := make(map[string]*storage.Node)
	for k, v := range allNodeVars {
		nodeVars[k] = v
	}
	edgeVars := make(map[string]*storage.Edge)
	for k, v := range allEdgeVars {
		edgeVars[k] = v
	}

	// Collect all patterns and their matching nodes for cartesian product
	patternMatches := make([]struct {
		variable string
		nodes    []*storage.Node
	}, 0)

	// Split by MATCH keyword to handle comma-separated patterns in MATCH
	// Uses pre-compiled matchKeywordPattern from regex_patterns.go
	matchClauses := matchKeywordPattern.Split(matchPart, -1)

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

		// Collect ALL matched nodes from MATCH clause (for cartesian product)
		for _, pattern := range patterns {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				continue
			}

			nodeInfo := e.parseNodePattern(pattern)
			if nodeInfo.variable == "" {
				continue
			}

			// Check if this variable already exists from previous blocks
			if existingNode, exists := nodeVars[nodeInfo.variable]; exists {
				// Already have this variable, use the existing node (single match from prior context)
				patternMatches = append(patternMatches, struct {
					variable string
					nodes    []*storage.Node
				}{
					variable: nodeInfo.variable,
					nodes:    []*storage.Node{existingNode},
				})
				continue
			}

			// Find ALL matching nodes for this pattern (for cartesian product)
			var matchingNodes []*storage.Node
			if len(nodeInfo.labels) > 0 {
				matchingNodes, _ = e.storage.GetNodesByLabel(nodeInfo.labels[0])
			} else {
				matchingNodes, _ = e.storage.AllNodes()
			}

			// Filter by additional labels
			if len(nodeInfo.labels) > 1 {
				var filtered []*storage.Node
				for _, node := range matchingNodes {
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
				matchingNodes = filtered
			}

			// Filter by properties
			if len(nodeInfo.properties) > 0 {
				var filtered []*storage.Node
				for _, node := range matchingNodes {
					if e.nodeMatchesProps(node, nodeInfo.properties) {
						filtered = append(filtered, node)
					}
				}
				matchingNodes = filtered
			}

			if len(matchingNodes) > 0 {
				patternMatches = append(patternMatches, struct {
					variable string
					nodes    []*storage.Node
				}{
					variable: nodeInfo.variable,
					nodes:    matchingNodes,
				})
			}
		}
	}

	// Build cartesian product of all pattern matches
	allCombinations := e.buildCartesianProduct(patternMatches)

	// If no combinations, fall back to using existing nodeVars
	if len(allCombinations) == 0 {
		allCombinations = []map[string]*storage.Node{nodeVars}
	}

	// Split CREATE part into individual CREATE statements
	// Uses pre-compiled createKeywordPattern from regex_patterns.go
	createClauses := createKeywordPattern.Split(createPart, -1)

	// For each combination in the cartesian product, execute CREATE
	for _, combination := range allCombinations {
		// Merge combination with existing nodeVars (combination takes precedence for cartesian product vars)
		combinedNodeVars := make(map[string]*storage.Node)
		for k, v := range nodeVars {
			combinedNodeVars[k] = v
		}
		for k, v := range combination {
			combinedNodeVars[k] = v
		}

		// TWO-PASS APPROACH: Create nodes first, then relationships
		// This ensures that nodes created in the same CREATE clause are available
		// when processing relationships that reference them.

		// Collect all patterns from all clauses
		var allNodePatterns []string
		var allRelPatterns []string

		for _, clause := range createClauses {
			clause = strings.TrimSpace(clause)
			if clause == "" {
				continue
			}

			// Split the clause into individual patterns (respecting nesting)
			patterns := e.splitCreatePatterns(clause)

			for _, pat := range patterns {
				pat = strings.TrimSpace(pat)
				if pat == "" {
					continue
				}

				// Check if this individual pattern is a relationship or node
				if containsOutsideStrings(pat, "->") || containsOutsideStrings(pat, "<-") || containsOutsideStrings(pat, "]-") {
					allRelPatterns = append(allRelPatterns, pat)
				} else {
					allNodePatterns = append(allNodePatterns, pat)
				}
			}
		}

		// PASS 1: Create all nodes first
		for _, np := range allNodePatterns {
			err := e.processCreateNode(np, combinedNodeVars, result)
			if err != nil {
				return nil, err
			}
		}

		// PASS 2: Create all relationships (now nodes are available)
		for _, rp := range allRelPatterns {
			err := e.processCreateRelationship(rp, combinedNodeVars, edgeVars, result)
			if err != nil {
				return nil, err
			}
		}

		// Copy created nodes back to nodeVars for cross-reference within same query
		for k, v := range combinedNodeVars {
			nodeVars[k] = v
		}
	}

	// Copy new vars back to allNodeVars for use in later blocks
	for k, v := range nodeVars {
		allNodeVars[k] = v
	}
	for k, v := range edgeVars {
		allEdgeVars[k] = v
	}

	// Execute DELETE if present (MATCH...CREATE...WITH...DELETE pattern)
	if deleteTarget != "" {
		if edge, exists := edgeVars[deleteTarget]; exists {
			if err := e.storage.DeleteEdge(edge.ID); err == nil {
				result.Stats.RelationshipsDeleted++
			}
		} else if node, exists := nodeVars[deleteTarget]; exists {
			// Delete connected edges first
			outEdges, _ := e.storage.GetOutgoingEdges(node.ID)
			inEdges, _ := e.storage.GetIncomingEdges(node.ID)
			for _, edge := range outEdges {
				if err := e.storage.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
				}
			}
			for _, edge := range inEdges {
				if err := e.storage.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
				}
			}
			if err := e.storage.DeleteNode(node.ID); err == nil {
				result.Stats.NodesDeleted++
			}
		}
	}

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

			// Handle count() after DELETE
			upperExpr := strings.ToUpper(item.expr)
			if strings.HasPrefix(upperExpr, "COUNT(") && deleteTarget != "" {
				row[i] = int64(1) // count of deleted items
				continue
			}

			// Find variable that matches
			for varName, node := range nodeVars {
				if strings.HasPrefix(item.expr, varName) {
					row[i] = e.resolveReturnItem(item, varName, node)
					break
				}
			}
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil
}

// processCreateNode creates a new node and adds it to the nodeVars map
func (e *StorageExecutor) processCreateNode(pattern string, nodeVars map[string]*storage.Node, result *ExecuteResult) error {
	nodeInfo := e.parseNodePattern(pattern)

	// Create the node
	node := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     nodeInfo.labels,
		Properties: nodeInfo.properties,
	}

	if err := e.storage.CreateNode(node); err != nil {
		return fmt.Errorf("failed to create node: %w", err)
	}
	e.notifyNodeCreated(string(node.ID))

	result.Stats.NodesCreated++

	// Store in nodeVars for later reference
	if nodeInfo.variable != "" {
		nodeVars[nodeInfo.variable] = node
	}

	return nil
}

// processCreateRelationship creates a relationship between nodes in nodeVars
func (e *StorageExecutor) processCreateRelationship(pattern string, nodeVars map[string]*storage.Node, edgeVars map[string]*storage.Edge, result *ExecuteResult) error {
	// Parse the relationship pattern: (a)-[r:TYPE {props}]->(b)
	// Supports both simple variable refs and inline node definitions
	// Uses pre-compiled patterns from regex_patterns.go for performance

	// Try forward arrow first: (a)-[...]->(b)
	matches := relForwardPattern.FindStringSubmatch(pattern)

	isReverse := false
	if matches == nil {
		// Try reverse arrow: (a)<-[...]-(b)
		matches = relReversePattern.FindStringSubmatch(pattern)
		isReverse = true
	}

	if len(matches) < 6 {
		return fmt.Errorf("invalid relationship pattern in CREATE: %s", pattern)
	}

	sourceContent := strings.TrimSpace(matches[1])
	relVar := matches[2]
	relType := matches[3]
	relPropsStr := matches[4]
	targetContent := strings.TrimSpace(matches[5])

	// Default relationship type
	if relType == "" {
		relType = "RELATED_TO"
	}

	// Parse relationship properties if present
	var relProps map[string]interface{}
	if relPropsStr != "" {
		relProps = e.parseProperties(relPropsStr)
	} else {
		relProps = make(map[string]interface{})
	}

	// Resolve source node - could be a variable reference or inline node definition
	sourceNode, err := e.resolveOrCreateNode(sourceContent, nodeVars, result)
	if err != nil {
		return fmt.Errorf("failed to resolve source node: %w", err)
	}

	// Resolve target node - could be a variable reference or inline node definition
	targetNode, err := e.resolveOrCreateNode(targetContent, nodeVars, result)
	if err != nil {
		return fmt.Errorf("failed to resolve target node: %w", err)
	}

	// Handle reverse direction
	startNode, endNode := sourceNode, targetNode
	if isReverse {
		startNode, endNode = targetNode, sourceNode
	}

	// Create the relationship
	edge := &storage.Edge{
		ID:         storage.EdgeID(e.generateID()),
		StartNode:  startNode.ID,
		EndNode:    endNode.ID,
		Type:       relType,
		Properties: relProps,
	}

	if err := e.storage.CreateEdge(edge); err != nil {
		return fmt.Errorf("failed to create relationship: %w", err)
	}

	result.Stats.RelationshipsCreated++

	// Store edge variable if present
	if relVar != "" {
		edgeVars[relVar] = edge
	}

	return nil
}

// resolveOrCreateNode resolves a node reference, creating it if it's an inline definition.
// Supports:
//   - Simple variable: "p" -> looks up in nodeVars
//   - Inline definition: "c:Company {name: 'Acme'}" -> creates node and adds to nodeVars
func (e *StorageExecutor) resolveOrCreateNode(content string, nodeVars map[string]*storage.Node, result *ExecuteResult) (*storage.Node, error) {
	content = strings.TrimSpace(content)

	// Check if this is a simple variable reference (just alphanumeric)
	if isSimpleVariable(content) {
		node, exists := nodeVars[content]
		if !exists {
			return nil, fmt.Errorf("variable '%s' not found (have: %v)", content, getKeys(nodeVars))
		}
		return node, nil
	}

	// Parse as inline node definition: "varName:Label {props}" or ":Label {props}" or "varName:Label"
	nodeInfo := e.parseNodePattern("(" + content + ")")

	// Check if we already have this variable
	if nodeInfo.variable != "" {
		if existingNode, exists := nodeVars[nodeInfo.variable]; exists {
			return existingNode, nil
		}
	}

	// Create new node
	node := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     nodeInfo.labels,
		Properties: nodeInfo.properties,
	}

	if err := e.storage.CreateNode(node); err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}
	e.notifyNodeCreated(string(node.ID))

	result.Stats.NodesCreated++

	// Store in nodeVars if it has a variable name
	if nodeInfo.variable != "" {
		nodeVars[nodeInfo.variable] = node
	}

	return node, nil
}

// isSimpleVariable checks if content is just a variable name (alphanumeric + underscore)
func isSimpleVariable(content string) bool {
	if content == "" {
		return false
	}
	for _, r := range content {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// getKeys returns the keys of a map as a slice
func getKeys(m map[string]*storage.Node) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// executeCompoundCreateWithDelete handles CREATE ... WITH ... DELETE queries.
// This pattern creates a node/relationship, passes it through WITH, then deletes it.
// Example: CREATE (t:TestNode {name: 'temp'}) WITH t DELETE t RETURN count(t)
func (e *StorageExecutor) executeCompoundCreateWithDelete(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Find clause boundaries
	withIdx := findKeywordIndex(cypher, "WITH")
	deleteIdx := findKeywordIndex(cypher, "DELETE")
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if withIdx < 0 || deleteIdx < 0 {
		return nil, fmt.Errorf("invalid CREATE...WITH...DELETE query")
	}

	// Extract CREATE part (everything before WITH)
	createPart := strings.TrimSpace(cypher[:withIdx])

	// Extract WITH variables (between WITH and DELETE)
	withPart := strings.TrimSpace(cypher[withIdx+4 : deleteIdx])

	// Extract DELETE target (between DELETE and RETURN, or end)
	var deletePart string
	if returnIdx > 0 {
		deletePart = strings.TrimSpace(cypher[deleteIdx+6 : returnIdx])
	} else {
		deletePart = strings.TrimSpace(cypher[deleteIdx+6:])
	}

	// Execute the CREATE part and get the created nodes/edges directly
	// This avoids expensive O(n) scans of GetNodesByLabel/AllEdges
	createResult, createdVars, createdEdges, err := e.executeCreateWithRefs(ctx, createPart)
	if err != nil {
		return nil, fmt.Errorf("CREATE failed: %w", err)
	}
	result.Stats.NodesCreated = createResult.Stats.NodesCreated
	result.Stats.RelationshipsCreated = createResult.Stats.RelationshipsCreated

	// Parse WITH clause to see what variables are passed through
	withVars := strings.Split(withPart, ",")
	for i := range withVars {
		withVars[i] = strings.TrimSpace(withVars[i])
	}

	// Execute DELETE
	deleteTarget := strings.TrimSpace(deletePart)
	if node, exists := createdVars[deleteTarget]; exists {
		// Node was JUST created in this query, so it has no pre-existing edges.
		// Only need to check for edges created in the same query (in createdEdges).
		// This avoids 2 unnecessary storage lookups per delete operation.
		for varName, edge := range createdEdges {
			if edge.StartNode == node.ID || edge.EndNode == node.ID {
				if err := e.storage.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
					delete(createdEdges, varName) // Mark as deleted
				}
			}
		}
		if err := e.storage.DeleteNode(node.ID); err != nil {
			return nil, fmt.Errorf("DELETE failed: %w", err)
		}
		result.Stats.NodesDeleted++
	} else if edge, exists := createdEdges[deleteTarget]; exists {
		if err := e.storage.DeleteEdge(edge.ID); err != nil {
			return nil, fmt.Errorf("DELETE failed: %w", err)
		}
		result.Stats.RelationshipsDeleted++
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(cypher[returnIdx+6:])

		// Parse return expression
		if strings.Contains(strings.ToLower(returnPart), "count(") {
			// count() after delete should return 1 (counted before delete conceptually)
			// But actually in Neo4j, count(t) after DELETE t returns 1 (the count of deleted items)
			result.Columns = []string{"count(" + deleteTarget + ")"}
			result.Rows = [][]interface{}{{int64(1)}}
		} else {
			result.Columns = []string{returnPart}
			result.Rows = [][]interface{}{{nil}}
		}
	}

	return result, nil
}

// executeCreateSet handles CREATE ... SET queries (Neo4j compatibility).
// Neo4j allows SET immediately after CREATE to set additional properties
// on newly created nodes/relationships.
// Example: CREATE (n:Node {id: 'test'}) SET n.content = 'value' RETURN n
func (e *StorageExecutor) executeCreateSet(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Normalize whitespace for index finding (newlines/tabs become spaces)
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")

	// Find clause boundaries
	setIdx := findKeywordIndex(normalized, "SET")
	returnIdx := findKeywordIndex(normalized, "RETURN")

	if setIdx < 0 {
		return nil, fmt.Errorf("SET clause not found in CREATE...SET query")
	}

	// Extract CREATE part (everything before SET)
	createPart := strings.TrimSpace(normalized[:setIdx])

	// Extract SET part (between SET and RETURN, or end)
	var setPart string
	if returnIdx > 0 {
		setPart = strings.TrimSpace(normalized[setIdx+4 : returnIdx])
	} else {
		setPart = strings.TrimSpace(normalized[setIdx+4:])
	}

	// Pre-validate SET assignments BEFORE executing CREATE
	// This ensures we fail fast and don't create nodes that would be orphaned
	if !strings.Contains(setPart, "+=") {
		assignments := e.splitSetAssignmentsRespectingBrackets(setPart)
		if err := e.validateSetAssignments(assignments); err != nil {
			return nil, err
		}
	}

	// Execute CREATE first and get references to created entities
	createResult, createdNodes, createdEdges, err := e.executeCreateWithRefs(ctx, createPart)
	if err != nil {
		return nil, fmt.Errorf("CREATE failed in CREATE...SET: %w", err)
	}
	result.Stats.NodesCreated = createResult.Stats.NodesCreated
	result.Stats.RelationshipsCreated = createResult.Stats.RelationshipsCreated

	// Check for property merge operator: n += $properties
	if strings.Contains(setPart, "+=") {
		// Handle property merge on created entities
		err := e.applySetMergeToCreated(setPart, createdNodes, createdEdges, result)
		if err != nil {
			return nil, err
		}
	} else {
		// Handle regular SET assignments
		// Split SET clause into individual assignments
		assignments := e.splitSetAssignmentsRespectingBrackets(setPart)

		for _, assignment := range assignments {
			assignment = strings.TrimSpace(assignment)
			if assignment == "" {
				continue
			}

			// Parse assignment: var.property = value
			eqIdx := strings.Index(assignment, "=")
			if eqIdx == -1 {
				// Could be a label assignment like "n:Label"
				colonIdx := strings.Index(assignment, ":")
				if colonIdx > 0 {
					varName := strings.TrimSpace(assignment[:colonIdx])
					newLabel := strings.TrimSpace(assignment[colonIdx+1:])
					if node, exists := createdNodes[varName]; exists {
						// Add label to existing node
						if !containsString(node.Labels, newLabel) {
							node.Labels = append(node.Labels, newLabel)
							if err := e.storage.UpdateNode(node); err != nil {
								return nil, fmt.Errorf("failed to add label: %w", err)
							}
							result.Stats.LabelsAdded++
						}
					}
				}
				continue
			}

			leftSide := strings.TrimSpace(assignment[:eqIdx])
			rightSide := strings.TrimSpace(assignment[eqIdx+1:])

			// Parse variable.property
			dotIdx := strings.Index(leftSide, ".")
			if dotIdx == -1 {
				return nil, fmt.Errorf("invalid SET assignment (expected var.property): %s", assignment)
			}

			varName := strings.TrimSpace(leftSide[:dotIdx])
			propName := strings.TrimSpace(leftSide[dotIdx+1:])

			// Note: Function validation is already done in validateSetAssignments()
			// which runs before CREATE to ensure rollback safety

			// Parse the value
			value := e.parseValue(rightSide)

			// Apply to created node or edge
			if node, exists := createdNodes[varName]; exists {
				node.Properties[propName] = value
				if err := e.storage.UpdateNode(node); err != nil {
					return nil, fmt.Errorf("failed to update node property: %w", err)
				}
				result.Stats.PropertiesSet++
			} else if edge, exists := createdEdges[varName]; exists {
				edge.Properties[propName] = value
				if err := e.storage.UpdateEdge(edge); err != nil {
					return nil, fmt.Errorf("failed to update edge property: %w", err)
				}
				result.Stats.PropertiesSet++
			} else {
				return nil, fmt.Errorf("unknown variable in SET clause: %s", varName)
			}
		}
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(normalized[returnIdx+6:])

		// Parse return items
		returnItems := splitReturnExpressions(returnPart)
		for _, item := range returnItems {
			item = strings.TrimSpace(item)
			alias := item

			// Check for alias
			upperItem := strings.ToUpper(item)
			if asIdx := strings.Index(upperItem, " AS "); asIdx > 0 {
				alias = strings.TrimSpace(item[asIdx+4:])
				item = strings.TrimSpace(item[:asIdx])
			}

			result.Columns = append(result.Columns, alias)

			// Resolve the value
			if node, exists := createdNodes[item]; exists {
				if len(result.Rows) == 0 {
					result.Rows = append(result.Rows, []interface{}{})
				}
				result.Rows[0] = append(result.Rows[0], node)
			} else if edge, exists := createdEdges[item]; exists {
				if len(result.Rows) == 0 {
					result.Rows = append(result.Rows, []interface{}{})
				}
				result.Rows[0] = append(result.Rows[0], edge)
			} else {
				// Could be an expression or property access
				if len(result.Rows) == 0 {
					result.Rows = append(result.Rows, []interface{}{})
				}
				result.Rows[0] = append(result.Rows[0], nil)
			}
		}
	} else {
		// No RETURN clause - return created entities by default
		for _, node := range createdNodes {
			if len(result.Columns) == 0 {
				result.Columns = append(result.Columns, "node")
			}
			if len(result.Rows) == 0 {
				result.Rows = append(result.Rows, []interface{}{})
			}
			result.Rows[0] = append(result.Rows[0], node)
		}
	}

	return result, nil
}

// applySetMergeToCreated applies SET += property merge to created entities.
func (e *StorageExecutor) applySetMergeToCreated(setPart string, createdNodes map[string]*storage.Node, createdEdges map[string]*storage.Edge, result *ExecuteResult) error {
	// Parse: n += {prop: value, ...}
	parts := strings.SplitN(setPart, "+=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid SET += syntax")
	}

	varName := strings.TrimSpace(parts[0])
	propsStr := strings.TrimSpace(parts[1])

	// Parse the properties map
	props := e.parseMapLiteral(propsStr)
	if props == nil {
		return fmt.Errorf("failed to parse properties in SET +=")
	}

	// Apply to node or edge
	if node, exists := createdNodes[varName]; exists {
		for k, v := range props {
			node.Properties[k] = v
			result.Stats.PropertiesSet++
		}
		if err := e.storage.UpdateNode(node); err != nil {
			return fmt.Errorf("failed to update node: %w", err)
		}
	} else if edge, exists := createdEdges[varName]; exists {
		for k, v := range props {
			edge.Properties[k] = v
			result.Stats.PropertiesSet++
		}
		if err := e.storage.UpdateEdge(edge); err != nil {
			return fmt.Errorf("failed to update edge: %w", err)
		}
	} else {
		return fmt.Errorf("unknown variable in SET +=: %s", varName)
	}

	return nil
}

// containsString checks if a slice contains a string.
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// validateSetAssignments pre-validates SET clause assignments before executing CREATE
// This ensures we fail fast on invalid function calls, preventing orphaned nodes
func (e *StorageExecutor) validateSetAssignments(assignments []string) error {
	// Known Cypher functions
	knownFunctions := map[string]bool{
		"COALESCE": true, "TOSTRING": true, "TOINT": true, "TOFLOAT": true,
		"TOBOOLEAN": true, "TOLOWER": true, "TOUPPER": true, "TRIM": true,
		"SIZE": true, "LENGTH": true, "ABS": true, "CEIL": true, "FLOOR": true,
		"ROUND": true, "RAND": true, "SQRT": true, "SIGN": true, "LOG": true,
		"LOG10": true, "EXP": true, "SIN": true, "COS": true, "TAN": true,
		"DATE": true, "DATETIME": true, "TIME": true, "TIMESTAMP": true,
		"DURATION": true, "LOCALDATETIME": true, "LOCALTIME": true,
		"HEAD": true, "LAST": true, "TAIL": true, "KEYS": true, "LABELS": true,
		"TYPE": true, "ID": true, "ELEMENTID": true, "PROPERTIES": true,
		"POINT": true, "DISTANCE": true, "REPLACE": true, "SUBSTRING": true,
		"LEFT": true, "RIGHT": true, "SPLIT": true, "REVERSE": true,
		"LTRIM": true, "RTRIM": true, "COLLECT": true, "RANGE": true,
	}

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		// Parse assignment: var.property = value or var:Label
		eqIdx := strings.Index(assignment, "=")
		if eqIdx == -1 {
			// Could be a label assignment like "n:Label" - these are valid
			continue
		}

		rightSide := strings.TrimSpace(assignment[eqIdx+1:])

		// Check if right side looks like a function call
		if strings.Contains(rightSide, "(") && strings.HasSuffix(strings.TrimSpace(rightSide), ")") {
			// Extract function name (before first parenthesis)
			parenIdx := strings.Index(rightSide, "(")
			funcName := strings.ToUpper(strings.TrimSpace(rightSide[:parenIdx]))
			if !knownFunctions[funcName] {
				return fmt.Errorf("unknown function: %s", funcName)
			}
		}
	}
	return nil
}

// executeMerge handles MERGE queries with ON CREATE SET / ON MATCH SET support.
// This implements Neo4j-compatible MERGE semantics:
// 1. Try to find an existing node matching the pattern
// 2. If found, apply ON MATCH SET if present
// 3. If not found, create the node and apply ON CREATE SET if present
