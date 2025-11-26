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
		if strings.Contains(p, "->") || strings.Contains(p, "<-") || strings.Contains(p, "-[") {
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

	// Then, create all relationships using variable references or inline node definitions
	for _, relPatternStr := range relPatterns {
		relPatternStr = strings.TrimSpace(relPatternStr)
		if relPatternStr == "" {
			continue
		}

		// Parse the relationship pattern: (varA)-[:TYPE {props}]->(varB)
		sourceContent, relStr, targetContent, isReverse, err := e.parseCreateRelPatternWithVars(relPatternStr)
		if err != nil {
			return nil, err
		}

		// Determine source node - either lookup by variable or create inline
		var sourceNode *storage.Node
		if node, exists := createdNodes[sourceContent]; exists {
			// Variable reference to existing node
			sourceNode = node
		} else {
			// Inline node definition - parse and create
			sourcePattern := e.parseNodePattern("(" + sourceContent + ")")
			sourceNode = &storage.Node{
				ID:         storage.NodeID(e.generateID()),
				Labels:     sourcePattern.labels,
				Properties: sourcePattern.properties,
			}
			if err := e.storage.CreateNode(sourceNode); err != nil {
				return nil, fmt.Errorf("failed to create source node: %w", err)
			}
			result.Stats.NodesCreated++
			if sourcePattern.variable != "" {
				createdNodes[sourcePattern.variable] = sourceNode
			}
		}

		// Determine target node - either lookup by variable or create inline
		var targetNode *storage.Node
		if node, exists := createdNodes[targetContent]; exists {
			// Variable reference to existing node
			targetNode = node
		} else {
			// Inline node definition - parse and create
			targetPattern := e.parseNodePattern("(" + targetContent + ")")
			targetNode = &storage.Node{
				ID:         storage.NodeID(e.generateID()),
				Labels:     targetPattern.labels,
				Properties: targetPattern.properties,
			}
			if err := e.storage.CreateNode(targetNode); err != nil {
				return nil, fmt.Errorf("failed to create target node: %w", err)
			}
			result.Stats.NodesCreated++
			if targetPattern.variable != "" {
				createdNodes[targetPattern.variable] = targetNode
			}
		}

		// Parse relationship type and properties
		relType, relProps := e.parseRelationshipTypeAndProps(relStr)

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

// splitCreatePatterns splits a CREATE pattern into individual patterns (nodes and relationships)
// respecting parentheses depth and handling relationship syntax
func (e *StorageExecutor) splitCreatePatterns(pattern string) []string {
	var patterns []string
	var current strings.Builder
	depth := 0
	inRelationship := false

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
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
// Returns: sourceVar, relContent, targetVar, isReverse, error
func (e *StorageExecutor) parseCreateRelPatternWithVars(pattern string) (string, string, string, bool, error) {
	pattern = strings.TrimSpace(pattern)

	// Find the first node: (varA)
	if !strings.HasPrefix(pattern, "(") {
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: must start with (")
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
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: unmatched parenthesis")
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
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: expected -[ or <-[, got: %s", rest[:min(20, len(rest))])
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
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: unmatched bracket")
	}

	relContent := rest[relStart:relEnd]
	afterRel := strings.TrimSpace(rest[relEnd+1:])

	// Now find the second node
	var secondNodeStart int
	if isReverse {
		if !strings.HasPrefix(afterRel, "-(") {
			return "", "", "", false, fmt.Errorf("invalid relationship pattern: expected -( after ]")
		}
		secondNodeStart = 2
	} else {
		if !strings.HasPrefix(afterRel, "->(") {
			return "", "", "", false, fmt.Errorf("invalid relationship pattern: expected ->( after ]")
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
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: unmatched parenthesis for second node")
	}

	targetVar := strings.TrimSpace(afterRel[secondNodeStart:secondNodeEnd])

	return sourceVar, relContent, targetVar, isReverse, nil
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

// executeCreateRelationship handles CREATE with relationships.
func (e *StorageExecutor) executeCreateRelationship(ctx context.Context, cypher, pattern string, returnIdx int) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Parse relationship pattern: (a:Label {props})-[r:TYPE {props}]->(b:Label {props})
	// We need custom parsing because nested brackets in properties break simple regex
	sourceStr, relStr, targetStr, isReverse, err := e.parseCreateRelPattern(pattern)
	if err != nil {
		return nil, err
	}

	// Parse source node
	sourcePattern := e.parseNodePattern("(" + sourceStr + ")")
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
	targetPattern := e.parseNodePattern("(" + targetStr + ")")
	targetNode := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     targetPattern.labels,
		Properties: targetPattern.properties,
	}
	if err := e.storage.CreateNode(targetNode); err != nil {
		return nil, fmt.Errorf("failed to create target node: %w", err)
	}
	result.Stats.NodesCreated++

	// Parse relationship type and properties from relStr (e.g., "r:ACTED_IN {roles: ['Neo']}")
	relType, relProps := e.parseRelationshipTypeAndProps(relStr)

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

// parseCreateRelPattern parses patterns like (a)-[r:TYPE {props}]->(b) or (a)<-[r:TYPE]-(b)
// Returns: sourceContent, relContent, targetContent, isReverse, error
func (e *StorageExecutor) parseCreateRelPattern(pattern string) (string, string, string, bool, error) {
	// Find the relationship bracket section by tracking bracket depth
	// Pattern forms: (...)- [...] ->(...)  or  (...)<- [...] -(...)

	// First, find the first node: (...)
	if !strings.HasPrefix(pattern, "(") {
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: must start with (")
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
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: unmatched parenthesis")
	}

	firstNode := pattern[1:firstNodeEnd] // Content inside first ()
	rest := pattern[firstNodeEnd+1:]

	// Detect direction and find relationship bracket
	isReverse := false
	var relStart, relEnd int

	if strings.HasPrefix(rest, "-[") {
		// Forward: -[...]->(...)
		relStart = 2 // Skip "-["
	} else if strings.HasPrefix(rest, "<-[") {
		// Reverse: <-[...]-(...)
		isReverse = true
		relStart = 3 // Skip "<-["
	} else {
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: expected -[ or <-[")
	}

	// Find matching ] considering nested brackets in properties
	depth = 1 // We're inside [
	relEnd = -1
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
			// Check for escape
			if i > 0 && rest[i-1] != '\\' {
				inQuote = false
			}
		}
	}
	if relEnd < 0 {
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: unmatched bracket")
	}

	relContent := rest[relStart:relEnd]
	afterRel := rest[relEnd+1:]

	// Now find the second node
	var secondNodeStart int
	if isReverse {
		// Expect -(...)
		if !strings.HasPrefix(afterRel, "-(") {
			return "", "", "", false, fmt.Errorf("invalid relationship pattern: expected -( after ]")
		}
		secondNodeStart = 2
	} else {
		// Expect ->(...)
		if !strings.HasPrefix(afterRel, "->(") {
			return "", "", "", false, fmt.Errorf("invalid relationship pattern: expected ->( after ]")
		}
		secondNodeStart = 3
	}

	// Find end of second node
	depth = 1 // We're inside (
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
		return "", "", "", false, fmt.Errorf("invalid relationship pattern: unmatched parenthesis in second node")
	}

	secondNode := afterRel[secondNodeStart:secondNodeEnd]

	return firstNode, relContent, secondNode, isReverse, nil
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

	// Find RETURN clause if present (only in last block typically)
	returnIdx := findKeywordIndex(createPart, "RETURN")
	var returnPart string
	if returnIdx > 0 {
		returnPart = strings.TrimSpace(createPart[returnIdx+6:])
		createPart = strings.TrimSpace(createPart[:returnIdx])
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

	// Split by MATCH keyword to handle comma-separated patterns in MATCH
	matchRe := regexp.MustCompile(`(?i)\bMATCH\s+`)
	matchClauses := matchRe.Split(matchPart, -1)

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

		// Find all matched nodes from MATCH clause
		for _, pattern := range patterns {
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

			// Filter by properties
			for _, node := range candidates {
				if e.nodeMatchesProps(node, nodeInfo.properties) {
					nodeVars[nodeInfo.variable] = node
					break
				}
			}
		}
	}

	// Split CREATE part into individual CREATE statements
	createRe := regexp.MustCompile(`(?i)\bCREATE\s+`)
	createClauses := createRe.Split(createPart, -1)

	for _, clause := range createClauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}

		// Check if this is a relationship pattern or node pattern
		if strings.Contains(clause, "->") || strings.Contains(clause, "<-") || strings.Contains(clause, "]-") {
			// Relationship pattern: (var)-[:TYPE]->(var)
			err := e.processCreateRelationship(clause, nodeVars, edgeVars, result)
			if err != nil {
				return nil, err
			}
		} else {
			// Node pattern: (var:Label {props})
			nodePatterns := e.splitNodePatterns(clause)
			for _, np := range nodePatterns {
				np = strings.TrimSpace(np)
				if np == "" {
					continue
				}
				err := e.processCreateNode(np, nodeVars, result)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// Copy new vars back to allNodeVars for use in later blocks
	for k, v := range nodeVars {
		allNodeVars[k] = v
	}
	for k, v := range edgeVars {
		allEdgeVars[k] = v
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
	// Use regex that handles optional variable and properties

	// Try forward arrow first: (a)-[...]->(b)
	relPattern := regexp.MustCompile(`\((\w+)\)\s*-\[(\w*)(?::(\w+))?(?:\s*(\{[^}]*\}))?\]\s*->\s*\((\w+)\)`)
	matches := relPattern.FindStringSubmatch(pattern)

	isReverse := false
	if matches == nil {
		// Try reverse arrow: (a)<-[...]-(b)
		relPattern = regexp.MustCompile(`\((\w+)\)\s*<-\[(\w*)(?::(\w+))?(?:\s*(\{[^}]*\}))?\]\s*-\s*\((\w+)\)`)
		matches = relPattern.FindStringSubmatch(pattern)
		isReverse = true
	}

	if len(matches) < 6 {
		return fmt.Errorf("invalid relationship pattern in CREATE: %s", pattern)
	}

	sourceVar := matches[1]
	relVar := matches[2]
	relType := matches[3]
	relPropsStr := matches[4]
	targetVar := matches[5]

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

	// Look up source and target nodes
	sourceNode, sourceExists := nodeVars[sourceVar]
	targetNode, targetExists := nodeVars[targetVar]

	if !sourceExists {
		return fmt.Errorf("variable '%s' not found in MATCH results (have: %v)",
			sourceVar, getKeys(nodeVars))
	}
	if !targetExists {
		return fmt.Errorf("variable '%s' not found in MATCH results (have: %v)",
			targetVar, getKeys(nodeVars))
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

// executeCompoundCreateWithDelete handles CREATE ... WITH ... DELETE queries.
// This pattern creates a node/relationship, passes it through WITH, then deletes it.
// Example: CREATE (t:TestNode {name: 'temp'}) WITH t DELETE t RETURN count(t)
func (e *StorageExecutor) executeCompoundCreateWithDelete(ctx context.Context, cypher string) (*ExecuteResult, error) {
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

	// Execute the CREATE part first
	createResult, err := e.executeCreate(ctx, createPart)
	if err != nil {
		return nil, fmt.Errorf("CREATE failed: %w", err)
	}
	result.Stats.NodesCreated = createResult.Stats.NodesCreated
	result.Stats.RelationshipsCreated = createResult.Stats.RelationshipsCreated

	// Parse the created variable from CREATE pattern
	// e.g., CREATE (t:TestNode...) -> variable is "t"
	// e.g., CREATE (a)-[r:KNOWS]->(b) -> relationship variable is "r"
	createdVars := make(map[string]*storage.Node)
	createdEdges := make(map[string]*storage.Edge)

	// Parse all node patterns from CREATE - find all (varName:Label) patterns
	nodePattern := regexp.MustCompile(`\((\w+)(?::(\w+))?`)
	allNodeMatches := nodePattern.FindAllStringSubmatch(createPart, -1)
	for _, matches := range allNodeMatches {
		if len(matches) > 1 {
			varName := matches[1]
			labelName := ""
			if len(matches) > 2 && matches[2] != "" {
				labelName = matches[2]
			}

			// Find the created node by label
			if labelName != "" {
				nodes, _ := e.storage.GetNodesByLabel(labelName)
				if len(nodes) > 0 {
					// Get the most recently created node with this label
					createdVars[varName] = nodes[len(nodes)-1]
				}
			}
		}
	}

	// Parse relationship variable if present: [r:TYPE] or [r] or [:TYPE]
	// Pattern: [varName:TYPE] where varName is optional
	relPattern := regexp.MustCompile(`\[(\w+)(?::(\w+))?\]`)
	if matches := relPattern.FindStringSubmatch(createPart); len(matches) > 1 {
		relVar := matches[1]
		// Check if first capture is actually a variable (not a type without variable)
		// If there's a colon, first part is variable, second is type
		// If no colon and first part exists, it could be variable or type
		if relVar != "" {
			edges, _ := e.storage.AllEdges()
			if len(edges) > 0 {
				createdEdges[relVar] = edges[len(edges)-1]
			}
		}
	}

	// Parse WITH clause to see what variables are passed through
	withVars := strings.Split(withPart, ",")
	for i := range withVars {
		withVars[i] = strings.TrimSpace(withVars[i])
	}

	// Execute DELETE
	deleteTarget := strings.TrimSpace(deletePart)
	if node, exists := createdVars[deleteTarget]; exists {
		// Delete the node (and any relationships)
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

// executeMerge handles MERGE queries with ON CREATE SET / ON MATCH SET support.
// This implements Neo4j-compatible MERGE semantics:
// 1. Try to find an existing node matching the pattern
// 2. If found, apply ON MATCH SET if present
// 3. If not found, create the node and apply ON CREATE SET if present
