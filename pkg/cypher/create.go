// CREATE clause implementation for NornicDB.
// This file contains CREATE execution for nodes and relationships.

package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
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
	store := e.getStorage(ctx)

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
	createdEdges := make(map[string]*storage.Edge)
	createdPaths := make(map[string]PathResult)
	for _, nodePatternStr := range nodePatterns {
		nodePatternStr = strings.TrimSpace(nodePatternStr)
		if nodePatternStr == "" {
			continue
		}
		if err := e.validateCreatePatternPropertyMap(ctx, nodePatternStr); err != nil {
			return nil, err
		}

		nodePattern := e.parseNodePattern(ctx, nodePatternStr)

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

		actualID, err := store.CreateNode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to create node: %w", err)
		}
		if actualID != "" {
			node.ID = actualID
		}
		e.notifyNodeMutated(string(node.ID))

		result.Stats.NodesCreated++
		addOptimisticNodeID(result, node.ID)

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
		pathVar, currentPattern := parseCreatePathAssignment(relPatternStr)
		var pathNodes []*storage.Node
		var pathEdges []*storage.Edge
		for currentPattern != "" {
			// Parse the relationship pattern: (varA)-[:TYPE {props}]->(varB)
			sourceContent, relStr, targetContent, isReverse, remainder, err := e.parseCreateRelPatternWithVars(currentPattern)
			if err != nil {
				return nil, err
			}

			// Parse node patterns first to get variable names for lookup
			sourcePattern := e.parseNodePattern(ctx, "("+sourceContent+")")
			targetPattern := e.parseNodePattern(ctx, "("+targetContent+")")

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
				actualID, err := store.CreateNode(sourceNode)
				if err != nil {
					return nil, fmt.Errorf("failed to create source node: %w", err)
				}
				if actualID != "" {
					sourceNode.ID = actualID
				}
				e.notifyNodeMutated(string(sourceNode.ID))
				result.Stats.NodesCreated++
				addOptimisticNodeID(result, sourceNode.ID)
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
				actualID, err := store.CreateNode(targetNode)
				if err != nil {
					return nil, fmt.Errorf("failed to create target node: %w", err)
				}
				if actualID != "" {
					targetNode.ID = actualID
				}
				e.notifyNodeMutated(string(targetNode.ID))
				result.Stats.NodesCreated++
				addOptimisticNodeID(result, targetNode.ID)
				if targetPattern.variable != "" {
					createdNodes[targetPattern.variable] = targetNode
				}
			}

			// Parse relationship type and properties
			relType, relProps := e.parseRelationshipTypeAndProps(ctx, relStr)

			// Extract relationship variable if present (e.g., "r:TYPE" -> "r").
			relVar := ""
			if colonIdx := strings.Index(relStr, ":"); colonIdx > 0 {
				relVar = strings.TrimSpace(relStr[:colonIdx])
			} else if !strings.Contains(relStr, "{") {
				// No colon and no props - entire string might be variable
				relVar = strings.TrimSpace(relStr)
			}

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
			if err := store.CreateEdge(edge); err != nil {
				return nil, fmt.Errorf("failed to create relationship: %w", err)
			}
			if relVar != "" {
				createdEdges[relVar] = edge
			}
			if pathVar != "" {
				if len(pathNodes) == 0 {
					pathNodes = append(pathNodes, startNode)
				}
				pathEdges = append(pathEdges, edge)
				pathNodes = append(pathNodes, endNode)
			}
			result.Stats.RelationshipsCreated++
			addOptimisticRelationshipID(result, edge.ID)

			// If there's more chain to process, continue with target as new source
			if remainder != "" && (strings.HasPrefix(remainder, "-[") || strings.HasPrefix(remainder, "<-[")) {
				// Build the next pattern: (targetContent) + remainder
				currentPattern = "(" + targetContent + ")" + remainder
			} else {
				currentPattern = ""
			}
		}

		if pathVar != "" {
			createdPaths[pathVar] = PathResult{
				Nodes:         pathNodes,
				Relationships: pathEdges,
				Length:        len(pathEdges),
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

			// Path variables first (RETURN p, nodes(p), relationships(p), length(p))
			if varName := extractVariableNameFromReturnItem(item.expr); varName != "" {
				if path, ok := createdPaths[varName]; ok {
					row[i] = e.pathToValue(path, item.expr, varName)
					continue
				}
			}

			// Relationship variables next (RETURN r, r.prop, id(r), type(r), ...)
			//
			// This matches Neo4j expectations that `r` is returned as a relationship
			// structure and can be used in functions like id(r)/type(r).
			if varName := extractVariableNameFromReturnItem(item.expr); varName != "" {
				if edge, ok := createdEdges[varName]; ok && edge != nil {
					// Direct relationship reference.
					if item.expr == varName {
						row[i] = edge
						continue
					}
					// Relationship property access: r.someProp
					if strings.HasPrefix(item.expr, varName+".") {
						propName := strings.TrimSpace(item.expr[len(varName)+1:])
						if v, ok := edge.Properties[propName]; ok {
							row[i] = v
						} else {
							row[i] = nil
						}
						continue
					}
					// Functions over relationships (id(r), type(r), properties(r), ...)
					row[i] = e.evaluateExpressionWithContext(ctx, item.expr, createdNodes, createdEdges)
					continue
				}
			}

			// Resolve the return expression against the created variables.
			//
			// The RETURN item may be:
			// - variable: a
			// - property access: a.name
			// - function call: id(a), elementId(a), labels(a)
			// - other expressions that reference a single variable
			varName := extractVariableNameFromReturnItem(item.expr)
			if varName != "" {
				if node, ok := createdNodes[varName]; ok {
					row[i] = e.resolveReturnItem(ctx, item, varName, node)
					continue
				}
			}

			// Fallback: keep previous prefix-based behavior.
			for variable, node := range createdNodes {
				if strings.HasPrefix(item.expr, variable) || item.expr == variable {
					row[i] = e.resolveReturnItem(ctx, item, variable, node)
					break
				}
			}
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil
}

func (e *StorageExecutor) validateCreatePatternPropertyMap(ctx context.Context, pattern string) error {
	propsStart := strings.Index(pattern, "{")
	if propsStart < 0 {
		return nil
	}
	propsEnd := strings.LastIndex(pattern, "}")
	if propsEnd < propsStart {
		return fmt.Errorf("invalid property map syntax in pattern: %s", pattern)
	}
	propsLiteral := strings.TrimSpace(pattern[propsStart : propsEnd+1])
	if _, err := e.parseSetMergeMapLiteralStrict(ctx, propsLiteral); err != nil {
		return fmt.Errorf("invalid property map syntax in pattern: %w", err)
	}
	return nil
}

func parseCreatePathAssignment(pattern string) (string, string) {
	trimmed := strings.TrimSpace(pattern)
	eqIdx := strings.Index(trimmed, "=")
	if eqIdx <= 0 {
		return "", pattern
	}
	left := strings.TrimSpace(trimmed[:eqIdx])
	right := strings.TrimSpace(trimmed[eqIdx+1:])
	if left == "" || !isValidIdentifier(left) || !strings.HasPrefix(right, "(") {
		return "", pattern
	}
	return left, right
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
	store := e.getStorage(ctx)

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

		nodePattern := e.parseNodePattern(ctx, nodePatternStr)

		// Create the node
		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     nodePattern.labels,
			Properties: nodePattern.properties,
		}

		actualID, err := store.CreateNode(node)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create node: %w", err)
		}
		if actualID != "" {
			node.ID = actualID
		}
		e.notifyNodeMutated(string(node.ID))

		result.Stats.NodesCreated++
		addOptimisticNodeID(result, node.ID)

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
			sourcePattern := e.parseNodePattern(ctx, "("+sourceContent+")")
			targetPattern := e.parseNodePattern(ctx, "("+targetContent+")")

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
				actualID, err := store.CreateNode(sourceNode)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to create source node: %w", err)
				}
				if actualID != "" {
					sourceNode.ID = actualID
				}
				e.notifyNodeMutated(string(sourceNode.ID))
				result.Stats.NodesCreated++
				addOptimisticNodeID(result, sourceNode.ID)
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
				actualID, err := store.CreateNode(targetNode)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to create target node: %w", err)
				}
				if actualID != "" {
					targetNode.ID = actualID
				}
				e.notifyNodeMutated(string(targetNode.ID))
				result.Stats.NodesCreated++
				addOptimisticNodeID(result, targetNode.ID)
				if targetPattern.variable != "" {
					createdNodes[targetPattern.variable] = targetNode
				}
			}

			// Parse relationship type and properties
			relType, relProps := e.parseRelationshipTypeAndProps(ctx, relStr)

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

			if err := store.CreateEdge(edge); err != nil {
				return nil, nil, nil, fmt.Errorf("failed to create relationship: %w", err)
			}

			if relVar != "" {
				createdEdges[relVar] = edge
			}
			result.Stats.RelationshipsCreated++
			addOptimisticRelationshipID(result, edge.ID)

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
				// Fast path: direct variable/property prefix match.
				if strings.HasPrefix(item.expr, variable) || item.expr == variable {
					row[i] = e.resolveReturnItem(ctx, item, variable, node)
					break
				}
			}

			// If not resolved by prefix match, try extracting the referenced variable
			// (covers function calls like id(a), elementId(a), etc.).
			if row[i] == nil {
				if varName := extractVariableNameFromReturnItem(item.expr); varName != "" {
					if node, ok := createdNodes[varName]; ok {
						row[i] = e.resolveReturnItem(ctx, item, varName, node)
					}
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
	braceDepth := 0

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
		case '{':
			braceDepth++
			current.WriteByte(c)
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
			current.WriteByte(c)
		case '(':
			if braceDepth == 0 {
				depth++
				current.WriteByte(c)
			} else {
				current.WriteByte(c)
			}
		case ')':
			if braceDepth == 0 {
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
			} else {
				current.WriteByte(c)
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
				continue
			}
			// Preserve path assignment prefixes like "p=(:A)-[:R]->(:B)".
			// We drop whitespace, but keep identifiers and "=" before the first "(".
			if depth == 0 && !inRelationship {
				if isWordChar(byte(c)) || c == '=' {
					current.WriteByte(c)
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
	braceDepth := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if (c == '\'' || c == '"') && (i == 0 || pattern[i-1] != '\\') {
			if !inString {
				inString = true
				stringChar = c
			} else if c == stringChar {
				inString = false
				stringChar = 0
			}
			if depth > 0 {
				current.WriteByte(c)
			}
			continue
		}
		if inString {
			if depth > 0 {
				current.WriteByte(c)
			}
			continue
		}
		switch c {
		case '{':
			if depth > 0 {
				braceDepth++
				current.WriteByte(c)
			}
		case '}':
			if depth > 0 {
				if braceDepth > 0 {
					braceDepth--
				}
				current.WriteByte(c)
			}
		case '(':
			if braceDepth == 0 {
				depth++
				current.WriteByte(c)
			} else {
				current.WriteByte(c)
			}
		case ')':
			if braceDepth == 0 {
				depth--
				current.WriteByte(c)
				if depth == 0 {
					patterns = append(patterns, current.String())
					current.Reset()
				}
			} else {
				current.WriteByte(c)
			}
		case ',':
			if depth == 0 {
				// Skip comma between patterns
				continue
			}
			current.WriteByte(c)
		default:
			if depth > 0 {
				current.WriteByte(c)
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
func (e *StorageExecutor) parseRelationshipTypeAndProps(ctx context.Context, relStr string) (string, map[string]interface{}) {
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
			relProps = e.parseProperties(ctx, relStr[propsStart:propsEnd+1])
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
	// Substitute parameters AFTER routing to avoid keyword detection issues.
	// If the query includes SET += (map merge), keep the SET segment intact
	// so $props can be resolved from context instead of string-substitution.
	if params := getParamsFromContext(ctx); params != nil {
		if containsKeywordOutsideStrings(cypher, "SET") && strings.Contains(cypher, "+=") {
			setIdx := findKeywordIndex(cypher, "SET")
			if setIdx >= 0 {
				returnIdx := findKeywordIndex(cypher, "RETURN")
				prefix := cypher[:setIdx]
				setSegment := ""
				suffix := ""
				if returnIdx > setIdx {
					setSegment = cypher[setIdx:returnIdx]
					suffix = cypher[returnIdx:]
				} else {
					setSegment = cypher[setIdx:]
				}
				prefix = e.substituteParams(prefix, params)
				if suffix != "" {
					suffix = e.substituteParams(suffix, params)
				}
				cypher = prefix + setSegment + suffix
			} else {
				cypher = e.substituteParams(cypher, params)
			}
		} else {
			cypher = e.substituteParams(cypher, params)
		}
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

		// Copy Columns and Rows from last block with RETURN
		if len(blockResult.Columns) > 0 {
			result.Columns = blockResult.Columns
			result.Rows = append(result.Rows, blockResult.Rows...)
		}
	}

	return result, nil
}

// splitMatchCreateBlocks splits a query into independent MATCH...CREATE blocks
// Consecutive MATCH clauses without intervening CREATE are merged into a single block
func (e *StorageExecutor) splitMatchCreateBlocks(cypher string) []string {
	var blocks []string

	// Find all MATCH and CREATE keyword positions
	matchPositions := findAllKeywordPositions(cypher, "MATCH")
	createPositions := findAllKeywordPositions(cypher, "CREATE")

	if len(matchPositions) == 0 {
		return []string{cypher}
	}

	// If there's only one MATCH or no CREATEs, return as single block
	if len(matchPositions) == 1 || len(createPositions) == 0 {
		return []string{cypher}
	}

	// Group consecutive MATCHes that share a CREATE
	// A new block starts when there's a CREATE between two MATCH positions
	var blockStarts []int
	blockStarts = append(blockStarts, matchPositions[0])

	for i := 1; i < len(matchPositions); i++ {
		prevMatchPos := matchPositions[i-1]
		currMatchPos := matchPositions[i]

		// Check if there's a CREATE between the previous MATCH and current MATCH
		hasCreateBetween := false
		for _, createPos := range createPositions {
			if createPos > prevMatchPos && createPos < currMatchPos {
				hasCreateBetween = true
				break
			}
		}

		// Only start a new block if there was a CREATE between MATCHes
		if hasCreateBetween {
			blockStarts = append(blockStarts, currMatchPos)
		}
	}

	// Create blocks from start positions
	for i, startPos := range blockStarts {
		var endPos int
		if i+1 < len(blockStarts) {
			endPos = blockStarts[i+1]
		} else {
			endPos = len(cypher)
		}

		block := strings.TrimSpace(cypher[startPos:endPos])
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
	store := e.getStorage(ctx)

	// Split into MATCH and CREATE parts
	createIdx := findKeywordIndex(block, "CREATE")
	if createIdx < 0 {
		// No CREATE in this block, just MATCH - skip
		return result, nil
	}

	matchPart := strings.TrimSpace(block[:createIdx])
	createPart := strings.TrimSpace(block[createIdx:]) // Keep "CREATE" for splitting

	// allCombinations is filled either by the MATCH+WITH pipeline (when WITH is non-trivial) or by MATCH parsing below
	var allCombinations []map[string]*storage.Node

	// Extract WITH clause with LIMIT/SKIP from matchPart (handles MATCH ... WITH ... LIMIT 1 CREATE ...)
	// When WITH is non-trivial (projections like expr AS var, collect, UNWIND, etc.), we run the full
	// pipeline and pass its bindings into CREATE so variables such as "pharmacy" from WITH are available.
	var withLimit, withSkip int
	fullMatchPart := matchPart
	if withInMatch := findKeywordIndex(matchPart, "WITH"); withInMatch > 0 {
		withPart := strings.TrimSpace(matchPart[withInMatch:])
		// Extract LIMIT and SKIP from WITH clause (for simple WITH ... LIMIT n CREATE)
		limitIdx := findKeywordIndex(withPart, "LIMIT")
		skipIdx := findKeywordIndex(withPart, "SKIP")

		if limitIdx > 0 {
			limitPart := strings.TrimSpace(withPart[limitIdx+5:])
			if fields := strings.Fields(limitPart); len(fields) > 0 {
				if l, err := strconv.Atoi(fields[0]); err == nil {
					withLimit = l
				}
			}
		}
		if skipIdx > 0 && (limitIdx < 0 || skipIdx < limitIdx) {
			skipPart := strings.TrimSpace(withPart[skipIdx+4:])
			if fields := strings.Fields(skipPart); len(fields) > 0 {
				if s, err := strconv.Atoi(fields[0]); err == nil {
					withSkip = s
				}
			}
		}

		// Check if WITH is non-trivial (has AS, collect, UNWIND, or multiple WITH) — then run pipeline
		createVars := extractCreateVariableRefs(createPart)
		hasNonTrivialWith := strings.Contains(strings.ToUpper(withPart), " AS ") ||
			strings.Contains(strings.ToUpper(withPart), "COLLECT(") ||
			strings.Contains(strings.ToUpper(withPart), "UNWIND ") ||
			findKeywordIndex(withPart, "WITH") > 0
		if hasNonTrivialWith && len(createVars) > 0 {
			// Run MATCH+WITH pipeline and use its rows to drive CREATE (so WITH bindings like "pharmacy" are available)
			pipelineRows, pipelineErr := e.executeMatchWithPipelineToRows(ctx, fullMatchPart, createVars, store)
			if pipelineErr == nil && len(pipelineRows) > 0 {
				allCombinations = make([]map[string]*storage.Node, 0, len(pipelineRows))
				for _, row := range pipelineRows {
					combo := make(map[string]*storage.Node)
					for k, v := range row {
						if node, ok := v.(*storage.Node); ok {
							combo[k] = node
						}
					}
					if len(combo) > 0 {
						allCombinations = append(allCombinations, combo)
					}
				}
			}
		}

		// Strip WITH clause from matchPart for building combinations from MATCH only (when not using pipeline)
		if len(allCombinations) == 0 {
			matchPart = strings.TrimSpace(matchPart[:withInMatch])
		}
	}

	// Find WITH clause (for MATCH...CREATE...WITH...DELETE pattern)
	withIdx := findKeywordIndex(createPart, "WITH")
	deleteIdx := findKeywordIndex(createPart, "DELETE")
	var deleteTarget string
	hasWithDelete := withIdx > 0 && deleteIdx > withIdx
	hasDirectDelete := deleteIdx > 0 && withIdx <= 0 // DELETE without WITH in createPart

	// Find RETURN clause if present (only in last block typically)
	// Note: findKeywordIndex returns -1 if not found, 0 if at start
	returnIdx := findKeywordIndex(createPart, "RETURN")
	var returnPart string
	if returnIdx >= 0 {
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

	// Extract SET clause (e.g., MATCH ... CREATE ... SET r += $props)
	setIdx := findKeywordIndex(createPart, "SET")
	var setPart string
	if setIdx > 0 {
		setPart = strings.TrimSpace(createPart[setIdx+3:])
		createPart = strings.TrimSpace(createPart[:setIdx])
	}

	// Parse all node patterns from MATCH clause and find matching nodes (skip when pipeline already filled allCombinations)
	nodeVars := make(map[string]*storage.Node)
	for k, v := range allNodeVars {
		nodeVars[k] = v
	}
	edgeVars := make(map[string]*storage.Edge)
	for k, v := range allEdgeVars {
		edgeVars[k] = v
	}

	if len(allCombinations) == 0 {
		// Collect all patterns and their matching nodes for cartesian product.
		// Parse per-MATCH WHERE: each "MATCH ... WHERE ..." segment keeps its own WHERE
		// so we do not truncate matchPart after the first WHERE (which would drop
		// subsequent MATCH clauses and leave variables like 'a' or 'b' unbound).
		hadMatchPatterns := false
		anyPatternUnmatched := false
		patternMatches := make([]struct {
			variable string
			nodes    []*storage.Node
		}, 0)
		var postFilterWhere string // WHERE that references multiple vars in one segment, applied after cartesian product

		// Split by MATCH keyword; matchPart is left intact (includes all WHERE clauses)
		matchClauses := matchKeywordPattern.Split(matchPart, -1)
		nonEmptyMatchClauses := make([]string, 0, len(matchClauses))
		for _, clause := range matchClauses {
			clause = strings.TrimSpace(clause)
			if clause != "" {
				nonEmptyMatchClauses = append(nonEmptyMatchClauses, clause)
			}
		}

		// Fast path: resolve nodes directly from WHERE elementId/id equality
		// predicates. This avoids label scans for the common "MATCH by id +
		// CREATE rel" pattern (e.g. MATCH (a), (b) WHERE elementId(a) = $x
		// AND elementId(b) = $y CREATE (a)-[:REL]->(b)).
		if len(allNodeVars) == 0 && len(allEdgeVars) == 0 {
			if combo, ok := e.tryResolveMatchNodesByIDFromWhere(ctx, nonEmptyMatchClauses, getParamsFromContext(ctx)); ok {
				allCombinations = []map[string]*storage.Node{combo}
				hadMatchPatterns = true
			}
		}

		if len(allNodeVars) == 0 && len(allEdgeVars) == 0 && len(nonEmptyMatchClauses) == 1 && allCombinations == nil {
			shortcutSafe := true
			if whereIdx := findKeywordIndex(nonEmptyMatchClauses[0], "WHERE"); whereIdx > 0 {
				whereClause := strings.TrimSpace(nonEmptyMatchClauses[0][whereIdx+5:])
				for _, term := range splitTopLevelAndCartesian(whereClause) {
					if _, _, _, ok := parseNotRelationshipExistenceTerm(strings.TrimSpace(term)); ok {
						shortcutSafe = false
						break
					}
				}
			}
			if shortcutSafe {
				clauseMatches, _, err := e.executeMatchForContext(ctx, "MATCH "+nonEmptyMatchClauses[0])
				if err != nil {
					return nil, err
				}
				allCombinations = clauseMatches
				hadMatchPatterns = true
			}
		}

		for _, clause := range nonEmptyMatchClauses {
			if allCombinations != nil {
				break
			}

			// Extract this segment's WHERE (only the first WHERE in this segment)
			whereForClause := ""
			if whereIdx := findKeywordIndex(clause, "WHERE"); whereIdx > 0 {
				whereForClause = strings.TrimSpace(clause[whereIdx+5:])
				clause = strings.TrimSpace(clause[:whereIdx])
			}

			// Split by comma but respect parentheses
			patterns := e.splitNodePatterns(clause)
			numPatternsInSegment := 0
			for _, p := range patterns {
				if strings.TrimSpace(p) != "" {
					hadMatchPatterns = true
					numPatternsInSegment++
				}
			}
			addedClauseWhereToPostFilter := false
			if whereForClause != "" && numPatternsInSegment > 1 {
				if postFilterWhere != "" {
					postFilterWhere = postFilterWhere + " AND " + whereForClause
				} else {
					postFilterWhere = whereForClause
				}
				addedClauseWhereToPostFilter = true
			}

			// Collect ALL matched nodes from this MATCH clause (for cartesian product)
			for _, pattern := range patterns {
				pattern = strings.TrimSpace(pattern)
				if pattern == "" {
					continue
				}

				nodeInfo := e.parseNodePattern(ctx, pattern)
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
					matchingNodes, _ = store.GetNodesByLabel(nodeInfo.labels[0])
				} else {
					matchingNodes, _ = store.AllNodes()
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

				// Apply per-node WHERE only when the predicate references this variable.
				// Predicates involving other variables must be evaluated post-cartesian.
				applyWherePerNode := false
				if whereForClause != "" && numPatternsInSegment == 1 {
					if whereClauseReferencesOnlyVariable(whereForClause, nodeInfo.variable) {
						applyWherePerNode = true
					} else if !addedClauseWhereToPostFilter {
						if postFilterWhere != "" {
							postFilterWhere = postFilterWhere + " AND " + whereForClause
						} else {
							postFilterWhere = whereForClause
						}
						addedClauseWhereToPostFilter = true
					}
				}

				if applyWherePerNode {
					var filtered []*storage.Node
					for _, node := range matchingNodes {
						if e.evaluateWhere(ctx, node, nodeInfo.variable, whereForClause) {
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
				} else {
					anyPatternUnmatched = true
				}
			}
		}

		// Build combinations of all pattern matches.
		// Prefer a join-aware construction for common equality-join WHERE shapes
		// to avoid full cartesian expansion in MATCH...CREATE hot paths.
		if anyPatternUnmatched {
			allCombinations = []map[string]*storage.Node{}
		} else {
			// Push down selective multi-variable WHERE predicates before
			// cartesian expansion to avoid combinatorial blow-ups on join-shapes.
			if postFilterWhere != "" && len(patternMatches) > 1 {
				patternMatches = e.applyCartesianWherePushdown(patternMatches, postFilterWhere)
				if joined, ok := e.buildCombinationsUsingWhereJoin(patternMatches, postFilterWhere); ok {
					allCombinations = joined
				}
			}
			if allCombinations == nil {
				allCombinations = e.buildCartesianProduct(patternMatches)
			}
		}

		// Apply WITH LIMIT/SKIP if present (from MATCH ... WITH ... LIMIT ... CREATE pattern)
		// This limits how many matched rows are processed before CREATE
		if withLimit > 0 || withSkip > 0 {
			startIdx := withSkip
			if startIdx > len(allCombinations) {
				startIdx = len(allCombinations)
			}
			endIdx := len(allCombinations)
			if withLimit > 0 && startIdx+withLimit < endIdx {
				endIdx = startIdx + withLimit
			}
			if startIdx < len(allCombinations) {
				allCombinations = allCombinations[startIdx:endIdx]
			} else {
				allCombinations = []map[string]*storage.Node{} // No matches after SKIP
			}
		}

		// Apply post-filter WHERE (multi-variable conditions from a single MATCH segment) if present
		if postFilterWhere != "" {
			relChecks := make([]struct {
				startVar string
				edgeType string
				endVar   string
			}, 0, 2)
			remainingTerms := make([]string, 0, 4)
			for _, term := range splitTopLevelAndCartesian(postFilterWhere) {
				term = strings.TrimSpace(term)
				if term == "" {
					continue
				}
				if sv, et, ev, ok := parseNotRelationshipExistenceTerm(term); ok {
					relChecks = append(relChecks, struct {
						startVar string
						edgeType string
						endVar   string
					}{startVar: sv, edgeType: et, endVar: ev})
					continue
				}
				remainingTerms = append(remainingTerms, term)
			}
			remainingWhere := strings.Join(remainingTerms, " AND ")

			var filtered []map[string]*storage.Node
			for _, combination := range allCombinations {
				keep := true
				if remainingWhere != "" {
					keep = e.evaluateWhereForContext(ctx, remainingWhere, combination)
				}
				if keep && len(relChecks) > 0 {
					for _, rc := range relChecks {
						startNode := combination[rc.startVar]
						endNode := combination[rc.endVar]
						if startNode == nil || endNode == nil {
							keep = false
							break
						}
						exists, err := hasRelationshipOfType(store, startNode.ID, endNode.ID, rc.edgeType)
						if err != nil {
							keep = false
							break
						}
						if exists {
							keep = false
							break
						}
					}
				}
				if keep {
					filtered = append(filtered, combination)
				}
			}
			allCombinations = filtered
		}

		// If no combinations, fall back to using existing nodeVars
		if len(allCombinations) == 0 {
			// openCypher semantics: MATCH producing zero rows short-circuits the CREATE path
			// and returns no rows unless RETURN is aggregation-only (then one row).
			if hadMatchPatterns {
				if returnPart != "" {
					returnItems := e.parseReturnItems(returnPart)
					result.Columns = make([]string, len(returnItems))
					for i, item := range returnItems {
						if item.alias != "" {
							result.Columns[i] = item.alias
						} else {
							result.Columns[i] = item.expr
						}
					}
					if len(returnItems) > 0 {
						allAggregates := true
						row := make([]interface{}, len(returnItems))
						for i, item := range returnItems {
							if !isAggregateFunc(item.expr) {
								allAggregates = false
								break
							}
							if isAggregateFuncName(item.expr, "count") {
								row[i] = int64(0)
							} else {
								row[i] = nil
							}
						}
						if allAggregates {
							result.Rows = [][]interface{}{row}
						}
					}
				}
				return result, nil
			}
			allCombinations = []map[string]*storage.Node{nodeVars}
		}
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
			err := e.processCreateNode(ctx, np, combinedNodeVars, result, store)
			if err != nil {
				return nil, err
			}
		}

		// PASS 2: Create all relationships (now nodes are available)
		for _, rp := range allRelPatterns {
			err := e.processCreateRelationship(ctx, rp, combinedNodeVars, edgeVars, result, store)
			if err != nil {
				return nil, err
			}
		}

		// Apply SET clause after CREATE (supports chained/mixed SET forms).
		if setPart != "" {
			setPart = collapseChainedSetClauses(setPart)
			setPartForAssignments := setPart
			if params := getParamsFromContext(ctx); params != nil && !strings.Contains(setPart, "+=") {
				setPartForAssignments = e.substituteParams(setPart, params)
			}
			assignments := e.splitSetAssignmentsRespectingBrackets(setPartForAssignments)
			for _, assignment := range assignments {
				assignment = strings.TrimSpace(assignment)
				if assignment == "" {
					continue
				}
				if strings.Contains(assignment, "+=") {
					if err := e.applySetMergeToCreated(ctx, assignment, combinedNodeVars, edgeVars, result, store); err != nil {
						return nil, err
					}
					continue
				}
				eqIdx := strings.Index(assignment, "=")
				if eqIdx == -1 {
					// Label assignment: n:Label
					colonIdx := strings.Index(assignment, ":")
					if colonIdx > 0 {
						varName := strings.TrimSpace(assignment[:colonIdx])
						newLabel := strings.TrimSpace(assignment[colonIdx+1:])
						if len(newLabel) >= 2 && strings.HasPrefix(newLabel, "`") && strings.HasSuffix(newLabel, "`") {
							newLabel = strings.ReplaceAll(newLabel[1:len(newLabel)-1], "``", "`")
						}
						if !isValidIdentifier(newLabel) {
							return nil, fmt.Errorf("invalid label name: %q (must be alphanumeric starting with letter or underscore)", newLabel)
						}
						if containsReservedKeyword(newLabel) {
							return nil, fmt.Errorf("invalid label name: %q (contains reserved keyword)", newLabel)
						}
						if node, exists := combinedNodeVars[varName]; exists {
							if !containsString(node.Labels, newLabel) {
								oldLabels := make([]string, len(node.Labels))
								copy(oldLabels, node.Labels)
								node.Labels = append(node.Labels, newLabel)
								if err := validatePolicyOnLabelChange(store, node, oldLabels); err != nil {
									node.Labels = oldLabels // restore
									return nil, err
								}
								if err := store.UpdateNode(node); err != nil {
									node.Labels = oldLabels // restore
									return nil, fmt.Errorf("failed to add label: %w", err)
								}
								result.Stats.LabelsAdded++
								e.notifyNodeMutated(string(node.ID))
							}
						}
					}
					continue
				}
				leftSide := strings.TrimSpace(assignment[:eqIdx])
				rightSide := strings.TrimSpace(assignment[eqIdx+1:])
				dotIdx := strings.Index(leftSide, ".")
				value := e.parseValue(ctx, rightSide)
				if strings.HasPrefix(rightSide, "$") {
					paramName := strings.TrimSpace(rightSide[1:])
					if paramName == "" {
						return nil, fmt.Errorf("SET assignment requires a valid parameter name after $")
					}
					params := getParamsFromContext(ctx)
					if params == nil {
						return nil, fmt.Errorf("SET assignment parameter $%s requires parameters to be provided", paramName)
					}
					paramValue, exists := params[paramName]
					if !exists {
						return nil, fmt.Errorf("SET assignment parameter $%s not found in provided parameters", paramName)
					}
					value = normalizePropValue(paramValue)
				}
				if dotIdx == -1 {
					varName := strings.TrimSpace(leftSide)
					props, err := normalizePropsMap(value, "SET assignment")
					if err != nil {
						return nil, err
					}
					if node, exists := combinedNodeVars[varName]; exists {
						node.Properties = cloneStringAnyMap(props)
						if err := store.UpdateNode(node); err != nil {
							return nil, fmt.Errorf("failed to replace node properties: %w", err)
						}
						result.Stats.PropertiesSet++
						e.notifyNodeMutated(string(node.ID))
					} else if edge, exists := edgeVars[varName]; exists {
						edge.Properties = cloneStringAnyMap(props)
						if err := store.UpdateEdge(edge); err != nil {
							return nil, fmt.Errorf("failed to replace edge properties: %w", err)
						}
						result.Stats.PropertiesSet++
					} else {
						return nil, fmt.Errorf("unknown variable in SET clause: %s", varName)
					}
					continue
				}

				varName := strings.TrimSpace(leftSide[:dotIdx])
				propName := strings.TrimSpace(leftSide[dotIdx+1:])
				if node, exists := combinedNodeVars[varName]; exists {
					if node.Properties == nil {
						node.Properties = make(map[string]interface{})
					}
					node.Properties[propName] = value
					if err := store.UpdateNode(node); err != nil {
						return nil, fmt.Errorf("failed to update node property: %w", err)
					}
					result.Stats.PropertiesSet++
					e.notifyNodeMutated(string(node.ID))
				} else if edge, exists := edgeVars[varName]; exists {
					if edge.Properties == nil {
						edge.Properties = make(map[string]interface{})
					}
					edge.Properties[propName] = value
					if err := store.UpdateEdge(edge); err != nil {
						return nil, fmt.Errorf("failed to update edge property: %w", err)
					}
					result.Stats.PropertiesSet++
				} else {
					return nil, fmt.Errorf("unknown variable in SET clause: %s", varName)
				}
			}
		}

		// Copy created nodes AND matched nodes back to nodeVars for RETURN clause
		for k, v := range combinedNodeVars {
			nodeVars[k] = v
		}
	}

	// Ensure all matched nodes from last combination are in nodeVars for RETURN
	// (in case there were multiple combinations, use the last one)
	if len(allCombinations) > 0 {
		lastCombination := allCombinations[len(allCombinations)-1]
		for k, v := range lastCombination {
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
			if err := store.DeleteEdge(edge.ID); err == nil {
				result.Stats.RelationshipsDeleted++
			}
		} else if node, exists := nodeVars[deleteTarget]; exists {
			// Delete connected edges first
			outEdges, _ := store.GetOutgoingEdges(node.ID)
			inEdges, _ := store.GetIncomingEdges(node.ID)
			for _, edge := range outEdges {
				if err := store.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
				}
			}
			for _, edge := range inEdges {
				if err := store.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
				}
			}
			if err := store.DeleteNode(node.ID); err == nil {
				result.Stats.NodesDeleted++
				e.removeNodeFromSearch(string(node.ID))
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
			if strings.HasPrefix(upperExpr, "COUNT(") {
				inner := strings.TrimSpace(extractFuncInner(item.expr))
				if inner == "" || inner == "*" {
					row[i] = int64(len(allCombinations))
					continue
				}
				var cnt int64
				for _, combination := range allCombinations {
					if e.evaluateExpressionWithContext(ctx, inner, combination, edgeVars) != nil {
						cnt++
					}
				}
				row[i] = cnt
				continue
			}

			// Check edge variables first (for RETURN e after CREATE relationship)
			found := false
			for varName, edge := range edgeVars {
				if item.expr == varName || strings.HasPrefix(item.expr, varName+".") {
					if item.expr == varName {
						// Return the edge directly
						row[i] = edge
					} else {
						// Return edge property
						propName := item.expr[len(varName)+1:]
						row[i] = edge.Properties[propName]
					}
					found = true
					break
				}
			}
			if found {
				continue
			}

			// Find node variable that matches
			// Check if expression contains the variable (for function calls like id(a))
			found = false
			for varName, node := range nodeVars {
				// Check if expression starts with variable (property access) or contains it (function calls)
				if strings.HasPrefix(item.expr, varName) || strings.Contains(item.expr, "("+varName+")") || strings.Contains(item.expr, "("+varName+" ") {
					row[i] = e.resolveReturnItem(ctx, item, varName, node)
					found = true
					break
				}
			}
			if !found {
				// Expression doesn't match any variable - might be a literal or complex expression
				// Try to evaluate it with empty context (will return nil if can't evaluate)
				row[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeVars, edgeVars)
			}
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil
}

// processCreateNode creates a new node and adds it to the nodeVars map
func (e *StorageExecutor) processCreateNode(ctx context.Context, pattern string, nodeVars map[string]*storage.Node, result *ExecuteResult, store storage.Engine) error {
	nodeInfo := e.parseNodePattern(ctx, pattern)

	// Create the node
	node := &storage.Node{
		ID:         storage.NodeID(e.generateID()),
		Labels:     nodeInfo.labels,
		Properties: nodeInfo.properties,
	}

	actualID, err := store.CreateNode(node)
	if err != nil {
		return fmt.Errorf("failed to create node: %w", err)
	}
	if actualID != "" {
		node.ID = actualID
	}
	e.notifyNodeMutated(string(node.ID))

	result.Stats.NodesCreated++
	addOptimisticNodeID(result, node.ID)

	// Store in nodeVars for later reference
	if nodeInfo.variable != "" {
		nodeVars[nodeInfo.variable] = node
	}

	return nil
}

// processCreateRelationship creates a relationship between nodes in nodeVars
func (e *StorageExecutor) processCreateRelationship(ctx context.Context, pattern string, nodeVars map[string]*storage.Node, edgeVars map[string]*storage.Edge, result *ExecuteResult, store storage.Engine) error {
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
		relProps = e.parseProperties(ctx, relPropsStr)
	} else {
		relProps = make(map[string]interface{})
	}

	// Resolve source node - could be a variable reference or inline node definition
	sourceNode, err := e.resolveOrCreateNode(ctx, sourceContent, nodeVars, result, store)
	if err != nil {
		return fmt.Errorf("failed to resolve source node: %w", err)
	}

	// Resolve target node - could be a variable reference or inline node definition
	targetNode, err := e.resolveOrCreateNode(ctx, targetContent, nodeVars, result, store)
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

	if err := store.CreateEdge(edge); err != nil {
		return fmt.Errorf("failed to create relationship: %w", err)
	}

	result.Stats.RelationshipsCreated++
	addOptimisticRelationshipID(result, edge.ID)

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
func (e *StorageExecutor) resolveOrCreateNode(ctx context.Context, content string, nodeVars map[string]*storage.Node, result *ExecuteResult, store storage.Engine) (*storage.Node, error) {
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
	nodeInfo := e.parseNodePattern(ctx, "("+content+")")

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

	actualID, err := store.CreateNode(node)
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}
	if actualID != "" {
		node.ID = actualID
	}
	e.notifyNodeMutated(string(node.ID))

	result.Stats.NodesCreated++
	addOptimisticNodeID(result, node.ID)

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
	store := e.getStorage(ctx)

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
				if err := store.DeleteEdge(edge.ID); err == nil {
					result.Stats.RelationshipsDeleted++
					delete(createdEdges, varName) // Mark as deleted
				}
			}
		}
		if err := store.DeleteNode(node.ID); err != nil {
			return nil, fmt.Errorf("DELETE failed: %w", err)
		}
		result.Stats.NodesDeleted++
		e.removeNodeFromSearch(string(node.ID))
	} else if edge, exists := createdEdges[deleteTarget]; exists {
		if err := store.DeleteEdge(edge.ID); err != nil {
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
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

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
	setPart = collapseChainedSetClauses(setPart)
	setPartForAssignments := setPart

	// Substitute params for simple SET assignments (e.g., n.prop = $value).
	// For SET += $props, defer to applySetMergeToCreated which reads params directly.
	if params := getParamsFromContext(ctx); params != nil && !strings.Contains(setPart, "+=") {
		setPartForAssignments = e.substituteParams(setPart, params)
	}

	// Pre-validate SET assignments BEFORE executing CREATE
	// This ensures we fail fast and don't create nodes that would be orphaned
	if !strings.Contains(setPartForAssignments, "+=") {
		assignments := e.splitSetAssignmentsRespectingBrackets(setPartForAssignments)
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

	// Handle regular and SET += assignments in order.
	assignments := e.splitSetAssignmentsRespectingBrackets(setPartForAssignments)
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}
		if strings.Contains(assignment, "+=") {
			if err := e.applySetMergeToCreated(ctx, assignment, createdNodes, createdEdges, result, store); err != nil {
				return nil, err
			}
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
				if len(newLabel) >= 2 && strings.HasPrefix(newLabel, "`") && strings.HasSuffix(newLabel, "`") {
					newLabel = strings.ReplaceAll(newLabel[1:len(newLabel)-1], "``", "`")
				}
				if !isValidIdentifier(newLabel) {
					return nil, fmt.Errorf("invalid label name: %q (must be alphanumeric starting with letter or underscore)", newLabel)
				}
				if containsReservedKeyword(newLabel) {
					return nil, fmt.Errorf("invalid label name: %q (contains reserved keyword)", newLabel)
				}
				if node, exists := createdNodes[varName]; exists {
					// Add label to existing node
					if !containsString(node.Labels, newLabel) {
						oldLabels := make([]string, len(node.Labels))
						copy(oldLabels, node.Labels)
						node.Labels = append(node.Labels, newLabel)
						if err := validatePolicyOnLabelChange(store, node, oldLabels); err != nil {
							node.Labels = oldLabels // restore
							return nil, err
						}
						if err := store.UpdateNode(node); err != nil {
							node.Labels = oldLabels // restore
							return nil, fmt.Errorf("failed to add label: %w", err)
						}
						result.Stats.LabelsAdded++
						e.notifyNodeMutated(string(node.ID))
					}
				}
			}
			continue
		}

		leftSide := strings.TrimSpace(assignment[:eqIdx])
		rightSide := strings.TrimSpace(assignment[eqIdx+1:])

		// Parse variable.property
		dotIdx := strings.Index(leftSide, ".")
		value := e.parseValue(ctx, rightSide)
		if strings.HasPrefix(rightSide, "$") {
			paramName := strings.TrimSpace(rightSide[1:])
			if paramName == "" {
				return nil, fmt.Errorf("SET assignment requires a valid parameter name after $")
			}
			params := getParamsFromContext(ctx)
			if params == nil {
				return nil, fmt.Errorf("SET assignment parameter $%s requires parameters to be provided", paramName)
			}
			paramValue, exists := params[paramName]
			if !exists {
				return nil, fmt.Errorf("SET assignment parameter $%s not found in provided parameters", paramName)
			}
			value = normalizePropValue(paramValue)
		}
		if dotIdx == -1 {
			varName := strings.TrimSpace(leftSide)
			props, err := normalizePropsMap(value, "SET assignment")
			if err != nil {
				return nil, err
			}
			if node, exists := createdNodes[varName]; exists {
				node.Properties = cloneStringAnyMap(props)
				if err := store.UpdateNode(node); err != nil {
					return nil, fmt.Errorf("failed to replace node properties: %w", err)
				}
				result.Stats.PropertiesSet++
				e.notifyNodeMutated(string(node.ID))
			} else if edge, exists := createdEdges[varName]; exists {
				edge.Properties = cloneStringAnyMap(props)
				if err := store.UpdateEdge(edge); err != nil {
					return nil, fmt.Errorf("failed to replace edge properties: %w", err)
				}
				result.Stats.PropertiesSet++
			} else {
				return nil, fmt.Errorf("unknown variable in SET clause: %s", varName)
			}
			continue
		}

		varName := strings.TrimSpace(leftSide[:dotIdx])
		propName := strings.TrimSpace(leftSide[dotIdx+1:])

		// Apply to created node or edge
		if node, exists := createdNodes[varName]; exists {
			if node.Properties == nil {
				node.Properties = make(map[string]interface{})
			}
			node.Properties[propName] = value
			if err := store.UpdateNode(node); err != nil {
				return nil, fmt.Errorf("failed to update node property: %w", err)
			}
			result.Stats.PropertiesSet++
			e.notifyNodeMutated(string(node.ID))
		} else if edge, exists := createdEdges[varName]; exists {
			if edge.Properties == nil {
				edge.Properties = make(map[string]interface{})
			}
			edge.Properties[propName] = value
			if err := store.UpdateEdge(edge); err != nil {
				return nil, fmt.Errorf("failed to update edge property: %w", err)
			}
			result.Stats.PropertiesSet++
		} else {
			return nil, fmt.Errorf("unknown variable in SET clause: %s", varName)
		}
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(normalized[returnIdx+6:])
		returnItems := e.parseReturnItems(returnPart)
		row := make([]interface{}, len(returnItems))

		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns = append(result.Columns, item.alias)
			} else {
				result.Columns = append(result.Columns, item.expr)
			}

			// Prefer variable extracted from the return expression (handles elementId(n), n.prop, etc.)
			if varName := extractVariableNameFromReturnItem(item.expr); varName != "" {
				if node, ok := createdNodes[varName]; ok {
					row[i] = e.resolveReturnItem(ctx, item, varName, node)
					continue
				}
				if edge, ok := createdEdges[varName]; ok {
					// Functions like type(r) need context evaluation; fall back to expression eval below.
					if item.expr == varName {
						row[i] = edge
						continue
					}
				}
			}

			// Direct variable match (RETURN n)
			if node, exists := createdNodes[item.expr]; exists {
				row[i] = node
				continue
			}
			if edge, exists := createdEdges[item.expr]; exists {
				row[i] = edge
				continue
			}

			// Evaluate expression with full context (e.g., elementId(n), type(r))
			row[i] = e.evaluateExpressionWithContext(ctx, item.expr, createdNodes, createdEdges)
		}

		result.Rows = [][]interface{}{row}
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
func (e *StorageExecutor) applySetMergeToCreated(ctx context.Context, setPart string, createdNodes map[string]*storage.Node, createdEdges map[string]*storage.Edge, result *ExecuteResult, store storage.Engine) error {
	// Parse: n += {prop: value, ...}
	parts := strings.SplitN(setPart, "+=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid SET += syntax")
	}

	varName := strings.TrimSpace(parts[0])
	propsStr := strings.TrimSpace(parts[1])

	var props map[string]interface{}
	if strings.HasPrefix(propsStr, "$") {
		// Parameter reference: $props
		paramName := strings.TrimSpace(propsStr[1:])
		if paramName == "" {
			return fmt.Errorf("SET += requires a valid parameter name after $")
		}
		params := getParamsFromContext(ctx)
		if params == nil {
			return fmt.Errorf("SET += parameter $%s requires parameters to be provided", paramName)
		}
		paramValue, exists := params[paramName]
		if !exists {
			return fmt.Errorf("SET += parameter $%s not found in provided parameters", paramName)
		}
		propsMap, err := normalizePropsMap(paramValue, fmt.Sprintf("parameter $%s", paramName))
		if err != nil {
			return err
		}
		props = propsMap
	} else if strings.HasPrefix(propsStr, "{") {
		// Inline map literal: {key: value, ...}
		parsedProps, err := e.parseSetMergeMapLiteralStrict(ctx, propsStr)
		if err != nil {
			return fmt.Errorf("failed to parse properties in SET +=: %w", err)
		}
		props = parsedProps
	} else {
		if propsStr == "" {
			return fmt.Errorf("failed to parse properties in SET +=: map literal must be enclosed in { ... }")
		}
		// Map variable source in scope (e.g. SET n += row or SET n += row.properties).
		// CREATE...SET execution does not carry row-scope columns, so resolve from params context.
		sourceVal, found := resolveSetMergeSourceFromParams(getParamsFromContext(ctx), propsStr)
		if !found {
			return fmt.Errorf("SET += map variable %q not found in scope", propsStr)
		}
		propsMap, err := normalizePropsMap(sourceVal, fmt.Sprintf("variable %s", propsStr))
		if err != nil {
			return err
		}
		props = propsMap
	}

	// Apply to node or edge
	if node, exists := createdNodes[varName]; exists {
		for k, v := range props {
			node.Properties[k] = v
			result.Stats.PropertiesSet++
		}
		if err := store.UpdateNode(node); err != nil {
			return fmt.Errorf("failed to update node: %w", err)
		}
		e.notifyNodeMutated(string(node.ID))
	} else if edge, exists := createdEdges[varName]; exists {
		for k, v := range props {
			edge.Properties[k] = v
			result.Stats.PropertiesSet++
		}
		if err := store.UpdateEdge(edge); err != nil {
			return fmt.Errorf("failed to update edge: %w", err)
		}
	} else {
		return fmt.Errorf("unknown variable in SET +=: %s", varName)
	}

	return nil
}

// resolveSetMergeSourceFromParams resolves identifier or dotted-path map sources from params.
// Supported forms:
//   - row
//   - row.properties
func resolveSetMergeSourceFromParams(params map[string]interface{}, source string) (interface{}, bool) {
	if params == nil {
		return nil, false
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, false
	}
	parts := strings.Split(source, ".")
	if len(parts) == 0 {
		return nil, false
	}
	for _, part := range parts {
		if !isValidIdentifier(strings.TrimSpace(part)) {
			return nil, false
		}
	}

	current, ok := params[parts[0]]
	if !ok {
		return nil, false
	}
	for _, part := range parts[1:] {
		switch m := current.(type) {
		case map[string]interface{}:
			next, exists := m[part]
			if !exists {
				return nil, false
			}
			current = next
		case map[interface{}]interface{}:
			next, exists := m[part]
			if !exists {
				return nil, false
			}
			current = next
		default:
			return nil, false
		}
	}
	return current, true
}

// executeMultipleCreates handles queries with multiple CREATE statements.
// Example: CREATE (a:Person {name: "Alice"}) CREATE (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b) RETURN a, b
func (e *StorageExecutor) executeMultipleCreates(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Context to track created nodes and edges for variable references
	nodeContext := make(map[string]*storage.Node)
	edgeContext := make(map[string]*storage.Edge)

	// Split into CREATE segments
	segments := e.splitMultipleCreates(cypher)

	// Process each CREATE segment
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		upperSeg := strings.ToUpper(segment)

		if strings.HasPrefix(upperSeg, "CREATE") {
			createContent := strings.TrimSpace(segment[6:])

			// Check if this is a relationship CREATE
			if strings.Contains(createContent, "-[") || strings.Contains(createContent, "]-") {
				// Relationship CREATE - need to resolve variable references
				err := e.executeCreateRelSegment(ctx, segment, nodeContext, edgeContext, result)
				if err != nil {
					return nil, fmt.Errorf("relationship CREATE failed: %w", err)
				}
			} else {
				// Node CREATE
				node, varName, err := e.executeCreateNodeSegment(ctx, segment)
				if err != nil {
					return nil, fmt.Errorf("node CREATE failed: %w", err)
				}
				if node != nil && varName != "" {
					nodeContext[varName] = node
					result.Stats.NodesCreated++
					addOptimisticNodeID(result, node.ID)
				}
			}
		} else if strings.HasPrefix(upperSeg, "WITH") {
			// Process WITH clause - extract variables and update context
			// WITH a, b means we keep a and b in context, filtering out others
			withClause := strings.TrimSpace(segment[4:]) // Skip "WITH"

			// Remove WHERE, ORDER BY, SKIP, LIMIT if present
			for _, keyword := range []string{" WHERE ", " ORDER BY ", " SKIP ", " LIMIT "} {
				if idx := findKeywordIndex(withClause, keyword); idx >= 0 {
					withClause = withClause[:idx]
				}
			}

			// Parse WITH items (similar to RETURN items)
			withItems := e.parseReturnItems(withClause)

			// Create new context with only the WITH variables
			newNodeContext := make(map[string]*storage.Node)
			newEdgeContext := make(map[string]*storage.Edge)

			for _, item := range withItems {
				varName := item.expr
				if item.alias != "" {
					varName = item.alias
				}

				// Look up in existing context
				if node, ok := nodeContext[varName]; ok {
					newNodeContext[varName] = node
				} else if edge, ok := edgeContext[varName]; ok {
					newEdgeContext[varName] = edge
				}
			}

			// Update context
			nodeContext = newNodeContext
			edgeContext = newEdgeContext
		} else if strings.HasPrefix(upperSeg, "RETURN") {
			// Build result from context
			returnClause := strings.TrimSpace(segment[6:])
			items := e.parseReturnItems(returnClause)

			row := make([]interface{}, len(items))
			for i, item := range items {
				if item.alias != "" {
					result.Columns = append(result.Columns, item.alias)
				} else {
					result.Columns = append(result.Columns, item.expr)
				}
				row[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeContext, edgeContext)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// executeMerge handles MERGE queries with ON CREATE SET / ON MATCH SET support.
// This implements Neo4j-compatible MERGE semantics:
// 1. Try to find an existing node matching the pattern
// 2. If found, apply ON MATCH SET if present
// 3. If not found, create the node and apply ON CREATE SET if present

func whereClauseReferencesOnlyVariable(whereClause string, variable string) bool {
	clause := strings.TrimSpace(whereClause)
	variable = strings.TrimSpace(variable)
	if clause == "" || variable == "" {
		return false
	}
	for i := 0; i < len(clause); {
		if !isIdentByte(clause[i]) {
			i++
			continue
		}
		start := i
		for i < len(clause) && isIdentByte(clause[i]) {
			i++
		}
		if i < len(clause) && clause[i] == '.' {
			refVar := clause[start:i]
			if !strings.EqualFold(refVar, variable) {
				return false
			}
		}
	}
	return true
}

var notRelationshipExistencePattern = regexp.MustCompile(`(?i)^NOT\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*-\s*\[:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*->\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$`)

func parseNotRelationshipExistenceTerm(term string) (startVar, edgeType, endVar string, ok bool) {
	m := notRelationshipExistencePattern.FindStringSubmatch(strings.TrimSpace(term))
	if len(m) != 4 {
		return "", "", "", false
	}
	return strings.TrimSpace(m[1]), strings.TrimSpace(m[2]), strings.TrimSpace(m[3]), true
}

func hasRelationshipOfType(store storage.Engine, startID, endID storage.NodeID, edgeType string) (bool, error) {
	edges, err := store.GetEdgesBetween(startID, endID)
	if err != nil {
		return false, err
	}
	for _, edge := range edges {
		if edge != nil && strings.EqualFold(edge.Type, edgeType) {
			return true, nil
		}
	}
	return false, nil
}

func (e *StorageExecutor) buildCombinationsUsingWhereJoin(
	patternMatches []struct {
		variable string
		nodes    []*storage.Node
	},
	whereClause string,
) ([]map[string]*storage.Node, bool) {
	if len(patternMatches) < 2 || strings.TrimSpace(whereClause) == "" {
		return nil, false
	}
	varOrder := make([]string, 0, len(patternMatches))
	varToNodes := make(map[string][]*storage.Node, len(patternMatches))
	for _, pm := range patternMatches {
		if pm.variable == "" {
			return nil, false
		}
		if _, exists := varToNodes[pm.variable]; exists {
			return nil, false
		}
		varOrder = append(varOrder, pm.variable)
		varToNodes[pm.variable] = pm.nodes
	}

	eqConstraints := make([]cartesianEqConstraint, 0, 4)
	for _, term := range splitTopLevelAndCartesian(whereClause) {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		lv, lp, rv, rp, ok := parseCartesianVarPropEqualityTerm(term)
		if !ok {
			continue
		}
		if _, lok := varToNodes[lv]; !lok {
			continue
		}
		if _, rok := varToNodes[rv]; !rok {
			continue
		}
		eqConstraints = append(eqConstraints, cartesianEqConstraint{
			leftVar:   lv,
			leftProp:  lp,
			rightVar:  rv,
			rightProp: rp,
		})
	}
	if len(eqConstraints) == 0 {
		return nil, false
	}

	parent := make(map[string]string, len(varOrder))
	for _, v := range varOrder {
		parent[v] = v
	}
	var find func(string) string
	find = func(v string) string {
		p := parent[v]
		if p != v {
			parent[v] = find(p)
		}
		return parent[v]
	}
	union := func(a, b string) {
		ra := find(a)
		rb := find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	for _, c := range eqConstraints {
		union(c.leftVar, c.rightVar)
	}

	components := make(map[string][]string, len(varOrder))
	componentOrder := make([]string, 0, len(varOrder))
	for _, v := range varOrder {
		root := find(v)
		if _, exists := components[root]; !exists {
			componentOrder = append(componentOrder, root)
		}
		components[root] = append(components[root], v)
	}

	constraintsByRoot := make(map[string][]cartesianEqConstraint, len(componentOrder))
	for _, c := range eqConstraints {
		root := find(c.leftVar)
		constraintsByRoot[root] = append(constraintsByRoot[root], c)
	}

	indexCache := map[string]map[string][]*storage.Node{}
	buildIndex := func(variable, prop string) map[string][]*storage.Node {
		cacheKey := variable + "|" + prop
		if idx, exists := indexCache[cacheKey]; exists {
			return idx
		}
		nodes := varToNodes[variable]
		idx := make(map[string][]*storage.Node, len(nodes))
		for _, n := range nodes {
			if n == nil || n.Properties == nil {
				continue
			}
			v, ok := n.Properties[prop]
			if !ok {
				continue
			}
			k := cartesianValueKey(v)
			idx[k] = append(idx[k], n)
		}
		indexCache[cacheKey] = idx
		return idx
	}

	buildComponentRows := func(vars []string, constraints []cartesianEqConstraint) []map[string]*storage.Node {
		if len(vars) == 0 {
			return []map[string]*storage.Node{{}}
		}
		if len(vars) == 1 {
			v := vars[0]
			rows := make([]map[string]*storage.Node, 0, len(varToNodes[v]))
			for _, n := range varToNodes[v] {
				rows = append(rows, map[string]*storage.Node{v: n})
			}
			return rows
		}

		seed := vars[0]
		rows := make([]map[string]*storage.Node, 0, len(varToNodes[seed]))
		for _, n := range varToNodes[seed] {
			rows = append(rows, map[string]*storage.Node{seed: n})
		}
		added := map[string]struct{}{seed: {}}

		for len(added) < len(vars) {
			progressed := false
			for _, c := range constraints {
				baseVar := ""
				baseProp := ""
				newVar := ""
				newProp := ""
				if _, ok := added[c.leftVar]; ok {
					if _, seen := added[c.rightVar]; !seen {
						baseVar, baseProp = c.leftVar, c.leftProp
						newVar, newProp = c.rightVar, c.rightProp
					}
				}
				if baseVar == "" {
					if _, ok := added[c.rightVar]; ok {
						if _, seen := added[c.leftVar]; !seen {
							baseVar, baseProp = c.rightVar, c.rightProp
							newVar, newProp = c.leftVar, c.leftProp
						}
					}
				}
				if baseVar == "" {
					continue
				}

				idx := buildIndex(newVar, newProp)
				nextRows := make([]map[string]*storage.Node, 0, len(rows))
				for _, row := range rows {
					baseNode := row[baseVar]
					if baseNode == nil || baseNode.Properties == nil {
						continue
					}
					baseVal, ok := baseNode.Properties[baseProp]
					if !ok {
						continue
					}
					for _, matchNode := range idx[cartesianValueKey(baseVal)] {
						joined := make(map[string]*storage.Node, len(row)+1)
						for k, v := range row {
							joined[k] = v
						}
						joined[newVar] = matchNode
						nextRows = append(nextRows, joined)
					}
				}
				rows = nextRows
				added[newVar] = struct{}{}
				progressed = true
			}
			if !progressed {
				return nil
			}
		}

		filtered := make([]map[string]*storage.Node, 0, len(rows))
		for _, row := range rows {
			keep := true
			for _, c := range constraints {
				ln := row[c.leftVar]
				rn := row[c.rightVar]
				if ln == nil || rn == nil || ln.Properties == nil || rn.Properties == nil {
					keep = false
					break
				}
				lv, lok := ln.Properties[c.leftProp]
				rv, rok := rn.Properties[c.rightProp]
				if !lok || !rok || cartesianValueKey(lv) != cartesianValueKey(rv) {
					keep = false
					break
				}
			}
			if keep {
				filtered = append(filtered, row)
			}
		}
		return filtered
	}

	componentRows := make([][]map[string]*storage.Node, 0, len(componentOrder))
	for _, root := range componentOrder {
		rows := buildComponentRows(components[root], constraintsByRoot[root])
		if rows == nil {
			return nil, false
		}
		componentRows = append(componentRows, rows)
	}

	out := []map[string]*storage.Node{{}}
	for _, rows := range componentRows {
		if len(rows) == 0 {
			return []map[string]*storage.Node{}, true
		}
		next := make([]map[string]*storage.Node, 0, len(out)*len(rows))
		for _, base := range out {
			for _, row := range rows {
				merged := make(map[string]*storage.Node, len(base)+len(row))
				for k, v := range base {
					merged[k] = v
				}
				for k, v := range row {
					merged[k] = v
				}
				next = append(next, merged)
			}
		}
		out = next
	}
	return out, true
}

// tryResolveMatchNodesByIDFromWhere attempts to resolve all MATCH pattern
// variables via direct ID lookup from WHERE elementId(var) = $param or
// id(var) = $param predicates. This is the relationship-create hot path:
// it avoids label scans when both endpoints are identified by ID.
//
// It collects all WHERE clauses across MATCH segments, splits on top-level
// AND, and tries to parse each conjunct as an ID equality predicate. If
// every pattern variable can be resolved this way, it returns a single
// combination map. Otherwise it returns (nil, false) and the caller falls
// through to the generic label-scan path.
func (e *StorageExecutor) tryResolveMatchNodesByIDFromWhere(
	ctx context.Context,
	matchClauses []string,
	params map[string]interface{},
) (map[string]*storage.Node, bool) {
	if len(matchClauses) == 0 {
		return nil, false
	}

	// Collect all pattern variables and their label constraints, plus all
	// WHERE conjuncts across segments.
	type varInfo struct {
		labels     []string
		properties map[string]interface{}
	}
	variables := make(map[string]*varInfo)
	var allWhereTerms []string

	for _, clause := range matchClauses {
		whereForClause := ""
		patternPart := clause
		if whereIdx := findKeywordIndex(clause, "WHERE"); whereIdx > 0 {
			whereForClause = strings.TrimSpace(clause[whereIdx+5:])
			patternPart = strings.TrimSpace(clause[:whereIdx])
		}

		// Extract pattern variables from comma-separated node patterns.
		patterns := e.splitNodePatterns(patternPart)
		for _, p := range patterns {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			// Skip relationship patterns — they contain arrows.
			if containsOutsideStrings(p, "->") || containsOutsideStrings(p, "<-") || containsOutsideStrings(p, "-[") {
				return nil, false
			}
			info := e.parseNodePattern(ctx, p)
			if info.variable == "" {
				continue
			}
			variables[info.variable] = &varInfo{
				labels:     info.labels,
				properties: info.properties,
			}
		}

		if whereForClause != "" {
			for _, term := range splitTopLevelAndCartesian(whereForClause) {
				term = strings.TrimSpace(term)
				if term != "" {
					allWhereTerms = append(allWhereTerms, term)
				}
			}
		}
	}

	if len(variables) == 0 || len(allWhereTerms) == 0 {
		return nil, false
	}

	// Try to resolve each variable from an ID equality predicate.
	resolved := make(map[string]*storage.Node, len(variables))
	for _, term := range allWhereTerms {
		varName, node := e.resolveNodeFromIDEqualityTerm(term, params)
		if varName == "" || node == nil {
			continue
		}
		if _, known := variables[varName]; !known {
			continue
		}
		resolved[varName] = node
	}

	// All variables must be resolved for the fast path to apply.
	if len(resolved) != len(variables) {
		return nil, false
	}

	// Validate label and property constraints from the patterns.
	for varName, info := range variables {
		node := resolved[varName]
		if len(info.labels) > 0 && !nodeHasAnyLabel(node, info.labels) {
			return nil, false
		}
		for k, v := range info.properties {
			if node.Properties[k] != v {
				return nil, false
			}
		}
	}

	return resolved, true
}

// resolveNodeFromIDEqualityTerm parses a single WHERE conjunct of the form
// elementId(<var>) = <value> or id(<var>) = <value> and resolves the node.
// Returns the variable name and resolved node, or ("", nil) if the term
// doesn't match the expected shape.
func (e *StorageExecutor) resolveNodeFromIDEqualityTerm(
	term string,
	params map[string]interface{},
) (string, *storage.Node) {
	term = strings.TrimSpace(term)
	if term == "" {
		return "", nil
	}

	eqIdx := strings.Index(term, "=")
	if eqIdx <= 0 || eqIdx >= len(term)-1 {
		return "", nil
	}
	// Reject !=, <=, >=, ==
	if eqIdx > 0 && (term[eqIdx-1] == '!' || term[eqIdx-1] == '<' || term[eqIdx-1] == '>') {
		return "", nil
	}
	if eqIdx+1 < len(term) && term[eqIdx+1] == '=' {
		return "", nil
	}

	left := strings.TrimSpace(term[:eqIdx])
	right := strings.TrimSpace(term[eqIdx+1:])
	if left == "" || right == "" {
		return "", nil
	}

	// LHS must be id(var) or elementId(var).
	kind := ""
	varName := ""
	lowerLeft := strings.ToLower(left)
	switch {
	case strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")"):
		kind = "id"
		varName = strings.TrimSpace(left[3 : len(left)-1])
	case strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")"):
		kind = "elementId"
		varName = strings.TrimSpace(left[10 : len(left)-1])
	default:
		return "", nil
	}
	if varName == "" {
		return "", nil
	}

	// Resolve the RHS value.
	var idValue string
	if strings.HasPrefix(right, "$") {
		// Parameter reference.
		if params == nil {
			return "", nil
		}
		paramName := strings.TrimSpace(right[1:])
		raw, ok := params[paramName]
		if !ok {
			return "", nil
		}
		s, ok := raw.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return "", nil
		}
		idValue = strings.TrimSpace(s)
	} else {
		// Literal value — strip surrounding quotes.
		idValue = strings.TrimSpace(right)
		if len(idValue) >= 2 {
			if (idValue[0] == '\'' && idValue[len(idValue)-1] == '\'') ||
				(idValue[0] == '"' && idValue[len(idValue)-1] == '"') {
				idValue = idValue[1 : len(idValue)-1]
			}
		}
		if idValue == "" {
			return "", nil
		}
	}

	// Strip canonical element ID prefix (4:dbname:rawid).
	lookupID := idValue
	if kind == "elementId" || strings.HasPrefix(lookupID, "4:") {
		if parts := strings.SplitN(lookupID, ":", 3); len(parts) == 3 && parts[0] == "4" {
			lookupID = parts[2]
		}
	}

	node, err := e.storage.GetNode(storage.NodeID(lookupID))
	if err != nil || node == nil {
		return "", nil
	}

	return varName, node
}
