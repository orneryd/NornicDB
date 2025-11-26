// Package cypher provides Cypher query execution for NornicDB.
package cypher

import (
	"context"
	"crypto/rand"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// StorageExecutor executes Cypher queries against storage.
type StorageExecutor struct {
	parser  *Parser
	storage *storage.MemoryEngine
}

// NewStorageExecutor creates a new executor with storage backend.
func NewStorageExecutor(store *storage.MemoryEngine) *StorageExecutor {
	return &StorageExecutor{
		parser:  NewParser(),
		storage: store,
	}
}

// ExecuteResult holds execution results in Neo4j-compatible format.
type ExecuteResult struct {
	Columns []string
	Rows    [][]interface{}
	Stats   *QueryStats
}

// QueryStats holds query execution statistics.
type QueryStats struct {
	NodesCreated         int `json:"nodes_created"`
	NodesDeleted         int `json:"nodes_deleted"`
	RelationshipsCreated int `json:"relationships_created"`
	RelationshipsDeleted int `json:"relationships_deleted"`
	PropertiesSet        int `json:"properties_set"`
}

// Execute parses and executes a Cypher query.
func (e *StorageExecutor) Execute(ctx context.Context, cypher string, params map[string]interface{}) (*ExecuteResult, error) {
	// Normalize query
	cypher = strings.TrimSpace(cypher)
	if cypher == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Validate basic syntax
	if err := e.validateSyntax(cypher); err != nil {
		return nil, err
	}

	// Substitute parameters
	cypher = e.substituteParams(cypher, params)

	// Route to appropriate handler based on query type
	upperQuery := strings.ToUpper(cypher)

	// MERGE queries get special handling - they have their own ON CREATE SET / ON MATCH SET logic
	if strings.HasPrefix(upperQuery, "MERGE") {
		return e.executeMerge(ctx, cypher)
	}

	// Compound queries: MATCH ... MERGE ... (with variable references)
	// This is more complex than simple MERGE and requires executing MATCH first
	if strings.HasPrefix(upperQuery, "MATCH") && strings.Contains(upperQuery, " MERGE ") {
		return e.executeCompoundMatchMerge(ctx, cypher)
	}

	// Check for compound queries - MATCH ... DELETE, MATCH ... SET, etc.
	// These need special handling (but not MERGE queries which handle SET internally)
	if strings.Contains(upperQuery, " DELETE ") || strings.HasSuffix(upperQuery, " DELETE") ||
		strings.Contains(upperQuery, "DETACH DELETE") {
		return e.executeDelete(ctx, cypher)
	}
	// Only route to executeSet if it's a MATCH ... SET or standalone SET, not ON CREATE SET / ON MATCH SET
	if strings.Contains(upperQuery, " SET ") && 
		!strings.Contains(upperQuery, "ON CREATE SET") && 
		!strings.Contains(upperQuery, "ON MATCH SET") {
		return e.executeSet(ctx, cypher)
	}

	switch {
	case strings.HasPrefix(upperQuery, "MATCH"):
		return e.executeMatch(ctx, cypher)
	case strings.HasPrefix(upperQuery, "CREATE CONSTRAINT"), strings.HasPrefix(upperQuery, "CREATE INDEX"):
		// Schema commands - treat as no-op (NornicDB manages indexes internally)
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	case strings.HasPrefix(upperQuery, "CREATE"):
		return e.executeCreate(ctx, cypher)
	case strings.HasPrefix(upperQuery, "DELETE"), strings.HasPrefix(upperQuery, "DETACH DELETE"):
		return e.executeDelete(ctx, cypher)
	case strings.HasPrefix(upperQuery, "CALL"):
		return e.executeCall(ctx, cypher)
	case strings.HasPrefix(upperQuery, "RETURN"):
		return e.executeReturn(ctx, cypher)
	case strings.HasPrefix(upperQuery, "DROP"):
		// DROP INDEX/CONSTRAINT - treat as no-op (NornicDB manages indexes internally)
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	default:
		return nil, fmt.Errorf("unsupported query type: %s", strings.Split(upperQuery, " ")[0])
	}
}

// executeReturn handles simple RETURN statements (e.g., "RETURN 1").
func (e *StorageExecutor) executeReturn(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse RETURN clause
	upper := strings.ToUpper(cypher)
	returnIdx := strings.Index(upper, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause not found")
	}

	returnClause := strings.TrimSpace(cypher[returnIdx+6:])
	
	// Handle simple literal returns like "RETURN 1" or "RETURN true"
	parts := strings.Split(returnClause, ",")
	columns := make([]string, 0, len(parts))
	values := make([]interface{}, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		
		// Check for alias (AS)
		alias := part
		upperPart := strings.ToUpper(part)
		if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
			alias = strings.TrimSpace(part[asIdx+4:])
			part = strings.TrimSpace(part[:asIdx])
		}

		columns = append(columns, alias)

		// Parse literal value
		if part == "1" || strings.HasPrefix(strings.ToLower(part), "true") {
			values = append(values, int64(1))
		} else if part == "0" || strings.HasPrefix(strings.ToLower(part), "false") {
			values = append(values, int64(0))
		} else if strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'") {
			values = append(values, part[1:len(part)-1])
		} else if strings.HasPrefix(part, "\"") && strings.HasSuffix(part, "\"") {
			values = append(values, part[1:len(part)-1])
		} else {
			// Try to parse as number
			if val, err := strconv.ParseInt(part, 10, 64); err == nil {
				values = append(values, val)
			} else if val, err := strconv.ParseFloat(part, 64); err == nil {
				values = append(values, val)
			} else {
				// Return as string
				values = append(values, part)
			}
		}
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

// validateSyntax performs basic syntax validation.
func (e *StorageExecutor) validateSyntax(cypher string) error {
	upper := strings.ToUpper(cypher)

	// Check for valid starting keyword
	validStarts := []string{"MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH", "UNWIND", "OPTIONAL", "DROP"}
	hasValidStart := false
	for _, start := range validStarts {
		if strings.HasPrefix(upper, start) {
			hasValidStart = true
			break
		}
	}
	if !hasValidStart {
		return fmt.Errorf("syntax error: query must start with a valid clause (MATCH, CREATE, MERGE, DELETE, CALL, etc.)")
	}

	// Check balanced parentheses
	parenCount := 0
	bracketCount := 0
	braceCount := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(cypher); i++ {
		c := cypher[i]

		if inString {
			if c == stringChar && (i == 0 || cypher[i-1] != '\\') {
				inString = false
			}
			continue
		}

		switch c {
		case '"', '\'':
			inString = true
			stringChar = c
		case '(':
			parenCount++
		case ')':
			parenCount--
		case '[':
			bracketCount++
		case ']':
			bracketCount--
		case '{':
			braceCount++
		case '}':
			braceCount--
		}

		if parenCount < 0 || bracketCount < 0 || braceCount < 0 {
			return fmt.Errorf("syntax error: unbalanced brackets at position %d", i)
		}
	}

	if parenCount != 0 {
		return fmt.Errorf("syntax error: unbalanced parentheses")
	}
	if bracketCount != 0 {
		return fmt.Errorf("syntax error: unbalanced square brackets")
	}
	if braceCount != 0 {
		return fmt.Errorf("syntax error: unbalanced curly braces")
	}
	if inString {
		return fmt.Errorf("syntax error: unclosed quote")
	}

	return nil
}

// getParamKeys returns the keys from a params map for debugging
func getParamKeys(params map[string]interface{}) []string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	return keys
}

// substituteParams replaces $param with actual values.
// substituteParams replaces $paramName placeholders with actual values.
// This implements Neo4j-style parameter substitution with proper escaping and type handling.
func (e *StorageExecutor) substituteParams(cypher string, params map[string]interface{}) string {
	if params == nil || len(params) == 0 {
		return cypher
	}

	// Use regex to find all parameter references
	// Parameters are: $name or $name123 (alphanumeric starting with letter)
	paramPattern := regexp.MustCompile(`\$([a-zA-Z_][a-zA-Z0-9_]*)`)
	
	result := paramPattern.ReplaceAllStringFunc(cypher, func(match string) string {
		// Extract parameter name (without $)
		paramName := match[1:]
		
		// Look up the value
		value, exists := params[paramName]
		if !exists {
			// Parameter not provided, leave as-is (might be handled elsewhere or is an error)
			return match
		}
		
		return e.valueToLiteral(value)
	})
	
	return result
}

// valueToLiteral converts a Go value to a Cypher literal string.
func (e *StorageExecutor) valueToLiteral(v interface{}) string {
	if v == nil {
		return "null"
	}
	
	switch val := v.(type) {
	case string:
		// Escape single quotes by doubling them (Cypher standard)
		escaped := strings.ReplaceAll(val, "'", "''")
		// Also escape backslashes
		escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
		return fmt.Sprintf("'%s'", escaped)
		
	case int:
		return strconv.FormatInt(int64(val), 10)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
		
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
		
	case bool:
		if val {
			return "true"
		}
		return "false"
		
	case []interface{}:
		// Convert array to Cypher list literal: [val1, val2, ...]
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = e.valueToLiteral(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
		
	case []string:
		// String array
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = e.valueToLiteral(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
		
	case []int:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = strconv.Itoa(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
		
	case []int64:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = strconv.FormatInt(item, 10)
		}
		return "[" + strings.Join(parts, ", ") + "]"
		
	case []float64:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = strconv.FormatFloat(item, 'f', -1, 64)
		}
		return "[" + strings.Join(parts, ", ") + "]"
		
	case map[string]interface{}:
		// Convert map to Cypher map literal: {key1: val1, key2: val2}
		parts := make([]string, 0, len(val))
		for k, v := range val {
			parts = append(parts, fmt.Sprintf("%s: %s", k, e.valueToLiteral(v)))
		}
		return "{" + strings.Join(parts, ", ") + "}"
		
	default:
		// Fallback: convert to string
		return fmt.Sprintf("'%v'", v)
	}
}

// executeMatch handles MATCH queries.
func (e *StorageExecutor) executeMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	upper := strings.ToUpper(cypher)

	// Extract return variables
	returnIdx := strings.Index(upper, "RETURN")
	if returnIdx == -1 {
		// No RETURN clause - just match and return count
		result.Columns = []string{"matched"}
		result.Rows = [][]interface{}{{true}}
		return result, nil
	}

	// Parse RETURN part (everything after RETURN, before ORDER BY/SKIP/LIMIT)
	returnPart := cypher[returnIdx+6:]

	// Find end of RETURN clause
	returnEndIdx := len(returnPart)
	for _, keyword := range []string{" ORDER BY ", " SKIP ", " LIMIT "} {
		idx := strings.Index(strings.ToUpper(returnPart), keyword)
		if idx >= 0 && idx < returnEndIdx {
			returnEndIdx = idx
		}
	}
	returnClause := strings.TrimSpace(returnPart[:returnEndIdx])

	// Check for DISTINCT
	distinct := false
	if strings.HasPrefix(strings.ToUpper(returnClause), "DISTINCT ") {
		distinct = true
		returnClause = strings.TrimSpace(returnClause[9:])
	}

	// Parse RETURN items
	returnItems := e.parseReturnItems(returnClause)
	result.Columns = make([]string, len(returnItems))
	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	// Check if this is an aggregation query
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

	// Extract pattern between MATCH and WHERE/RETURN
	matchPart := cypher[5:] // Skip "MATCH"
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx > 0 {
		matchPart = cypher[5:whereIdx]
	} else if returnIdx > 0 {
		matchPart = cypher[5:returnIdx]
	}
	matchPart = strings.TrimSpace(matchPart)

	// Parse node pattern
	nodePattern := e.parseNodePattern(matchPart)

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

	// Apply WHERE filter if present
	if whereIdx > 0 {
		// Find end of WHERE clause (before RETURN)
		wherePart := cypher[whereIdx+5 : returnIdx]
		nodes = e.filterNodes(nodes, nodePattern.variable, strings.TrimSpace(wherePart))
	}

	// Handle aggregation queries
	if hasAggregation {
		return e.executeAggregation(nodes, nodePattern.variable, returnItems, result)
	}

	// Parse ORDER BY
	orderByIdx := strings.Index(upper, "ORDER BY")
	if orderByIdx > 0 {
		orderPart := upper[orderByIdx+8:]
		// Find end
		endIdx := len(orderPart)
		for _, kw := range []string{" SKIP ", " LIMIT "} {
			if idx := strings.Index(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(cypher[orderByIdx+8 : orderByIdx+8+endIdx])
		nodes = e.orderNodes(nodes, nodePattern.variable, orderExpr)
	}

	// Parse SKIP
	skipIdx := strings.Index(upper, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(cypher[skipIdx+4:])
		skipPart = strings.Split(skipPart, " ")[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	// Parse LIMIT
	limitIdx := strings.Index(upper, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(cypher[limitIdx+5:])
		limitPart = strings.Split(limitPart, " ")[0]
		if l, err := strconv.Atoi(limitPart); err == nil {
			limit = l
		}
	}

	// Build result rows with SKIP and LIMIT
	seen := make(map[string]bool) // For DISTINCT
	rowCount := 0
	for i, node := range nodes {
		// Apply SKIP
		if i < skip {
			continue
		}

		// Apply LIMIT
		if limit >= 0 && rowCount >= limit {
			break
		}

		row := make([]interface{}, len(returnItems))
		for j, item := range returnItems {
			row[j] = e.resolveReturnItem(item, nodePattern.variable, node)
		}

		// Handle DISTINCT
		if distinct {
			key := fmt.Sprintf("%v", row)
			if seen[key] {
				continue
			}
			seen[key] = true
		}

		result.Rows = append(result.Rows, row)
		rowCount++
	}

	return result, nil
}

// executeAggregation handles aggregate functions (COUNT, SUM, AVG, etc.)
func (e *StorageExecutor) executeAggregation(nodes []*storage.Node, variable string, items []returnItem, result *ExecuteResult) (*ExecuteResult, error) {
	row := make([]interface{}, len(items))

	// Case-insensitive regex patterns for aggregation functions
	countPropRe := regexp.MustCompile(`(?i)COUNT\((\w+)\.(\w+)\)`)
	sumRe := regexp.MustCompile(`(?i)SUM\((\w+)\.(\w+)\)`)
	avgRe := regexp.MustCompile(`(?i)AVG\((\w+)\.(\w+)\)`)
	minRe := regexp.MustCompile(`(?i)MIN\((\w+)\.(\w+)\)`)
	maxRe := regexp.MustCompile(`(?i)MAX\((\w+)\.(\w+)\)`)
	collectRe := regexp.MustCompile(`(?i)COLLECT\((\w+)(?:\.(\w+))?\)`)

	for i, item := range items {
		upperExpr := strings.ToUpper(item.expr)

		switch {
		case strings.HasPrefix(upperExpr, "COUNT("):
			// COUNT(*) or COUNT(n)
			if strings.Contains(upperExpr, "*") || strings.Contains(upperExpr, "("+strings.ToUpper(variable)+")") {
				row[i] = int64(len(nodes))
			} else {
				// COUNT(n.property) - count non-null values
				propMatch := countPropRe.FindStringSubmatch(item.expr)
				if len(propMatch) == 3 {
					count := int64(0)
					for _, node := range nodes {
						if _, exists := node.Properties[propMatch[2]]; exists {
							count++
						}
					}
					row[i] = count
				} else {
					row[i] = int64(len(nodes))
				}
			}

		case strings.HasPrefix(upperExpr, "SUM("):
			propMatch := sumRe.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				sum := float64(0)
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
				}
				row[i] = sum
			} else {
				row[i] = float64(0)
			}

		case strings.HasPrefix(upperExpr, "AVG("):
			propMatch := avgRe.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				sum := float64(0)
				count := 0
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							sum += num
							count++
						}
					}
				}
				if count > 0 {
					row[i] = sum / float64(count)
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "MIN("):
			propMatch := minRe.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				var min *float64
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							if min == nil || num < *min {
								minVal := num
								min = &minVal
							}
						}
					}
				}
				if min != nil {
					row[i] = *min
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "MAX("):
			propMatch := maxRe.FindStringSubmatch(item.expr)
			if len(propMatch) == 3 {
				var max *float64
				for _, node := range nodes {
					if val, exists := node.Properties[propMatch[2]]; exists {
						if num, ok := toFloat64(val); ok {
							if max == nil || num > *max {
								maxVal := num
								max = &maxVal
							}
						}
					}
				}
				if max != nil {
					row[i] = *max
				} else {
					row[i] = nil
				}
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "COLLECT("):
			propMatch := collectRe.FindStringSubmatch(item.expr)
			collected := make([]interface{}, 0)
			if len(propMatch) >= 2 {
				for _, node := range nodes {
					if len(propMatch) == 3 && propMatch[2] != "" {
						// COLLECT(n.property)
						if val, exists := node.Properties[propMatch[2]]; exists {
							collected = append(collected, val)
						}
					} else {
						// COLLECT(n)
						collected = append(collected, map[string]interface{}{
							"id":         string(node.ID),
							"labels":     node.Labels,
							"properties": node.Properties,
						})
					}
				}
			}
			row[i] = collected

		default:
			// Non-aggregate in aggregation query - return first value
			if len(nodes) > 0 {
				row[i] = e.resolveReturnItem(item, variable, nodes[0])
			} else {
				row[i] = nil
			}
		}
	}

	result.Rows = [][]interface{}{row}
	return result, nil
}

// orderNodes sorts nodes by the given expression
func (e *StorageExecutor) orderNodes(nodes []*storage.Node, variable, orderExpr string) []*storage.Node {
	// Parse: n.property [ASC|DESC]
	desc := strings.HasSuffix(strings.ToUpper(orderExpr), " DESC")
	orderExpr = strings.TrimSuffix(strings.TrimSuffix(orderExpr, " DESC"), " ASC")
	orderExpr = strings.TrimSpace(orderExpr)

	// Extract property name
	var propName string
	if strings.HasPrefix(orderExpr, variable+".") {
		propName = orderExpr[len(variable)+1:]
	} else {
		propName = orderExpr
	}

	// Sort using a simple bubble sort (could use sort.Slice for efficiency)
	sorted := make([]*storage.Node, len(nodes))
	copy(sorted, nodes)

	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			val1, _ := sorted[j].Properties[propName]
			val2, _ := sorted[j+1].Properties[propName]

			shouldSwap := false
			num1, ok1 := toFloat64(val1)
			num2, ok2 := toFloat64(val2)

			if ok1 && ok2 {
				if desc {
					shouldSwap = num1 < num2
				} else {
					shouldSwap = num1 > num2
				}
			} else {
				str1 := fmt.Sprintf("%v", val1)
				str2 := fmt.Sprintf("%v", val2)
				if desc {
					shouldSwap = str1 < str2
				} else {
					shouldSwap = str1 > str2
				}
			}

			if shouldSwap {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	return sorted
}

// executeCreate handles CREATE queries.
func (e *StorageExecutor) executeCreate(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Parse CREATE pattern
	pattern := cypher[6:] // Skip "CREATE"
	upper := strings.ToUpper(cypher)

	returnIdx := strings.Index(upper, "RETURN")
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

// executeMerge handles MERGE queries with ON CREATE SET / ON MATCH SET support.
// This implements Neo4j-compatible MERGE semantics:
// 1. Try to find an existing node matching the pattern
// 2. If found, apply ON MATCH SET if present
// 3. If not found, create the node and apply ON CREATE SET if present
func (e *StorageExecutor) executeMerge(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	upper := strings.ToUpper(cypher)

	// Extract the main MERGE pattern
	mergeIdx := strings.Index(upper, "MERGE")
	if mergeIdx == -1 {
		return nil, fmt.Errorf("MERGE clause not found")
	}

	// Find ON CREATE SET, ON MATCH SET, standalone SET, and RETURN clauses
	onCreateIdx := strings.Index(upper, "ON CREATE SET")
	onMatchIdx := strings.Index(upper, "ON MATCH SET")
	returnIdx := strings.Index(upper, "RETURN")
	withIdx := strings.Index(upper, "WITH")
	
	// Find standalone SET clause (after ON CREATE SET / ON MATCH SET)
	setIdx := -1
	searchStart := 0
	if onCreateIdx > 0 {
		searchStart = onCreateIdx + 13 // After "ON CREATE SET"
	}
	if onMatchIdx > 0 && onMatchIdx > searchStart {
		searchStart = onMatchIdx + 12 // After "ON MATCH SET"
	}
	if searchStart > 0 {
		// Look for " SET " or "\nSET " after ON CREATE/MATCH SET clauses
		rest := upper[searchStart:]
		// Find SET that is not part of ON CREATE/MATCH SET
		for i := 0; i < len(rest)-4; i++ {
			if (rest[i] == ' ' || rest[i] == '\n' || rest[i] == '\t') && 
			   strings.HasPrefix(rest[i+1:], "SET ") &&
			   !strings.HasPrefix(rest[max(0,i-10):], "ON CREATE ") &&
			   !strings.HasPrefix(rest[max(0,i-9):], "ON MATCH ") {
				setIdx = searchStart + i + 1
				break
			}
		}
	} else {
		// No ON CREATE/MATCH SET, look for standalone SET
		setIdx = strings.Index(upper, " SET ")
		if setIdx > 0 {
			setIdx++ // Point to S in SET
		}
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
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	upper := strings.ToUpper(cypher)
	
	// Find the MATCH and MERGE boundaries
	matchIdx := strings.Index(upper, "MATCH")
	mergeIdx := strings.Index(upper, " MERGE ")
	if mergeIdx == -1 {
		mergeIdx = strings.Index(upper, "\nMERGE ")
	}
	
	if matchIdx == -1 || mergeIdx == -1 {
		return nil, fmt.Errorf("invalid MATCH ... MERGE query")
	}
	
	// Extract MATCH clause
	matchClause := strings.TrimSpace(cypher[matchIdx:mergeIdx])
	mergeClause := strings.TrimSpace(cypher[mergeIdx+1:])
	
	// Execute MATCH to get context
	matchedNodes, matchedRels, err := e.executeMatchForContext(ctx, matchClause)
	if err != nil {
		return nil, fmt.Errorf("failed to execute MATCH: %v", err)
	}
	
	// If no matches found and not OPTIONAL MATCH, return empty
	if len(matchedNodes) == 0 && !strings.Contains(upper, "OPTIONAL MATCH") {
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
func (e *StorageExecutor) executeMatchForContext(ctx context.Context, matchClause string) ([]map[string]*storage.Node, map[string]*storage.Edge, error) {
	var allMatches []map[string]*storage.Node
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
	
	// Parse pattern to get variable names and labels
	patternPart = strings.TrimSpace(patternPart)
	
	// Simple pattern: (var:Label) or (var:Label {props})
	nodeInfo := e.parseNodePattern(patternPart)
	
	// Find matching nodes
	var candidates []*storage.Node
	if len(nodeInfo.labels) > 0 {
		candidates, _ = e.storage.GetNodesByLabel(nodeInfo.labels[0])
	} else {
		candidates = e.storage.GetAllNodes()
	}
	
	// Apply WHERE clause and property filters
	for _, node := range candidates {
		// Check property filters from pattern
		if !e.nodeMatchesProps(node, nodeInfo.properties) {
			continue
		}
		
		// Check WHERE clause
		if wherePart != "" && !e.evaluateWhere(node, nodeInfo.variable, wherePart) {
			continue
		}
		
		// Add to matches
		nodeMap := map[string]*storage.Node{
			nodeInfo.variable: node,
		}
		allMatches = append(allMatches, nodeMap)
	}
	
	return allMatches, relMatches, nil
}

// executeMergeWithContext executes a MERGE clause with context from a prior MATCH.
func (e *StorageExecutor) executeMergeWithContext(ctx context.Context, cypher string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	upper := strings.ToUpper(cypher)

	// Find clauses
	mergeIdx := strings.Index(upper, "MERGE")
	if mergeIdx == -1 {
		mergeIdx = 0 // Already stripped
	}
	
	onCreateIdx := strings.Index(upper, "ON CREATE SET")
	onMatchIdx := strings.Index(upper, "ON MATCH SET")
	returnIdx := strings.Index(upper, "RETURN")
	withIdx := strings.Index(upper, "WITH")
	
	// Find standalone SET (not ON CREATE/MATCH SET)
	setIdx := -1
	searchStart := 0
	if onCreateIdx > 0 {
		searchStart = onCreateIdx + 13
	}
	if onMatchIdx > 0 && onMatchIdx > searchStart {
		searchStart = onMatchIdx + 12
	}
	if searchStart > 0 {
		rest := upper[searchStart:]
		for i := 0; i < len(rest)-4; i++ {
			if (rest[i] == ' ' || rest[i] == '\n' || rest[i] == '\t') && 
			   strings.HasPrefix(rest[i+1:], "SET ") {
				setIdx = searchStart + i + 1
				break
			}
		}
	} else {
		idx := strings.Index(upper, " SET ")
		if idx > 0 {
			setIdx = idx + 1
		}
	}
	
	// Find MERGE pattern end
	patternEnd := len(cypher)
	for _, idx := range []int{onCreateIdx, onMatchIdx, setIdx, returnIdx, withIdx} {
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}
	
	// Handle second MERGE in compound query
	secondMergeIdx := strings.Index(upper[mergeIdx+5:], " MERGE ")
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
		for _, idx := range []int{withIdx, returnIdx} {
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
	
	upper := strings.ToUpper(cypher)
	returnIdx := strings.Index(upper, "RETURN")
	
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
		e.storage.CreateEdge(edge)
		result.Stats.RelationshipsCreated = 1
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
		node.Properties[propName] = e.evaluateSetExpressionWithContext(propValue, fullContext, relContext)
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

// evaluateExpression evaluates an expression against a node.
// Supports all standard Cypher functions: id(), labels(), keys(), properties(), type(),
// count(), size(), exists(), coalesce(), head(), last(), tail(), range(), etc.
func (e *StorageExecutor) evaluateExpression(expr string, varName string, node *storage.Node) interface{} {
	return e.evaluateExpressionWithContext(expr, map[string]*storage.Node{varName: node}, nil)
}

// evaluateExpressionWithContext evaluates an expression with multiple variable bindings.
// This supports complex queries like MATCH (a) MERGE (b) where we need access to both nodes.
func (e *StorageExecutor) evaluateExpressionWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	
	lowerExpr := strings.ToLower(expr)
	
	// ========================================
	// Cypher Functions (Neo4j compatible)
	// ========================================
	
	// id(n) - return internal node/relationship ID
	if strings.HasPrefix(lowerExpr, "id(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[3 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return string(node.ID)
		}
		if rel, ok := rels[inner]; ok {
			return string(rel.ID)
		}
		return nil
	}
	
	// elementId(n) - same as id() for compatibility
	if strings.HasPrefix(lowerExpr, "elementid(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return fmt.Sprintf("4:nornicdb:%s", node.ID)
		}
		if rel, ok := rels[inner]; ok {
			return fmt.Sprintf("5:nornicdb:%s", rel.ID)
		}
		return nil
	}
	
	// labels(n) - return list of labels for a node
	if strings.HasPrefix(lowerExpr, "labels(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			// Return labels as a list of strings
			result := make([]interface{}, len(node.Labels))
			for i, label := range node.Labels {
				result[i] = label
			}
			return result
		}
		return nil
	}
	
	// type(r) - return relationship type
	if strings.HasPrefix(lowerExpr, "type(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if rel, ok := rels[inner]; ok {
			return rel.Type
		}
		return nil
	}
	
	// keys(n) - return list of property keys
	if strings.HasPrefix(lowerExpr, "keys(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			keys := make([]interface{}, 0, len(node.Properties))
			for k := range node.Properties {
				if !e.isInternalProperty(k) {
					keys = append(keys, k)
				}
			}
			return keys
		}
		if rel, ok := rels[inner]; ok {
			keys := make([]interface{}, 0, len(rel.Properties))
			for k := range rel.Properties {
				keys = append(keys, k)
			}
			return keys
		}
		return nil
	}
	
	// properties(n) - return all properties as a map
	if strings.HasPrefix(lowerExpr, "properties(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[11 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			props := make(map[string]interface{})
			for k, v := range node.Properties {
				if !e.isInternalProperty(k) {
					props[k] = v
				}
			}
			return props
		}
		if rel, ok := rels[inner]; ok {
			return rel.Properties
		}
		return nil
	}
	
	// count(*) or count(n) - simplified aggregation (returns 1 for single row context)
	if strings.HasPrefix(lowerExpr, "count(") && strings.HasSuffix(expr, ")") {
		return int64(1)
	}
	
	// size(list) or size(string) - return length
	if strings.HasPrefix(lowerExpr, "size(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := innerVal.(type) {
		case string:
			return int64(len(v))
		case []interface{}:
			return int64(len(v))
		case []string:
			return int64(len(v))
		}
		return int64(0)
	}
	
	// length(path) - same as size for compatibility
	if strings.HasPrefix(lowerExpr, "length(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := innerVal.(type) {
		case string:
			return int64(len(v))
		case []interface{}:
			return int64(len(v))
		}
		return int64(0)
	}
	
	// exists(n.prop) - check if property exists
	if strings.HasPrefix(lowerExpr, "exists(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		// Check for property access
		if dotIdx := strings.Index(inner, "."); dotIdx > 0 {
			varName := inner[:dotIdx]
			propName := inner[dotIdx+1:]
			if node, ok := nodes[varName]; ok {
				_, exists := node.Properties[propName]
				return exists
			}
		}
		return false
	}
	
	// coalesce(val1, val2, ...) - return first non-null value
	if strings.HasPrefix(lowerExpr, "coalesce(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		for _, arg := range args {
			val := e.evaluateExpressionWithContext(strings.TrimSpace(arg), nodes, rels)
			if val != nil {
				return val
			}
		}
		return nil
	}
	
	// head(list) - return first element
	if strings.HasPrefix(lowerExpr, "head(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[0]
		}
		return nil
	}
	
	// last(list) - return last element
	if strings.HasPrefix(lowerExpr, "last(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[len(list)-1]
		}
		return nil
	}
	
	// tail(list) - return list without first element
	if strings.HasPrefix(lowerExpr, "tail(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 1 {
			return list[1:]
		}
		return []interface{}{}
	}
	
	// reverse(list) - return reversed list
	if strings.HasPrefix(lowerExpr, "reverse(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok {
			result := make([]interface{}, len(list))
			for i, v := range list {
				result[len(list)-1-i] = v
			}
			return result
		}
		if str, ok := innerVal.(string); ok {
			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		}
		return nil
	}
	
	// range(start, end) or range(start, end, step)
	if strings.HasPrefix(lowerExpr, "range(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			start, _ := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64)
			end, _ := strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64)
			step := int64(1)
			if len(args) >= 3 {
				step, _ = strconv.ParseInt(strings.TrimSpace(args[2]), 10, 64)
			}
			if step == 0 {
				step = 1
			}
			var result []interface{}
			if step > 0 {
				for i := start; i <= end; i += step {
					result = append(result, i)
				}
			} else {
				for i := start; i >= end; i += step {
					result = append(result, i)
				}
			}
			return result
		}
		return []interface{}{}
	}
	
	// ========================================
	// String Functions
	// ========================================
	
	// toString(value)
	if strings.HasPrefix(lowerExpr, "tostring(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		return fmt.Sprintf("%v", val)
	}
	
	// toInteger(value) / toInt(value)
	if (strings.HasPrefix(lowerExpr, "tointeger(") || strings.HasPrefix(lowerExpr, "toint(")) && strings.HasSuffix(expr, ")") {
		startIdx := 10
		if strings.HasPrefix(lowerExpr, "toint(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
		return nil
	}
	
	// toFloat(value)
	if strings.HasPrefix(lowerExpr, "tofloat(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int64:
			return float64(v)
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
		return nil
	}
	
	// toBoolean(value)
	if strings.HasPrefix(lowerExpr, "toboolean(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case bool:
			return v
		case string:
			return strings.ToLower(v) == "true"
		}
		return nil
	}
	
	// toLower(string) / lower(string)
	if (strings.HasPrefix(lowerExpr, "tolower(") || strings.HasPrefix(lowerExpr, "lower(")) && strings.HasSuffix(expr, ")") {
		startIdx := 8
		if strings.HasPrefix(lowerExpr, "lower(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToLower(str)
		}
		return nil
	}
	
	// toUpper(string) / upper(string)
	if (strings.HasPrefix(lowerExpr, "toupper(") || strings.HasPrefix(lowerExpr, "upper(")) && strings.HasSuffix(expr, ")") {
		startIdx := 8
		if strings.HasPrefix(lowerExpr, "upper(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToUpper(str)
		}
		return nil
	}
	
	// trim(string) / ltrim(string) / rtrim(string)
	if strings.HasPrefix(lowerExpr, "trim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimSpace(str)
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "ltrim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimLeft(str, " \t\n\r")
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "rtrim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimRight(str, " \t\n\r")
		}
		return nil
	}
	
	// replace(string, search, replacement)
	if strings.HasPrefix(lowerExpr, "replace(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 3 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			search := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			repl := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels))
			return strings.ReplaceAll(str, search, repl)
		}
		return nil
	}
	
	// split(string, delimiter)
	if strings.HasPrefix(lowerExpr, "split(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			delim := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			parts := strings.Split(str, delim)
			result := make([]interface{}, len(parts))
			for i, p := range parts {
				result[i] = p
			}
			return result
		}
		return nil
	}
	
	// substring(string, start, [length])
	if strings.HasPrefix(lowerExpr, "substring(") && strings.HasSuffix(expr, ")") {
		return e.evaluateSubstring(expr)
	}
	
	// left(string, n) - return first n characters
	if strings.HasPrefix(lowerExpr, "left(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			n, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			if n > len(str) {
				n = len(str)
			}
			return str[:n]
		}
		return nil
	}
	
	// right(string, n) - return last n characters
	if strings.HasPrefix(lowerExpr, "right(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			n, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			if n > len(str) {
				n = len(str)
			}
			return str[len(str)-n:]
		}
		return nil
	}
	
	// ========================================
	// Date/Time Functions
	// ========================================
	
	// timestamp() - current Unix timestamp in milliseconds
	if lowerExpr == "timestamp()" {
		return e.idCounter() // Use counter as pseudo-timestamp for consistency
	}
	
	// datetime() - current datetime as string
	if lowerExpr == "datetime()" {
		return fmt.Sprintf("%d", e.idCounter())
	}
	
	// date() - current date
	if lowerExpr == "date()" {
		return fmt.Sprintf("%d", e.idCounter())
	}
	
	// time() - current time
	if lowerExpr == "time()" {
		return fmt.Sprintf("%d", e.idCounter())
	}
	
	// ========================================
	// Math Functions
	// ========================================
	
	// abs(number)
	if strings.HasPrefix(lowerExpr, "abs(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			if v < 0 {
				return -v
			}
			return v
		case float64:
			if v < 0 {
				return -v
			}
			return v
		}
		return nil
	}
	
	// ceil(number)
	if strings.HasPrefix(lowerExpr, "ceil(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.999999999)
		}
		return nil
	}
	
	// floor(number)
	if strings.HasPrefix(lowerExpr, "floor(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f)
		}
		return nil
	}
	
	// round(number)
	if strings.HasPrefix(lowerExpr, "round(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.5)
		}
		return nil
	}
	
	// sign(number)
	if strings.HasPrefix(lowerExpr, "sign(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			if f > 0 {
				return int64(1)
			} else if f < 0 {
				return int64(-1)
			}
			return int64(0)
		}
		return nil
	}
	
	// randomUUID()
	if lowerExpr == "randomuuid()" {
		return e.generateUUID()
	}
	
	// rand() - random float between 0 and 1
	if lowerExpr == "rand()" {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		// Convert to float between 0 and 1
		val := float64(b[0]^b[1]^b[2]^b[3]) / 256.0
		return val
	}
	
	// ========================================
	// String Concatenation (+ operator)
	// ========================================
	// Only check for concatenation if + is outside of string literals
	// to avoid infinite recursion when property values contain " + "
	if e.hasConcatOperator(expr) {
		return e.evaluateStringConcatWithContext(expr, nodes, rels)
	}
	
	// ========================================
	// Property Access: n.property
	// ========================================
	if dotIdx := strings.Index(expr, "."); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]
		
		if node, ok := nodes[varName]; ok {
			// Don't return internal properties like embeddings
			if e.isInternalProperty(propName) {
				return nil
			}
			if val, ok := node.Properties[propName]; ok {
				return val
			}
			return nil
		}
		if rel, ok := rels[varName]; ok {
			if val, ok := rel.Properties[propName]; ok {
				return val
			}
			return nil
		}
	}
	
	// ========================================
	// Variable Reference - return whole node/rel
	// ========================================
	if node, ok := nodes[expr]; ok {
		return e.nodeToMap(node)
	}
	if rel, ok := rels[expr]; ok {
		return map[string]interface{}{
			"id":         string(rel.ID),
			"type":       rel.Type,
			"properties": rel.Properties,
		}
	}
	
	// ========================================
	// Literals
	// ========================================
	
	// null
	if lowerExpr == "null" {
		return nil
	}
	
	// Boolean
	if lowerExpr == "true" {
		return true
	}
	if lowerExpr == "false" {
		return false
	}
	
	// String literal (single or double quotes)
	if len(expr) >= 2 {
		if (expr[0] == '\'' && expr[len(expr)-1] == '\'') ||
			(expr[0] == '"' && expr[len(expr)-1] == '"') {
			return expr[1 : len(expr)-1]
		}
	}
	
	// Number literal
	if num, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return num
	}
	if num, err := strconv.ParseFloat(expr, 64); err == nil {
		return num
	}
	
	// Array literal [a, b, c]
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		return e.parseArrayValue(expr)
	}
	
	// Map literal {key: value}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return e.parseProperties(expr)
	}
	
	// Unknown - return as string
	return expr
}

// evaluateStringConcatWithContext handles string concatenation with + operator.
func (e *StorageExecutor) evaluateStringConcatWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) string {
	var result strings.Builder
	
	// Split by + but respect quotes and parentheses
	parts := e.splitByPlus(expr)
	
	for _, part := range parts {
		val := e.evaluateExpressionWithContext(part, nodes, rels)
		result.WriteString(fmt.Sprintf("%v", val))
	}
	
	return result.String()
}

// parseMergePattern parses a MERGE pattern like "(n:Label {prop: value})"
func (e *StorageExecutor) parseMergePattern(pattern string) (string, []string, map[string]interface{}, error) {
	pattern = strings.TrimSpace(pattern)
	if !strings.HasPrefix(pattern, "(") || !strings.HasSuffix(pattern, ")") {
		return "", nil, nil, fmt.Errorf("invalid pattern: %s", pattern)
	}
	pattern = pattern[1 : len(pattern)-1]

	// Extract variable name and labels
	varName := ""
	labels := []string{}
	props := make(map[string]interface{})

	// Find properties block
	propsStart := strings.Index(pattern, "{")
	labelPart := pattern
	if propsStart > 0 {
		labelPart = pattern[:propsStart]
		propsEnd := strings.LastIndex(pattern, "}")
		if propsEnd > propsStart {
			propsStr := pattern[propsStart+1 : propsEnd]
			props = e.parseProperties(propsStr)
		}
	}

	// Parse variable and labels
	parts := strings.Split(labelPart, ":")
	if len(parts) > 0 {
		varName = strings.TrimSpace(parts[0])
	}
	for i := 1; i < len(parts); i++ {
		label := strings.TrimSpace(parts[i])
		if label != "" {
			labels = append(labels, label)
		}
	}

	return varName, labels, props, nil
}

// applySetToNode applies SET clauses to a node.
func (e *StorageExecutor) applySetToNode(node *storage.Node, varName string, setClause string) {
	// Split SET clause into individual assignments, respecting parentheses and quotes
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
		
		// Evaluate the expression and set the property
		node.Properties[propName] = e.evaluateSetExpression(propValue)
	}
}

// splitSetAssignments splits a SET clause into individual assignments,
// respecting parentheses and quotes.
func (e *StorageExecutor) splitSetAssignments(setClause string) []string {
	var assignments []string
	var current strings.Builder
	parenDepth := 0
	inQuote := false
	quoteChar := rune(0)
	
	for i, c := range setClause {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				// Check for escaped quote
				if i > 0 && setClause[i-1] != '\\' {
					inQuote = false
				}
			}
			current.WriteRune(c)
		case c == '(' && !inQuote:
			parenDepth++
			current.WriteRune(c)
		case c == ')' && !inQuote:
			parenDepth--
			current.WriteRune(c)
		case c == ',' && !inQuote && parenDepth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				assignments = append(assignments, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	
	// Add final assignment
	if s := strings.TrimSpace(current.String()); s != "" {
		assignments = append(assignments, s)
	}
	
	return assignments
}

// evaluateSetExpression evaluates a Cypher expression for SET clauses.
func (e *StorageExecutor) evaluateSetExpression(expr string) interface{} {
	expr = strings.TrimSpace(expr)
	
	// Handle null
	if strings.ToLower(expr) == "null" {
		return nil
	}
	
	// Handle simple literals
	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") {
		return expr[1 : len(expr)-1]
	}
	if strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"") {
		return expr[1 : len(expr)-1]
	}
	if expr == "true" {
		return true
	}
	if expr == "false" {
		return false
	}
	
	// Handle numbers
	if val, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return val
	}
	if val, err := strconv.ParseFloat(expr, 64); err == nil {
		return val
	}
	
	// Handle arrays (simplified)
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == "" {
			return []interface{}{}
		}
		// Simple split for basic arrays
		parts := strings.Split(inner, ",")
		result := make([]interface{}, len(parts))
		for i, p := range parts {
			result[i] = e.evaluateSetExpression(strings.TrimSpace(p))
		}
		return result
	}
	
	// Handle function calls and expressions
	lowerExpr := strings.ToLower(expr)
	
	// timestamp() - returns current timestamp
	if lowerExpr == "timestamp()" {
		return e.idCounter()
	}
	
	// datetime() - returns ISO date string
	if lowerExpr == "datetime()" {
		return fmt.Sprintf("%d", e.idCounter())
	}
	
	// randomUUID() or randomuuid()
	if lowerExpr == "randomuuid()" {
		return e.generateUUID()
	}
	
	// Handle string concatenation: 'prefix-' + toString(timestamp()) + '-' + substring(randomUUID(), 0, 8)
	if strings.Contains(expr, " + ") {
		return e.evaluateStringConcat(expr)
	}
	
	// Handle toString(expr)
	if strings.HasPrefix(lowerExpr, "tostring(") && strings.HasSuffix(expr, ")") {
		inner := expr[9 : len(expr)-1]
		val := e.evaluateSetExpression(inner)
		return fmt.Sprintf("%v", val)
	}
	
	// Handle substring(str, start, length)
	if strings.HasPrefix(lowerExpr, "substring(") && strings.HasSuffix(expr, ")") {
		return e.evaluateSubstring(expr)
	}
	
	// If nothing else matched, return as-is (already substituted parameter value)
	return expr
}

// evaluateStringConcat handles string concatenation with +
func (e *StorageExecutor) evaluateStringConcat(expr string) string {
	var result strings.Builder
	
	// Split by + but respect quotes and parentheses
	parts := e.splitByPlus(expr)
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		val := e.evaluateSetExpression(part)
		result.WriteString(fmt.Sprintf("%v", val))
	}
	
	return result.String()
}

// hasConcatOperator checks if the expression has a + operator outside of quotes.
// This prevents infinite recursion when property values contain " + " in text.
func (e *StorageExecutor) hasConcatOperator(expr string) bool {
	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0
	
	for i := 0; i < len(expr); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		case c == '(' && !inQuote:
			parenDepth++
		case c == ')' && !inQuote:
			parenDepth--
		case c == '+' && !inQuote && parenDepth == 0:
			// Check for space before and after (to avoid matching ++ or += etc)
			hasBefore := i > 0 && expr[i-1] == ' '
			hasAfter := i < len(expr)-1 && expr[i+1] == ' '
			if hasBefore && hasAfter {
				return true
			}
		}
	}
	return false
}

// splitByPlus splits an expression by + operator, respecting quotes and parentheses
func (e *StorageExecutor) splitByPlus(expr string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	inQuote := false
	quoteChar := rune(0)
	
	for i := 0; i < len(expr); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			current.WriteRune(c)
		case c == '(' && !inQuote:
			parenDepth++
			current.WriteRune(c)
		case c == ')' && !inQuote:
			parenDepth--
			current.WriteRune(c)
		case c == '+' && !inQuote && parenDepth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				parts = append(parts, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	
	if s := strings.TrimSpace(current.String()); s != "" {
		parts = append(parts, s)
	}
	
	return parts
}

// evaluateSubstring handles substring(str, start, length)
func (e *StorageExecutor) evaluateSubstring(expr string) string {
	// Extract arguments from substring(str, start, length)
	inner := expr[10 : len(expr)-1] // Remove "substring(" and ")"
	
	// Split by comma, respecting parentheses
	args := e.splitFunctionArgs(inner)
	if len(args) < 2 {
		return ""
	}
	
	// Evaluate the string argument
	str := fmt.Sprintf("%v", e.evaluateSetExpression(args[0]))
	
	// Parse start
	start, err := strconv.Atoi(strings.TrimSpace(args[1]))
	if err != nil {
		start = 0
	}
	
	// Parse optional length
	length := len(str) - start
	if len(args) >= 3 {
		if l, err := strconv.Atoi(strings.TrimSpace(args[2])); err == nil {
			length = l
		}
	}
	
	// Apply substring
	if start >= len(str) {
		return ""
	}
	end := start + length
	if end > len(str) {
		end = len(str)
	}
	return str[start:end]
}

// splitFunctionArgs splits function arguments by comma, respecting parentheses
func (e *StorageExecutor) splitFunctionArgs(args string) []string {
	var result []string
	var current strings.Builder
	parenDepth := 0
	
	for _, c := range args {
		switch c {
		case '(':
			parenDepth++
			current.WriteRune(c)
		case ')':
			parenDepth--
			current.WriteRune(c)
		case ',':
			if parenDepth == 0 {
				result = append(result, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteRune(c)
			}
		default:
			current.WriteRune(c)
		}
	}
	
	if s := strings.TrimSpace(current.String()); s != "" {
		result = append(result, s)
	}
	
	return result
}

// generateUUID generates a simple UUID-like string
func (e *StorageExecutor) generateUUID() string {
	// Use crypto/rand for proper UUID
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// nodeToMap converts a storage.Node to a map for result output.
// Filters out internal properties like embeddings which are huge.
func (e *StorageExecutor) nodeToMap(node *storage.Node) map[string]interface{} {
	// Filter out internal/large properties
	filteredProps := make(map[string]interface{})
	for k, v := range node.Properties {
		if e.isInternalProperty(k) {
			continue
		}
		filteredProps[k] = v
	}
	
	return map[string]interface{}{
		"id":         string(node.ID),
		"labels":     node.Labels,
		"properties": filteredProps,
	}
}

// isInternalProperty returns true for properties that should not be returned in results.
// This includes embeddings (huge float arrays) and other internal metadata.
func (e *StorageExecutor) isInternalProperty(propName string) bool {
	internalProps := map[string]bool{
		"embedding":        true,
		"embeddings":       true,
		"vector":           true,
		"vectors":          true,
		"_embedding":       true,
		"_embeddings":      true,
		"chunk_embedding":  true,
		"chunk_embeddings": true,
	}
	return internalProps[strings.ToLower(propName)]
}

// extractVarName extracts the variable name from a pattern like "(n:Label {...})"
func (e *StorageExecutor) extractVarName(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if strings.HasPrefix(pattern, "(") {
		pattern = pattern[1:]
	}
	// Find first : or { or )
	for i, c := range pattern {
		if c == ':' || c == '{' || c == ')' || c == ' ' {
			name := strings.TrimSpace(pattern[:i])
			if name != "" {
				return name
			}
			break
		}
	}
	return "n" // Default variable name
}

// extractLabels extracts labels from a pattern like "(n:Label1:Label2 {...})"
func (e *StorageExecutor) extractLabels(pattern string) []string {
	pattern = strings.TrimSpace(pattern)
	if strings.HasPrefix(pattern, "(") {
		pattern = pattern[1:]
	}
	if strings.HasSuffix(pattern, ")") {
		pattern = pattern[:len(pattern)-1]
	}
	
	// Remove properties block
	if propsStart := strings.Index(pattern, "{"); propsStart > 0 {
		pattern = pattern[:propsStart]
	}
	
	// Split by : and extract labels
	parts := strings.Split(pattern, ":")
	labels := []string{}
	for i := 1; i < len(parts); i++ {
		label := strings.TrimSpace(parts[i])
		// Remove spaces and trailing characters
		if spaceIdx := strings.IndexAny(label, " {"); spaceIdx > 0 {
			label = label[:spaceIdx]
		}
		if label != "" {
			labels = append(labels, label)
		}
	}
	return labels
}

// executeDelete handles DELETE queries.
func (e *StorageExecutor) executeDelete(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Parse: MATCH (n) WHERE ... DELETE n or DETACH DELETE n
	upper := strings.ToUpper(cypher)
	detach := strings.Contains(upper, "DETACH")

	// Get MATCH part
	matchIdx := strings.Index(upper, "MATCH")

	// Find the delete clause - could be "DELETE" or "DETACH DELETE"
	var deleteIdx int
	if detach {
		deleteIdx = strings.Index(upper, "DETACH DELETE")
		if deleteIdx == -1 {
			deleteIdx = strings.Index(upper, "DETACH")
		}
	} else {
		deleteIdx = strings.Index(upper, " DELETE ")
		if deleteIdx == -1 {
			deleteIdx = strings.Index(upper, " DELETE")
		}
	}

	if matchIdx == -1 || deleteIdx == -1 {
		return nil, fmt.Errorf("DELETE requires a MATCH clause")
	}

	// Execute the match first
	matchQuery := cypher[matchIdx:deleteIdx] + " RETURN *"
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}

	// Delete matched nodes
	for _, row := range matchResult.Rows {
		for _, val := range row {
			if node, ok := val.(map[string]interface{}); ok {
				if id, ok := node["id"].(string); ok {
					if detach {
						// Delete all connected edges first
						edges, _ := e.storage.GetOutgoingEdges(storage.NodeID(id))
						for _, edge := range edges {
							e.storage.DeleteEdge(edge.ID)
							result.Stats.RelationshipsDeleted++
						}
						edges, _ = e.storage.GetIncomingEdges(storage.NodeID(id))
						for _, edge := range edges {
							e.storage.DeleteEdge(edge.ID)
							result.Stats.RelationshipsDeleted++
						}
					}
					if err := e.storage.DeleteNode(storage.NodeID(id)); err == nil {
						result.Stats.NodesDeleted++
					}
				}
			}
		}
	}

	return result, nil
}

// executeSet handles MATCH ... SET queries.
func (e *StorageExecutor) executeSet(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	upper := strings.ToUpper(cypher)
	matchIdx := strings.Index(upper, "MATCH")
	setIdx := strings.Index(upper, " SET ")
	returnIdx := strings.Index(upper, "RETURN")

	if matchIdx == -1 || setIdx == -1 {
		return nil, fmt.Errorf("SET requires a MATCH clause")
	}

	// Execute the match first
	var matchQuery string
	if returnIdx > 0 {
		matchQuery = cypher[matchIdx:setIdx] + " RETURN *"
	} else {
		matchQuery = cypher[matchIdx:setIdx] + " RETURN *"
	}
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}

	// Parse SET clause: SET n.property = value
	var setPart string
	if returnIdx > 0 {
		setPart = strings.TrimSpace(cypher[setIdx+5 : returnIdx])
	} else {
		setPart = strings.TrimSpace(cypher[setIdx+5:])
	}

	// Parse assignment: n.property = value
	eqIdx := strings.Index(setPart, "=")
	if eqIdx == -1 {
		return nil, fmt.Errorf("SET requires an assignment")
	}

	left := strings.TrimSpace(setPart[:eqIdx])
	right := strings.TrimSpace(setPart[eqIdx+1:])

	// Extract variable and property
	parts := strings.SplitN(left, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("SET requires property access (n.property)")
	}
	variable := parts[0]
	propName := parts[1]
	propValue := e.parseValue(right)

	// Update matched nodes
	for _, row := range matchResult.Rows {
		for _, val := range row {
			if node, ok := val.(map[string]interface{}); ok {
				if id, ok := node["id"].(string); ok {
					storageNode, err := e.storage.GetNode(storage.NodeID(id))
					if err != nil {
						continue
					}
					if storageNode.Properties == nil {
						storageNode.Properties = make(map[string]interface{})
					}
					storageNode.Properties[propName] = propValue
					if err := e.storage.UpdateNode(storageNode); err == nil {
						result.Stats.PropertiesSet++
					}
				}
			}
		}
	}

	// Handle RETURN
	if returnIdx > 0 {
		returnPart := strings.TrimSpace(cypher[returnIdx+6:])
		returnItems := e.parseReturnItems(returnPart)
		result.Columns = make([]string, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}

		// Re-fetch and return updated nodes
		for _, row := range matchResult.Rows {
			for _, val := range row {
				if node, ok := val.(map[string]interface{}); ok {
					if id, ok := node["id"].(string); ok {
						storageNode, _ := e.storage.GetNode(storage.NodeID(id))
						if storageNode != nil {
							newRow := make([]interface{}, len(returnItems))
							for j, item := range returnItems {
								newRow[j] = e.resolveReturnItem(item, variable, storageNode)
							}
							result.Rows = append(result.Rows, newRow)
						}
					}
				}
			}
		}
	}

	return result, nil
}

// executeCall handles CALL procedure queries.
func (e *StorageExecutor) executeCall(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	switch {
	case strings.Contains(upper, "NORNICDB.VERSION"):
		return e.callNornicDbVersion()
	case strings.Contains(upper, "NORNICDB.STATS"):
		return e.callNornicDbStats()
	case strings.Contains(upper, "NORNICDB.DECAY.INFO"):
		return e.callNornicDbDecayInfo()
	case strings.Contains(upper, "DB.SCHEMA.VISUALIZATION"):
		return e.callDbSchemaVisualization()
	case strings.Contains(upper, "DB.SCHEMA.NODEPROPERTIES"):
		return e.callDbSchemaNodeProperties()
	case strings.Contains(upper, "DB.SCHEMA.RELPROPERTIES"):
		return e.callDbSchemaRelProperties()
	case strings.Contains(upper, "DB.LABELS"):
		return e.callDbLabels()
	case strings.Contains(upper, "DB.RELATIONSHIPTYPES"):
		return e.callDbRelationshipTypes()
	case strings.Contains(upper, "DB.INDEXES"):
		return e.callDbIndexes()
	case strings.Contains(upper, "DB.CONSTRAINTS"):
		return e.callDbConstraints()
	case strings.Contains(upper, "DB.PROPERTYKEYS"):
		return e.callDbPropertyKeys()
	case strings.Contains(upper, "DBMS.COMPONENTS"):
		return e.callDbmsComponents()
	case strings.Contains(upper, "DBMS.PROCEDURES"):
		return e.callDbmsProcedures()
	case strings.Contains(upper, "DBMS.FUNCTIONS"):
		return e.callDbmsFunctions()
	default:
		return nil, fmt.Errorf("unknown procedure: %s", cypher)
	}
}

func (e *StorageExecutor) callDbLabels() (*ExecuteResult, error) {
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return nil, err
	}

	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"label"},
		Rows:    make([][]interface{}, 0, len(labelSet)),
	}
	for label := range labelSet {
		result.Rows = append(result.Rows, []interface{}{label})
	}
	return result, nil
}

func (e *StorageExecutor) callDbRelationshipTypes() (*ExecuteResult, error) {
	edges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	typeSet := make(map[string]bool)
	for _, edge := range edges {
		typeSet[edge.Type] = true
	}

	result := &ExecuteResult{
		Columns: []string{"relationshipType"},
		Rows:    make([][]interface{}, 0, len(typeSet)),
	}
	for relType := range typeSet {
		result.Rows = append(result.Rows, []interface{}{relType})
	}
	return result, nil
}

func (e *StorageExecutor) callDbIndexes() (*ExecuteResult, error) {
	// Return empty for now - no indexes implemented yet
	return &ExecuteResult{
		Columns: []string{"name", "type", "labelsOrTypes", "properties", "state"},
		Rows:    [][]interface{}{},
	}, nil
}

func (e *StorageExecutor) callDbConstraints() (*ExecuteResult, error) {
	// Return empty for now
	return &ExecuteResult{
		Columns: []string{"name", "type", "labelsOrTypes", "properties"},
		Rows:    [][]interface{}{},
	}, nil
}

func (e *StorageExecutor) callDbmsComponents() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"name", "versions", "edition"},
		Rows: [][]interface{}{
			{"NornicDB", []string{"1.0.0"}, "community"},
		},
	}, nil
}

// NornicDB-specific procedures

func (e *StorageExecutor) callNornicDbVersion() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"version", "build", "edition"},
		Rows: [][]interface{}{
			{"1.0.0", "development", "community"},
		},
	}, nil
}

func (e *StorageExecutor) callNornicDbStats() (*ExecuteResult, error) {
	nodeCount, _ := e.storage.NodeCount()
	edgeCount, _ := e.storage.EdgeCount()

	return &ExecuteResult{
		Columns: []string{"nodes", "relationships", "labels", "relationshipTypes"},
		Rows: [][]interface{}{
			{nodeCount, edgeCount, e.countLabels(), e.countRelTypes()},
		},
	}, nil
}

func (e *StorageExecutor) countLabels() int {
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return 0
	}
	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}
	return len(labelSet)
}

func (e *StorageExecutor) countRelTypes() int {
	edges, err := e.storage.AllEdges()
	if err != nil {
		return 0
	}
	typeSet := make(map[string]bool)
	for _, edge := range edges {
		typeSet[edge.Type] = true
	}
	return len(typeSet)
}

func (e *StorageExecutor) callNornicDbDecayInfo() (*ExecuteResult, error) {
	return &ExecuteResult{
		Columns: []string{"enabled", "halfLifeEpisodic", "halfLifeSemantic", "halfLifeProcedural", "archiveThreshold"},
		Rows: [][]interface{}{
			{true, "7 days", "69 days", "693 days", 0.05},
		},
	}, nil
}

// Neo4j schema procedures

func (e *StorageExecutor) callDbSchemaVisualization() (*ExecuteResult, error) {
	// Return a simplified schema visualization
	nodes, _ := e.storage.AllNodes()
	edges, _ := e.storage.AllEdges()

	// Collect unique labels and relationship types
	labelSet := make(map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			labelSet[label] = true
		}
	}

	relTypeSet := make(map[string]bool)
	for _, edge := range edges {
		relTypeSet[edge.Type] = true
	}

	// Build schema nodes (one per label)
	var schemaNodes []map[string]interface{}
	for label := range labelSet {
		schemaNodes = append(schemaNodes, map[string]interface{}{
			"label": label,
		})
	}

	// Build schema relationships
	var schemaRels []map[string]interface{}
	for relType := range relTypeSet {
		schemaRels = append(schemaRels, map[string]interface{}{
			"type": relType,
		})
	}

	return &ExecuteResult{
		Columns: []string{"nodes", "relationships"},
		Rows: [][]interface{}{
			{schemaNodes, schemaRels},
		},
	}, nil
}

func (e *StorageExecutor) callDbSchemaNodeProperties() (*ExecuteResult, error) {
	nodes, _ := e.storage.AllNodes()

	// Collect properties per label
	labelProps := make(map[string]map[string]bool)
	for _, node := range nodes {
		for _, label := range node.Labels {
			if _, ok := labelProps[label]; !ok {
				labelProps[label] = make(map[string]bool)
			}
			for prop := range node.Properties {
				labelProps[label][prop] = true
			}
		}
	}

	result := &ExecuteResult{
		Columns: []string{"nodeLabel", "propertyName", "propertyType"},
		Rows:    [][]interface{}{},
	}

	for label, props := range labelProps {
		for prop := range props {
			result.Rows = append(result.Rows, []interface{}{label, prop, "ANY"})
		}
	}

	return result, nil
}

func (e *StorageExecutor) callDbSchemaRelProperties() (*ExecuteResult, error) {
	edges, _ := e.storage.AllEdges()

	// Collect properties per relationship type
	typeProps := make(map[string]map[string]bool)
	for _, edge := range edges {
		if _, ok := typeProps[edge.Type]; !ok {
			typeProps[edge.Type] = make(map[string]bool)
		}
		for prop := range edge.Properties {
			typeProps[edge.Type][prop] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"relType", "propertyName", "propertyType"},
		Rows:    [][]interface{}{},
	}

	for relType, props := range typeProps {
		for prop := range props {
			result.Rows = append(result.Rows, []interface{}{relType, prop, "ANY"})
		}
	}

	return result, nil
}

func (e *StorageExecutor) callDbPropertyKeys() (*ExecuteResult, error) {
	nodes, _ := e.storage.AllNodes()
	edges, _ := e.storage.AllEdges()

	propSet := make(map[string]bool)
	for _, node := range nodes {
		for prop := range node.Properties {
			propSet[prop] = true
		}
	}
	for _, edge := range edges {
		for prop := range edge.Properties {
			propSet[prop] = true
		}
	}

	result := &ExecuteResult{
		Columns: []string{"propertyKey"},
		Rows:    make([][]interface{}, 0, len(propSet)),
	}
	for prop := range propSet {
		result.Rows = append(result.Rows, []interface{}{prop})
	}

	return result, nil
}

func (e *StorageExecutor) callDbmsProcedures() (*ExecuteResult, error) {
	procedures := [][]interface{}{
		{"db.labels", "Lists all labels in the database", "READ"},
		{"db.relationshipTypes", "Lists all relationship types", "READ"},
		{"db.propertyKeys", "Lists all property keys", "READ"},
		{"db.indexes", "Lists all indexes", "READ"},
		{"db.constraints", "Lists all constraints", "READ"},
		{"db.schema.visualization", "Visualizes the database schema", "READ"},
		{"db.schema.nodeProperties", "Lists node properties by label", "READ"},
		{"db.schema.relProperties", "Lists relationship properties by type", "READ"},
		{"dbms.components", "Lists database components", "DBMS"},
		{"dbms.procedures", "Lists available procedures", "DBMS"},
		{"dbms.functions", "Lists available functions", "DBMS"},
		{"nornicdb.version", "Returns NornicDB version", "READ"},
		{"nornicdb.stats", "Returns database statistics", "READ"},
		{"nornicdb.decay.info", "Returns memory decay configuration", "READ"},
	}

	return &ExecuteResult{
		Columns: []string{"name", "description", "mode"},
		Rows:    procedures,
	}, nil
}

func (e *StorageExecutor) callDbmsFunctions() (*ExecuteResult, error) {
	functions := [][]interface{}{
		{"count", "Counts items", "Aggregating"},
		{"sum", "Sums numeric values", "Aggregating"},
		{"avg", "Averages numeric values", "Aggregating"},
		{"min", "Returns minimum value", "Aggregating"},
		{"max", "Returns maximum value", "Aggregating"},
		{"collect", "Collects values into a list", "Aggregating"},
		{"id", "Returns internal ID", "Scalar"},
		{"labels", "Returns labels of a node", "Scalar"},
		{"type", "Returns type of relationship", "Scalar"},
		{"properties", "Returns properties map", "Scalar"},
		{"keys", "Returns property keys", "Scalar"},
		{"coalesce", "Returns first non-null value", "Scalar"},
		{"toString", "Converts to string", "Scalar"},
		{"toInteger", "Converts to integer", "Scalar"},
		{"toFloat", "Converts to float", "Scalar"},
		{"toBoolean", "Converts to boolean", "Scalar"},
		{"size", "Returns size of list/string", "Scalar"},
		{"length", "Returns path length", "Scalar"},
		{"head", "Returns first list element", "List"},
		{"tail", "Returns list without first element", "List"},
		{"last", "Returns last list element", "List"},
		{"range", "Creates a range list", "List"},
	}

	return &ExecuteResult{
		Columns: []string{"name", "description", "category"},
		Rows:    functions,
	}, nil
}

// Helper types and functions

type nodePatternInfo struct {
	variable   string
	labels     []string
	properties map[string]interface{}
}

type returnItem struct {
	expr  string
	alias string
}

func (e *StorageExecutor) parseNodePattern(pattern string) nodePatternInfo {
	info := nodePatternInfo{
		labels:     []string{},
		properties: make(map[string]interface{}),
	}

	// Remove outer parens
	pattern = strings.TrimSpace(pattern)
	if strings.HasPrefix(pattern, "(") && strings.HasSuffix(pattern, ")") {
		pattern = pattern[1 : len(pattern)-1]
	}

	// Extract properties
	braceIdx := strings.Index(pattern, "{")
	if braceIdx >= 0 {
		propsStr := pattern[braceIdx:]
		pattern = pattern[:braceIdx]
		info.properties = e.parseProperties(propsStr)
	}

	// Parse variable:Label:Label2
	parts := strings.Split(strings.TrimSpace(pattern), ":")
	if len(parts) > 0 && parts[0] != "" {
		info.variable = strings.TrimSpace(parts[0])
	}
	for i := 1; i < len(parts); i++ {
		if label := strings.TrimSpace(parts[i]); label != "" {
			info.labels = append(info.labels, label)
		}
	}

	return info
}

func (e *StorageExecutor) parseProperties(propsStr string) map[string]interface{} {
	props := make(map[string]interface{})

	// Remove outer braces
	propsStr = strings.TrimSpace(propsStr)
	if strings.HasPrefix(propsStr, "{") && strings.HasSuffix(propsStr, "}") {
		propsStr = propsStr[1 : len(propsStr)-1]
	}
	propsStr = strings.TrimSpace(propsStr)
	
	if propsStr == "" {
		return props
	}

	// Parse key-value pairs using a state machine that respects quotes, brackets, and nested structures
	pairs := e.splitPropertyPairs(propsStr)
	
	for _, pair := range pairs {
		colonIdx := strings.Index(pair, ":")
		if colonIdx <= 0 {
			continue
		}
		
		key := strings.TrimSpace(pair[:colonIdx])
		valueStr := strings.TrimSpace(pair[colonIdx+1:])
		
		// Parse the value
		props[key] = e.parsePropertyValue(valueStr)
	}

	return props
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// splitPropertyPairs splits a property string into key:value pairs,
// respecting quotes, brackets, and nested braces.
func (e *StorageExecutor) splitPropertyPairs(propsStr string) []string {
	var pairs []string
	var current strings.Builder
	depth := 0        // Track [], {} nesting
	inQuote := false
	quoteChar := rune(0)
	
	for i, c := range propsStr {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				// Check for escaped quote (look back for \)
				escaped := false
				if i > 0 {
					// Count consecutive backslashes before this quote
					backslashes := 0
					for j := i - 1; j >= 0 && propsStr[j] == '\\'; j-- {
						backslashes++
					}
					escaped = backslashes%2 == 1
				}
				if !escaped {
					inQuote = false
				}
			}
			current.WriteRune(c)
		case (c == '[' || c == '{' || c == '(') && !inQuote:
			depth++
			current.WriteRune(c)
		case (c == ']' || c == '}' || c == ')') && !inQuote:
			depth--
			current.WriteRune(c)
		case c == ',' && !inQuote && depth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				pairs = append(pairs, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	
	// Add final pair
	if s := strings.TrimSpace(current.String()); s != "" {
		pairs = append(pairs, s)
	}
	
	return pairs
}

// parsePropertyValue parses a single property value string into the appropriate Go type.
func (e *StorageExecutor) parsePropertyValue(valueStr string) interface{} {
	valueStr = strings.TrimSpace(valueStr)
	
	if valueStr == "" {
		return nil
	}
	
	// Handle null
	if strings.ToLower(valueStr) == "null" {
		return nil
	}
	
	// Handle quoted strings
	if len(valueStr) >= 2 {
		first, last := valueStr[0], valueStr[len(valueStr)-1]
		if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
			// Unescape the string content
			content := valueStr[1 : len(valueStr)-1]
			// Handle escaped quotes
			if first == '\'' {
				content = strings.ReplaceAll(content, "''", "'")
			} else {
				content = strings.ReplaceAll(content, "\\\"", "\"")
			}
			content = strings.ReplaceAll(content, "\\\\", "\\")
			return content
		}
	}
	
	// Handle booleans
	lowerVal := strings.ToLower(valueStr)
	if lowerVal == "true" {
		return true
	}
	if lowerVal == "false" {
		return false
	}
	
	// Handle integers
	if intVal, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
		return intVal
	}
	
	// Handle floats
	if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return floatVal
	}
	
	// Handle arrays
	if strings.HasPrefix(valueStr, "[") && strings.HasSuffix(valueStr, "]") {
		return e.parseArrayValue(valueStr)
	}
	
	// Handle nested maps (rare in properties, but possible)
	if strings.HasPrefix(valueStr, "{") && strings.HasSuffix(valueStr, "}") {
		return e.parseProperties(valueStr)
	}
	
	// Otherwise return as string (handles unquoted identifiers, etc.)
	return valueStr
}

// parseArrayValue parses a Cypher array literal like [1, 2, 3] or ['a', 'b', 'c']
func (e *StorageExecutor) parseArrayValue(arrayStr string) []interface{} {
	// Remove brackets
	inner := strings.TrimSpace(arrayStr[1 : len(arrayStr)-1])
	if inner == "" {
		return []interface{}{}
	}
	
	// Split array elements respecting nested structures
	elements := e.splitArrayElements(inner)
	result := make([]interface{}, len(elements))
	
	for i, elem := range elements {
		result[i] = e.parsePropertyValue(strings.TrimSpace(elem))
	}
	
	return result
}

// splitArrayElements splits array contents by comma, respecting nested structures and quotes
func (e *StorageExecutor) splitArrayElements(inner string) []string {
	var elements []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)
	
	for i, c := range inner {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				escaped := false
				if i > 0 && inner[i-1] == '\\' {
					escaped = true
				}
				if !escaped {
					inQuote = false
				}
			}
			current.WriteRune(c)
		case (c == '[' || c == '{') && !inQuote:
			depth++
			current.WriteRune(c)
		case (c == ']' || c == '}') && !inQuote:
			depth--
			current.WriteRune(c)
		case c == ',' && !inQuote && depth == 0:
			if s := strings.TrimSpace(current.String()); s != "" {
				elements = append(elements, s)
			}
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	
	if s := strings.TrimSpace(current.String()); s != "" {
		elements = append(elements, s)
	}
	
	return elements
}

func (e *StorageExecutor) parseReturnItems(returnPart string) []returnItem {
	items := []returnItem{}

	// Handle LIMIT clause
	upper := strings.ToUpper(returnPart)
	limitIdx := strings.Index(upper, "LIMIT")
	if limitIdx > 0 {
		returnPart = returnPart[:limitIdx]
	}

	// Handle ORDER BY clause
	orderIdx := strings.Index(upper, "ORDER")
	if orderIdx > 0 {
		returnPart = returnPart[:orderIdx]
	}

	// Split by comma
	parts := strings.Split(returnPart, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || part == "*" {
			continue
		}

		item := returnItem{expr: part}

		// Check for AS alias
		upperPart := strings.ToUpper(part)
		asIdx := strings.Index(upperPart, " AS ")
		if asIdx > 0 {
			item.expr = strings.TrimSpace(part[:asIdx])
			item.alias = strings.TrimSpace(part[asIdx+4:])
		}

		items = append(items, item)
	}

	// If empty or *, return all
	if len(items) == 0 {
		items = append(items, returnItem{expr: "*"})
	}

	return items
}

func (e *StorageExecutor) filterNodes(nodes []*storage.Node, variable, whereClause string) []*storage.Node {
	var filtered []*storage.Node

	for _, node := range nodes {
		if e.evaluateWhere(node, variable, whereClause) {
			filtered = append(filtered, node)
		}
	}

	return filtered
}

func (e *StorageExecutor) evaluateWhere(node *storage.Node, variable, whereClause string) bool {
	// Handle multiple conditions with AND/OR
	upperClause := strings.ToUpper(whereClause)

	// Handle AND conditions
	if strings.Contains(upperClause, " AND ") {
		andIdx := strings.Index(upperClause, " AND ")
		left := strings.TrimSpace(whereClause[:andIdx])
		right := strings.TrimSpace(whereClause[andIdx+5:])
		return e.evaluateWhere(node, variable, left) && e.evaluateWhere(node, variable, right)
	}

	// Handle OR conditions
	if strings.Contains(upperClause, " OR ") {
		orIdx := strings.Index(upperClause, " OR ")
		left := strings.TrimSpace(whereClause[:orIdx])
		right := strings.TrimSpace(whereClause[orIdx+4:])
		return e.evaluateWhere(node, variable, left) || e.evaluateWhere(node, variable, right)
	}

	// Handle string operators (case-insensitive check)
	if strings.Contains(upperClause, " CONTAINS ") {
		return e.evaluateStringOp(node, variable, whereClause, "CONTAINS")
	}
	if strings.Contains(upperClause, " STARTS WITH ") {
		return e.evaluateStringOp(node, variable, whereClause, "STARTS WITH")
	}
	if strings.Contains(upperClause, " ENDS WITH ") {
		return e.evaluateStringOp(node, variable, whereClause, "ENDS WITH")
	}
	if strings.Contains(upperClause, " IN ") {
		return e.evaluateInOp(node, variable, whereClause)
	}
	if strings.Contains(upperClause, " IS NULL") {
		return e.evaluateIsNull(node, variable, whereClause, false)
	}
	if strings.Contains(upperClause, " IS NOT NULL") {
		return e.evaluateIsNull(node, variable, whereClause, true)
	}

	// Determine operator and split accordingly
	var op string
	var opIdx int

	// Check operators in order of length (longest first to avoid partial matches)
	operators := []string{"<>", "!=", ">=", "<=", "=~", ">", "<", "="}
	for _, testOp := range operators {
		idx := strings.Index(whereClause, testOp)
		if idx >= 0 {
			op = testOp
			opIdx = idx
			break
		}
	}

	if op == "" {
		return true // No valid operator found, include all
	}

	left := strings.TrimSpace(whereClause[:opIdx])
	right := strings.TrimSpace(whereClause[opIdx+len(op):])

	// Extract property from left side (e.g., "n.name")
	if !strings.HasPrefix(left, variable+".") {
		return true // Not a property comparison we can handle
	}

	propName := left[len(variable)+1:]

	// Get actual value
	actualVal, exists := node.Properties[propName]
	if !exists {
		return false
	}

	// Parse the expected value from right side
	expectedVal := e.parseValue(right)

	// Perform comparison based on operator
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
	case "=~":
		return e.compareRegex(actualVal, expectedVal)
	default:
		return true
	}
}

// parseValue extracts the actual value from a Cypher literal
func (e *StorageExecutor) parseValue(s string) interface{} {
	s = strings.TrimSpace(s)

	// Handle quoted strings
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		return s[1 : len(s)-1]
	}

	// Handle booleans
	upper := strings.ToUpper(s)
	if upper == "TRUE" {
		return true
	}
	if upper == "FALSE" {
		return false
	}
	if upper == "NULL" {
		return nil
	}

	// Handle numbers
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return float64(i) // Normalize to float64 for comparison
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	return s
}

// nodeMatchesProps checks if a node's properties match the expected values.
func (e *StorageExecutor) nodeMatchesProps(node *storage.Node, props map[string]interface{}) bool {
	if props == nil {
		return true
	}
	for key, expected := range props {
		actual, exists := node.Properties[key]
		if !exists {
			return false
		}
		if !e.compareEqual(actual, expected) {
			return false
		}
	}
	return true
}

// compareEqual handles equality comparison with type coercion
func (e *StorageExecutor) compareEqual(actual, expected interface{}) bool {
	// Handle nil
	if actual == nil && expected == nil {
		return true
	}
	if actual == nil || expected == nil {
		return false
	}

	// Try numeric comparison
	actualNum, actualOk := toFloat64(actual)
	expectedNum, expectedOk := toFloat64(expected)
	if actualOk && expectedOk {
		return actualNum == expectedNum
	}

	// String comparison
	return fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
}

// compareGreater handles > comparison
func (e *StorageExecutor) compareGreater(actual, expected interface{}) bool {
	actualNum, actualOk := toFloat64(actual)
	expectedNum, expectedOk := toFloat64(expected)
	if actualOk && expectedOk {
		return actualNum > expectedNum
	}

	// String comparison as fallback
	return fmt.Sprintf("%v", actual) > fmt.Sprintf("%v", expected)
}

// compareLess handles < comparison
func (e *StorageExecutor) compareLess(actual, expected interface{}) bool {
	actualNum, actualOk := toFloat64(actual)
	expectedNum, expectedOk := toFloat64(expected)
	if actualOk && expectedOk {
		return actualNum < expectedNum
	}

	// String comparison as fallback
	return fmt.Sprintf("%v", actual) < fmt.Sprintf("%v", expected)
}

// compareRegex handles =~ regex comparison
func (e *StorageExecutor) compareRegex(actual, expected interface{}) bool {
	pattern, ok := expected.(string)
	if !ok {
		return false
	}

	actualStr := fmt.Sprintf("%v", actual)
	matched, err := regexp.MatchString(pattern, actualStr)
	if err != nil {
		return false
	}
	return matched
}

// evaluateStringOp handles CONTAINS, STARTS WITH, ENDS WITH
func (e *StorageExecutor) evaluateStringOp(node *storage.Node, variable, whereClause, op string) bool {
	upperClause := strings.ToUpper(whereClause)
	opIdx := strings.Index(upperClause, " "+op+" ")
	if opIdx < 0 {
		return true
	}

	left := strings.TrimSpace(whereClause[:opIdx])
	right := strings.TrimSpace(whereClause[opIdx+len(op)+2:])

	// Extract property
	if !strings.HasPrefix(left, variable+".") {
		return true
	}
	propName := left[len(variable)+1:]

	actualVal, exists := node.Properties[propName]
	if !exists {
		return false
	}

	actualStr := fmt.Sprintf("%v", actualVal)
	expectedStr := fmt.Sprintf("%v", e.parseValue(right))

	switch op {
	case "CONTAINS":
		return strings.Contains(actualStr, expectedStr)
	case "STARTS WITH":
		return strings.HasPrefix(actualStr, expectedStr)
	case "ENDS WITH":
		return strings.HasSuffix(actualStr, expectedStr)
	}
	return true
}

// evaluateInOp handles IN [list] operator
func (e *StorageExecutor) evaluateInOp(node *storage.Node, variable, whereClause string) bool {
	upperClause := strings.ToUpper(whereClause)
	inIdx := strings.Index(upperClause, " IN ")
	if inIdx < 0 {
		return true
	}

	left := strings.TrimSpace(whereClause[:inIdx])
	right := strings.TrimSpace(whereClause[inIdx+4:])

	// Extract property
	if !strings.HasPrefix(left, variable+".") {
		return true
	}
	propName := left[len(variable)+1:]

	actualVal, exists := node.Properties[propName]
	if !exists {
		return false
	}

	// Parse list: [val1, val2, ...]
	if strings.HasPrefix(right, "[") && strings.HasSuffix(right, "]") {
		listContent := right[1 : len(right)-1]
		items := strings.Split(listContent, ",")
		for _, item := range items {
			itemVal := e.parseValue(strings.TrimSpace(item))
			if e.compareEqual(actualVal, itemVal) {
				return true
			}
		}
	}
	return false
}

// evaluateIsNull handles IS NULL / IS NOT NULL
func (e *StorageExecutor) evaluateIsNull(node *storage.Node, variable, whereClause string, expectNotNull bool) bool {
	upperClause := strings.ToUpper(whereClause)
	var propExpr string

	if expectNotNull {
		idx := strings.Index(upperClause, " IS NOT NULL")
		propExpr = strings.TrimSpace(whereClause[:idx])
	} else {
		idx := strings.Index(upperClause, " IS NULL")
		propExpr = strings.TrimSpace(whereClause[:idx])
	}

	// Extract property
	if !strings.HasPrefix(propExpr, variable+".") {
		return true
	}
	propName := propExpr[len(variable)+1:]

	_, exists := node.Properties[propName]

	if expectNotNull {
		return exists
	}
	return !exists
}

// toFloat64 attempts to convert a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (e *StorageExecutor) resolveReturnItem(item returnItem, variable string, node *storage.Node) interface{} {
	// Use the comprehensive expression evaluator for all expressions
	// This supports: id(n), labels(n), keys(n), properties(n), n.prop, literals, etc.
	return e.evaluateExpression(item.expr, variable, node)
}

func (e *StorageExecutor) generateID() string {
	// Simple ID generation - use UUID in production
	return fmt.Sprintf("node-%d", e.idCounter())
}

var idCounter int64

func (e *StorageExecutor) idCounter() int64 {
	idCounter++
	return idCounter
}
