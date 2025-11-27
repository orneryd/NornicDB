// Package cypher provides Neo4j-compatible Cypher query execution for NornicDB.
//
// This package implements a Cypher query parser and executor that supports
// the core Neo4j Cypher query language features. It enables NornicDB to be
// compatible with existing Neo4j applications and tools.
//
// Supported Cypher Features:
//   - MATCH: Pattern matching with node and relationship patterns
//   - CREATE: Creating nodes and relationships
//   - MERGE: Upsert operations with ON CREATE/ON MATCH clauses
//   - DELETE/DETACH DELETE: Removing nodes and relationships
//   - SET: Updating node and relationship properties
//   - REMOVE: Removing properties and labels
//   - RETURN: Returning query results
//   - WHERE: Filtering with conditions
//   - WITH: Passing results between query parts
//   - OPTIONAL MATCH: Left outer joins
//   - CALL: Procedure calls
//   - UNWIND: List expansion
//
// Example Usage:
//
//	// Create executor with storage backend
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Execute Cypher queries
//	result, err := executor.Execute(ctx, "CREATE (n:Person {name: 'Alice', age: 30})", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Query with parameters
//	params := map[string]interface{}{
//		"name": "Alice",
//		"minAge": 25,
//	}
//	result, err = executor.Execute(ctx,
//		"MATCH (n:Person {name: $name}) WHERE n.age >= $minAge RETURN n", params)
//
//	// Complex query with relationships
//	result, err = executor.Execute(ctx, `
//		MATCH (a:Person)-[r:KNOWS]->(b:Person)
//		WHERE a.age > 25
//		RETURN a.name, r.since, b.name
//		ORDER BY a.age DESC
//		LIMIT 10
//	`, nil)
//
//	// Process results
//	for _, row := range result.Rows {
//		fmt.Printf("Row: %v\n", row)
//	}
//
// Neo4j Compatibility:
//
// The executor aims for high compatibility with Neo4j Cypher:
//   - Same syntax and semantics for core operations
//   - Parameter substitution with $param syntax
//   - Neo4j-style error messages and codes
//   - Compatible result format for drivers
//   - Support for Neo4j built-in functions
//
// Query Processing Pipeline:
//
// 1. **Parsing**: Query is parsed into an AST (Abstract Syntax Tree)
// 2. **Validation**: Syntax and semantic validation
// 3. **Parameter Substitution**: Replace $param with actual values
// 4. **Execution Planning**: Determine optimal execution strategy
// 5. **Execution**: Execute against storage backend
// 6. **Result Formatting**: Format results for Neo4j compatibility
//
// Performance Considerations:
//
//   - Pattern matching is optimized for common cases
//   - Indexes are used automatically when available
//   - Query planning chooses efficient execution paths
//   - Bulk operations are optimized for large datasets
//
// Limitations:
//
// Current limitations compared to full Neo4j:
//   - No user-defined procedures (CALL is limited to built-ins)
//   - No complex path expressions
//   - No graph algorithms (shortest path, etc.)
//   - No schema constraints (handled by storage layer)
//   - No transactions (single-query atomicity only)
//
// ELI12 (Explain Like I'm 12):
//
// Think of Cypher like asking questions about a social network:
//
//  1. **MATCH**: "Find all people named Alice" - like searching through
//     a phone book for everyone with a specific name.
//
//  2. **CREATE**: "Add a new person named Bob" - like writing a new
//     entry in the phone book.
//
//  3. **Relationships**: "Find who Alice knows" - like following the
//     lines between people on a friendship map.
//
//  4. **WHERE**: "Find people older than 25" - like adding a filter
//     to only show certain results.
//
//  5. **RETURN**: "Show me their names and ages" - like deciding which
//     information to display from your search.
//
// The Cypher executor is like a smart assistant that understands these
// questions and knows how to find the answers in your data!
package cypher

import (
	"context"
	"crypto/rand"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// StorageExecutor executes Cypher queries against a storage backend.
//
// The StorageExecutor provides the main interface for executing Cypher queries
// in NornicDB. It handles query parsing, validation, parameter substitution,
// and execution against the underlying storage engine.
//
// Key features:
//   - Neo4j-compatible Cypher syntax support
//   - Parameter substitution with $param syntax
//   - Query validation and error reporting
//   - Optimized execution planning
//   - Thread-safe concurrent execution
//
// Example:
//
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Simple node creation
//	result, _ := executor.Execute(ctx, "CREATE (n:Person {name: 'Alice'})", nil)
//
//	// Parameterized query
//	params := map[string]interface{}{"name": "Bob", "age": 30}
//	result, _ = executor.Execute(ctx,
//		"CREATE (n:Person {name: $name, age: $age})", params)
//
//	// Complex pattern matching
//	result, _ = executor.Execute(ctx, `
//		MATCH (a:Person)-[:KNOWS]->(b:Person)
//		WHERE a.age > 25
//		RETURN a.name, b.name
//	`, nil)
//
// Thread Safety:
//
//	The executor is thread-safe and can handle concurrent queries.
type StorageExecutor struct {
	parser    *Parser
	storage   storage.Engine
	txContext *TransactionContext // Active transaction context
	cache     *QueryCache         // Query result cache
}

// NewStorageExecutor creates a new Cypher executor with the given storage backend.
//
// The executor is initialized with a parser and connected to the storage engine.
// It's ready to execute Cypher queries immediately after creation.
//
// Parameters:
//   - store: Storage engine to execute queries against (required)
//
// Returns:
//   - StorageExecutor ready for query execution
//
// Example:
//
//	// Create storage and executor
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Executor is ready for queries
//	result, err := executor.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
func NewStorageExecutor(store storage.Engine) *StorageExecutor {
	return &StorageExecutor{
		parser:  NewParser(),
		storage: store,
		cache:   NewQueryCache(1000), // Cache up to 1000 query results
	}
}

// Execute parses and executes a Cypher query with optional parameters.
//
// This is the main entry point for Cypher query execution. The method handles
// the complete query lifecycle: parsing, validation, parameter substitution,
// execution planning, and result formatting.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - cypher: Cypher query string
//   - params: Optional parameters for $param substitution
//
// Returns:
//   - ExecuteResult with columns and rows
//   - Error if query parsing or execution fails
//
// Example:
//
//	// Simple query without parameters
//	result, err := executor.Execute(ctx, "MATCH (n:Person) RETURN n.name", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Parameterized query
//	params := map[string]interface{}{
//		"name": "Alice",
//		"minAge": 25,
//	}
//	result, err = executor.Execute(ctx, `
//		MATCH (n:Person {name: $name})
//		WHERE n.age >= $minAge
//		RETURN n.name, n.age
//	`, params)
//
//	// Process results
//	fmt.Printf("Columns: %v\n", result.Columns)
//	for _, row := range result.Rows {
//		fmt.Printf("Row: %v\n", row)
//	}
//
// Supported Query Types:
//   - CREATE: Node and relationship creation
//   - MATCH: Pattern matching and traversal
//   - MERGE: Upsert operations
//   - DELETE: Node and relationship deletion
//   - SET: Property updates
//   - REMOVE: Property and label removal
//   - RETURN: Result projection
//   - WHERE: Filtering conditions
//   - WITH: Query chaining
//   - OPTIONAL MATCH: Left outer joins
//
// Error Handling:
//
//	Returns detailed error messages for syntax errors, type mismatches,
//	and execution failures with Neo4j-compatible error codes.
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

	// Check if query is read-only and cacheable
	upperQuery := strings.ToUpper(cypher)
	isReadOnly := strings.HasPrefix(upperQuery, "MATCH") ||
		strings.HasPrefix(upperQuery, "CALL DB.") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		strings.HasPrefix(upperQuery, "EXPLAIN") ||
		strings.HasPrefix(upperQuery, "PROFILE") ||
		strings.HasPrefix(upperQuery, "RETURN")

	// Try cache for read-only queries
	if isReadOnly && e.cache != nil {
		if cached, found := e.cache.Get(cypher, params); found {
			return cached, nil
		}
	}

	// Check for transaction control statements FIRST
	if result, err := e.parseTransactionStatement(cypher); result != nil || err != nil {
		return result, err
	}

	// Check for EXPLAIN/PROFILE execution modes
	mode, innerQuery := parseExecutionMode(cypher)
	if mode == ModeExplain {
		return e.executeExplain(ctx, innerQuery)
	}
	if mode == ModeProfile {
		return e.executeProfile(ctx, innerQuery)
	}

	// If in explicit transaction, execute within it
	if e.txContext != nil && e.txContext.active {
		return e.executeInTransaction(ctx, cypher, upperQuery)
	}

	// Otherwise, auto-commit single query (implicit transaction)
	// This maintains Neo4j compatibility: single queries are atomic
	result, err := e.executeImplicit(ctx, cypher, upperQuery)

	// Cache successful read-only queries
	if err == nil && isReadOnly && e.cache != nil {
		// Determine TTL based on query type
		ttl := 60 * time.Second // Default: 60s for data queries
		if strings.Contains(upperQuery, "CALL DB.") || strings.Contains(upperQuery, "SHOW") {
			ttl = 300 * time.Second // 5 minutes for schema queries
		}
		e.cache.Put(cypher, params, result, ttl)
	}

	// Invalidate cache on write operations
	if !isReadOnly && e.cache != nil {
		e.cache.Invalidate()
	}

	return result, err
}

// executeImplicit wraps a single query in an implicit transaction.
func (e *StorageExecutor) executeImplicit(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// Start implicit transaction
	if _, err := e.handleBegin(); err != nil {
		return nil, fmt.Errorf("implicit transaction begin failed: %w", err)
	}

	// Execute query
	result, err := e.executeInTransaction(ctx, cypher, upperQuery)

	// Auto-commit or rollback
	if err != nil {
		e.handleRollback()
		return nil, err
	}

	if _, commitErr := e.handleCommit(); commitErr != nil {
		return nil, fmt.Errorf("implicit transaction commit failed: %w", commitErr)
	}

	return result, nil
}

// executeWithoutTransaction executes query without transaction wrapping (original path).
func (e *StorageExecutor) executeWithoutTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// Route to appropriate handler based on query type
	// upperQuery is passed in to avoid redundant conversion
	
	// Cache keyword checks to avoid repeated searches
	startsWithMatch := strings.HasPrefix(upperQuery, "MATCH")
	startsWithCreate := strings.HasPrefix(upperQuery, "CREATE")
	startsWithMerge := strings.HasPrefix(upperQuery, "MERGE")
	
	// MERGE queries get special handling - they have their own ON CREATE SET / ON MATCH SET logic
	if startsWithMerge {
		return e.executeMerge(ctx, cypher)
	}

	// Cache findKeywordIndex results for compound query detection
	var mergeIdx, createIdx, withIdx, deleteIdx, optionalMatchIdx int = -1, -1, -1, -1, -1
	
	if startsWithMatch {
		// Only search for keywords if query starts with MATCH
		mergeIdx = findKeywordIndex(cypher, "MERGE")
		createIdx = findKeywordIndex(cypher, "CREATE")
		optionalMatchIdx = findKeywordIndex(cypher, "OPTIONAL MATCH")
	} else if startsWithCreate {
		// Only search for WITH/DELETE if query starts with CREATE
		withIdx = findKeywordIndex(cypher, "WITH")
		if withIdx > 0 {
			deleteIdx = findKeywordIndex(cypher, "DELETE")
		}
	}

	// Compound queries: MATCH ... MERGE ... (with variable references)
	if startsWithMatch && mergeIdx > 0 {
		return e.executeCompoundMatchMerge(ctx, cypher)
	}

	// Compound queries: MATCH ... CREATE ... (create relationship between matched nodes)
	if startsWithMatch && createIdx > 0 {
		return e.executeCompoundMatchCreate(ctx, cypher)
	}

	// Compound queries: CREATE ... WITH ... DELETE (create then delete in same statement)
	if startsWithCreate && withIdx > 0 && deleteIdx > 0 {
		return e.executeCompoundCreateWithDelete(ctx, cypher)
	}

	// Cache contains checks for DELETE
	hasDeleteSpace := strings.Contains(upperQuery, " DELETE ")
	hasDeleteEnd := strings.HasSuffix(upperQuery, " DELETE")
	hasDetachDelete := strings.Contains(upperQuery, "DETACH DELETE")
	
	// Check for compound queries - MATCH ... DELETE, MATCH ... SET, etc.
	if hasDeleteSpace || hasDeleteEnd || hasDetachDelete {
		return e.executeDelete(ctx, cypher)
	}
	
	// Normalize whitespace once for SET/REMOVE detection
	normalizedUpper := strings.ReplaceAll(strings.ReplaceAll(upperQuery, "\n", " "), "\t", " ")
	
	// Cache SET-related checks
	hasSet := strings.Contains(normalizedUpper, " SET ")
	hasOnCreateSet := strings.Contains(normalizedUpper, "ON CREATE SET")
	hasOnMatchSet := strings.Contains(normalizedUpper, "ON MATCH SET")
	
	// Only route to executeSet if it's a MATCH ... SET or standalone SET
	if hasSet && !hasOnCreateSet && !hasOnMatchSet {
		return e.executeSet(ctx, cypher)
	}

	// Handle MATCH ... REMOVE (property removal)
	if strings.Contains(normalizedUpper, " REMOVE ") {
		return e.executeRemove(ctx, cypher)
	}

	// Compound queries: MATCH ... OPTIONAL MATCH ...
	if startsWithMatch && optionalMatchIdx > 0 {
		return e.executeCompoundMatchOptionalMatch(ctx, cypher)
	}

	switch {
	case strings.HasPrefix(upperQuery, "OPTIONAL MATCH"):
		return e.executeOptionalMatch(ctx, cypher)
	case startsWithMatch && isShortestPathQuery(cypher):
		// Handle shortestPath() and allShortestPaths() queries
		query, err := e.parseShortestPathQuery(cypher)
		if err != nil {
			return nil, err
		}
		return e.executeShortestPathQuery(query)
	case startsWithMatch:
		return e.executeMatch(ctx, cypher)
	case strings.HasPrefix(upperQuery, "CREATE CONSTRAINT"),
		strings.HasPrefix(upperQuery, "CREATE FULLTEXT INDEX"),
		strings.HasPrefix(upperQuery, "CREATE VECTOR INDEX"),
		strings.HasPrefix(upperQuery, "CREATE INDEX"):
		// Schema commands - constraints and indexes (check more specific patterns first)
		return e.executeSchemaCommand(ctx, cypher)
	case startsWithCreate:
		return e.executeCreate(ctx, cypher)
	case strings.HasPrefix(upperQuery, "DELETE"), hasDetachDelete:
		return e.executeDelete(ctx, cypher)
	case strings.HasPrefix(upperQuery, "CALL"):
		return e.executeCall(ctx, cypher)
	case strings.HasPrefix(upperQuery, "RETURN"):
		return e.executeReturn(ctx, cypher)
	case strings.HasPrefix(upperQuery, "DROP"):
		// DROP INDEX/CONSTRAINT - treat as no-op (NornicDB manages indexes internally)
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	case strings.HasPrefix(upperQuery, "WITH"):
		return e.executeWith(ctx, cypher)
	case strings.HasPrefix(upperQuery, "UNWIND"):
		return e.executeUnwind(ctx, cypher)
	case strings.Contains(upperQuery, " UNION ALL "):
		return e.executeUnion(ctx, cypher, true)
	case strings.Contains(upperQuery, " UNION "):
		return e.executeUnion(ctx, cypher, false)
	case strings.HasPrefix(upperQuery, "FOREACH"):
		return e.executeForeach(ctx, cypher)
	case strings.HasPrefix(upperQuery, "LOAD CSV"):
		return e.executeLoadCSV(ctx, cypher)
	// SHOW commands for Neo4j compatibility
	case strings.HasPrefix(upperQuery, "SHOW INDEXES"), strings.HasPrefix(upperQuery, "SHOW INDEX"):
		return e.executeShowIndexes(ctx, cypher)
	case strings.HasPrefix(upperQuery, "SHOW CONSTRAINTS"), strings.HasPrefix(upperQuery, "SHOW CONSTRAINT"):
		return e.executeShowConstraints(ctx, cypher)
	case strings.HasPrefix(upperQuery, "SHOW PROCEDURES"):
		return e.executeShowProcedures(ctx, cypher)
	case strings.HasPrefix(upperQuery, "SHOW FUNCTIONS"):
		return e.executeShowFunctions(ctx, cypher)
	case strings.HasPrefix(upperQuery, "SHOW DATABASE"):
		return e.executeShowDatabase(ctx, cypher)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", strings.Split(upperQuery, " ")[0])
	}
}

// executeReturn handles simple RETURN statements (e.g., "RETURN 1").
func (e *StorageExecutor) executeReturn(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse RETURN clause - use word boundary detection
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause not found")
	}

	returnClause := strings.TrimSpace(cypher[returnIdx+6:])

	// Handle simple literal returns like "RETURN 1" or "RETURN true"
	parts := splitReturnExpressions(returnClause)
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

		// Try to evaluate as a function or expression first
		result := e.evaluateExpressionWithContext(part, nil, nil)
		if result != nil {
			values = append(values, result)
			continue
		}

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

// splitReturnExpressions splits RETURN expressions by comma, respecting parentheses depth
func splitReturnExpressions(clause string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, ch := range clause {
		switch {
		case (ch == '\'' || ch == '"') && !inQuote:
			inQuote = true
			quoteChar = ch
			current.WriteRune(ch)
		case ch == quoteChar && inQuote:
			inQuote = false
			quoteChar = 0
			current.WriteRune(ch)
		case ch == '(' && !inQuote:
			depth++
			current.WriteRune(ch)
		case ch == ')' && !inQuote:
			depth--
			current.WriteRune(ch)
		case ch == ',' && depth == 0 && !inQuote:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// validateSyntax performs basic syntax validation.
func (e *StorageExecutor) validateSyntax(cypher string) error {
	upper := strings.ToUpper(cypher)

	// Check for valid starting keyword (including EXPLAIN/PROFILE prefixes)
	validStarts := []string{"MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH", "UNWIND", "OPTIONAL", "DROP", "SHOW", "FOREACH", "LOAD", "EXPLAIN", "PROFILE"}
	hasValidStart := false
	for _, start := range validStarts {
		if strings.HasPrefix(upper, start) {
			hasValidStart = true
			break
		}
	}
	if !hasValidStart {
		return fmt.Errorf("syntax error: query must start with a valid clause (MATCH, CREATE, MERGE, DELETE, CALL, SHOW, EXPLAIN, PROFILE, etc.)")
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

// ========================================
// WITH Clause
// ========================================

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

	// Use pre-compiled regex to find all parameter references
	// Parameters are: $name or $name123 (alphanumeric starting with letter)
	result := parameterPattern.ReplaceAllStringFunc(cypher, func(match string) string {
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
		setNodeProperty(node, propName, e.evaluateSetExpression(propValue))
	}
}

// setNodeProperty sets a property on a node.
// "embedding" goes to node.Embedding (not Properties).
func setNodeProperty(node *storage.Node, propName string, value interface{}) {
	if propName == "embedding" {
		node.Embedding = toFloat32Slice(value)
		return
	}
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	node.Properties[propName] = value
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
	if strings.EqualFold(expr, "null") {
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

// splitFunctionArgs splits function arguments by comma, respecting parentheses and quotes.
//
// This function intelligently parses function arguments, handling:
//   - Nested parentheses: function(a, func(b, c), d)
//   - Quoted strings: "hello, world" treated as single argument
//   - Escape sequences: \"escaped quote\" within strings
//   - Mixed quotes: 'single' and "double" quotes
//
// Parameters:
//   - args: The argument string to split (without outer parentheses)
//
// Returns:
//   - []string: List of individual arguments, trimmed of whitespace
//
// Example 1 - Simple Arguments:
//
//	args := "name, age, email"
//	result := executor.splitFunctionArgs(args)
//	// result = ["name", "age", "email"]
//
// Example 2 - Nested Function Calls:
//
//	args := "toLower(n.name), toUpper(n.city)"
//	result := executor.splitFunctionArgs(args)
//	// result = ["toLower(n.name)", "toUpper(n.city)"]
//	// The nested parentheses are respected
//
// Example 3 - Quoted Strings with Commas:
//
//	args := `"Hello, World", 'Test, Data', 42`
//	result := executor.splitFunctionArgs(args)
//	// result = ["\"Hello, World\"", "'Test, Data'", "42"]
//	// Commas inside quotes don't split
//
// Example 4 - Complex Nested Case:
//
//	args := "n.name, count(n.id), substring(n.desc, 0, 10), 'status: active, verified'"
//	result := executor.splitFunctionArgs(args)
//	// result = ["n.name", "count(n.id)", "substring(n.desc, 0, 10)", "'status: active, verified'"]
//
// ELI12:
//
// Imagine you're reading a sentence: "I like pizza, pasta, and ice cream"
// You need to split it by commas, but what if someone says:
// "I like pizza, 'pasta, with sauce', and ice cream"
//
// You can't just split at every comma! The comma inside the quotes is PART of
// the item, not a separator. This function is smart enough to know:
//   - Commas outside quotes = split here
//   - Commas inside quotes = keep them
//   - Parentheses = keep everything inside together
//
// It's like reading with understanding, not just blindly cutting at commas.
//
// Use Cases:
//   - Parsing Cypher function arguments
//   - Splitting RETURN clause items
//   - Processing WITH clause expressions
func (e *StorageExecutor) splitFunctionArgs(args string) []string {
	var result []string
	var current strings.Builder
	parenDepth := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(args); i++ {
		c := args[i]
		switch c {
		case '\'':
			if !inDoubleQuote {
				// Check for escape sequence
				if i > 0 && args[i-1] == '\\' {
					current.WriteByte(c)
				} else {
					inSingleQuote = !inSingleQuote
					current.WriteByte(c)
				}
			} else {
				current.WriteByte(c)
			}
		case '"':
			if !inSingleQuote {
				// Check for escape sequence
				if i > 0 && args[i-1] == '\\' {
					current.WriteByte(c)
				} else {
					inDoubleQuote = !inDoubleQuote
					current.WriteByte(c)
				}
			} else {
				current.WriteByte(c)
			}
		case '(':
			if !inSingleQuote && !inDoubleQuote {
				parenDepth++
			}
			current.WriteByte(c)
		case ')':
			if !inSingleQuote && !inDoubleQuote {
				parenDepth--
			}
			current.WriteByte(c)
		case ',':
			if parenDepth == 0 && !inSingleQuote && !inDoubleQuote {
				result = append(result, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteByte(c)
			}
		default:
			current.WriteByte(c)
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
// Properties are included at the top level for Neo4j compatibility.
func (e *StorageExecutor) nodeToMap(node *storage.Node) map[string]interface{} {
	// Start with node metadata
	result := map[string]interface{}{
		"id":     string(node.ID),
		"labels": node.Labels,
	}

	// Add properties at top level for Neo4j compatibility
	for k, v := range node.Properties {
		if e.isInternalProperty(k) {
			continue
		}
		result[k] = v
	}

	return result
}

// edgeToMap converts a storage.Edge to a map for result output.
func (e *StorageExecutor) edgeToMap(edge *storage.Edge) map[string]interface{} {
	return map[string]interface{}{
		"id":         string(edge.ID),
		"type":       edge.Type,
		"startNode":  string(edge.StartNode),
		"endNode":    string(edge.EndNode),
		"properties": edge.Properties,
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
	pattern = strings.TrimPrefix(pattern, "(")
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
	pattern = strings.TrimPrefix(pattern, "(")
	pattern = strings.TrimSuffix(pattern, ")")

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

	// Get MATCH part - use word boundary detection
	matchIdx := findKeywordIndex(cypher, "MATCH")

	// Find the delete clause - could be "DELETE" or "DETACH DELETE"
	var deleteIdx int
	if detach {
		deleteIdx = findKeywordIndex(cypher, "DETACH DELETE")
		if deleteIdx == -1 {
			deleteIdx = findKeywordIndex(cypher, "DETACH")
		}
	} else {
		deleteIdx = findKeywordIndex(cypher, "DELETE")
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

	// Normalize whitespace for index finding (newlines/tabs become spaces)
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")

	// Use word boundary detection to avoid matching substrings
	matchIdx := findKeywordIndex(normalized, "MATCH")
	setIdx := findKeywordIndex(normalized, "SET")
	returnIdx := findKeywordIndex(normalized, "RETURN")

	if matchIdx == -1 || setIdx == -1 {
		return nil, fmt.Errorf("SET requires a MATCH clause")
	}

	// Execute the match first (use normalized query for slicing)
	var matchQuery string
	if returnIdx > 0 {
		matchQuery = normalized[matchIdx:setIdx] + " RETURN *"
	} else {
		matchQuery = normalized[matchIdx:setIdx] + " RETURN *"
	}
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}

	// Parse SET clause: SET n.property = value or SET n += $properties
	var setPart string
	if returnIdx > 0 {
		setPart = strings.TrimSpace(normalized[setIdx+5 : returnIdx])
	} else {
		setPart = strings.TrimSpace(normalized[setIdx+5:])
	}

	// Check for property merge operator: n += $properties
	if strings.Contains(setPart, "+=") {
		return e.executeSetMerge(matchResult, setPart, result)
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
	_ = variable // silence unused warning

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

// executeSetMerge handles SET n += $properties for property merging
func (e *StorageExecutor) executeSetMerge(matchResult *ExecuteResult, setPart string, result *ExecuteResult) (*ExecuteResult, error) {
	// Parse: n += $properties or n += {key: value}
	plusEqIdx := strings.Index(setPart, "+=")
	if plusEqIdx == -1 {
		return nil, fmt.Errorf("expected += operator")
	}

	variable := strings.TrimSpace(setPart[:plusEqIdx])
	right := strings.TrimSpace(setPart[plusEqIdx+2:])

	// Parse the properties to merge
	var propsToMerge map[string]interface{}

	if strings.HasPrefix(right, "{") {
		// Inline properties: {key: value, ...}
		propsToMerge = e.parseProperties(right)
	} else if strings.HasPrefix(right, "$") {
		// Parameter reference - for now, just skip since params are substituted earlier
		// In a full implementation, we'd look up the param value
		propsToMerge = make(map[string]interface{})
	} else {
		return nil, fmt.Errorf("SET += requires a map or parameter")
	}

	// Update matched nodes
	for _, row := range matchResult.Rows {
		for _, val := range row {
			nodeMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			id, ok := nodeMap["id"].(string)
			if !ok {
				continue
			}
			storageNode, err := e.storage.GetNode(storage.NodeID(id))
			if err != nil {
				continue
			}
			if storageNode.Properties == nil {
				storageNode.Properties = make(map[string]interface{})
			}
			// Merge properties (new values override existing)
			for k, v := range propsToMerge {
				storageNode.Properties[k] = v
				result.Stats.PropertiesSet++
			}
			_ = e.storage.UpdateNode(storageNode)
		}
	}
	_ = variable // silence unused warning

	// Return matched rows info
	result.Columns = []string{"matched"}
	result.Rows = [][]interface{}{{len(matchResult.Rows)}}

	return result, nil
}

// executeRemove handles MATCH ... REMOVE queries for property removal.
// Syntax: MATCH (n:Label) REMOVE n.property [, n.property2] [RETURN ...]
func (e *StorageExecutor) executeRemove(ctx context.Context, cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Normalize whitespace
	normalized := strings.ReplaceAll(strings.ReplaceAll(cypher, "\n", " "), "\t", " ")

	// Use word boundary detection to avoid matching substrings
	matchIdx := findKeywordIndex(normalized, "MATCH")
	removeIdx := findKeywordIndex(normalized, "REMOVE")
	returnIdx := findKeywordIndex(normalized, "RETURN")

	if matchIdx == -1 || removeIdx == -1 {
		return nil, fmt.Errorf("REMOVE requires a MATCH clause")
	}

	// Execute the match first
	matchQuery := normalized[matchIdx:removeIdx] + " RETURN *"
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}

	// Parse REMOVE clause: REMOVE n.prop1, n.prop2
	var removePart string
	removeLen := len("REMOVE")
	if returnIdx > 0 && returnIdx > removeIdx {
		removePart = strings.TrimSpace(normalized[removeIdx+removeLen : returnIdx])
	} else {
		removePart = strings.TrimSpace(normalized[removeIdx+removeLen:])
	}

	// Split by comma and parse each property to remove
	propsToRemove := e.parseRemoveProperties(removePart)

	// Update matched nodes
	for _, row := range matchResult.Rows {
		for _, val := range row {
			nodeMap, ok := val.(map[string]interface{})
			if !ok {
				continue
			}
			id, ok := nodeMap["id"].(string)
			if !ok {
				continue
			}
			storageNode, err := e.storage.GetNode(storage.NodeID(id))
			if err != nil {
				continue
			}
			// Remove specified properties
			for _, prop := range propsToRemove {
				if _, exists := storageNode.Properties[prop]; exists {
					delete(storageNode.Properties, prop)
					result.Stats.PropertiesSet++ // Neo4j counts removals as properties set
				}
			}
			_ = e.storage.UpdateNode(storageNode)
		}
	}

	// Handle RETURN
	if returnIdx > 0 && returnIdx > removeIdx {
		returnPart := strings.TrimSpace(normalized[returnIdx+6:])
		returnItems := e.parseReturnItems(returnPart)
		result.Columns = make([]string, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}
		// Re-fetch nodes for return
		for _, row := range matchResult.Rows {
			for _, val := range row {
				if nodeMap, ok := val.(map[string]interface{}); ok {
					if id, ok := nodeMap["id"].(string); ok {
						storageNode, _ := e.storage.GetNode(storage.NodeID(id))
						if storageNode != nil {
							resultRow := make([]interface{}, len(returnItems))
							for i, item := range returnItems {
								resultRow[i] = e.resolveReturnItem(item, "n", storageNode)
							}
							result.Rows = append(result.Rows, resultRow)
						}
					}
				}
			}
		}
	}

	return result, nil
}

// parseRemoveProperties parses "n.prop1, n.prop2, m.prop3" into property names
func (e *StorageExecutor) parseRemoveProperties(removePart string) []string {
	var props []string
	parts := strings.Split(removePart, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if dotIdx := strings.Index(part, "."); dotIdx >= 0 {
			propName := strings.TrimSpace(part[dotIdx+1:])
			if propName != "" {
				props = append(props, propName)
			}
		}
	}
	return props
}

// executeCall handles CALL procedure queries.

// Helper functions

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

// splitPropertyPairs splits a property string into key:value pairs,
// respecting quotes, brackets, and nested braces.
func (e *StorageExecutor) splitPropertyPairs(propsStr string) []string {
	var pairs []string
	var current strings.Builder
	depth := 0 // Track [], {} nesting
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
	if strings.EqualFold(valueStr, "null") {
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

// smartSplitReturnItems splits a RETURN clause by commas, but respects:
// - CASE/END boundaries
// - Parentheses (function calls)
// - String literals
func (e *StorageExecutor) smartSplitReturnItems(returnPart string) []string {
	var result []string
	var current strings.Builder
	var inString bool
	var stringChar rune
	var parenDepth int
	var caseDepth int

	upper := strings.ToUpper(returnPart)

	for i := 0; i < len(returnPart); i++ {
		ch := rune(returnPart[i])

		// Track string literals
		if ch == '\'' || ch == '"' {
			if !inString {
				inString = true
				stringChar = ch
			} else if ch == stringChar {
				inString = false
			}
			current.WriteRune(ch)
			continue
		}

		if inString {
			current.WriteRune(ch)
			continue
		}

		// Track parentheses
		if ch == '(' {
			parenDepth++
			current.WriteRune(ch)
			continue
		}
		if ch == ')' {
			parenDepth--
			current.WriteRune(ch)
			continue
		}

		// Track CASE/END keywords
		if i+4 <= len(returnPart) && upper[i:i+4] == "CASE" {
			// Check if CASE is a word boundary
			if (i == 0 || !isAlphaNum(rune(returnPart[i-1]))) &&
				(i+4 >= len(returnPart) || !isAlphaNum(rune(returnPart[i+4]))) {
				caseDepth++
			}
		}
		if i+3 <= len(returnPart) && upper[i:i+3] == "END" {
			// Check if END is a word boundary
			if (i == 0 || !isAlphaNum(rune(returnPart[i-1]))) &&
				(i+3 >= len(returnPart) || !isAlphaNum(rune(returnPart[i+3]))) {
				if caseDepth > 0 {
					caseDepth--
				}
			}
		}

		// Split on comma only if we're not inside parens, CASE, or strings
		if ch == ',' && parenDepth == 0 && caseDepth == 0 {
			result = append(result, current.String())
			current.Reset()
			continue
		}

		current.WriteRune(ch)
	}

	// Add remaining content
	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// isAlphaNum checks if a character is alphanumeric or underscore
func isAlphaNum(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
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

	// Split by comma, but respect CASE/END boundaries and parentheses
	parts := e.smartSplitReturnItems(returnPart)
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

	// Handle NOT EXISTS { } subquery (check before EXISTS)
	if strings.Contains(upperClause, "NOT EXISTS") {
		return e.evaluateNotExistsSubquery(node, variable, whereClause)
	}

	// Handle EXISTS { } subquery
	if strings.Contains(upperClause, "EXISTS") && strings.Contains(whereClause, "{") {
		return e.evaluateExistsSubquery(node, variable, whereClause)
	}

	// Handle COUNT { } subquery with comparison
	// Examples: COUNT { MATCH (n)-[:KNOWS]->(m) } > 5
	if strings.Contains(upperClause, "COUNT {") {
		return e.evaluateCountSubqueryComparison(node, variable, whereClause)
	}

	// Handle AND conditions (but not inside EXISTS subqueries)
	if strings.Contains(upperClause, " AND ") && !strings.Contains(whereClause, "{") {
		andIdx := strings.Index(upperClause, " AND ")
		left := strings.TrimSpace(whereClause[:andIdx])
		right := strings.TrimSpace(whereClause[andIdx+5:])
		return e.evaluateWhere(node, variable, left) && e.evaluateWhere(node, variable, right)
	}

	// Handle OR conditions
	if strings.Contains(upperClause, " OR ") && !strings.Contains(whereClause, "{") {
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

	// Handle id(variable) = value comparisons
	lowerLeft := strings.ToLower(left)
	if strings.HasPrefix(lowerLeft, "id(") && strings.HasSuffix(left, ")") {
		// Extract variable name from id(varName)
		idVar := strings.TrimSpace(left[3 : len(left)-1])
		if idVar == variable {
			// Compare node ID with expected value
			expectedVal := e.parseValue(right)
			actualId := string(node.ID)
			switch op {
			case "=":
				return e.compareEqual(actualId, expectedVal)
			case "<>", "!=":
				return !e.compareEqual(actualId, expectedVal)
			default:
				return true
			}
		}
		return true // Different variable, not our concern
	}

	// Handle elementId(variable) = value comparisons
	if strings.HasPrefix(lowerLeft, "elementid(") && strings.HasSuffix(left, ")") {
		// Extract variable name from elementId(varName)
		idVar := strings.TrimSpace(left[10 : len(left)-1])
		if idVar == variable {
			// Compare node ID with expected value
			expectedVal := e.parseValue(right)
			actualId := string(node.ID)
			switch op {
			case "=":
				return e.compareEqual(actualId, expectedVal)
			case "<>", "!=":
				return !e.compareEqual(actualId, expectedVal)
			default:
				return true
			}
		}
		return true // Different variable, not our concern
	}

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

func (e *StorageExecutor) resolveReturnItem(item returnItem, variable string, node *storage.Node) interface{} {
	expr := item.expr

	// Handle wildcard - return the whole node
	if expr == "*" || expr == variable {
		return e.nodeToMap(node)
	}

	// Check for CASE expression FIRST (before property access check)
	// CASE expressions contain dots (like p.age) but should not be treated as property access
	if isCaseExpression(expr) {
		return e.evaluateExpression(expr, variable, node)
	}

	// Handle property access: variable.property
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		varName := parts[0]
		propName := parts[1]

		// Check if variable matches
		if varName != variable {
			// Different variable - return nil (variable not in scope)
			return nil
		}

		// Handle special "id" property - return node's internal ID
		if propName == "id" {
			// Check if there's an "id" property first
			if val, ok := node.Properties["id"]; ok {
				return val
			}
			// Fall back to internal node ID
			return string(node.ID)
		}

		// Regular property access
		if val, ok := node.Properties[propName]; ok {
			return val
		}
		return nil
	}

	// Use the comprehensive expression evaluator for all expressions
	// This supports: id(n), labels(n), keys(n), properties(n), literals, etc.
	result := e.evaluateExpression(expr, variable, node)

	// If the result is just the expression string unchanged, return nil
	// (expression wasn't recognized/evaluated)
	if str, ok := result.(string); ok && str == expr && !strings.HasPrefix(expr, "'") && !strings.HasPrefix(expr, "\"") {
		return nil
	}

	return result
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

// evaluateExistsSubquery checks if an EXISTS { } subquery returns any matches
// Syntax: EXISTS { MATCH (node)<-[:TYPE]-(other) }
func (e *StorageExecutor) evaluateExistsSubquery(node *storage.Node, variable, whereClause string) bool {
	// Extract the subquery from EXISTS { ... }
	subquery := e.extractSubquery(whereClause, "EXISTS")
	if subquery == "" {
		return true // No valid subquery, pass through
	}

	// Execute the subquery with the current node as context
	return e.checkSubqueryMatch(node, variable, subquery)
}

// evaluateNotExistsSubquery checks if a NOT EXISTS { } subquery returns no matches
func (e *StorageExecutor) evaluateNotExistsSubquery(node *storage.Node, variable, whereClause string) bool {
	// Extract the subquery from NOT EXISTS { ... }
	subquery := e.extractSubquery(whereClause, "NOT EXISTS")
	if subquery == "" {
		return true // No valid subquery, pass through
	}

	// Return true if no matches found
	return !e.checkSubqueryMatch(node, variable, subquery)
}

// extractSubquery extracts the MATCH pattern from EXISTS { MATCH ... } or NOT EXISTS { MATCH ... }
func (e *StorageExecutor) extractSubquery(whereClause, prefix string) string {
	upperClause := strings.ToUpper(whereClause)
	prefixUpper := strings.ToUpper(prefix)

	// Find the prefix position
	prefixIdx := strings.Index(upperClause, prefixUpper)
	if prefixIdx < 0 {
		return ""
	}

	// Find the opening brace
	rest := whereClause[prefixIdx+len(prefix):]
	braceStart := strings.Index(rest, "{")
	if braceStart < 0 {
		return ""
	}

	// Find matching closing brace
	depth := 0
	for i := braceStart; i < len(rest); i++ {
		if rest[i] == '{' {
			depth++
		} else if rest[i] == '}' {
			depth--
			if depth == 0 {
				return strings.TrimSpace(rest[braceStart+1 : i])
			}
		}
	}

	return ""
}

// checkSubqueryMatch checks if the subquery matches for a given node
func (e *StorageExecutor) checkSubqueryMatch(node *storage.Node, variable, subquery string) bool {
	// Parse the MATCH pattern from the subquery
	// Format: MATCH (var)<-[:TYPE]-(other) or MATCH (var)-[:TYPE]->(other)
	upperSub := strings.ToUpper(subquery)

	if !strings.HasPrefix(upperSub, "MATCH ") {
		return false
	}

	pattern := strings.TrimSpace(subquery[6:])

	// Check if pattern references our variable
	if !strings.Contains(pattern, "("+variable+")") && !strings.Contains(pattern, "("+variable+":") {
		return false
	}

	// Parse relationship pattern
	// Simplified: check for incoming or outgoing relationships
	var checkIncoming, checkOutgoing bool
	var relTypes []string

	if strings.Contains(pattern, "<-[") {
		checkIncoming = true
		// Extract relationship type if specified
		relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
	}
	if strings.Contains(pattern, "]->(") || strings.Contains(pattern, "]->") {
		checkOutgoing = true
		relTypes = e.extractRelTypesFromPattern(pattern, "-[")
	}

	// Check for matching edges
	if checkIncoming {
		edges, _ := e.storage.GetIncomingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				return true
			}
		}
	}

	if checkOutgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				return true
			}
		}
	}

	// If no direction specified, check both
	if !checkIncoming && !checkOutgoing {
		incoming, _ := e.storage.GetIncomingEdges(node.ID)
		outgoing, _ := e.storage.GetOutgoingEdges(node.ID)
		return len(incoming) > 0 || len(outgoing) > 0
	}

	return false
}

// extractRelTypesFromPattern extracts relationship types from a pattern
func (e *StorageExecutor) extractRelTypesFromPattern(pattern, prefix string) []string {
	var types []string

	idx := strings.Index(pattern, prefix)
	if idx < 0 {
		return types
	}

	rest := pattern[idx+len(prefix):]
	endIdx := strings.Index(rest, "]")
	if endIdx < 0 {
		return types
	}

	relPart := rest[:endIdx]

	// Extract type after colon
	if colonIdx := strings.Index(relPart, ":"); colonIdx >= 0 {
		typePart := relPart[colonIdx+1:]
		// Handle multiple types (TYPE1|TYPE2)
		for _, t := range strings.Split(typePart, "|") {
			t = strings.TrimSpace(t)
			if t != "" {
				types = append(types, t)
			}
		}
	}

	return types
}

// edgeTypeMatches checks if an edge type matches any of the allowed types
func (e *StorageExecutor) edgeTypeMatches(edgeType string, allowedTypes []string) bool {
	for _, t := range allowedTypes {
		if edgeType == t {
			return true
		}
	}
	return false
}

// evaluateCountSubqueryComparison evaluates COUNT { } subquery with comparison
// Syntax: COUNT { MATCH (node)-[:TYPE]->(other) } > 5
// Returns true if the comparison holds
func (e *StorageExecutor) evaluateCountSubqueryComparison(node *storage.Node, variable, whereClause string) bool {
	// Extract the subquery from COUNT { ... }
	subquery := e.extractSubquery(whereClause, "COUNT")
	if subquery == "" {
		return true // No valid subquery, pass through
	}

	// Count matching relationships
	count := e.countSubqueryMatches(node, variable, subquery)

	// Extract and evaluate the comparison operator
	// Find the closing brace to get what comes after
	upperClause := strings.ToUpper(whereClause)
	countIdx := strings.Index(upperClause, "COUNT")
	if countIdx < 0 {
		return false
	}

	remaining := whereClause[countIdx:]
	braceDepth := 0
	closeIdx := -1
	for i := 0; i < len(remaining); i++ {
		if remaining[i] == '{' {
			braceDepth++
		} else if remaining[i] == '}' {
			braceDepth--
			if braceDepth == 0 {
				closeIdx = i
				break
			}
		}
	}

	if closeIdx == -1 {
		// No closing brace, invalid
		return false
	}

	// Get comparison part after COUNT { }
	comparison := strings.TrimSpace(remaining[closeIdx+1:])
	if comparison == "" {
		// No comparison, return true if count > 0
		return count > 0
	}

	// Parse comparison operator and value
	var op string
	var valueStr string
	
	if strings.HasPrefix(comparison, ">=") {
		op = ">="
		valueStr = strings.TrimSpace(comparison[2:])
	} else if strings.HasPrefix(comparison, "<=") {
		op = "<="
		valueStr = strings.TrimSpace(comparison[2:])
	} else if strings.HasPrefix(comparison, ">") {
		op = ">"
		valueStr = strings.TrimSpace(comparison[1:])
	} else if strings.HasPrefix(comparison, "<") {
		op = "<"
		valueStr = strings.TrimSpace(comparison[1:])
	} else if strings.HasPrefix(comparison, "=") {
		op = "="
		valueStr = strings.TrimSpace(comparison[1:])
	} else if strings.HasPrefix(comparison, "!=") || strings.HasPrefix(comparison, "<>") {
		op = "!="
		if strings.HasPrefix(comparison, "!=") {
			valueStr = strings.TrimSpace(comparison[2:])
		} else {
			valueStr = strings.TrimSpace(comparison[2:])
		}
	} else {
		// No valid operator, treat as > 0
		return count > 0
	}

	// Parse the comparison value
	var compareValue int64
	_, err := fmt.Sscanf(valueStr, "%d", &compareValue)
	if err != nil {
		// Invalid number, treat as false
		return false
	}

	// Perform comparison
	switch op {
	case ">":
		return count > compareValue
	case ">=":
		return count >= compareValue
	case "<":
		return count < compareValue
	case "<=":
		return count <= compareValue
	case "=":
		return count == compareValue
	case "!=":
		return count != compareValue
	default:
		return false
	}
}

// countSubqueryMatches counts how many matches a subquery produces
func (e *StorageExecutor) countSubqueryMatches(node *storage.Node, variable, subquery string) int64 {
	// Parse the MATCH pattern from the subquery
	upperSub := strings.ToUpper(subquery)

	if !strings.HasPrefix(upperSub, "MATCH ") {
		return 0
	}

	pattern := strings.TrimSpace(subquery[6:])

	// Check if pattern references our variable
	if !strings.Contains(pattern, "("+variable+")") && !strings.Contains(pattern, "("+variable+":") {
		return 0
	}

	// Parse relationship pattern
	var checkIncoming, checkOutgoing bool
	var relTypes []string

	if strings.Contains(pattern, "<-[") {
		checkIncoming = true
		relTypes = e.extractRelTypesFromPattern(pattern, "<-[")
	}
	if strings.Contains(pattern, "]->(") || strings.Contains(pattern, "]->") {
		checkOutgoing = true
		relTypes = e.extractRelTypesFromPattern(pattern, "-[")
	}

	// Count matching edges
	var count int64

	if checkIncoming {
		edges, _ := e.storage.GetIncomingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				count++
			}
		}
	}

	if checkOutgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				count++
			}
		}
	}

	// If no direction specified, count both
	if !checkIncoming && !checkOutgoing {
		incoming, _ := e.storage.GetIncomingEdges(node.ID)
		outgoing, _ := e.storage.GetOutgoingEdges(node.ID)
		count = int64(len(incoming) + len(outgoing))
	}

	return count
}

// ===== SHOW Commands (Neo4j compatibility) =====

// executeShowIndexes handles SHOW INDEXES command
func (e *StorageExecutor) executeShowIndexes(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// NornicDB manages indexes internally, return empty list
	return &ExecuteResult{
		Columns: []string{"id", "name", "state", "populationPercent", "type", "entityType", "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount"},
		Rows:    [][]interface{}{},
	}, nil
}

// executeShowConstraints handles SHOW CONSTRAINTS command
func (e *StorageExecutor) executeShowConstraints(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// NornicDB manages constraints internally, return empty list
	return &ExecuteResult{
		Columns: []string{"id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType"},
		Rows:    [][]interface{}{},
	}, nil
}

// executeShowProcedures handles SHOW PROCEDURES command
func (e *StorageExecutor) executeShowProcedures(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Return list of available procedures
	procedures := [][]interface{}{
		{"db.labels", "db.labels() :: (label :: STRING)", "Lists all labels in the database", "READ", false},
		{"db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING)", "Lists all relationship types in the database", "READ", false},
		{"db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING)", "Lists all property keys in the database", "READ", false},
		{"db.indexes", "db.indexes() :: (name :: STRING, state :: STRING, ...)", "Lists all indexes in the database", "READ", false},
		{"db.constraints", "db.constraints() :: (name :: STRING, ...)", "Lists all constraints in the database", "READ", false},
		{"db.info", "db.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)", "Database information", "READ", false},
		{"db.ping", "db.ping() :: (success :: BOOLEAN)", "Database ping", "READ", false},
		{"db.schema.visualization", "db.schema.visualization() :: (...)", "Database schema visualization", "READ", false},
		{"db.schema.nodeTypeProperties", "db.schema.nodeTypeProperties() :: (...)", "Node type properties", "READ", false},
		{"db.schema.relTypeProperties", "db.schema.relTypeProperties() :: (...)", "Relationship type properties", "READ", false},
		{"db.index.fulltext.queryNodes", "db.index.fulltext.queryNodes(indexName :: STRING, query :: STRING) :: (node :: NODE, score :: FLOAT)", "Fulltext search on nodes", "READ", false},
		{"db.index.fulltext.queryRelationships", "db.index.fulltext.queryRelationships(indexName :: STRING, query :: STRING) :: (relationship :: RELATIONSHIP, score :: FLOAT)", "Fulltext search on relationships", "READ", false},
		{"db.index.vector.queryNodes", "db.index.vector.queryNodes(indexName :: STRING, numberOfResults :: INTEGER, query :: LIST<FLOAT>) :: (node :: NODE, score :: FLOAT)", "Vector similarity search on nodes", "READ", false},
		{"db.index.vector.queryRelationships", "db.index.vector.queryRelationships(...) :: (...)", "Vector similarity search on relationships", "READ", false},
		{"dbms.components", "dbms.components() :: (name :: STRING, versions :: LIST<STRING>, edition :: STRING)", "DBMS components", "DBMS", false},
		{"dbms.procedures", "dbms.procedures() :: (name :: STRING, ...)", "List all procedures", "DBMS", false},
		{"dbms.functions", "dbms.functions() :: (name :: STRING, ...)", "List all functions", "DBMS", false},
		{"dbms.info", "dbms.info() :: (id :: STRING, name :: STRING, creationDate :: STRING)", "DBMS information", "DBMS", false},
		{"dbms.listConfig", "dbms.listConfig() :: (name :: STRING, ...)", "List DBMS configuration", "DBMS", false},
		{"dbms.clientConfig", "dbms.clientConfig() :: (name :: STRING, value :: ANY)", "Client configuration", "DBMS", false},
		{"dbms.listConnections", "dbms.listConnections() :: (...)", "List active connections", "DBMS", false},
		{"apoc.path.subgraphNodes", "apoc.path.subgraphNodes(startNode :: NODE, config :: MAP) :: (node :: NODE)", "Return all nodes in a subgraph", "READ", false},
		{"apoc.path.expand", "apoc.path.expand(startNode :: NODE, relationshipFilter :: STRING, labelFilter :: STRING, minLevel :: INTEGER, maxLevel :: INTEGER) :: (path :: PATH)", "Expand paths from start node", "READ", false},
		{"apoc.path.spanningTree", "apoc.path.spanningTree(startNode :: NODE, config :: MAP) :: (path :: PATH)", "Return spanning tree from start node", "READ", false},
		{"nornicdb.version", "nornicdb.version() :: (version :: STRING)", "NornicDB version", "READ", false},
		{"nornicdb.stats", "nornicdb.stats() :: (...)", "NornicDB statistics", "READ", false},
		{"nornicdb.decay.info", "nornicdb.decay.info() :: (...)", "NornicDB decay information", "READ", false},
	}

	return &ExecuteResult{
		Columns: []string{"name", "signature", "description", "mode", "worksOnSystem"},
		Rows:    procedures,
	}, nil
}

// executeShowFunctions handles SHOW FUNCTIONS command
func (e *StorageExecutor) executeShowFunctions(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Return list of available functions
	functions := [][]interface{}{
		// Scalar functions
		{"id", "id(entity :: ANY) :: INTEGER", "Returns the id of a node or relationship", false, false, false},
		{"elementId", "elementId(entity :: ANY) :: STRING", "Returns the element id of a node or relationship", false, false, false},
		{"labels", "labels(node :: NODE) :: LIST<STRING>", "Returns labels of a node", false, false, false},
		{"type", "type(relationship :: RELATIONSHIP) :: STRING", "Returns the type of a relationship", false, false, false},
		{"keys", "keys(entity :: ANY) :: LIST<STRING>", "Returns the property keys of a node or relationship", false, false, false},
		{"properties", "properties(entity :: ANY) :: MAP", "Returns all properties of a node or relationship", false, false, false},
		{"coalesce", "coalesce(expression :: ANY...) :: ANY", "Returns first non-null value", false, false, false},
		{"head", "head(list :: LIST<ANY>) :: ANY", "Returns the first element of a list", false, false, false},
		{"last", "last(list :: LIST<ANY>) :: ANY", "Returns the last element of a list", false, false, false},
		{"tail", "tail(list :: LIST<ANY>) :: LIST<ANY>", "Returns all but the first element of a list", false, false, false},
		{"size", "size(list :: LIST<ANY>) :: INTEGER", "Returns the number of elements in a list", false, false, false},
		{"length", "length(path :: PATH) :: INTEGER", "Returns the length of a path", false, false, false},
		{"reverse", "reverse(original :: LIST<ANY> | STRING) :: LIST<ANY> | STRING", "Reverses a list or string", false, false, false},
		{"range", "range(start :: INTEGER, end :: INTEGER, step :: INTEGER = 1) :: LIST<INTEGER>", "Returns a list of integers", false, false, false},
		{"toString", "toString(expression :: ANY) :: STRING", "Converts expression to string", false, false, false},
		{"toInteger", "toInteger(expression :: ANY) :: INTEGER", "Converts expression to integer", false, false, false},
		{"toFloat", "toFloat(expression :: ANY) :: FLOAT", "Converts expression to float", false, false, false},
		{"toBoolean", "toBoolean(expression :: ANY) :: BOOLEAN", "Converts expression to boolean", false, false, false},
		{"toLower", "toLower(original :: STRING) :: STRING", "Converts string to lowercase", false, false, false},
		{"toUpper", "toUpper(original :: STRING) :: STRING", "Converts string to uppercase", false, false, false},
		{"trim", "trim(original :: STRING) :: STRING", "Trims whitespace from string", false, false, false},
		{"ltrim", "ltrim(original :: STRING) :: STRING", "Trims leading whitespace", false, false, false},
		{"rtrim", "rtrim(original :: STRING) :: STRING", "Trims trailing whitespace", false, false, false},
		{"replace", "replace(original :: STRING, search :: STRING, replace :: STRING) :: STRING", "Replaces all occurrences", false, false, false},
		{"split", "split(original :: STRING, splitDelimiter :: STRING) :: LIST<STRING>", "Splits string by delimiter", false, false, false},
		{"substring", "substring(original :: STRING, start :: INTEGER, length :: INTEGER = NULL) :: STRING", "Returns substring", false, false, false},
		{"left", "left(original :: STRING, length :: INTEGER) :: STRING", "Returns left part of string", false, false, false},
		{"right", "right(original :: STRING, length :: INTEGER) :: STRING", "Returns right part of string", false, false, false},
		// Math functions
		{"abs", "abs(expression :: NUMBER) :: NUMBER", "Returns absolute value", false, false, false},
		{"ceil", "ceil(expression :: FLOAT) :: INTEGER", "Returns ceiling value", false, false, false},
		{"floor", "floor(expression :: FLOAT) :: INTEGER", "Returns floor value", false, false, false},
		{"round", "round(expression :: FLOAT) :: INTEGER", "Rounds to nearest integer", false, false, false},
		{"sign", "sign(expression :: NUMBER) :: INTEGER", "Returns sign of number", false, false, false},
		{"sqrt", "sqrt(expression :: FLOAT) :: FLOAT", "Returns square root", false, false, false},
		{"rand", "rand() :: FLOAT", "Returns random float between 0 and 1", false, false, false},
		{"randomUUID", "randomUUID() :: STRING", "Returns a random UUID", false, false, false},
		{"sin", "sin(expression :: FLOAT) :: FLOAT", "Returns sine", false, false, false},
		{"cos", "cos(expression :: FLOAT) :: FLOAT", "Returns cosine", false, false, false},
		{"tan", "tan(expression :: FLOAT) :: FLOAT", "Returns tangent", false, false, false},
		{"log", "log(expression :: FLOAT) :: FLOAT", "Returns natural logarithm", false, false, false},
		{"log10", "log10(expression :: FLOAT) :: FLOAT", "Returns base-10 logarithm", false, false, false},
		{"exp", "exp(expression :: FLOAT) :: FLOAT", "Returns e raised to power", false, false, false},
		{"pi", "pi() :: FLOAT", "Returns pi constant", false, false, false},
		{"e", "e() :: FLOAT", "Returns Euler's number", false, false, false},
		// Temporal functions
		{"timestamp", "timestamp() :: INTEGER", "Returns current timestamp in milliseconds", false, false, false},
		{"datetime", "datetime(input :: ANY = NULL) :: DATETIME", "Creates a datetime", false, false, false},
		{"date", "date(input :: ANY = NULL) :: DATE", "Creates a date", false, false, false},
		{"time", "time(input :: ANY = NULL) :: TIME", "Creates a time", false, false, false},
		// Aggregation functions
		{"count", "count(expression :: ANY) :: INTEGER", "Returns count", true, false, false},
		{"sum", "sum(expression :: NUMBER) :: NUMBER", "Returns sum", true, false, false},
		{"avg", "avg(expression :: NUMBER) :: FLOAT", "Returns average", true, false, false},
		{"min", "min(expression :: ANY) :: ANY", "Returns minimum", true, false, false},
		{"max", "max(expression :: ANY) :: ANY", "Returns maximum", true, false, false},
		{"collect", "collect(expression :: ANY) :: LIST<ANY>", "Collects values into list", true, false, false},
		// Predicate functions
		{"exists", "exists(expression :: ANY) :: BOOLEAN", "Returns true if expression is not null", false, false, false},
		{"isEmpty", "isEmpty(list :: LIST<ANY> | MAP | STRING) :: BOOLEAN", "Returns true if empty", false, false, false},
		{"all", "all(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if all match", false, false, false},
		{"any", "any(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if any match", false, false, false},
		{"none", "none(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if none match", false, false, false},
		{"single", "single(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if exactly one matches", false, false, false},
		// Spatial functions
		{"point", "point(input :: MAP) :: POINT", "Creates a point", false, false, false},
		{"distance", "distance(point1 :: POINT, point2 :: POINT) :: FLOAT", "Returns distance between points", false, false, false},
		{"polygon", "polygon(points :: LIST<POINT>) :: POLYGON", "Creates a polygon from a list of points", false, false, false},
		{"lineString", "lineString(points :: LIST<POINT>) :: LINESTRING", "Creates a lineString from a list of points", false, false, false},
		{"point.intersects", "point.intersects(point :: POINT, polygon :: POLYGON) :: BOOLEAN", "Checks if point intersects with polygon", false, false, false},
		{"point.contains", "point.contains(polygon :: POLYGON, point :: POINT) :: BOOLEAN", "Checks if polygon contains point", false, false, false},
		// Vector functions
		{"vector.similarity.cosine", "vector.similarity.cosine(vector1 :: LIST<FLOAT>, vector2 :: LIST<FLOAT>) :: FLOAT", "Cosine similarity", false, false, false},
		{"vector.similarity.euclidean", "vector.similarity.euclidean(vector1 :: LIST<FLOAT>, vector2 :: LIST<FLOAT>) :: FLOAT", "Euclidean similarity", false, false, false},
	}

	return &ExecuteResult{
		Columns: []string{"name", "signature", "description", "aggregating", "isBuiltIn", "argumentDescription"},
		Rows:    functions,
	}, nil
}

// executeShowDatabase handles SHOW DATABASE command
func (e *StorageExecutor) executeShowDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	nodeCount, _ := e.storage.NodeCount()
	edgeCount, _ := e.storage.EdgeCount()

	return &ExecuteResult{
		Columns: []string{"name", "type", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "constituents"},
		Rows: [][]interface{}{
			{"nornicdb", "standard", "read-write", "localhost:7687", "primary", true, "online", "online", "", true, true, []string{}},
		},
		Stats: &QueryStats{
			NodesCreated:         int(nodeCount),
			RelationshipsCreated: int(edgeCount),
		},
	}, nil
}
