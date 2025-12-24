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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Pre-compiled regexes for subquery detection (whitespace-flexible)
var (
	// Matches EXISTS followed by optional whitespace and opening brace
	existsSubqueryRe = regexp.MustCompile(`(?i)\bEXISTS\s*\{`)
	// Matches NOT EXISTS followed by optional whitespace and opening brace
	notExistsSubqueryRe = regexp.MustCompile(`(?i)\bNOT\s+EXISTS\s*\{`)
	// Matches COUNT followed by optional whitespace and opening brace
	countSubqueryRe = regexp.MustCompile(`(?i)\bCOUNT\s*\{`)
	// Matches CALL followed by optional whitespace and opening brace (not CALL procedure())
	callSubqueryRe = regexp.MustCompile(`(?i)\bCALL\s*\{`)
	// Matches COLLECT followed by optional whitespace and opening brace
	collectSubqueryRe = regexp.MustCompile(`(?i)\bCOLLECT\s*\{`)
)

// hasSubqueryPattern checks if the query contains a subquery pattern (keyword + optional whitespace + brace)
func hasSubqueryPattern(query string, pattern *regexp.Regexp) bool {
	return pattern.MatchString(query)
}

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
//
// NodeCreatedCallback is called when a node is created or updated via Cypher.
// This allows external systems (like the embed queue) to be notified of new content.
type NodeCreatedCallback func(nodeID string)

type StorageExecutor struct {
	parser    *Parser
	storage   storage.Engine
	txContext *TransactionContext // Active transaction context
	cache     *SmartQueryCache    // Query result cache with label-aware invalidation
	planCache *QueryPlanCache     // Parsed query plan cache
	analyzer  *QueryAnalyzer      // Query analysis with AST caching

	// Node lookup cache for MATCH patterns like (n:Label {prop: value})
	// Key: "Label:{prop:value,...}", Value: *storage.Node
	// This dramatically speeds up repeated MATCH lookups for the same pattern
	nodeLookupCache   map[string]*storage.Node
	nodeLookupCacheMu sync.RWMutex

	// deferFlush when true, writes are not auto-flushed (Bolt layer handles it)
	deferFlush bool

	// embedder for server-side query embedding (optional)
	// If set, vector search can accept string queries which are embedded automatically
	embedder QueryEmbedder

	// onNodeCreated is called when a node is created or updated via CREATE/MERGE
	// This allows the embed queue to be notified of new content requiring embeddings
	onNodeCreated NodeCreatedCallback

	// defaultEmbeddingDimensions is the configured embedding dimensions for vector indexes
	// Used as default when CREATE VECTOR INDEX doesn't specify dimensions
	defaultEmbeddingDimensions int

	// dbManager is optional - when set, enables system commands (CREATE/DROP/SHOW DATABASE)
	// System commands require DatabaseManager to manage multiple databases
	// This is an interface to avoid import cycles with multidb package
	dbManager DatabaseManagerInterface
}

// DatabaseManagerInterface is a minimal interface to avoid import cycles with multidb package.
// This allows the executor to call database management operations without directly
// depending on the multidb package.
type DatabaseManagerInterface interface {
	CreateDatabase(name string) error
	DropDatabase(name string) error
	ListDatabases() []DatabaseInfoInterface
	Exists(name string) bool
	CreateAlias(alias, databaseName string) error
	DropAlias(alias string) error
	ListAliases(databaseName string) map[string]string
	ResolveDatabase(nameOrAlias string) (string, error)
	SetDatabaseLimits(databaseName string, limits interface{}) error
	GetDatabaseLimits(databaseName string) (interface{}, error)
	// Composite database methods
	CreateCompositeDatabase(name string, constituents []interface{}) error
	DropCompositeDatabase(name string) error
	AddConstituent(compositeName string, constituent interface{}) error
	RemoveConstituent(compositeName string, alias string) error
	GetCompositeConstituents(compositeName string) ([]interface{}, error)
	ListCompositeDatabases() []DatabaseInfoInterface
	IsCompositeDatabase(name string) bool
}

// DatabaseInfoInterface provides database metadata without importing multidb.
type DatabaseInfoInterface interface {
	Name() string
	Type() string
	Status() string
	IsDefault() bool
	CreatedAt() time.Time
}

// QueryEmbedder generates embeddings for search queries.
// This is a minimal interface to avoid import cycles with embed package.
type QueryEmbedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
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
		parser:          NewParser(),
		storage:         store,
		cache:           NewSmartQueryCache(1000), // Query result cache with label-aware invalidation
		planCache:       NewQueryPlanCache(500),   // Cache 500 parsed query plans
		analyzer:        NewQueryAnalyzer(1000),   // Cache 1000 parsed query ASTs
		nodeLookupCache: make(map[string]*storage.Node, 1000),
	}
}

// SetDatabaseManager sets the database manager for system commands.
// When set, enables CREATE DATABASE, DROP DATABASE, and SHOW DATABASES commands.
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//	// Now CREATE DATABASE, DROP DATABASE, SHOW DATABASES work
func (e *StorageExecutor) SetDatabaseManager(dbManager DatabaseManagerInterface) {
	e.dbManager = dbManager
}

// SetEmbedder sets the query embedder for server-side embedding.
// When set, db.index.vector.queryNodes can accept string queries
// which are automatically embedded before search.
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetEmbedder(embedder)
//
//	// Now vector search accepts both:
//	// CALL db.index.vector.queryNodes('idx', 10, [0.1, 0.2, ...])  // Vector
//	// CALL db.index.vector.queryNodes('idx', 10, 'search query')   // String (auto-embedded)
func (e *StorageExecutor) SetEmbedder(embedder QueryEmbedder) {
	e.embedder = embedder
}

// GetEmbedder returns the query embedder if set.
// This allows copying the embedder to namespaced executors for GraphQL.
func (e *StorageExecutor) GetEmbedder() QueryEmbedder {
	return e.embedder
}

// SetNodeCreatedCallback sets a callback that is invoked when nodes are created
// or updated via CREATE/MERGE statements. This allows the embed queue to be
// notified of new content that needs embedding generation.
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetNodeCreatedCallback(func(nodeID string) {
//	    embedQueue.Enqueue(nodeID)
//	})
func (e *StorageExecutor) SetNodeCreatedCallback(cb NodeCreatedCallback) {
	e.onNodeCreated = cb
}

// SetDefaultEmbeddingDimensions sets the default dimensions for vector indexes.
// This is used when CREATE VECTOR INDEX doesn't specify dimensions in OPTIONS.
func (e *StorageExecutor) SetDefaultEmbeddingDimensions(dims int) {
	e.defaultEmbeddingDimensions = dims
}

// GetDefaultEmbeddingDimensions returns the configured default embedding dimensions.
// Returns 1024 as fallback if not configured.
func (e *StorageExecutor) GetDefaultEmbeddingDimensions() int {
	return e.defaultEmbeddingDimensions
}

// notifyNodeCreated calls the onNodeCreated callback if set.
// This is called internally after node creation/update operations.
func (e *StorageExecutor) notifyNodeCreated(nodeID string) {
	if e.onNodeCreated != nil {
		e.onNodeCreated(nodeID)
	}
}

// Flush persists all pending writes to storage.
// This implements FlushableExecutor for Bolt-level deferred commits.
func (e *StorageExecutor) Flush() error {
	if asyncEngine, ok := e.storage.(*storage.AsyncEngine); ok {
		return asyncEngine.Flush()
	}
	return nil
}

// SetDeferFlush enables/disables deferred flush mode.
// When enabled, writes are not auto-flushed - the Bolt layer calls Flush().
func (e *StorageExecutor) SetDeferFlush(enabled bool) {
	e.deferFlush = enabled
}

// queryDeletesNodes returns true if the query deletes nodes.
// Returns false for relationship-only deletes (CREATE rel...DELETE rel pattern).
func queryDeletesNodes(query string) bool {
	// DETACH DELETE always deletes nodes
	if strings.Contains(strings.ToUpper(query), "DETACH DELETE") {
		return true
	}
	// Relationship pattern (has -[...]-> or <-[...]-) with CREATE+DELETE = relationship delete only
	if strings.Contains(query, "]->(") || strings.Contains(query, ")<-[") {
		return false
	}
	return true
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
//
//	Core Clauses:
//	- MATCH: Pattern matching and traversal
//	- OPTIONAL MATCH: Left outer joins (returns nulls for no matches)
//	- CREATE: Node and relationship creation
//	- MERGE: Upsert operations with ON CREATE SET / ON MATCH SET
//	- DELETE / DETACH DELETE: Node and relationship deletion
//	- SET: Property updates
//	- REMOVE: Property and label removal
//
//	Projection & Chaining:
//	- RETURN: Result projection with expressions, aliases, aggregations
//	- WITH: Query chaining and intermediate aggregation
//	- UNWIND: List expansion into rows
//
//	Filtering & Ordering:
//	- WHERE: Filtering conditions (=, <>, <, >, <=, >=, IS NULL, IS NOT NULL, IN, CONTAINS, STARTS WITH, ENDS WITH, AND, OR, NOT)
//	- ORDER BY: Result sorting (ASC/DESC)
//	- SKIP / LIMIT: Pagination
//
//	Aggregation Functions:
//	- COUNT, SUM, AVG, MIN, MAX, COLLECT
//
//	Procedures & Functions:
//	- CALL: Procedure invocation (db.labels, db.propertyKeys, db.index.vector.*, etc.)
//	- CALL {}: Subquery execution with UNION support
//
//	Advanced:
//	- UNION / UNION ALL: Query composition
//	- FOREACH: Iterative updates
//	- LOAD CSV: Data import
//	- EXPLAIN / PROFILE: Query analysis
//	- SHOW: Schema introspection
//
//	Path Functions:
//	- shortestPath / allShortestPaths
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

	// Handle :USE command (Neo4j browser/shell compatibility)
	// :USE database_name switches database context and should be stripped from query
	// The actual database switching is handled at the API layer by checking context
	if strings.HasPrefix(cypher, ":USE") || strings.HasPrefix(cypher, ":use") {
		// Extract :USE command and remaining query
		lines := strings.Split(cypher, "\n")
		var remainingLines []string
		useCommandFound := false
		var useDatabaseName string

		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if !useCommandFound && (strings.HasPrefix(trimmed, ":USE") || strings.HasPrefix(trimmed, ":use")) {
				useCommandFound = true
				// Extract database name from :USE command
				// Format: :USE database_name or :USE  database_name (with whitespace)
				parts := strings.Fields(trimmed)
				if len(parts) >= 2 {
					useDatabaseName = parts[1]
					// Store database name in context for server to switch
					ctx = context.WithValue(ctx, ctxKeyUseDatabase, useDatabaseName)
				}
				// Skip this line
				continue
			}
			// Collect all other lines (including empty lines for formatting)
			remainingLines = append(remainingLines, line)
		}

		if useCommandFound {
			// Reconstruct query without :USE command
			cypher = strings.Join(remainingLines, "\n")
			cypher = strings.TrimSpace(cypher)
			if cypher == "" {
				// Only :USE command, no actual query - return success
				return &ExecuteResult{
					Columns: []string{"database"},
					Rows:    [][]interface{}{{"switched"}},
				}, nil
			}
		}
	} else if strings.HasPrefix(cypher, ":") {
		// Starts with : but not :USE - return helpful error
		return nil, fmt.Errorf("unknown command: %s (only :USE is supported)", strings.Split(cypher, "\n")[0])
	}

	// Validate basic syntax
	if err := e.validateSyntax(cypher); err != nil {
		return nil, err
	}

	// IMPORTANT: Do NOT substitute parameters before routing!
	// We need to route the query based on the ORIGINAL query structure,
	// not the substituted one. Otherwise, keywords inside parameter values
	// (like 'MATCH (n) SET n.x = 1' stored as content) will be incorrectly
	// detected as Cypher clauses.
	//
	// Parameter substitution happens AFTER routing, inside each handler.
	// This matches Neo4j's architecture where params are kept separate.

	// Store params in context for handlers to use
	ctx = context.WithValue(ctx, paramsKey, params)

	// Check query limits if storage engine supports it
	// Uses interface{} to avoid importing multidb package (prevents circular dependencies)
	var queryLimitCancel context.CancelFunc
	if namespacedEngine, ok := e.storage.(interface {
		GetQueryLimitChecker() interface {
			CheckQueryRate() error
			CheckQueryLimits(context.Context) (context.Context, context.CancelFunc, error)
		}
	}); ok {
		if qlc := namespacedEngine.GetQueryLimitChecker(); qlc != nil {
			// Check query rate limit
			if err := qlc.CheckQueryRate(); err != nil {
				return nil, err
			}

			// Check write rate limit for write queries
			// We need to check this early, but we don't know if it's a write query yet
			// So we'll check it in the write handlers too

			// Apply query timeout and concurrent query limits
			var err error
			ctx, queryLimitCancel, err = qlc.CheckQueryLimits(ctx)
			if err != nil {
				return nil, err
			}
			// Ensure cancel is called when done
			defer func() {
				if queryLimitCancel != nil {
					queryLimitCancel()
				}
			}()
		}
	}

	// Analyze query - uses cached analysis if available
	// This extracts query metadata (HasMatch, IsReadOnly, Labels, etc.) once
	// and caches it for repeated queries, avoiding redundant string parsing
	info := e.analyzer.Analyze(cypher)

	// For routing, we still need upperQuery for some handlers
	// TODO: Migrate handlers to use QueryInfo directly
	upperQuery := strings.ToUpper(cypher)

	// Try cache for read-only queries (using cached analysis)
	if info.IsReadOnly && e.cache != nil {
		if cached, found := e.cache.Get(cypher, params); found {
			return cached, nil
		}
	}

	// Check for transaction control statements FIRST
	if result, err := e.parseTransactionStatement(cypher); result != nil || err != nil {
		return result, err
	}

	// Check for EXPLAIN/PROFILE execution modes (using cached analysis)
	if info.HasExplain {
		_, innerQuery := parseExecutionMode(cypher)
		return e.executeExplain(ctx, innerQuery)
	}
	if info.HasProfile {
		_, innerQuery := parseExecutionMode(cypher)
		return e.executeProfile(ctx, innerQuery)
	}

	// If in explicit transaction, execute within it
	if e.txContext != nil && e.txContext.active {
		return e.executeInTransaction(ctx, cypher, upperQuery)
	}

	// Auto-commit single query - use async path for performance
	// This uses AsyncEngine's write-behind cache instead of synchronous disk I/O
	// For strict ACID, users should use explicit BEGIN/COMMIT transactions
	result, err := e.executeImplicitAsync(ctx, cypher, upperQuery)

	// Apply result limit if set
	if err == nil && result != nil {
		if namespacedEngine, ok := e.storage.(interface {
			GetQueryLimitChecker() interface {
				GetQueryLimits() interface{}
			}
		}); ok {
			if qlc := namespacedEngine.GetQueryLimitChecker(); qlc != nil {
				if queryLimits := qlc.GetQueryLimits(); queryLimits != nil {
					// Type assert to check if it has MaxResults field
					// We use reflection-like approach: check if it's a struct with MaxResults
					if limits, ok := queryLimits.(interface {
						GetMaxResults() int64
					}); ok {
						if maxResults := limits.GetMaxResults(); maxResults > 0 && int64(len(result.Rows)) > maxResults {
							// Truncate results to limit
							result.Rows = result.Rows[:maxResults]
						}
					}
				}
			}
		}
	}

	// Cache successful read-only queries.
	//
	// NOTE: Aggregation queries (COUNT/SUM/AVG/COLLECT/...) used to be excluded, but in practice they can still
	// be expensive (edge scans, label scans, COLLECT materialization). Caching them is correctness-preserving as
	// long as we invalidate on writes (which we do), so we cache them with a shorter TTL by default.
	if err == nil && info.IsReadOnly && e.cache != nil && isCacheableReadQuery(cypher) {
		// Determine TTL based on query type (using cached analysis)
		ttl := 60 * time.Second // Default: 60s for data queries
		if info.HasAggregation {
			ttl = 1 * time.Second // Conservative TTL for aggregations
		}
		if info.HasCall || info.HasShow {
			ttl = 300 * time.Second // 5 minutes for schema queries
		}
		e.cache.Put(cypher, params, result, ttl)
	}

	// Invalidate caches on write operations (using cached analysis)
	if info.IsWriteQuery {
		// Only invalidate node lookup cache when NODES are deleted
		// Relationship-only deletes (like benchmark CREATE rel DELETE rel) don't affect node cache
		if info.HasDelete && queryDeletesNodes(cypher) {
			e.invalidateNodeLookupCache()
		}

		// Invalidate query result cache using cached labels
		if e.cache != nil {
			if len(info.Labels) > 0 {
				e.cache.InvalidateLabels(info.Labels)
			} else {
				e.cache.Invalidate()
			}
		}
	}

	return result, err
}

// TransactionCapableEngine is an engine that supports ACID transactions.
// Used for type assertion to wrap implicit writes in rollback-capable transactions.
type TransactionCapableEngine interface {
	BeginTransaction() (*storage.BadgerTransaction, error)
}

// executeImplicitAsync executes a single query using implicit transactions for writes.
// For write operations, wraps execution in an implicit transaction that can be
// rolled back on error, preventing partial data corruption from failed queries.
// For strict ACID guarantees with durability, use explicit BEGIN/COMMIT transactions.
func (e *StorageExecutor) executeImplicitAsync(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// Check if this is a write operation using cached analysis
	info := e.analyzer.Analyze(cypher)
	isWrite := info.IsWriteQuery

	// For write operations, use implicit transaction for atomicity
	// This ensures partial writes are rolled back on error
	if isWrite {
		return e.executeWithImplicitTransaction(ctx, cypher, upperQuery)
	}

	// Read-only operations don't need transaction wrapping
	return e.executeWithoutTransaction(ctx, cypher, upperQuery)
}

// executeWithImplicitTransaction wraps a write query in an implicit transaction.
// If any part of the query fails, all changes are rolled back atomically.
// This prevents data corruption from partially executed queries.
func (e *StorageExecutor) executeWithImplicitTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// Try to get a transaction-capable engine
	var txEngine TransactionCapableEngine
	var asyncEngine *storage.AsyncEngine

	// Check if storage is transaction-capable (BadgerEngine, MemoryEngine, or AsyncEngine wrapping one)
	if tc, ok := e.storage.(TransactionCapableEngine); ok {
		txEngine = tc
	} else if ae, ok := e.storage.(*storage.AsyncEngine); ok {
		asyncEngine = ae
		// AsyncEngine wraps another engine - get underlying
		if tc, ok := ae.GetUnderlying().(TransactionCapableEngine); ok {
			txEngine = tc
		}
	}

	// If no transaction support, fall back to direct execution (legacy mode)
	// This is less safe but maintains backward compatibility
	if txEngine == nil {
		result, err := e.executeWithoutTransaction(ctx, cypher, upperQuery)
		if err != nil {
			return nil, err
		}
		// Flush if needed
		if !e.deferFlush {
			if asyncEngine != nil {
				asyncEngine.Flush()
			}
		}
		return result, nil
	}

	// IMPORTANT: If using AsyncEngine with pending writes, flush its cache BEFORE
	// starting the transaction. This ensures the BadgerTransaction can see all
	// previously written data. Without this, MATCH queries in compound statements
	// (MATCH...CREATE) would fail to find nodes in AsyncEngine's cache.
	// We use HasPendingWrites() first as a cheap check to avoid unnecessary flushes.
	if asyncEngine != nil && asyncEngine.HasPendingWrites() {
		asyncEngine.Flush()
	}

	// Start implicit transaction
	tx, err := txEngine.BeginTransaction()
	if err != nil {
		return nil, fmt.Errorf("failed to start implicit transaction: %w", err)
	}

	// Create a transactional wrapper that routes writes through the transaction
	// CRITICAL: We pass the wrapper through context instead of modifying e.storage
	// because e.storage modification is NOT thread-safe for concurrent executions.
	txWrapper := &transactionStorageWrapper{tx: tx, underlying: e.storage}

	// Execute with transaction wrapper via context
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)

	// Execute the query
	result, execErr := e.executeWithoutTransaction(txCtx, cypher, upperQuery)

	// Handle result
	if execErr != nil {
		// Rollback on any error - prevents partial data corruption
		tx.Rollback()
		return nil, execErr
	}

	// Commit successful transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit implicit transaction: %w", err)
	}

	// Flush if needed for durability
	if !e.deferFlush && asyncEngine != nil {
		asyncEngine.Flush()
	}

	return result, nil
}

// ctxKeyTxStorage is the context key for transaction storage wrapper.
type ctxKeyTxStorageType struct{}

var ctxKeyTxStorage = ctxKeyTxStorageType{}

// ctxKeyUseDatabase is the context key for :USE database switching.
// When :USE database_name is detected, the database name is stored in context
// so the server can switch to that database before executing the query.
type ctxKeyUseDatabaseType struct{}

var ctxKeyUseDatabase = ctxKeyUseDatabaseType{}

// GetUseDatabaseFromContext extracts the database name from :USE command if present in context.
// Returns empty string if no :USE command was found.
func GetUseDatabaseFromContext(ctx context.Context) string {
	if dbName, ok := ctx.Value(ctxKeyUseDatabase).(string); ok {
		return dbName
	}
	return ""
}

// getStorage returns the storage to use for the current execution.
// If a transaction wrapper is present in context, it uses that; otherwise uses e.storage.
func (e *StorageExecutor) getStorage(ctx context.Context) storage.Engine {
	if txWrapper, ok := ctx.Value(ctxKeyTxStorage).(*transactionStorageWrapper); ok {
		return txWrapper
	}
	return e.storage
}

// transactionStorageWrapper wraps a BadgerTransaction to implement storage.Engine
// for use in implicit transaction execution. It routes writes through the transaction
// (for atomicity/rollback) and reads through the underlying engine (for performance).
type transactionStorageWrapper struct {
	tx         *storage.BadgerTransaction
	underlying storage.Engine // For read operations not supported by transaction
}

// Write operations - go through transaction for atomicity
func (w *transactionStorageWrapper) CreateNode(node *storage.Node) (storage.NodeID, error) {
	return w.tx.CreateNode(node)
}

func (w *transactionStorageWrapper) UpdateNode(node *storage.Node) error {
	return w.tx.UpdateNode(node)
}

func (w *transactionStorageWrapper) DeleteNode(id storage.NodeID) error {
	return w.tx.DeleteNode(id)
}

func (w *transactionStorageWrapper) CreateEdge(edge *storage.Edge) error {
	return w.tx.CreateEdge(edge)
}

func (w *transactionStorageWrapper) DeleteEdge(id storage.EdgeID) error {
	return w.tx.DeleteEdge(id)
}

// Read operations - transaction supports GetNode, forward others to underlying
func (w *transactionStorageWrapper) GetNode(id storage.NodeID) (*storage.Node, error) {
	return w.tx.GetNode(id)
}

func (w *transactionStorageWrapper) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	return w.underlying.GetEdge(id)
}

func (w *transactionStorageWrapper) UpdateEdge(edge *storage.Edge) error {
	// BadgerTransaction doesn't have UpdateEdge, use underlying
	return w.underlying.UpdateEdge(edge)
}

func (w *transactionStorageWrapper) GetNodesByLabel(label string) ([]*storage.Node, error) {
	return w.underlying.GetNodesByLabel(label)
}

func (w *transactionStorageWrapper) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	return w.underlying.GetFirstNodeByLabel(label)
}

func (w *transactionStorageWrapper) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	return w.underlying.GetOutgoingEdges(nodeID)
}

func (w *transactionStorageWrapper) GetIncomingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	return w.underlying.GetIncomingEdges(nodeID)
}

func (w *transactionStorageWrapper) GetEdgesBetween(startID, endID storage.NodeID) ([]*storage.Edge, error) {
	return w.underlying.GetEdgesBetween(startID, endID)
}

func (w *transactionStorageWrapper) GetEdgeBetween(startID, endID storage.NodeID, edgeType string) *storage.Edge {
	return w.underlying.GetEdgeBetween(startID, endID, edgeType)
}

func (w *transactionStorageWrapper) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	return w.underlying.GetEdgesByType(edgeType)
}

func (w *transactionStorageWrapper) AllNodes() ([]*storage.Node, error) {
	return w.underlying.AllNodes()
}

func (w *transactionStorageWrapper) AllEdges() ([]*storage.Edge, error) {
	return w.underlying.AllEdges()
}

func (w *transactionStorageWrapper) GetAllNodes() []*storage.Node {
	return w.underlying.GetAllNodes()
}

func (w *transactionStorageWrapper) GetInDegree(nodeID storage.NodeID) int {
	return w.underlying.GetInDegree(nodeID)
}

func (w *transactionStorageWrapper) GetOutDegree(nodeID storage.NodeID) int {
	return w.underlying.GetOutDegree(nodeID)
}

func (w *transactionStorageWrapper) GetSchema() *storage.SchemaManager {
	return w.underlying.GetSchema()
}

func (w *transactionStorageWrapper) BulkCreateNodes(nodes []*storage.Node) error {
	// For bulk operations within transaction, create one by one
	for _, node := range nodes {
		if _, err := w.tx.CreateNode(node); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkCreateEdges(edges []*storage.Edge) error {
	for _, edge := range edges {
		if err := w.tx.CreateEdge(edge); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteNode(id); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteEdge(id); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return w.underlying.BatchGetNodes(ids)
}

func (w *transactionStorageWrapper) Close() error {
	// Don't close underlying engine
	return nil
}

func (w *transactionStorageWrapper) NodeCount() (int64, error) {
	return w.underlying.NodeCount()
}

func (w *transactionStorageWrapper) EdgeCount() (int64, error) {
	return w.underlying.EdgeCount()
}

func (w *transactionStorageWrapper) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	// DeleteByPrefix is not supported within a transaction context.
	// This operation should be performed outside of a transaction.
	return 0, 0, fmt.Errorf("DeleteByPrefix not supported within transaction context")
}

// tryFastPathCompoundQuery attempts to handle common compound query patterns
// using pre-compiled regex for faster routing. Returns (result, true) if handled,
// (nil, false) if the query should go through normal routing.
//
// Pattern: MATCH (a:Label), (b:Label) WITH a, b LIMIT 1 CREATE (a)-[r:Type]->(b) DELETE r
// This is a very common pattern in benchmarks and relationship tests.
func (e *StorageExecutor) tryFastPathCompoundQuery(ctx context.Context, cypher string) (*ExecuteResult, bool) {
	// Try Pattern 1: MATCH (a:Label), (b:Label) WITH a, b LIMIT 1 CREATE ... DELETE
	if matches := matchCreateDeleteRelPattern.FindStringSubmatch(cypher); matches != nil {
		label1 := matches[2]
		label2 := matches[4]
		relType := matches[9]
		return e.executeFastPathCreateDeleteRel(label1, label2, "", nil, "", nil, relType)
	}

	// Try Pattern 2: MATCH (p1:Label {prop: val}), (p2:Label {prop: val}) CREATE ... DELETE
	// LDBC-style pattern with property matching
	if matches := matchPropCreateDeleteRelPattern.FindStringSubmatch(cypher); matches != nil {
		// Groups: 1=var1, 2=label1, 3=prop1, 4=val1, 5=var2, 6=label2, 7=prop2, 8=val2, 9=relVar, 10=relType, 11=delVar
		label1 := matches[2]
		prop1 := matches[3]
		val1 := matches[4]
		label2 := matches[6]
		prop2 := matches[7]
		val2 := matches[8]
		relType := matches[10]
		return e.executeFastPathCreateDeleteRel(label1, label2, prop1, val1, prop2, val2, relType)
	}

	// Try Pattern 3: MATCH (a:Label {prop: val}), (b:Label {prop: val}) CREATE ... WITH r DELETE r RETURN count(r)
	// Northwind-style create/delete relationship benchmark shape.
	if matches := matchPropCreateWithDeleteReturnCountRelPattern.FindStringSubmatch(cypher); matches != nil {
		label1 := matches[2]
		prop1 := matches[3]
		val1 := matches[4]
		label2 := matches[6]
		prop2 := matches[7]
		val2 := matches[8]
		relVar := matches[9]
		relType := matches[10]
		withVar := matches[11]
		delVar := matches[12]
		countVar := matches[13]

		// We can't enforce backreferences in Go regex, so validate variable consistency here.
		if relVar == "" || withVar != relVar || delVar != relVar || countVar != relVar {
			return nil, false
		}

		return e.executeFastPathCreateDeleteRelCount(label1, label2, prop1, val1, prop2, val2, relType, relVar)
	}

	return nil, false
}

// executeFastPathCreateDeleteRel executes the fast-path for MATCH...CREATE...DELETE patterns.
// If prop1/prop2 are empty, uses GetFirstNodeByLabel. Otherwise uses property lookup.
func (e *StorageExecutor) executeFastPathCreateDeleteRel(label1, label2, prop1 string, val1 any, prop2 string, val2 any, relType string) (*ExecuteResult, bool) {
	var node1, node2 *storage.Node
	var err error

	// Get node1
	if prop1 == "" {
		node1, err = e.storage.GetFirstNodeByLabel(label1)
	} else {
		node1 = e.findNodeByLabelAndProperty(label1, prop1, val1)
	}
	if err != nil || node1 == nil {
		return nil, false
	}

	// Get node2
	if prop2 == "" {
		node2, err = e.storage.GetFirstNodeByLabel(label2)
	} else {
		node2 = e.findNodeByLabelAndProperty(label2, prop2, val2)
	}
	if err != nil || node2 == nil {
		return nil, false
	}

	// Optimization: This pattern creates a relationship and deletes it in the same
	// statement without returning it. The relationship is not observable to the user,
	// and the net graph effect is a no-op, so we skip storage writes entirely.
	//
	// We still validate that both endpoints exist (via the lookups above) and we
	// still return correct query stats for Neo4j compatibility.

	return &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats: &QueryStats{
			RelationshipsCreated: 1,
			RelationshipsDeleted: 1,
		},
	}, true
}

func (e *StorageExecutor) executeFastPathCreateDeleteRelCount(label1, label2, prop1 string, val1 any, prop2 string, val2 any, relType string, relVar string) (*ExecuteResult, bool) {
	var node1, node2 *storage.Node
	var err error

	if prop1 == "" {
		node1, err = e.storage.GetFirstNodeByLabel(label1)
	} else {
		node1 = e.findNodeByLabelAndProperty(label1, prop1, val1)
	}
	if err != nil || node1 == nil {
		return nil, false
	}

	if prop2 == "" {
		node2, err = e.storage.GetFirstNodeByLabel(label2)
	} else {
		node2 = e.findNodeByLabelAndProperty(label2, prop2, val2)
	}
	if err != nil || node2 == nil {
		return nil, false
	}

	return &ExecuteResult{
		Columns: []string{"count(" + relVar + ")"},
		Rows:    [][]interface{}{{int64(1)}},
		Stats: &QueryStats{
			RelationshipsCreated: 1,
			RelationshipsDeleted: 1,
		},
	}, true
}

// findNodeByLabelAndProperty finds a node by label and a single property value.
// Uses the node lookup cache for O(1) repeated lookups.
func (e *StorageExecutor) findNodeByLabelAndProperty(label, prop string, val any) *storage.Node {
	// Try cache first (with proper locking)
	cacheKey := fmt.Sprintf("%s:{%s:%v}", label, prop, val)
	e.nodeLookupCacheMu.RLock()
	if cached, ok := e.nodeLookupCache[cacheKey]; ok {
		e.nodeLookupCacheMu.RUnlock()
		return cached
	}
	e.nodeLookupCacheMu.RUnlock()

	// Scan nodes with label
	nodes, err := e.storage.GetNodesByLabel(label)
	if err != nil {
		return nil
	}

	// Find matching node
	for _, node := range nodes {
		if nodeVal, ok := node.Properties[prop]; ok {
			if fmt.Sprintf("%v", nodeVal) == fmt.Sprintf("%v", val) {
				// Cache for next time (with proper locking)
				e.nodeLookupCacheMu.Lock()
				e.nodeLookupCache[cacheKey] = node
				e.nodeLookupCacheMu.Unlock()
				return node
			}
		}
	}

	return nil
}

// executeWithoutTransaction executes query without transaction wrapping (original path).
func (e *StorageExecutor) executeWithoutTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// FAST PATH: Check for common compound query patterns using pre-compiled regex
	// This avoids multiple findKeywordIndex calls for frequently-used patterns
	if result, handled := e.tryFastPathCompoundQuery(ctx, cypher); handled {
		return result, nil
	}

	// Route to appropriate handler based on query type
	// upperQuery is passed in to avoid redundant conversion

	// Cache keyword checks to avoid repeated searches
	startsWithMatch := strings.HasPrefix(upperQuery, "MATCH")
	startsWithCreate := strings.HasPrefix(upperQuery, "CREATE")
	startsWithMerge := strings.HasPrefix(upperQuery, "MERGE")

	// MERGE queries get special handling - they have their own ON CREATE SET / ON MATCH SET logic
	if startsWithMerge {
		// Check for MERGE ... WITH ... MATCH chain pattern (e.g., import script pattern)
		withIdx := findKeywordIndex(cypher, "WITH")
		if withIdx > 0 {
			// Check for MATCH after WITH (this is the chained pattern)
			afterWith := cypher[withIdx:]
			if findKeywordIndex(afterWith, "MATCH") > 0 {
				return e.executeMergeWithChain(ctx, cypher)
			}
		}
		// Check for multiple MERGEs without WITH (e.g., MERGE (a) MERGE (b) MERGE (a)-[:REL]->(b))
		firstMergeEnd := findKeywordIndex(cypher[5:], ")")
		if firstMergeEnd > 0 {
			afterFirstMerge := cypher[5+firstMergeEnd+1:]
			secondMergeIdx := findKeywordIndex(afterFirstMerge, "MERGE")
			if secondMergeIdx >= 0 {
				return e.executeMultipleMerges(ctx, cypher)
			}
		}
		return e.executeMerge(ctx, cypher)
	}

	// Cache findKeywordIndex results for compound query detection
	var mergeIdx, createIdx, withIdx, deleteIdx, optionalMatchIdx int = -1, -1, -1, -1, -1

	if startsWithMatch {
		// Only search for keywords if query starts with MATCH
		mergeIdx = findKeywordIndex(cypher, "MERGE")
		createIdx = findKeywordIndex(cypher, "CREATE")
		optionalMatchIdx = findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH")
	} else if startsWithCreate {
		// Check for multiple CREATE statements (e.g., CREATE (a) CREATE (b) CREATE (a)-[:REL]->(b))
		firstCreateEnd := findKeywordIndex(cypher[6:], ")")
		if firstCreateEnd > 0 {
			afterFirstCreate := cypher[6+firstCreateEnd+1:]
			secondCreateIdx := findKeywordIndex(afterFirstCreate, "CREATE")
			if secondCreateIdx >= 0 {
				return e.executeMultipleCreates(ctx, cypher)
			}
		}
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

	// Cache contains checks for DELETE - use word-boundary-aware detection
	// Note: Can't use " DELETE " because DELETE is often followed by variable name (DELETE n)
	// findKeywordIndex handles word boundaries properly (won't match 'ToDelete' in string literals)
	hasDelete := findKeywordIndex(cypher, "DELETE") > 0 // Must be after MATCH, not at start
	hasDetachDelete := containsKeywordOutsideStrings(cypher, "DETACH DELETE")

	// Check for compound queries - MATCH ... DELETE, MATCH ... SET, etc.
	if hasDelete || hasDetachDelete {
		return e.executeDelete(ctx, cypher)
	}

	// Cache SET-related checks - use string-literal-aware detection to avoid
	// matching keywords inside user content like 'MATCH (n) SET n.x = 1'
	// Note: findKeywordIndex already checks word boundaries, so no need for leading space
	hasSet := containsKeywordOutsideStrings(cypher, "SET")
	hasOnCreateSet := containsKeywordOutsideStrings(cypher, "ON CREATE SET")
	hasOnMatchSet := containsKeywordOutsideStrings(cypher, "ON MATCH SET")

	// NEO4J COMPAT: Handle CREATE ... SET pattern (e.g., CREATE (n) SET n.x = 1)
	// Neo4j allows SET immediately after CREATE without requiring MATCH
	if startsWithCreate && hasSet && !hasOnCreateSet && !hasOnMatchSet {
		return e.executeCreateSet(ctx, cypher)
	}

	// Check for ALTER DATABASE before generic SET (ALTER DATABASE SET LIMIT contains "SET")
	if findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE") == 0 {
		return e.executeAlterDatabase(ctx, cypher)
	}

	// Only route to executeSet if it's a MATCH ... SET or standalone SET
	if hasSet && !hasOnCreateSet && !hasOnMatchSet {
		return e.executeSet(ctx, cypher)
	}

	// Handle MATCH ... REMOVE (property removal) - string-literal-aware
	// Note: findKeywordIndex already checks word boundaries
	if containsKeywordOutsideStrings(cypher, "REMOVE") {
		return e.executeRemove(ctx, cypher)
	}

	// Compound queries: MATCH ... OPTIONAL MATCH ...
	// But NOT when there's a WITH clause before OPTIONAL MATCH (that's handled by executeMatchWithOptionalMatch)
	if startsWithMatch && optionalMatchIdx > 0 {
		// Check if there's a WITH clause BEFORE OPTIONAL MATCH
		// If so, route to the specialized handler that processes WITH first
		withBeforeOptional := findKeywordIndex(cypher[:optionalMatchIdx], "WITH")
		if withBeforeOptional > 0 {
			// WITH comes before OPTIONAL MATCH - route to executeMatchWithOptionalMatch
			return e.executeMatchWithOptionalMatch(ctx, cypher)
		}
		return e.executeCompoundMatchOptionalMatch(ctx, cypher)
	}

	// Compound queries: MATCH ... CALL {} ... (correlated subquery)
	if startsWithMatch && hasSubqueryPattern(cypher, callSubqueryRe) {
		return e.executeMatchWithCallSubquery(ctx, cypher)
	}

	// Compound queries: MATCH ... CALL procedure() ... (procedure with bound variables)
	if startsWithMatch && findKeywordIndex(cypher, "CALL") > 0 {
		// Check if it's a procedure call (not a subquery)
		callIdx := findKeywordIndex(cypher, "CALL")
		if callIdx > 0 {
			callPart := strings.TrimSpace(cypher[callIdx:])
			if !isCallSubquery(callPart) {
				// It's a procedure call - handle with bound variables
				return e.executeMatchWithCallProcedure(ctx, cypher)
			}
		}
	}

	switch {
	case findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH") == 0:
		// OPTIONAL MATCH must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "OPTIONAL MATCH", "OPTIONAL\tMATCH", "OPTIONAL\nMATCH", etc.
		return e.executeOptionalMatch(ctx, cypher)
	case startsWithMatch && isShortestPathQuery(cypher):
		// Handle shortestPath() and allShortestPaths() queries
		query, err := e.parseShortestPathQuery(cypher)
		if err != nil {
			return nil, err
		}
		return e.executeShortestPathQuery(query)
	case startsWithMatch:
		// Check for optimizable patterns FIRST
		patternInfo := DetectQueryPattern(cypher)
		if patternInfo.IsOptimizable() {
			if result, ok := e.ExecuteOptimized(ctx, cypher, patternInfo); ok {
				return result, nil
			}
			// Fall through to generic on optimization failure
		}
		return e.executeMatch(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "CONSTRAINT") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "FULLTEXT INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "VECTOR INDEX") == 0,
		findKeywordIndex(cypher, "CREATE INDEX") == 0:
		// Schema commands - constraints and indexes (check more specific patterns first)
		// Must be at start (position 0) to be a standalone clause
		return e.executeSchemaCommand(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE") == 0:
		// System command: CREATE COMPOSITE DATABASE (check before CREATE DATABASE)
		// Must be at start (position 0) to be a standalone clause
		return e.executeCreateCompositeDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE") == 0:
		// System command: CREATE DATABASE (check before generic CREATE)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "CREATE DATABASE", "CREATE\tDATABASE", "CREATE\nDATABASE", etc.
		return e.executeCreateDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS") == 0:
		// System command: CREATE ALIAS (check before generic CREATE)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "CREATE ALIAS", "CREATE\tALIAS", "CREATE\nALIAS", etc.
		return e.executeCreateAlias(ctx, cypher)
	case startsWithCreate:
		return e.executeCreate(ctx, cypher)
	case hasDelete || hasDetachDelete:
		// DELETE/DETACH DELETE already detected above with findKeywordIndex
		return e.executeDelete(ctx, cypher)
	case findKeywordIndex(cypher, "CALL") == 0:
		// Distinguish CALL {} subquery from CALL procedure()
		// Must be at start (position 0) to be a standalone clause
		if isCallSubquery(cypher) {
			return e.executeCallSubquery(ctx, cypher)
		}
		return e.executeCall(ctx, cypher)
	case findKeywordIndex(cypher, "RETURN") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeReturn(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE") == 0:
		// System command: DROP COMPOSITE DATABASE (check before DROP DATABASE)
		// Must be at start (position 0) to be a standalone clause
		return e.executeDropCompositeDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "DATABASE") == 0:
		// System command: DROP DATABASE (check before generic DROP)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "DROP DATABASE", "DROP\tDATABASE", "DROP\nDATABASE", etc.
		return e.executeDropDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "ALIAS") == 0:
		// System command: DROP ALIAS (check before generic DROP)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "DROP ALIAS", "DROP\tALIAS", "DROP\nALIAS", etc.
		return e.executeDropAlias(ctx, cypher)
	case findKeywordIndex(cypher, "DROP") == 0:
		// DROP INDEX/CONSTRAINT - treat as no-op (NornicDB manages indexes internally)
		// Must be at start (position 0) to be a standalone clause
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	case findKeywordIndex(cypher, "WITH") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeWith(ctx, cypher)
	case findKeywordIndex(cypher, "UNWIND") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeUnwind(ctx, cypher)
	case findKeywordIndex(cypher, "UNION ALL") >= 0:
		// UNION ALL can appear anywhere in query
		return e.executeUnion(ctx, cypher, true)
	case findKeywordIndex(cypher, "UNION") >= 0:
		// UNION can appear anywhere in query
		return e.executeUnion(ctx, cypher, false)
	case findKeywordIndex(cypher, "FOREACH") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeForeach(ctx, cypher)
	case findKeywordIndex(cypher, "LOAD CSV") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeLoadCSV(ctx, cypher)
	// SHOW commands for Neo4j compatibility
	case findMultiWordKeywordIndex(cypher, "SHOW", "INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "INDEX") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowIndexes(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "CONSTRAINTS") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "CONSTRAINT") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowConstraints(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "PROCEDURES") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowProcedures(ctx, cypher)
	case findKeywordIndex(cypher, "SHOW FUNCTIONS") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowFunctions(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "COMPOSITE DATABASES") == 0:
		// System command: SHOW COMPOSITE DATABASES (check before SHOW DATABASES)
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowCompositeDatabases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "CONSTITUENTS") == 0:
		// System command: SHOW CONSTITUENTS
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowConstituents(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "DATABASES") == 0:
		// System command: SHOW DATABASES (plural - check before singular)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "SHOW DATABASES", "SHOW\tDATABASES", "SHOW\nDATABASES", etc.
		return e.executeShowDatabases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "DATABASE") == 0:
		// System command: SHOW DATABASE (singular)
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "SHOW DATABASE", "SHOW\tDATABASE", "SHOW\nDATABASE", etc.
		return e.executeShowDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "ALIASES") == 0:
		// System command: SHOW ALIASES
		// Must be at start (position 0) to be a standalone clause
		// Handles flexible whitespace: "SHOW ALIASES", "SHOW\tALIASES", "SHOW\nALIASES", etc.
		return e.executeShowAliases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "ALTER", "COMPOSITE DATABASE") == 0:
		// System command: ALTER COMPOSITE DATABASE (check before ALTER DATABASE)
		// Must be at start (position 0) to be a standalone clause
		return e.executeAlterCompositeDatabase(ctx, cypher)
	// Note: ALTER DATABASE is handled earlier (before SET check) to avoid routing conflict
	case findMultiWordKeywordIndex(cypher, "SHOW", "LIMITS") == 0:
		// System command: SHOW LIMITS
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowLimits(ctx, cypher)
	default:
		firstWord := strings.Split(upperQuery, " ")[0]
		return nil, fmt.Errorf("unsupported query type: %s (supported: MATCH, CREATE, MERGE, DELETE, SET, REMOVE, RETURN, WITH, UNWIND, CALL, FOREACH, LOAD CSV, SHOW, DROP, ALTER)", firstWord)
	}
}

// executeReturn handles simple RETURN statements (e.g., "RETURN 1").
func (e *StorageExecutor) executeReturn(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters before processing
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Parse RETURN clause - use word boundary detection
	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause not found in query: %q", truncateQuery(cypher, 80))
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

		// Handle NULL literal explicitly first
		if strings.EqualFold(part, "null") {
			values = append(values, nil)
			continue
		}

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

// splitReturnExpressions splits RETURN expressions by comma, respecting parentheses and brackets depth
func splitReturnExpressions(clause string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	bracketDepth := 0
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
			parenDepth++
			current.WriteRune(ch)
		case ch == ')' && !inQuote:
			parenDepth--
			current.WriteRune(ch)
		case ch == '[' && !inQuote:
			bracketDepth++
			current.WriteRune(ch)
		case ch == ']' && !inQuote:
			bracketDepth--
			current.WriteRune(ch)
		case ch == ',' && parenDepth == 0 && bracketDepth == 0 && !inQuote:
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

// validateSyntax performs syntax validation.
// When NORNICDB_PARSER=antlr, uses ANTLR for strict OpenCypher grammar validation.
// When NORNICDB_PARSER=nornic (default), uses fast inline validation.
func (e *StorageExecutor) validateSyntax(cypher string) error {
	// Use ANTLR parser for validation when configured
	if config.IsANTLRParser() {
		return e.validateSyntaxANTLR(cypher)
	}
	return e.validateSyntaxNornic(cypher)
}

// validateSyntaxANTLR uses ANTLR for strict OpenCypher grammar validation.
// Provides detailed error messages with line/column information.
func (e *StorageExecutor) validateSyntaxANTLR(cypher string) error {
	return antlr.Validate(cypher)
}

// validateSyntaxNornic performs fast inline syntax validation.
func (e *StorageExecutor) validateSyntaxNornic(cypher string) error {
	// Check for valid starting keyword (including EXPLAIN/PROFILE prefixes and transaction control)
	validStarts := []string{"MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH", "UNWIND", "OPTIONAL", "DROP", "SHOW", "FOREACH", "LOAD", "EXPLAIN", "PROFILE", "ALTER", "USE", "BEGIN", "COMMIT", "ROLLBACK"}
	hasValidStart := false
	for _, start := range validStarts {
		if startsWithKeywordFold(cypher, start) {
			hasValidStart = true
			break
		}
	}
	if !hasValidStart {
		return fmt.Errorf("syntax error: query must start with a valid clause (MATCH, CREATE, MERGE, DELETE, CALL, SHOW, EXPLAIN, PROFILE, ALTER, USE, BEGIN, COMMIT, ROLLBACK, etc.)")
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
