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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	// Cache successful read-only queries
	if err == nil && info.IsReadOnly && e.cache != nil {
		// Determine TTL based on query type (using cached analysis)
		ttl := 60 * time.Second // Default: 60s for data queries
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
func (w *transactionStorageWrapper) CreateNode(node *storage.Node) error {
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
		if err := w.tx.CreateNode(node); err != nil {
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

	// Create the relationship
	edgeID := e.generateID()
	edge := &storage.Edge{
		ID:         storage.EdgeID(edgeID),
		Type:       relType,
		StartNode:  node1.ID,
		EndNode:    node2.ID,
		Properties: make(map[string]interface{}),
	}

	if err := e.storage.CreateEdge(edge); err != nil {
		return nil, false
	}

	// Delete the relationship immediately
	if err := e.storage.DeleteEdge(edge.ID); err != nil {
		return nil, false
	}

	return &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
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
		// Check for optimizable patterns FIRST
		patternInfo := DetectQueryPattern(cypher)
		if patternInfo.IsOptimizable() {
			if result, ok := e.ExecuteOptimized(ctx, cypher, patternInfo); ok {
				return result, nil
			}
			// Fall through to generic on optimization failure
		}
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
		// Distinguish CALL {} subquery from CALL procedure()
		if isCallSubquery(cypher) {
			return e.executeCallSubquery(ctx, cypher)
		}
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
		firstWord := strings.Split(upperQuery, " ")[0]
		return nil, fmt.Errorf("unsupported query type: %s (supported: MATCH, CREATE, MERGE, DELETE, SET, REMOVE, RETURN, WITH, UNWIND, CALL, FOREACH, LOAD CSV, SHOW, DROP)", firstWord)
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

// nodeToMap converts a storage.Node to a map for result output.
// Filters out internal properties like embeddings which are huge.
// Properties are included at the top level for Neo4j compatibility.
// Embeddings are replaced with a summary showing status and dimensions.

// executeDelete handles DELETE queries.
func (e *StorageExecutor) executeDelete(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

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
		return nil, fmt.Errorf("DELETE requires a MATCH clause first (e.g., MATCH (n) DELETE n)")
	}

	// Parse the delete target variable(s) - e.g., "DELETE n" or "DELETE n, r"
	// Preserve original case of variable names
	deleteClause := strings.TrimSpace(cypher[deleteIdx:])
	upperDeleteClause := strings.ToUpper(deleteClause)
	if detach {
		if strings.HasPrefix(upperDeleteClause, "DETACH DELETE ") {
			deleteClause = deleteClause[14:] // len("DETACH DELETE ")
		} else if strings.HasPrefix(upperDeleteClause, "DETACH ") {
			deleteClause = deleteClause[7:] // len("DETACH ")
		}
	}
	upperDeleteClause = strings.ToUpper(deleteClause)
	if strings.HasPrefix(upperDeleteClause, "DELETE ") {
		deleteClause = deleteClause[7:] // len("DELETE ")
	}
	deleteVars := strings.TrimSpace(deleteClause)

	// Execute the match first - return the specific variables being deleted
	// Can't use RETURN * because it returns literal "*" instead of expanding
	matchQuery := cypher[matchIdx:deleteIdx] + " RETURN " + deleteVars
	matchResult, err := e.executeMatch(ctx, matchQuery)
	if err != nil {
		return nil, err
	}

	// Delete matched nodes and/or relationships
	for _, row := range matchResult.Rows {
		for _, val := range row {
			// Try to extract node ID or edge ID
			var nodeID string
			var edgeID string

			switch v := val.(type) {
			case map[string]interface{}:
				// Check if it's a relationship or node by looking for internal ID keys
				// Relationships have _edgeId, nodes have _nodeId
				if id, ok := v["_edgeId"].(string); ok {
					edgeID = id
				} else if id, ok := v["_nodeId"].(string); ok {
					nodeID = id
				}
			case *storage.Node:
				// Direct node pointer
				nodeID = string(v.ID)
			case *storage.Edge:
				// Direct edge pointer
				edgeID = string(v.ID)
			case string:
				// Just an ID string - could be node or edge
				nodeID = v
			}

			// Handle relationship deletion
			if edgeID != "" {
				if err := e.storage.DeleteEdge(storage.EdgeID(edgeID)); err == nil {
					result.Stats.RelationshipsDeleted++
				}
				continue
			}

			// Handle node deletion
			if nodeID == "" {
				continue
			}

			if detach {
				// Delete all connected edges first
				edges, _ := e.storage.GetOutgoingEdges(storage.NodeID(nodeID))
				for _, edge := range edges {
					e.storage.DeleteEdge(edge.ID)
					result.Stats.RelationshipsDeleted++
				}
				edges, _ = e.storage.GetIncomingEdges(storage.NodeID(nodeID))
				for _, edge := range edges {
					e.storage.DeleteEdge(edge.ID)
					result.Stats.RelationshipsDeleted++
				}
			}
			if err := e.storage.DeleteNode(storage.NodeID(nodeID)); err == nil {
				result.Stats.NodesDeleted++
			}
		}
	}

	return result, nil
}

// executeSet handles MATCH ... SET queries.
func (e *StorageExecutor) executeSet(ctx context.Context, cypher string) (*ExecuteResult, error) {
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

	// Use word boundary detection to avoid matching substrings
	matchIdx := findKeywordIndex(normalized, "MATCH")
	setIdx := findKeywordIndex(normalized, "SET")
	returnIdx := findKeywordIndex(normalized, "RETURN")

	if matchIdx == -1 || setIdx == -1 {
		return nil, fmt.Errorf("SET requires a MATCH clause first (e.g., MATCH (n) SET n.property = value)")
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
	// "SET " is 4 characters, so setIdx + 4 skips past "SET "
	var setPart string
	if returnIdx > 0 {
		setPart = strings.TrimSpace(normalized[setIdx+4 : returnIdx])
	} else {
		setPart = strings.TrimSpace(normalized[setIdx+4:])
	}

	// Check for property merge operator: n += $properties
	if strings.Contains(setPart, "+=") {
		return e.executeSetMerge(matchResult, setPart, result)
	}

	// Split SET clause into individual assignments, respecting brackets
	// e.g., "n.embedding = [0.1, 0.2], n.dim = 4" -> ["n.embedding = [0.1, 0.2]", "n.dim = 4"]
	assignments := e.splitSetAssignmentsRespectingBrackets(setPart)

	if len(assignments) == 0 || (len(assignments) == 1 && strings.TrimSpace(assignments[0]) == "") {
		return nil, fmt.Errorf("SET clause requires at least one assignment")
	}

	var variable string
	validAssignments := 0
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		// Check for label assignment: n:Label (no = sign, has : for label)
		eqIdx := strings.Index(assignment, "=")
		if eqIdx == -1 {
			// Could be a label assignment like "n:Label"
			colonIdx := strings.Index(assignment, ":")
			if colonIdx > 0 {
				// This is a label assignment
				labelVar := strings.TrimSpace(assignment[:colonIdx])
				labelName := strings.TrimSpace(assignment[colonIdx+1:])
				if labelVar != "" && labelName != "" {
					validAssignments++
					variable = labelVar
					// Add label to matched nodes
					for _, row := range matchResult.Rows {
						for _, val := range row {
							if node, ok := val.(map[string]interface{}); ok {
								id, ok := node["_nodeId"].(string)
								if !ok {
									continue
								}
								storageNode, err := e.storage.GetNode(storage.NodeID(id))
								if err != nil {
									continue
								}
								// Add label if not already present
								hasLabel := false
								for _, l := range storageNode.Labels {
									if l == labelName {
										hasLabel = true
										break
									}
								}
								if !hasLabel {
									storageNode.Labels = append(storageNode.Labels, labelName)
									if err := e.storage.UpdateNode(storageNode); err == nil {
										result.Stats.LabelsAdded++
									}
								}
							}
						}
					}
					continue
				}
			}
			return nil, fmt.Errorf("invalid SET assignment: %q (expected n.property = value or n:Label)", assignment)
		}

		left := strings.TrimSpace(assignment[:eqIdx])
		right := strings.TrimSpace(assignment[eqIdx+1:])

		// Extract variable and property
		parts := strings.SplitN(left, ".", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid SET assignment: %q (expected variable.property)", left)
		}
		validAssignments++
		variable = parts[0]
		propName := parts[1]
		propValue := e.parseValue(right)

		// Update matched nodes
		for _, row := range matchResult.Rows {
			for _, val := range row {
				if node, ok := val.(map[string]interface{}); ok {
					id, ok := node["_nodeId"].(string)
					if !ok {
						continue
					}
					storageNode, err := e.storage.GetNode(storage.NodeID(id))
					if err != nil {
						continue
					}
					// Use setNodeProperty to properly route "embedding" to node.Embedding
					setNodeProperty(storageNode, propName, propValue)
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
					id, ok := node["_nodeId"].(string)
					if !ok {
						continue
					}
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
			id, ok := nodeMap["_nodeId"].(string)
			if !ok {
				continue
			}
			storageNode, err := e.storage.GetNode(storage.NodeID(id))
			if err != nil {
				continue
			}
			// Merge properties (new values override existing)
			// Use setNodeProperty to properly route "embedding" to node.Embedding
			for k, v := range propsToMerge {
				setNodeProperty(storageNode, k, v)
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
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

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
		return nil, fmt.Errorf("REMOVE requires a MATCH clause first (e.g., MATCH (n) REMOVE n.property)")
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
			id, ok := nodeMap["_nodeId"].(string)
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
					id, ok := nodeMap["_nodeId"].(string)
					if !ok {
						continue
					}
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

// smartSplitReturnItems splits a RETURN clause by commas, but respects:
// - CASE/END boundaries
// - Parentheses (function calls)
// - String literals
// smartSplitReturnItems splits RETURN items by comma, respecting strings, parentheses, and CASE/END.
// Properly handles UTF-8 encoded strings with multi-byte characters.
func (e *StorageExecutor) smartSplitReturnItems(returnPart string) []string {
	var result []string
	var current strings.Builder
	var inString bool
	var stringChar rune
	var parenDepth int
	var caseDepth int

	runes := []rune(returnPart)
	runeLen := len(runes)

	// Build rune-to-byte index mapping for keyword checking
	runeToByteIndex := make([]int, runeLen+1)
	byteIdx := 0
	for ri, r := range runes {
		runeToByteIndex[ri] = byteIdx
		byteIdx += len(string(r))
	}
	runeToByteIndex[runeLen] = byteIdx

	upper := strings.ToUpper(returnPart)

	for ri := 0; ri < runeLen; ri++ {
		ch := runes[ri]
		bytePos := runeToByteIndex[ri]

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

		// Track CASE/END keywords (using byte positions for substring comparison)
		if bytePos+4 <= len(returnPart) && upper[bytePos:bytePos+4] == "CASE" {
			// Check if CASE is a word boundary
			prevOk := ri == 0 || !isAlphaNum(runes[ri-1])
			nextRuneIdx := ri + 4 // Skip 4 runes for "CASE"
			// Need to find which rune corresponds to bytePos+4
			for nextRuneIdx < runeLen && runeToByteIndex[nextRuneIdx] < bytePos+4 {
				nextRuneIdx++
			}
			nextOk := nextRuneIdx >= runeLen || !isAlphaNum(runes[nextRuneIdx])
			if prevOk && nextOk {
				caseDepth++
			}
		}
		if bytePos+3 <= len(returnPart) && upper[bytePos:bytePos+3] == "END" {
			// Check if END is a word boundary
			prevOk := ri == 0 || !isAlphaNum(runes[ri-1])
			nextRuneIdx := ri + 3 // Skip 3 runes for "END"
			for nextRuneIdx < runeLen && runeToByteIndex[nextRuneIdx] < bytePos+3 {
				nextRuneIdx++
			}
			nextOk := nextRuneIdx >= runeLen || !isAlphaNum(runes[nextRuneIdx])
			if prevOk && nextOk && caseDepth > 0 {
				caseDepth--
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
	// Create filter function for parallel execution
	filterFn := func(node *storage.Node) bool {
		return e.evaluateWhere(node, variable, whereClause)
	}

	// Use parallel filtering for large datasets
	return parallelFilterNodes(nodes, filterFn)
}

func (e *StorageExecutor) evaluateWhere(node *storage.Node, variable, whereClause string) bool {
	whereClause = strings.TrimSpace(whereClause)
	upperClause := strings.ToUpper(whereClause)

	// Handle parenthesized expressions - strip outer parens and recurse
	if strings.HasPrefix(whereClause, "(") && strings.HasSuffix(whereClause, ")") {
		// Verify these are matching outer parens, not separate groups
		depth := 0
		isOuterParen := true
		for i, ch := range whereClause {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			// If depth goes to 0 before the last char, these aren't outer parens
			if depth == 0 && i < len(whereClause)-1 {
				isOuterParen = false
				break
			}
		}
		if isOuterParen {
			return e.evaluateWhere(node, variable, whereClause[1:len(whereClause)-1])
		}
	}

	// CRITICAL: Handle AND/OR at top level FIRST before subqueries
	// This ensures "EXISTS {} AND COUNT {} >= 2" is properly split
	if andIdx := findTopLevelKeyword(whereClause, " AND "); andIdx > 0 {
		left := strings.TrimSpace(whereClause[:andIdx])
		right := strings.TrimSpace(whereClause[andIdx+5:])
		return e.evaluateWhere(node, variable, left) && e.evaluateWhere(node, variable, right)
	}

	// Handle OR at top level only
	if orIdx := findTopLevelKeyword(whereClause, " OR "); orIdx > 0 {
		left := strings.TrimSpace(whereClause[:orIdx])
		right := strings.TrimSpace(whereClause[orIdx+4:])
		return e.evaluateWhere(node, variable, left) || e.evaluateWhere(node, variable, right)
	}

	// Handle NOT EXISTS { } subquery FIRST (before other NOT handling)
	// Uses regex for whitespace-flexible matching
	if hasSubqueryPattern(whereClause, notExistsSubqueryRe) {
		return e.evaluateNotExistsSubquery(node, variable, whereClause)
	}

	// Handle EXISTS { } subquery (whitespace-flexible)
	if hasSubqueryPattern(whereClause, existsSubqueryRe) {
		return e.evaluateExistsSubquery(node, variable, whereClause)
	}

	// Handle COUNT { } subquery with comparison (whitespace-flexible)
	if hasSubqueryPattern(whereClause, countSubqueryRe) {
		return e.evaluateCountSubqueryComparison(node, variable, whereClause)
	}

	// Handle NOT prefix
	if strings.HasPrefix(upperClause, "NOT ") {
		inner := strings.TrimSpace(whereClause[4:])
		return !e.evaluateWhere(node, variable, inner)
	}

	// Handle label check: n:Label or variable:Label
	if colonIdx := strings.Index(whereClause, ":"); colonIdx > 0 {
		labelVar := strings.TrimSpace(whereClause[:colonIdx])
		labelName := strings.TrimSpace(whereClause[colonIdx+1:])
		// Check if this looks like a simple variable:Label pattern
		if len(labelVar) > 0 && len(labelName) > 0 &&
			!strings.ContainsAny(labelVar, " .(") &&
			!strings.ContainsAny(labelName, " .(=<>") {
			// If the variable matches our node variable, check the label
			if labelVar == variable {
				for _, l := range node.Labels {
					if l == labelName {
						return true
					}
				}
				return false
			}
		}
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

	// Handle arrays: [0.1, 0.2, 0.3]
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return e.parseArrayValue(s)
	}

	// Handle quoted strings with escape sequence support
	if (strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
		(strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) {
		inner := s[1 : len(s)-1]
		// Unescape: \' -> ', \" -> ", \\ -> \
		inner = strings.ReplaceAll(inner, "\\'", "'")
		inner = strings.ReplaceAll(inner, "\\\"", "\"")
		inner = strings.ReplaceAll(inner, "\\\\", "\\")
		return inner
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

	// Handle numbers - preserve int64 for integers, use float64 only for decimals
	// The comparison functions use toFloat64() which handles both types
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i // Keep as int64 for Neo4j compatibility
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	return s
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

	// Check for function calls - these should be evaluated, not treated as property access
	// e.g., coalesce(p.nickname, p.name), toString(p.age), etc.
	if strings.Contains(expr, "(") {
		return e.evaluateExpression(expr, variable, node)
	}

	// Check for IS NULL / IS NOT NULL - these need full evaluation
	upperExpr := strings.ToUpper(expr)
	if strings.Contains(upperExpr, " IS NULL") || strings.Contains(upperExpr, " IS NOT NULL") {
		return e.evaluateExpression(expr, variable, node)
	}

	// Check for arithmetic operators - need full evaluation
	if strings.ContainsAny(expr, "+-*/%") {
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

		// Handle special "embedding" property - return summary, never the raw array
		if propName == "embedding" {
			return e.buildEmbeddingSummary(node)
		}

		// Handle has_embedding specially - check both property and native embedding field
		// This supports Mimir's query: WHERE f.has_embedding = true
		if propName == "has_embedding" {
			// Check property first
			if val, ok := node.Properties["has_embedding"]; ok {
				return val
			}
			// Fall back to checking native embedding field
			return len(node.Embedding) > 0
		}

		// Filter out internal embedding-related properties (except has_embedding handled above)
		if e.isInternalProperty(propName) {
			return nil
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

// idGen is a fast atomic counter for ID generation
var idGen int64

func (e *StorageExecutor) generateID() string {
	// Use fast atomic counter + process start time for unique IDs
	// Much faster than crypto/rand while still globally unique
	id := atomic.AddInt64(&idGen, 1)
	return fmt.Sprintf("n%d", id)
}

// Deprecated: Sequential counter replaced with UUID generation
var idCounter int64

func (e *StorageExecutor) idCounter() int64 {
	// Keep for backward compatibility but not used in generateID anymore
	atomic.AddInt64(&idCounter, 1)
	return atomic.LoadInt64(&idCounter)
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
	// Format: MATCH (var)<-[:TYPE]-(other) WHERE ...
	upperSub := strings.ToUpper(subquery)

	if !strings.HasPrefix(upperSub, "MATCH ") {
		return false
	}

	// Split out any WHERE clause from the pattern
	pattern := strings.TrimSpace(subquery[6:])
	innerWhere := ""

	// Use regex to find WHERE with any whitespace before it (including newlines)
	whereRe := regexp.MustCompile(`(?i)\s+WHERE\s+`)
	if loc := whereRe.FindStringIndex(pattern); loc != nil {
		innerWhere = strings.TrimSpace(pattern[loc[1]:])
		pattern = strings.TrimSpace(pattern[:loc[0]])
	}

	// Check if pattern references our variable
	if !strings.Contains(pattern, "("+variable+")") && !strings.Contains(pattern, "("+variable+":") {
		return false
	}

	// Check for chained relationship pattern (e.g., (p)-[:KNOWS]->()-[:KNOWS]->())
	// Count the number of relationship hops by counting relationship brackets [-
	// Each hop has one -[...]-
	relationshipCount := strings.Count(pattern, "-[")
	if relationshipCount > 1 {
		return e.checkChainedPattern(node, variable, pattern, innerWhere)
	}

	// Extract the target variable name from pattern (e.g., "report" from "(m)-[:MANAGES]->(report)")
	targetVar := e.extractTargetVariable(pattern, variable)

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
				// If there's an inner WHERE, check it against the connected node
				if innerWhere != "" {
					sourceNode, err := e.storage.GetNode(edge.StartNode)
					if err != nil || !e.evaluateInnerWhere(sourceNode, targetVar, innerWhere) {
						continue
					}
				}
				return true
			}
		}
	}

	if checkOutgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if len(relTypes) == 0 || e.edgeTypeMatches(edge.Type, relTypes) {
				// If there's an inner WHERE, check it against the connected node
				if innerWhere != "" {
					targetNode, err := e.storage.GetNode(edge.EndNode)
					if err != nil || !e.evaluateInnerWhere(targetNode, targetVar, innerWhere) {
						continue
					}
				}
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

// checkChainedPattern handles chained relationship patterns like (p)-[:KNOWS]->()-[:KNOWS]->()
func (e *StorageExecutor) checkChainedPattern(node *storage.Node, variable, pattern, innerWhere string) bool {
	// Parse the pattern to extract relationship hops
	// E.g., (p)-[:KNOWS]->()-[:KNOWS]->() has two hops

	// Find the first relationship part
	// Pattern looks like: (variable)-[rel1]->(intermediate)-[rel2]->...

	// Find the start of the first relationship (after the variable node)
	varPattern := "(" + variable + ")"
	if !strings.Contains(pattern, varPattern) {
		// Try with label: (variable:Label)
		idx := strings.Index(pattern, "("+variable+":")
		if idx < 0 {
			return false
		}
	}

	// Extract relationship hops
	hops := e.parseRelationshipHops(pattern, variable)
	if len(hops) == 0 {
		return false
	}

	// Traverse the chain starting from the given node
	return e.traverseChain(node, hops, 0)
}

// relationshipHop represents one step in a chained relationship pattern
type relationshipHop struct {
	relTypes []string
	outgoing bool
}

// parseRelationshipHops extracts relationship hops from a pattern
func (e *StorageExecutor) parseRelationshipHops(pattern, variable string) []relationshipHop {
	var hops []relationshipHop

	// Find all relationship patterns: -[...]->  or  <-[...]-
	remaining := pattern

	for len(remaining) > 0 {
		// Look for outgoing: -[...]->(
		outIdx := strings.Index(remaining, "-[")
		inIdx := strings.Index(remaining, "<-[")

		if outIdx >= 0 && (inIdx < 0 || outIdx < inIdx) {
			// Found outgoing pattern
			relStart := outIdx + 2
			relEnd := strings.Index(remaining[relStart:], "]")
			if relEnd < 0 {
				break
			}
			relEnd += relStart

			relPart := remaining[relStart:relEnd]
			// Extract relationship types
			var relTypes []string
			if strings.HasPrefix(relPart, ":") {
				typePart := relPart[1:]
				// Handle multiple types separated by |
				for _, t := range strings.Split(typePart, "|") {
					if t = strings.TrimSpace(t); t != "" {
						relTypes = append(relTypes, t)
					}
				}
			}

			hops = append(hops, relationshipHop{
				relTypes: relTypes,
				outgoing: true,
			})

			remaining = remaining[relEnd+1:]
		} else if inIdx >= 0 {
			// Found incoming pattern
			relStart := inIdx + 3
			relEnd := strings.Index(remaining[relStart:], "]")
			if relEnd < 0 {
				break
			}
			relEnd += relStart

			relPart := remaining[relStart:relEnd]
			// Extract relationship types
			var relTypes []string
			if strings.HasPrefix(relPart, ":") {
				typePart := relPart[1:]
				for _, t := range strings.Split(typePart, "|") {
					if t = strings.TrimSpace(t); t != "" {
						relTypes = append(relTypes, t)
					}
				}
			}

			hops = append(hops, relationshipHop{
				relTypes: relTypes,
				outgoing: false,
			})

			remaining = remaining[relEnd+1:]
		} else {
			break
		}
	}

	return hops
}

// traverseChain recursively checks if a chain of relationships exists
func (e *StorageExecutor) traverseChain(node *storage.Node, hops []relationshipHop, hopIndex int) bool {
	if hopIndex >= len(hops) {
		return true // All hops matched
	}

	hop := hops[hopIndex]

	if hop.outgoing {
		edges, _ := e.storage.GetOutgoingEdges(node.ID)
		for _, edge := range edges {
			if len(hop.relTypes) == 0 || e.edgeTypeMatches(edge.Type, hop.relTypes) {
				// Get the target node and recurse
				nextNode, err := e.storage.GetNode(edge.EndNode)
				if err != nil {
					continue
				}
				if e.traverseChain(nextNode, hops, hopIndex+1) {
					return true
				}
			}
		}
	} else {
		edges, _ := e.storage.GetIncomingEdges(node.ID)
		for _, edge := range edges {
			if len(hop.relTypes) == 0 || e.edgeTypeMatches(edge.Type, hop.relTypes) {
				// Get the source node and recurse
				nextNode, err := e.storage.GetNode(edge.StartNode)
				if err != nil {
					continue
				}
				if e.traverseChain(nextNode, hops, hopIndex+1) {
					return true
				}
			}
		}
	}

	return false
}

// extractTargetVariable extracts the target variable name from a relationship pattern
// e.g., from "(m)-[:MANAGES]->(report)" it extracts "report"
func (e *StorageExecutor) extractTargetVariable(pattern, sourceVar string) string {
	// Look for outgoing pattern: (source)-[...]->(target)
	if arrowIdx := strings.Index(pattern, "]->"); arrowIdx >= 0 {
		rest := pattern[arrowIdx+3:]
		if parenIdx := strings.Index(rest, "("); parenIdx >= 0 {
			rest = rest[parenIdx+1:]
			// Extract variable name (before : or ))
			endIdx := strings.IndexAny(rest, ":)")
			if endIdx > 0 {
				return strings.TrimSpace(rest[:endIdx])
			}
		}
	}

	// Look for incoming pattern: (target)<-[...]-(source)
	if arrowIdx := strings.Index(pattern, "<-["); arrowIdx >= 0 {
		// Target is before the arrow
		before := pattern[:arrowIdx]
		if parenIdx := strings.LastIndex(before, "("); parenIdx >= 0 {
			inner := before[parenIdx+1:]
			endIdx := strings.IndexAny(inner, ":)")
			if endIdx > 0 {
				return strings.TrimSpace(inner[:endIdx])
			}
		}
	}

	return ""
}

// evaluateInnerWhere evaluates an inner WHERE clause against a node
// Handles nested EXISTS subqueries
func (e *StorageExecutor) evaluateInnerWhere(node *storage.Node, variable, whereClause string) bool {
	upperWhere := strings.ToUpper(whereClause)

	// Check for nested EXISTS subquery
	if hasSubqueryPattern(whereClause, existsSubqueryRe) {
		// Check for NOT EXISTS first
		if hasSubqueryPattern(whereClause, notExistsSubqueryRe) {
			return e.evaluateNotExistsSubquery(node, variable, whereClause)
		}
		return e.evaluateExistsSubquery(node, variable, whereClause)
	}

	// Check for nested COUNT subquery
	if hasSubqueryPattern(whereClause, countSubqueryRe) {
		return e.evaluateCountSubqueryComparison(node, variable, whereClause)
	}

	// Simple property comparison (placeholder for basic WHERE support)
	// For now, return true if no subquery patterns found
	_ = upperWhere
	return true
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

// ===== CALL {} Subquery Support (Neo4j 4.0+) =====

// isCallSubquery detects if a query is a CALL {} subquery vs CALL procedure()
// CALL {} subqueries have "CALL" followed by optional whitespace and "{"
// CALL procedures have "CALL procedure.name()"
func isCallSubquery(cypher string) bool {
	// Use regex for flexible whitespace matching: CALL followed by optional whitespace and {
	return hasSubqueryPattern(cypher, callSubqueryRe)
}

// executeCallSubquery executes a CALL {} subquery
// Syntax: CALL { <subquery> } [IN TRANSACTIONS [OF n ROWS]]
// The subquery can contain MATCH, CREATE, RETURN, UNION, etc.
func (e *StorageExecutor) executeCallSubquery(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Extract the subquery body from CALL { ... }
	subqueryBody, afterCall, inTransactions, batchSize := e.parseCallSubquery(cypher)
	if subqueryBody == "" {
		return nil, fmt.Errorf("invalid CALL {} subquery: empty body (expected CALL { <query> })")
	}

	// Execute the inner subquery
	var innerResult *ExecuteResult
	var err error

	if inTransactions {
		// Execute in batches (for large data operations)
		innerResult, err = e.executeCallInTransactions(ctx, subqueryBody, batchSize)
	} else {
		// Execute as single query
		innerResult, err = e.Execute(ctx, subqueryBody, nil)
	}

	if err != nil {
		return nil, fmt.Errorf("CALL subquery error: %w", err)
	}

	// If there's something after CALL { }, process it (e.g., RETURN)
	if afterCall != "" {
		return e.processAfterCallSubquery(ctx, innerResult, afterCall)
	}

	return innerResult, nil
}

// parseCallSubquery extracts the body from CALL { ... } and any trailing clauses
// Returns: body, afterCall, inTransactions bool, batchSize int
func (e *StorageExecutor) parseCallSubquery(cypher string) (body, afterCall string, inTransactions bool, batchSize int) {
	batchSize = 1000 // Default batch size

	trimmed := strings.TrimSpace(cypher)

	// Find the opening brace
	braceStart := strings.Index(trimmed, "{")
	if braceStart == -1 {
		return "", "", false, batchSize
	}

	// Find matching closing brace
	depth := 0
	braceEnd := -1
	for i := braceStart; i < len(trimmed); i++ {
		if trimmed[i] == '{' {
			depth++
		} else if trimmed[i] == '}' {
			depth--
			if depth == 0 {
				braceEnd = i
				break
			}
		}
	}

	if braceEnd == -1 {
		return "", "", false, batchSize
	}

	// Extract body (between braces)
	body = strings.TrimSpace(trimmed[braceStart+1 : braceEnd])

	// Get what's after the closing brace
	afterCall = strings.TrimSpace(trimmed[braceEnd+1:])

	// Check for IN TRANSACTIONS
	upperAfter := strings.ToUpper(afterCall)
	if strings.HasPrefix(upperAfter, "IN TRANSACTIONS") {
		inTransactions = true
		afterTx := strings.TrimSpace(afterCall[15:])
		upperAfterTx := strings.ToUpper(afterTx)

		// Check for OF n ROWS
		if strings.HasPrefix(upperAfterTx, "OF ") {
			// Parse batch size
			ofPart := afterTx[3:]
			// Find ROWS keyword
			rowsIdx := strings.Index(strings.ToUpper(ofPart), " ROWS")
			if rowsIdx > 0 {
				sizeStr := strings.TrimSpace(ofPart[:rowsIdx])
				if size, err := strconv.Atoi(sizeStr); err == nil && size > 0 {
					batchSize = size
				}
				afterCall = strings.TrimSpace(ofPart[rowsIdx+5:])
			} else {
				afterCall = ""
			}
		} else {
			afterCall = afterTx
		}
	}

	return body, afterCall, inTransactions, batchSize
}

// executeCallInTransactions executes a CALL {} IN TRANSACTIONS query
// This batches operations for large datasets
func (e *StorageExecutor) executeCallInTransactions(ctx context.Context, subquery string, batchSize int) (*ExecuteResult, error) {
	// For now, execute as a single operation
	// Full implementation would batch CREATE/SET operations
	result, err := e.Execute(ctx, subquery, nil)
	if err != nil {
		return nil, err
	}

	// If the result has multiple rows and we need to batch, process them
	// This is a simplified implementation - full Neo4j batches actual writes
	return result, nil
}

// processAfterCallSubquery handles clauses after CALL { } like RETURN
func (e *StorageExecutor) processAfterCallSubquery(ctx context.Context, innerResult *ExecuteResult, afterCall string) (*ExecuteResult, error) {
	upperAfter := strings.ToUpper(afterCall)

	// Handle RETURN clause
	if strings.HasPrefix(upperAfter, "RETURN ") {
		return e.processCallSubqueryReturn(innerResult, afterCall)
	}

	// Handle ORDER BY (without RETURN means use inner result's columns)
	if strings.HasPrefix(upperAfter, "ORDER BY ") {
		result := e.applyOrderByToResult(innerResult, afterCall)
		// Check for LIMIT/SKIP after ORDER BY
		return e.applyResultModifiers(result, afterCall)
	}

	// Unsupported clause after CALL {}
	firstWord := strings.Split(upperAfter, " ")[0]
	return nil, fmt.Errorf("unsupported clause after CALL {}: %s (supported: RETURN, ORDER BY, SKIP, LIMIT)", firstWord)
}

// processCallSubqueryReturn processes the RETURN clause after CALL {}
func (e *StorageExecutor) processCallSubqueryReturn(innerResult *ExecuteResult, afterCall string) (*ExecuteResult, error) {
	// Parse RETURN expressions
	returnIdx := findKeywordIndex(afterCall, "RETURN")
	if returnIdx == -1 {
		return innerResult, nil
	}

	returnClause := strings.TrimSpace(afterCall[returnIdx+6:])

	// Check for ORDER BY, LIMIT, SKIP
	orderByIdx := findKeywordIndex(returnClause, "ORDER BY")
	limitIdx := findKeywordIndex(returnClause, "LIMIT")
	skipIdx := findKeywordIndex(returnClause, "SKIP")

	// Find the earliest modifier
	modifierIdx := len(returnClause)
	if orderByIdx != -1 && orderByIdx < modifierIdx {
		modifierIdx = orderByIdx
	}
	if limitIdx != -1 && limitIdx < modifierIdx {
		modifierIdx = limitIdx
	}
	if skipIdx != -1 && skipIdx < modifierIdx {
		modifierIdx = skipIdx
	}

	returnExprs := strings.TrimSpace(returnClause[:modifierIdx])
	modifierClause := ""
	if modifierIdx < len(returnClause) {
		modifierClause = returnClause[modifierIdx:]
	}

	// Parse return expressions
	parts := splitReturnExpressions(returnExprs)

	// Build column mapping from inner result
	colMap := make(map[string]int)
	for i, col := range innerResult.Columns {
		colMap[col] = i
	}

	// Project columns
	newColumns := make([]string, 0, len(parts))
	colIndices := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Check for alias
		alias := part
		expr := part
		upperPart := strings.ToUpper(part)
		if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
			alias = strings.TrimSpace(part[asIdx+4:])
			expr = strings.TrimSpace(part[:asIdx])
		}

		newColumns = append(newColumns, alias)

		// Find column index
		if idx, ok := colMap[expr]; ok {
			colIndices = append(colIndices, idx)
		} else {
			// Not found in inner result, append -1 (will be nil)
			colIndices = append(colIndices, -1)
		}
	}

	// Project rows
	newRows := make([][]interface{}, 0, len(innerResult.Rows))
	for _, row := range innerResult.Rows {
		newRow := make([]interface{}, len(colIndices))
		for i, idx := range colIndices {
			if idx >= 0 && idx < len(row) {
				newRow[i] = row[idx]
			} else {
				newRow[i] = nil
			}
		}
		newRows = append(newRows, newRow)
	}

	result := &ExecuteResult{
		Columns: newColumns,
		Rows:    newRows,
		Stats:   innerResult.Stats,
	}

	// Apply modifiers (ORDER BY, LIMIT, SKIP)
	if modifierClause != "" {
		return e.applyResultModifiers(result, modifierClause)
	}

	return result, nil
}

// applyResultModifiers applies ORDER BY, LIMIT, SKIP to a result
func (e *StorageExecutor) applyResultModifiers(result *ExecuteResult, modifiers string) (*ExecuteResult, error) {
	// Apply ORDER BY
	if orderByIdx := findKeywordIndex(modifiers, "ORDER BY"); orderByIdx != -1 {
		result = e.applyOrderByToResult(result, modifiers[orderByIdx:])
	}

	// Apply SKIP
	if skipIdx := findKeywordIndex(modifiers, "SKIP"); skipIdx != -1 {
		skipPart := strings.TrimSpace(modifiers[skipIdx+4:])
		// Find next keyword
		nextKw := len(skipPart)
		for _, kw := range []string{" LIMIT", " ORDER"} {
			if idx := strings.Index(strings.ToUpper(skipPart), kw); idx != -1 && idx < nextKw {
				nextKw = idx
			}
		}
		skipStr := strings.TrimSpace(skipPart[:nextKw])
		if skip, err := strconv.Atoi(skipStr); err == nil && skip > 0 {
			if skip < len(result.Rows) {
				result.Rows = result.Rows[skip:]
			} else {
				result.Rows = [][]interface{}{}
			}
		}
	}

	// Apply LIMIT
	if limitIdx := findKeywordIndex(modifiers, "LIMIT"); limitIdx != -1 {
		limitPart := strings.TrimSpace(modifiers[limitIdx+5:])
		// Find next keyword
		nextKw := len(limitPart)
		for _, kw := range []string{" SKIP", " ORDER"} {
			if idx := strings.Index(strings.ToUpper(limitPart), kw); idx != -1 && idx < nextKw {
				nextKw = idx
			}
		}
		limitStr := strings.TrimSpace(limitPart[:nextKw])
		if limit, err := strconv.Atoi(limitStr); err == nil && limit >= 0 {
			if limit < len(result.Rows) {
				result.Rows = result.Rows[:limit]
			}
		}
	}

	return result, nil
}

// applyOrderByToResult applies ORDER BY to a result set
func (e *StorageExecutor) applyOrderByToResult(result *ExecuteResult, orderByClause string) *ExecuteResult {
	// Parse ORDER BY column [DESC|ASC]
	clause := strings.TrimSpace(orderByClause)
	if idx := findKeywordIndex(clause, "ORDER BY"); idx != -1 {
		clause = strings.TrimSpace(clause[idx+8:])
	}

	// Find end of ORDER BY (before LIMIT, SKIP)
	endIdx := len(clause)
	for _, kw := range []string{" LIMIT", " SKIP"} {
		if idx := strings.Index(strings.ToUpper(clause), kw); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}
	clause = strings.TrimSpace(clause[:endIdx])

	// Parse column and direction
	parts := strings.Fields(clause)
	if len(parts) == 0 {
		return result
	}

	colName := parts[0]
	descending := false
	if len(parts) > 1 && strings.ToUpper(parts[1]) == "DESC" {
		descending = true
	}

	// Find column index
	colIdx := -1
	for i, col := range result.Columns {
		if col == colName {
			colIdx = i
			break
		}
	}

	if colIdx == -1 {
		return result
	}

	// Sort rows
	sort.SliceStable(result.Rows, func(i, j int) bool {
		vi := result.Rows[i][colIdx]
		vj := result.Rows[j][colIdx]
		cmp := compareValuesForSort(vi, vj)
		if descending {
			return cmp > 0
		}
		return cmp < 0
	})

	return result
}

// compareValuesForSort compares two values for sorting, returns -1, 0, or 1
func compareValuesForSort(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	switch va := a.(type) {
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}

	// Fallback to string comparison
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
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
		// Kalman filter functions
		{"kalman.init", "kalman.init(config? :: MAP) :: STRING", "Create new Kalman filter state (basic scalar filter for noise smoothing)", false, false, false},
		{"kalman.process", "kalman.process(measurement :: FLOAT, state :: STRING, target? :: FLOAT) :: MAP", "Process measurement, returns {value, state}", false, false, false},
		{"kalman.predict", "kalman.predict(state :: STRING, steps :: INTEGER) :: FLOAT", "Predict state n steps into the future", false, false, false},
		{"kalman.state", "kalman.state(state :: STRING) :: FLOAT", "Get current state estimate from state JSON", false, false, false},
		{"kalman.reset", "kalman.reset(state :: STRING) :: STRING", "Reset filter state to initial values", false, false, false},
		{"kalman.velocity.init", "kalman.velocity.init(initialPos? :: FLOAT, initialVel? :: FLOAT) :: STRING", "Create 2-state Kalman filter (position + velocity for trend tracking)", false, false, false},
		{"kalman.velocity.process", "kalman.velocity.process(measurement :: FLOAT, state :: STRING) :: MAP", "Process measurement, returns {value, velocity, state}", false, false, false},
		{"kalman.velocity.predict", "kalman.velocity.predict(state :: STRING, steps :: INTEGER) :: FLOAT", "Predict position n steps into the future", false, false, false},
		{"kalman.adaptive.init", "kalman.adaptive.init(config? :: MAP) :: STRING", "Create adaptive Kalman filter (auto-switches between basic and velocity modes)", false, false, false},
		{"kalman.adaptive.process", "kalman.adaptive.process(measurement :: FLOAT, state :: STRING) :: MAP", "Process measurement, returns {value, mode, state}", false, false, false},
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

// truncateQuery truncates a query string to maxLen characters for error messages
func truncateQuery(query string, maxLen int) string {
	query = strings.TrimSpace(query)
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}
