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
//		// process row (e.g. emit "Row: %v" via the configured logger)
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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
	"go.opentelemetry.io/otel/attribute"
)

// Subquery detection tags. Routing uses scanner helpers below rather than regex.
const (
	existsSubqueryRe    = "EXISTS"
	notExistsSubqueryRe = "NOT EXISTS"
	countSubqueryRe     = "COUNT"
	callSubqueryRe      = "CALL"
	collectSubqueryRe   = "COLLECT"
)

// hasSubqueryPattern checks if the query contains a subquery pattern (keyword + optional whitespace + brace)
func hasSubqueryPattern(query string, pattern string) bool {
	switch pattern {
	case existsSubqueryRe:
		return hasKeywordFollowedByBrace(query, "EXISTS")
	case notExistsSubqueryRe:
		return hasNotExistsFollowedByBrace(query)
	case countSubqueryRe:
		return hasKeywordFollowedByBrace(query, "COUNT")
	case callSubqueryRe:
		return hasKeywordFollowedByBrace(query, "CALL")
	case collectSubqueryRe:
		return hasKeywordFollowedByBrace(query, "COLLECT")
	}
	return false
}

func hasNotExistsFollowedByBrace(query string) bool {
	for i := 0; i < len(query); i++ {
		if !matchKeywordAt(query, i, "NOT") {
			continue
		}
		j := skipSpaces(query, i+3)
		if !matchKeywordAt(query, j, "EXISTS") {
			continue
		}
		k := skipSpaces(query, j+6)
		if k < len(query) && query[k] == '{' {
			return true
		}
	}
	return false
}

func hasKeywordFollowedByBrace(query, keyword string) bool {
	kwLen := len(keyword)
	for i := 0; i < len(query); i++ {
		if !matchKeywordAt(query, i, keyword) {
			continue
		}
		j := skipSpaces(query, i+kwLen)
		if j < len(query) && query[j] == '{' {
			return true
		}
	}
	return false
}

func skipSpaces(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func matchKeywordAt(s string, i int, keyword string) bool {
	if i < 0 || i+len(keyword) > len(s) {
		return false
	}
	if i > 0 && isIdentCharByte(s[i-1]) {
		return false
	}
	if i+len(keyword) < len(s) && isIdentCharByte(s[i+len(keyword)]) {
		return false
	}
	return strings.EqualFold(s[i:i+len(keyword)], keyword)
}

func isIdentCharByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
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
// NodeMutatedCallback is called when a node is created or mutated via Cypher (CREATE, MERGE, SET, REMOVE, or procedures that update nodes).
// This allows external systems (like the embed queue) to be notified so embeddings can be (re)generated.
type NodeMutatedCallback func(nodeID string)

type StorageExecutor struct {
	parser    *Parser
	storage   storage.Engine
	txContext *TransactionContext // Active transaction context
	cache     *SmartQueryCache    // Query result cache with label-aware invalidation
	planCache *QueryPlanCache     // Parsed query plan cache
	// fabricPlanCache caches planned Fabric fragment trees (query + sessionDB).
	fabricPlanCache *fabric.PlanCache
	analyzer        *QueryAnalyzer // Query analysis with AST caching

	// Node lookup cache for MATCH patterns like (n:Label {prop: value})
	// Key: "Label:{prop:value,...}", Value: *storage.Node
	// This dramatically speeds up repeated MATCH lookups for the same pattern
	nodeLookupCache map[string]*storage.Node
	// cloneWithStorage shares nodeLookupCache, so clones must share this lock too.
	nodeLookupCacheMu *sync.RWMutex

	// deferFlush when true, writes are not auto-flushed (Bolt layer handles it)
	deferFlush bool

	// embedder for server-side query embedding (optional)
	// If set, vector search can accept string queries which are embedded automatically
	embedder QueryEmbedder

	// searchService optionally provides unified search semantics for Cypher procedures.
	// When set, db.index.vector.queryNodes delegates to search.Service.
	searchService *search.Service

	// inferenceManager optionally provides LLM inference for db.infer.
	inferenceManager InferenceManager

	// onNodeMutated is called when a node is created or mutated (CREATE, MERGE, SET, REMOVE).
	// This allows the embed queue to be notified so embeddings are (re)generated.
	onNodeMutated               NodeMutatedCallback
	inlineEmbeddingTextOptions  *embeddingutil.EmbedTextOptions
	inlineEmbeddingChunkSize    int
	inlineEmbeddingChunkOverlap int

	// defaultEmbeddingDimensions is the configured embedding dimensions for vector indexes
	// Used as default when CREATE VECTOR INDEX doesn't specify dimensions
	defaultEmbeddingDimensions int

	// dbManager is optional - when set, enables system commands (CREATE/DROP/SHOW DATABASE)
	// System commands require DatabaseManager to manage multiple databases
	// This is an interface to avoid import cycles with multidb package
	dbManager DatabaseManagerInterface

	// shellParams stores Neo4j shell-style parameters set via :param / :params.
	// These are session-scoped to the executor instance and merged with per-call params.
	shellParams   map[string]interface{}
	shellParamsMu sync.RWMutex

	// vectorRegistry maps Cypher vector index definitions to concrete vector spaces.
	vectorRegistry    *vectorspace.IndexRegistry
	vectorIndexSpaces map[string]vectorspace.VectorSpaceKey
	// fabricRecordBindings carries correlated APPLY input bindings for Fabric execution.
	// It is set only on per-query cloned executors.
	fabricRecordBindings map[string]interface{}

	decayMismatchLogged bool
	hotPathTraceState   *hotPathTraceState

	// vectorQueryEmbedCache caches server-side embeddings for db.index.vector.queryNodes/
	// queryRelationships string-input mode to avoid repeated embedding latency.
	// Key is canonicalized (case/whitespace normalized) query text.
	vectorQueryEmbedCache map[string][]float32
	// vectorQueryEmbedInflight de-duplicates concurrent embedding work per key.
	vectorQueryEmbedInflight map[string]*vectorEmbedInflight
	vectorQueryEmbedMu       sync.Mutex

	// unwindMergeChainPlanCache memoizes parsed plans for the generalized
	// UNWIND ... MERGE batch hot path keyed by mutation query text.
	unwindMergeChainPlanCache *unwindMergeChainPlanCache

	// log is the structured logger used for slow-query and operational log
	// emission. Threaded via SetLogger after construction (D-01 non-breaking
	// pattern — NewStorageExecutor signature unchanged). Nil-safe via the
	// internal logger() helper which lazily installs a discard fallback.
	log *slog.Logger

	// slowQueryThreshold gates the D-04c slow-query emission path. Zero or
	// negative values disable slow-query logging entirely. Set via
	// SetSlowQueryThreshold so the configured cfg.Logging.SlowQueryThreshold
	// flows in from the bootstrap site without breaking the ctor.
	slowQueryThreshold time.Duration

	// metrics is the Plan 04-03 CypherMetrics typed bag (MET-08). Injected
	// post-construction via SetCypherMetrics (D-01 non-breaking pattern,
	// mirrors SetLogger / SetSlowQueryThreshold). Nil-safe: the
	// observe* helpers no-op when metrics is nil so tests and alternate
	// constructors don't have to wire it.
	metrics *observability.CypherMetrics

	// database is the tenant identifier passed as the `database` label
	// on tenant-tagged Cypher families when D-08 tenantLabelsEnabled=true.
	// Empty string is acceptable when the bag was constructed with the
	// tenant flag off — Bind helpers drop the arg internally.
	database string
}

type unwindMergeChainPlanCache struct {
	mu    sync.RWMutex
	plans map[string]unwindMergeChainPlan
}

func (e *StorageExecutor) cloneWithStorage(override storage.Engine) *StorageExecutor {
	e.ensureNodeLookupCache()
	return &StorageExecutor{
		parser:                      e.parser,
		storage:                     override,
		txContext:                   e.txContext,
		cache:                       e.cache,
		planCache:                   e.planCache,
		fabricPlanCache:             e.fabricPlanCache,
		analyzer:                    e.analyzer,
		nodeLookupCache:             e.nodeLookupCache,
		nodeLookupCacheMu:           e.nodeLookupCacheLock(),
		deferFlush:                  e.deferFlush,
		embedder:                    e.embedder,
		searchService:               e.searchService,
		inferenceManager:            e.inferenceManager,
		onNodeMutated:               e.onNodeMutated,
		inlineEmbeddingTextOptions:  e.inlineEmbeddingTextOptions,
		inlineEmbeddingChunkSize:    e.inlineEmbeddingChunkSize,
		inlineEmbeddingChunkOverlap: e.inlineEmbeddingChunkOverlap,
		defaultEmbeddingDimensions:  e.defaultEmbeddingDimensions,
		dbManager:                   e.dbManager,
		shellParams:                 e.shellParams,
		vectorRegistry:              e.vectorRegistry,
		vectorIndexSpaces:           e.vectorIndexSpaces,
		fabricRecordBindings:        e.fabricRecordBindings,
		hotPathTraceState:           e.hotPathTraceState,
		vectorQueryEmbedCache:       e.vectorQueryEmbedCache,
		vectorQueryEmbedInflight:    e.vectorQueryEmbedInflight,
		unwindMergeChainPlanCache:   e.unwindMergeChainPlanCache,
		// Plan 04-03: propagate the metrics bag + database label through
		// per-query / per-storage clones so observation chokepoints in
		// Execute() see the same bag regardless of clone depth.
		metrics:  e.metrics,
		database: e.database,
	}
}

func (e *StorageExecutor) nodeLookupCacheLock() *sync.RWMutex {
	if e.nodeLookupCacheMu == nil {
		e.nodeLookupCacheMu = &sync.RWMutex{}
	}
	return e.nodeLookupCacheMu
}

func (e *StorageExecutor) ensureNodeLookupCache() {
	cacheMu := e.nodeLookupCacheLock()
	cacheMu.Lock()
	defer cacheMu.Unlock()
	if e.nodeLookupCache == nil {
		e.nodeLookupCache = make(map[string]*storage.Node, 1000)
	}
}

type vectorEmbedInflight struct {
	done chan struct{}
	vec  []float32
	err  error
}

type hotPathTraceState struct {
	mu    sync.RWMutex
	trace HotPathTrace
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
	// GetStorageForUse returns the storage engine for a database, supporting
	// composite databases. authToken is forwarded for remote constituents.
	GetStorageForUse(name string, authToken string) (interface{}, error)
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
	ChunkText(text string, maxTokens, overlap int) ([]string, error)
}

// InferenceManager is the minimal LLM contract used by Cypher db.infer.
// It mirrors Heimdall manager methods to keep adapters thin.
type InferenceManager interface {
	Generate(ctx context.Context, prompt string, params heimdall.GenerateParams) (string, error)
	Chat(ctx context.Context, req heimdall.ChatRequest) (*heimdall.ChatResponse, error)
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
	exec := &StorageExecutor{
		parser:                      NewParser(),
		storage:                     store,
		cache:                       NewSmartQueryCache(1000), // Query result cache with label-aware invalidation
		planCache:                   NewQueryPlanCache(500),   // Cache 500 parsed query plans
		fabricPlanCache:             fabric.NewPlanCache(500), // Cache 500 Fabric fragment plans
		analyzer:                    NewQueryAnalyzer(1000),   // Cache 1000 parsed query ASTs
		nodeLookupCache:             make(map[string]*storage.Node, 1000),
		nodeLookupCacheMu:           &sync.RWMutex{},
		shellParams:                 make(map[string]interface{}),
		searchService:               nil, // Lazy initialization - will be set via SetSearchService() to reuse DB's cached service
		vectorRegistry:              vectorspace.NewIndexRegistry(),
		vectorIndexSpaces:           make(map[string]vectorspace.VectorSpaceKey),
		hotPathTraceState:           &hotPathTraceState{},
		vectorQueryEmbedCache:       make(map[string][]float32, 512),
		vectorQueryEmbedInflight:    make(map[string]*vectorEmbedInflight, 64),
		unwindMergeChainPlanCache:   &unwindMergeChainPlanCache{plans: make(map[string]unwindMergeChainPlan, 128)},
		inlineEmbeddingTextOptions:  embeddingutil.EmbedTextOptionsFromConfig(config.LoadFromEnv()),
		inlineEmbeddingChunkSize:    maxInt(config.LoadFromEnv().EmbeddingWorker.ChunkSize, 1),
		inlineEmbeddingChunkOverlap: maxInt(config.LoadFromEnv().EmbeddingWorker.ChunkOverlap, 0),
	}
	ensureBuiltInProceduresRegistered()
	_ = exec.loadPersistedProcedures()
	return exec
}

// ClearQueryCaches clears executor-local caches that can retain stale read results.
func (e *StorageExecutor) ClearQueryCaches() {
	if e.cache != nil {
		e.cache.Invalidate()
	}
	if e.planCache != nil {
		e.planCache.Clear()
	}
	if e.analyzer != nil {
		e.analyzer.ClearCache()
	}
	cacheMu := e.nodeLookupCacheLock()
	cacheMu.Lock()
	e.nodeLookupCache = make(map[string]*storage.Node, 1000)
	cacheMu.Unlock()
}

// InvalidateEntityCaches evicts targeted cache entries affected by a specific entity state change.
func (e *StorageExecutor) InvalidateEntityCaches(entityID string, tokens []string) {
	if e.cache != nil && len(tokens) > 0 {
		e.cache.InvalidateLabels(tokens)
	}
	e.invalidateNodeLookupCacheForEntityID(storage.NodeID(entityID))
}

func (e *StorageExecutor) invalidateNodeLookupCacheForEntityID(entityID storage.NodeID) {
	if entityID == "" {
		return
	}
	cacheMu := e.nodeLookupCacheLock()
	cacheMu.Lock()
	for key, node := range e.nodeLookupCache {
		if node != nil && node.ID == entityID {
			delete(e.nodeLookupCache, key)
		}
	}
	cacheMu.Unlock()
}

// SetLogger installs the structured slog.Logger used for slow-query and
// operational records. D-01 non-breaking: NewStorageExecutor's signature is
// unchanged; callers (cmd/nornicdb/main.go) call SetLogger after construction
// so the *slog.Logger from observability.NewLogger flows through.
//
// Discard-fallback: passing nil installs a slog.Logger backed by io.Discard
// so subsequent log emissions cannot panic. The "component" attribute is
// pre-bound here (not per-call) to honor the RESEARCH "Per-call .With()
// allocation" anti-pattern.
func (e *StorageExecutor) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	e.log = logger.With("component", "cypher")
}

// SetSlowQueryThreshold configures the D-04c slow-query emission gate.
// Zero or negative durations disable slow-query logging entirely. Threaded
// from cfg.Logging.SlowQueryThreshold at the bootstrap site.
func (e *StorageExecutor) SetSlowQueryThreshold(d time.Duration) {
	e.slowQueryThreshold = d
}

// Logger returns the bound *slog.Logger. Exposed so transient executors
// (e.g., per-transaction sessions cloned from a base) can inherit the
// configured logger without re-threading from main.
func (e *StorageExecutor) Logger() *slog.Logger { return e.logger() }

// SlowQueryThreshold returns the configured slow-query emission gate.
// Exposed so cloned executors inherit the threshold from their base.
func (e *StorageExecutor) SlowQueryThreshold() time.Duration { return e.slowQueryThreshold }

// SetCypherMetrics installs the Plan 04-03 CypherMetrics typed bag (MET-08)
// and the database label value passed on tenant-tagged families when D-08
// tenantLabelsEnabled=true. Mirrors the SetLogger / SetSlowQueryThreshold
// non-breaking pattern (D-01 non-breaking ctor).
//
// Also propagates the bag into the executor's owned planCache so the
// planner_cache_{hits,misses,size} families fire from QueryPlanCache.Get/Put
// without callers having to reach into private fields.
//
// Nil-safe: passing m=nil leaves observation as a no-op so tests and
// alternate constructors that don't wire metrics don't have to. The three
// observation chokepoints in Execute() guard on m == nil.
//
// Cloned executors inherit metrics + database via cloneWithStorage so the
// bag flows through per-query / per-tx scoped clones.
func (e *StorageExecutor) SetCypherMetrics(m *observability.CypherMetrics, database string) {
	e.metrics = m
	e.database = database
	// D-12a planner cache wiring: propagate into the owned planCache so
	// the cypher subsystem's planner_cache_{hits,misses,size} families
	// observe automatically.
	if e.planCache != nil {
		e.planCache.SetCypherMetrics(m)
	}
}

// SetCacheMetrics installs the Plan 04-01 cross-cutting CacheMetrics bag
// for D-12a query-result cache observation. Routes the bag into the owned
// SmartQueryCache so cache_hits_total{cache="query_result"} +
// cache_misses_total + cache_evictions_total emit on every Get/Put/Evict.
//
// Nil-safe; mirrors SetCypherMetrics shape.
func (e *StorageExecutor) SetCacheMetrics(m *observability.CacheMetrics) {
	if e.cache != nil {
		e.cache.SetCacheMetrics(m)
	}
}

// CypherMetrics returns the injected metrics bag (or nil if unset). Exposed
// so cloned executors can re-inject when constructed via newTxScopedExecutor
// outside the cloneWithStorage pathway.
func (e *StorageExecutor) CypherMetrics() *observability.CypherMetrics { return e.metrics }

// Database returns the configured database label value used for tenant-tagged
// Cypher metric observations (D-08).
func (e *StorageExecutor) Database() string { return e.database }

// observeQuery is the single Cypher-side observation helper. Called at the
// three RISK-1 corrected chokepoints in Execute():
//
//	Site 1 — admin dispatch       (op_type="admin",       observeDuration=true)
//	Site 2 — parse-error          (op_type="parse_error", observeDuration=false)
//	Site 3 — normal-path-after-Analyze (op_type from classifyOpType, observeDuration=true)
//
// Nil-safe: no-ops when e.metrics is nil. Hot-path-cheap: per-call Bind via
// the bag's BindQueryDuration helper (one WithLabelValues lookup); future
// optimization can hoist Bind into struct fields cached at SetCypherMetrics
// time per MET-25 — see RowsReturned for the precedent. The current shape
// keeps SetCypherMetrics simple while still emitting via the typed bag.
func (e *StorageExecutor) observeQuery(opType string, observeDuration bool, start time.Time) {
	if e.metrics == nil {
		return
	}
	e.metrics.BindQueries(opType, e.database).Inc()
	if observeDuration {
		e.metrics.BindQueryDuration(opType, e.database).Observe(context.Background(), time.Since(start).Seconds())
	}
}

// observeTransactionConflict is the D-16 chokepoint helper: storage detects
// (returns storage.ErrConflict from the engine), Cypher counts (here, in the
// transaction-wrapper site that surfaces ErrConflict to the caller). Storage
// layer never imports observability — preserves AGENTS.md §8 separation.
//
// Nil-safe: no-ops when e.metrics is nil OR err is not ErrConflict OR err is
// nil. Defensive: errors.Is check rather than equality so wrapped errors
// (fmt.Errorf("...: %w", storage.ErrConflict)) still classify correctly.
func (e *StorageExecutor) observeTransactionConflict(err error) {
	if e.metrics == nil || err == nil {
		return
	}
	if !errors.Is(err, storage.ErrConflict) {
		return
	}
	e.metrics.BindTransactionConflicts(e.database).Inc()
}

// observeTransactionBegin increments the active_transactions gauge. Pair
// with observeTransactionEnd at every Commit/Rollback site so the gauge
// balances to 0 across normal, abort, and panic paths.
func (e *StorageExecutor) observeTransactionBegin() {
	if e.metrics == nil {
		return
	}
	e.metrics.ActiveTransactions.Inc()
}

// observeTransactionEnd decrements the active_transactions gauge. See
// observeTransactionBegin.
func (e *StorageExecutor) observeTransactionEnd() {
	if e.metrics == nil {
		return
	}
	e.metrics.ActiveTransactions.Dec()
}

// observeSlowQueryIfThresholded increments the slow_queries counter when
// duration meets the configured slowQueryThreshold (matches the Phase 2
// D-04c emitSlowQueryLog gate semantics: zero or negative threshold
// disables emission entirely).
func (e *StorageExecutor) observeSlowQueryIfThresholded(duration time.Duration) {
	if e.metrics == nil {
		return
	}
	if e.slowQueryThreshold <= 0 || duration < e.slowQueryThreshold {
		return
	}
	e.metrics.BindSlowQueries(e.database).Inc()
}

// logger returns the bound logger, lazily installing a discard fallback if
// SetLogger was never called. Internal — every emission site must read the
// logger via this helper, never via the stdlib package-level default
// (LOG-09 forbids that path).
func (e *StorageExecutor) logger() *slog.Logger {
	if e.log == nil {
		e.log = slog.New(slog.NewTextHandler(io.Discard, nil)).With("component", "cypher")
	}
	return e.log
}

// emitSlowQueryLog writes a single WARN record matching the LOG-07 schema
// when duration meets the configured threshold. RedactLiterals runs BEFORE
// truncation per D-04c so partial literals never leak via the truncation seam.
//
// Schema (D-04c):
//
//	level=WARN
//	msg="slow query"
//	event="slow_query"
//	plan_hash=<16-char hex from PlanHash; "0000000000000000" when plan is nil>
//	cypher.duration_ms=<int64 millisecond delta>
//	query=<RedactLiterals(query) truncated to 500 chars>
//
// Performance: PlanHash + RedactLiterals only fire when this method is called,
// i.e., only when the executor's measured duration exceeded the configured
// threshold. The hot path (Execute fast return) never enters this method.
func (e *StorageExecutor) emitSlowQueryLog(query string, plan *ExecutionPlan, duration time.Duration) {
	if e.slowQueryThreshold <= 0 || duration < e.slowQueryThreshold {
		return
	}
	redacted := RedactLiterals(query)
	if len(redacted) > 500 {
		redacted = redacted[:500]
	}
	e.logger().Warn("slow query",
		"event", "slow_query",
		"plan_hash", PlanHash(plan),
		"cypher.duration_ms", duration.Milliseconds(),
		"query", redacted,
	)
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
	e.vectorQueryEmbedMu.Lock()
	e.vectorQueryEmbedCache = make(map[string][]float32, 512)
	e.vectorQueryEmbedInflight = make(map[string]*vectorEmbedInflight, 64)
	e.vectorQueryEmbedMu.Unlock()
}

// SetSearchService sets the unified search service used by Cypher procedures.
// When set, db.index.vector.queryNodes will delegate to search.Service.
func (e *StorageExecutor) SetSearchService(svc *search.Service) {
	e.searchService = svc
}

// SetInferenceManager sets the inference manager used by db.infer.
func (e *StorageExecutor) SetInferenceManager(mgr InferenceManager) {
	e.inferenceManager = mgr
}

// GetInferenceManager returns the configured inference manager.
func (e *StorageExecutor) GetInferenceManager() InferenceManager {
	return e.inferenceManager
}

// SetVectorRegistry allows wiring a shared index registry (e.g., per database).
// Defaults to an internal registry when not set.
func (e *StorageExecutor) SetVectorRegistry(reg *vectorspace.IndexRegistry) {
	if reg == nil {
		reg = vectorspace.NewIndexRegistry()
	}
	e.vectorRegistry = reg
}

// GetVectorRegistry exposes the current registry (for tests and adapters).
func (e *StorageExecutor) GetVectorRegistry() *vectorspace.IndexRegistry {
	return e.vectorRegistry
}

// GetEmbedder returns the query embedder if set.
// This allows copying the embedder to namespaced executors for GraphQL.
func (e *StorageExecutor) GetEmbedder() QueryEmbedder {
	return e.embedder
}

// SetNodeMutatedCallback sets a callback that is invoked when nodes are created
// or mutated (CREATE, MERGE, SET, REMOVE, or procedures that update nodes).
// This allows the embed queue to be notified so embeddings can be (re)generated.
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetNodeMutatedCallback(func(nodeID string) {
//	    embedQueue.Enqueue(nodeID)
//	})
func (e *StorageExecutor) SetNodeMutatedCallback(cb NodeMutatedCallback) {
	e.onNodeMutated = cb
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

// notifyNodeMutated calls the onNodeMutated callback if set.
// Call after any node creation or mutation (CREATE, MERGE, SET, REMOVE) so the embed queue can re-process.
func (e *StorageExecutor) notifyNodeMutated(nodeID string) {
	if e.onNodeMutated != nil {
		e.onNodeMutated(nodeID)
	}
}

// removeNodeFromSearch removes a node from the search service (vector/fulltext indexes).
// Call after successfully deleting a node via Cypher so embeddings are not left orphaned.
// nodeID may be prefixed (e.g. "nornic:xyz") or local ("xyz"); the search service expects local ID.
func (e *StorageExecutor) removeNodeFromSearch(nodeID string) {
	if e.searchService == nil || nodeID == "" {
		return
	}
	localID := nodeID
	if _, unprefixed, ok := storage.ParseDatabasePrefix(nodeID); ok {
		localID = unprefixed
	}
	_ = e.searchService.RemoveNode(storage.NodeID(localID))
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
//	// emit "Columns: %v" via the configured logger
//	for _, row := range result.Rows {
//		// process row (e.g. emit "Row: %v" via the configured logger)
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
func (e *StorageExecutor) Execute(ctx context.Context, cypher string, params map[string]interface{}) (result *ExecuteResult, retErr error) {
	e.resetHotPathTrace()

	// TRC-15: top-level cypher execute span. Started before timing so the
	// span duration matches the metric observation window exactly. The span
	// is ended in the same defer that emits the slow-query log.
	ctx, execSpan := startExecuteSpan(ctx, "", cypher)

	// TRC-17: propagate span context to the storage layer so storage spans
	// nest as children of the cypher execute span.
	if te, ok := e.storage.(*storage.TracedEngine); ok {
		te.SetContext(ctx)
	}

	// D-04c slow-query log timing. Captured at the top so the threshold check
	// covers every Execute return path (early-out, fabric, normal). Pre-bind
	// the original query text — by the time the deferred emission fires,
	// `cypher` has been normalized; we want to log what the client submitted.
	slowStart := time.Now()
	originalCypher := cypher
	defer func() {
		dur := time.Since(slowStart)
		// Plan is unavailable for non-EXPLAIN/PROFILE queries; pass nil and
		// rely on PlanHash's zero-placeholder behavior. Phase 6 (TRC-04) will
		// thread the planned tree here once cypher EXPLAIN refactoring is in.
		e.emitSlowQueryLog(originalCypher, nil, dur)
		// Plan 04-03 / MET-08 slow_queries_total: increments only when
		// duration meets the configured threshold (matches the D-04c
		// emitSlowQueryLog gate semantics — single threshold, single
		// emission point per Execute return).
		e.observeSlowQueryIfThresholded(dur)
		recordSpanError(execSpan, retErr)
		execSpan.End()
	}()
	// Normalize query: trim BOM (some clients send it) then whitespace
	cypher = trimBOM(cypher)
	cypher = normalizeCypherSyntaxConfusables(cypher)
	cypher = strings.TrimSpace(cypher)
	cypher = trimTrailingStatementDelimiters(cypher)
	if cypher == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Handle Neo4j shell/browser commands like :USE and :param before validation.
	processedQuery, processedCtx, shellResult, err := e.preprocessShellCommands(ctx, cypher, params)
	if err != nil {
		return nil, err
	}
	ctx = processedCtx
	cypher = processedQuery
	if cypher == "" {
		return shellResult, nil
	}

	// Route multi-graph CALL { USE ... } queries through the Fabric planner/executor
	// so subquery decomposition and cross-graph routing use a single deterministic path.
	if e.shouldUseFabricPlanner(cypher) {
		mergedParams := e.mergeShellParams(params)
		ctx = context.WithValue(ctx, paramsKey, mergedParams)
		info := e.analyzer.Analyze(cypher)
		// Plan 04-03 Site 3 (fabric branch): isFabric=true → op_type="fabric"
		// per RISK-1 corrected classifier. Observation pre-execute so the
		// counter reflects intent regardless of execution outcome (errors
		// still bucket by intended op_type, matching how queries_total works
		// in Phase 3 reference catalogs).
		e.observeQuery(classifyOpType(info, true /* isFabric */, false), true, slowStart)
		execSpan.SetAttributes(attribute.String("cypher.op_type", "fabric"))
		inExplicitTx := e.txContext != nil && e.txContext.active
		preparedFabric, err := e.prepareFabricExecution(ctx, cypher)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, fabricPreparedExecKey{}, preparedFabric)
		allowResultCache := !preparedFabric.hasRemote

		// Mirror normal query-cache policy for Fabric reads (autocommit only).
		if allowResultCache && !inExplicitTx && info.IsReadOnly && e.cache != nil && isCacheableReadQuery(cypher) {
			if cached, found := e.cache.Get(cypher, mergedParams); found {
				return cached, nil
			}
		}

		var result *ExecuteResult
		var execErr error
		// When an explicit transaction is active on a composite route, execute through
		// the same FabricTransaction so many-read/one-write constraints are enforced
		// across all statements in the session.
		if inExplicitTx {
			if ftx, ok := e.txContext.tx.(*fabric.FabricTransaction); ok {
				result, execErr = e.executeViaPreparedFabricWithTx(ctx, cypher, mergedParams, ftx, false, preparedFabric)
			} else {
				result, execErr = e.executeViaFabric(ctx, cypher, mergedParams)
			}
		} else {
			result, execErr = e.executeViaFabric(ctx, cypher, mergedParams)
		}
		if execErr != nil {
			return nil, execErr
		}

		if allowResultCache && !inExplicitTx && info.IsReadOnly && e.cache != nil && isCacheableReadQuery(cypher) {
			ttl := 60 * time.Second
			if info.HasAggregation {
				ttl = 1 * time.Second
			}
			if info.HasCall || info.HasShow {
				ttl = 300 * time.Second
			}
			e.cache.Put(cypher, mergedParams, result, ttl)
		}

		if info.IsWriteQuery && e.cache != nil {
			if len(info.Labels) > 0 {
				e.cache.InvalidateLabels(info.Labels)
			} else {
				e.cache.Invalidate()
			}
		}

		return result, nil
	}

	// Handle leading Cypher USE clause (openCypher multi-graph syntax).
	if useDB, remaining, hasUse, err := parseLeadingUseClause(cypher); hasUse || err != nil {
		if err != nil {
			return nil, err
		}
		scopedExec, resolvedDB, err := e.scopedExecutorForUse(useDB, GetAuthTokenFromContext(ctx))
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, ctxKeyUseDatabase, resolvedDB)
		if strings.TrimSpace(remaining) == "" {
			return &ExecuteResult{
				Columns: []string{"database"},
				Rows:    [][]interface{}{{resolvedDB}},
			}, nil
		}
		return scopedExec.Execute(ctx, remaining, params)
	}

	// Reject data queries on composite root — callers must USE a constituent.
	// System/admin commands (SHOW DATABASES, CREATE/DROP DATABASE, ALTER, SHOW COMPOSITE,
	// SHOW CONSTITUENTS, SHOW ALIASES, SHOW LIMITS, BEGIN, COMMIT, ROLLBACK) are allowed.
	if isCompositeRoot(e.storage) && !isCompositeAllowedCommand(cypher) {
		return nil, fmt.Errorf("Neo.ClientError.Statement.NotAllowed: " +
			"Queries on composite databases require explicit graph targeting. " +
			"Use USE <composite>.<alias> to target a specific constituent")
	}

	// Merge session-scoped shell parameters with per-call parameters.
	// Explicit params win over shell params to preserve HTTP/Bolt semantics.
	params = e.mergeShellParams(params)

	// Check for transaction control statements and transaction scripts FIRST.
	// These are Nornic extensions and must bypass strict ANTLR validation.
	if result, err := e.executeTransactionScript(ctx, cypher); result != nil || err != nil {
		return result, err
	}
	if result, err := e.parseTransactionStatement(cypher); result != nil || err != nil {
		return result, err
	}

	// Validate basic syntax
	if err := e.validateSyntax(cypher); err != nil {
		// Plan 04-03 Site 2 (parse-error chokepoint): emit op_type="parse_error"
		// per D-04b sixth enum value. No duration observation — parse cost is
		// sub-microsecond and not meaningful to bucket. The queries_total
		// counter still increments so the SRE can alert on parse-error rate.
		e.observeQuery("parse_error", false /* observeDuration */, slowStart)
		execSpan.SetAttributes(attribute.String("cypher.op_type", "parse_error"))
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

	// TRC-15: plan span wraps the analysis/classification phase.
	_, planSpan := startPlanSpan(ctx)
	// Analyze query - uses cached analysis if available
	// This extracts query metadata (HasMatch, IsReadOnly, Labels, etc.) once
	// and caches it for repeated queries, avoiding redundant string parsing
	info := e.analyzer.Analyze(cypher)

	// Plan 04-03 Sites 1 + 3 (RISK-1 corrected): classify ONCE post-Analyze.
	// isFabric=false here because the fabric branch returns at line ~963
	// before reaching this code path. isAdmin=true overrides the QueryInfo-
	// derived classification (e.g., SHOW DATABASES would otherwise classify
	// as "schema" via HasShow → IsSchemaQuery; D-04a says system/admin
	// commands bucket as "admin" instead). Single observation point covers
	// cache-hit early-return AND every downstream execution path so the
	// counter reflects intent regardless of how the query resolves.
	opType := classifyOpType(info, false /* isFabric */, isSystemCommandNoGraph(cypher))
	e.observeQuery(opType, true /* observeDuration */, slowStart)
	planSpan.SetAttributes(attribute.String("cypher.op_type", opType))
	planSpan.End()
	// Update the execute span with the resolved op_type.
	execSpan.SetAttributes(attribute.String("cypher.op_type", opType))

	// For routing, we still need upperQuery for some handlers
	// TODO: Migrate handlers to use QueryInfo directly
	upperQuery := strings.ToUpper(strings.TrimSpace(cypher))

	// Try cache for read-only queries only when cache policy allows it.
	if info.IsReadOnly && e.cache != nil && isCacheableReadQuery(cypher) {
		if cached, found := e.cache.Get(cypher, params); found {
			return cached, nil
		}
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

	// System commands (CREATE/DROP DATABASE, SHOW DATABASES, etc.) must not use the async engine
	// or implicit transactions: they operate on dbManager/metadata, not graph storage.
	// Routing them through executeWithoutTransaction directly ensures correct handling and
	// avoids the write path (tryAsyncCreateNodeBatch / executeWithImplicitTransaction).
	if isSystemCommandNoGraph(cypher) {
		result, err := e.executeWithoutTransaction(ctx, cypher, upperQuery)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Auto-commit single query - use async path for performance
	// This uses AsyncEngine's write-behind cache instead of synchronous disk I/O
	// For strict ACID, users should use explicit BEGIN/COMMIT transactions
	result, err = e.executeImplicitAsync(ctx, cypher, upperQuery)

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

// trimTrailingStatementDelimiters removes trailing Cypher statement delimiters
// (';') and surrounding whitespace, while leaving internal semicolons untouched.
// This mirrors Neo4j-compatible client behavior where a final semicolon is optional.
func trimTrailingStatementDelimiters(query string) string {
	s := strings.TrimSpace(query)
	for {
		if !strings.HasSuffix(s, ";") {
			return s
		}
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}
}

func normalizeCypherSyntaxConfusables(query string) string {
	if query == "" {
		return query
	}

	const (
		normalizeDefault = iota
		normalizeSingleQuoted
		normalizeDoubleQuoted
		normalizeBacktickQuoted
		normalizeLineComment
		normalizeBlockComment
	)

	runes := []rune(query)
	var builder strings.Builder
	builder.Grow(len(query) + 8)
	changed := false
	state := normalizeDefault

	for i := 0; i < len(runes); i++ {
		r := runes[i]
		next := rune(0)
		if i+1 < len(runes) {
			next = runes[i+1]
		}

		switch state {
		case normalizeDefault:
			switch {
			case r == '/' && next == '/':
				builder.WriteRune(r)
				builder.WriteRune(next)
				i++
				state = normalizeLineComment
				continue
			case r == '/' && next == '*':
				builder.WriteRune(r)
				builder.WriteRune(next)
				i++
				state = normalizeBlockComment
				continue
			case r == '\'':
				builder.WriteRune(r)
				state = normalizeSingleQuoted
				continue
			case r == '"':
				builder.WriteRune(r)
				state = normalizeDoubleQuoted
				continue
			case r == '`':
				builder.WriteRune(r)
				state = normalizeBacktickQuoted
				continue
			}

			if replacement, ok := cypherSyntaxConfusableReplacement(r); ok {
				builder.WriteString(replacement)
				changed = changed || replacement != string(r)
				continue
			}

			if replacement, ok := cypherWhitespaceReplacement(r); ok {
				builder.WriteRune(replacement)
				changed = changed || replacement != r
				continue
			}

			if isIgnorableCypherFormatRune(r) {
				changed = true
				continue
			}

			builder.WriteRune(r)

		case normalizeSingleQuoted:
			builder.WriteRune(r)
			if r == '\\' && i+1 < len(runes) {
				builder.WriteRune(runes[i+1])
				i++
				continue
			}
			if r == '\'' {
				if i+1 < len(runes) && runes[i+1] == '\'' {
					builder.WriteRune(runes[i+1])
					i++
					continue
				}
				state = normalizeDefault
			}

		case normalizeDoubleQuoted:
			builder.WriteRune(r)
			if r == '\\' && i+1 < len(runes) {
				builder.WriteRune(runes[i+1])
				i++
				continue
			}
			if r == '"' {
				if i+1 < len(runes) && runes[i+1] == '"' {
					builder.WriteRune(runes[i+1])
					i++
					continue
				}
				state = normalizeDefault
			}

		case normalizeBacktickQuoted:
			builder.WriteRune(r)
			if r == '`' {
				if i+1 < len(runes) && runes[i+1] == '`' {
					builder.WriteRune(runes[i+1])
					i++
					continue
				}
				state = normalizeDefault
			}

		case normalizeLineComment:
			builder.WriteRune(r)
			if r == '\n' || r == '\r' {
				state = normalizeDefault
			}

		case normalizeBlockComment:
			builder.WriteRune(r)
			if r == '*' && next == '/' {
				builder.WriteRune(next)
				i++
				state = normalizeDefault
			}
		}
	}

	if !changed {
		return query
	}

	return builder.String()
}

func cypherSyntaxConfusableReplacement(r rune) (string, bool) {
	switch r {
	case '→':
		return "->", true
	case '←':
		return "<-", true
	case '—', '–', '−', '‐', '‑', '‒':
		return "-", true
	case '（':
		return "(", true
	case '）':
		return ")", true
	case '［':
		return "[", true
	case '］':
		return "]", true
	case '｛':
		return "{", true
	case '｝':
		return "}", true
	case '，':
		return ",", true
	case '：':
		return ":", true
	case '；':
		return ";", true
	case '．':
		return ".", true
	case '＝':
		return "=", true
	case '＜':
		return "<", true
	case '＞':
		return ">", true
	case '＄':
		return "$", true
	default:
		return "", false
	}
}

func cypherWhitespaceReplacement(r rune) (rune, bool) {
	switch r {
	case '\u0085', '\u2028', '\u2029':
		return '\n', true
	case ' ', '\t', '\n', '\r':
		return 0, false
	default:
		if unicode.IsSpace(r) {
			return ' ', true
		}
		return 0, false
	}
}

func isIgnorableCypherFormatRune(r rune) bool {
	switch r {
	case '\u200B', '\u200C', '\u200D', '\u2060', '\uFEFF':
		return true
	default:
		return false
	}
}

// TransactionCapableEngine is an engine that supports ACID transactions.
// Used for type assertion to wrap implicit writes in rollback-capable transactions.
type TransactionCapableEngine interface {
	BeginTransaction() (*storage.BadgerTransaction, error)
}

type implicitTxEngines struct {
	txEngine    TransactionCapableEngine
	asyncEngine *storage.AsyncEngine
	namespace   string
}

func (e *StorageExecutor) resolveImplicitTxEngines() implicitTxEngines {
	engine := e.storage
	visited := make(map[storage.Engine]bool)
	out := implicitTxEngines{}

	for engine != nil && !visited[engine] {
		visited[engine] = true

		if out.namespace == "" {
			if ns, ok := engine.(interface{ Namespace() string }); ok {
				out.namespace = ns.Namespace()
			}
		}
		if out.asyncEngine == nil {
			if ae, ok := engine.(*storage.AsyncEngine); ok {
				out.asyncEngine = ae
			}
		}
		if out.txEngine == nil {
			if tc, ok := engine.(TransactionCapableEngine); ok {
				out.txEngine = tc
			}
		}

		switch wrapper := engine.(type) {
		case interface{ GetUnderlying() storage.Engine }:
			engine = wrapper.GetUnderlying()
		case interface{ GetEngine() storage.Engine }:
			engine = wrapper.GetEngine()
		case interface{ GetInnerEngine() storage.Engine }:
			engine = wrapper.GetInnerEngine()
		default:
			engine = nil
		}
	}

	return out
}

func (e *StorageExecutor) tryAsyncCreateNodeBatch(ctx context.Context, cypher string) (*ExecuteResult, error, bool) {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	if !strings.HasPrefix(upper, "CREATE") {
		return nil, nil, false
	}
	// System commands and schema commands must not be handled here — route to executeSchemaCommand instead
	if findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "CONSTRAINT") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "INDEX") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "FULLTEXT") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "VECTOR") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "RANGE") == 0 {
		return nil, nil, false
	}
	for _, keyword := range []string{
		"MATCH",
		"MERGE",
		"SET",
		"DELETE",
		"DETACH",
		"REMOVE",
		"WITH",
		"CALL",
		"UNWIND",
		"FOREACH",
		"LOAD",
		"OPTIONAL",
	} {
		if containsKeywordOutsideStrings(cypher, keyword) {
			return nil, nil, false
		}
	}

	// Substitute parameters before parsing so (n:Label $props) becomes (n:Label { ... })
	// and the label is not mis-parsed as "Label $props".
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	returnIdx := findKeywordIndex(cypher, "RETURN")
	createPart := cypher
	if returnIdx > 0 {
		createPart = strings.TrimSpace(cypher[:returnIdx])
	}

	createClauses := createKeywordPattern.Split(createPart, -1)
	if len(createClauses) == 0 {
		return nil, nil, false
	}

	var nodePatterns []string
	for _, clause := range createClauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		patterns := e.splitCreatePatterns(clause)
		for _, pat := range patterns {
			pat = strings.TrimSpace(pat)
			if pat == "" {
				continue
			}
			if containsOutsideStrings(pat, "->") ||
				containsOutsideStrings(pat, "<-") ||
				containsOutsideStrings(pat, "]-") ||
				containsOutsideStrings(pat, "-[") {
				return nil, nil, false
			}
			nodePatterns = append(nodePatterns, pat)
		}
	}

	if len(nodePatterns) == 0 {
		return nil, nil, false
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	createdNodes := make(map[string]*storage.Node)
	nodes := make([]*storage.Node, 0, len(nodePatterns))
	for _, nodePatternStr := range nodePatterns {
		nodePattern := e.parseNodePattern(nodePatternStr)

		for _, label := range nodePattern.labels {
			if !isValidIdentifier(label) {
				return nil, fmt.Errorf("invalid label name: %q (must be alphanumeric starting with letter or underscore)", label), true
			}
			if containsReservedKeyword(label) {
				return nil, fmt.Errorf("invalid label name: %q (contains reserved keyword)", label), true
			}
		}

		for key, val := range nodePattern.properties {
			if !isValidIdentifier(key) {
				return nil, fmt.Errorf("invalid property key: %q (must be alphanumeric starting with letter or underscore)", key), true
			}
			if _, ok := val.(invalidPropertyValue); ok {
				return nil, fmt.Errorf("invalid property value for key %q: malformed syntax", key), true
			}
		}

		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     nodePattern.labels,
			Properties: nodePattern.properties,
		}
		nodes = append(nodes, node)
		if nodePattern.variable != "" {
			createdNodes[nodePattern.variable] = node
		}
	}

	store := e.getStorage(ctx)
	if err := store.BulkCreateNodes(nodes); err != nil {
		return nil, err, true
	}

	for _, node := range nodes {
		e.notifyNodeMutated(string(node.ID))
		addOptimisticNodeID(result, node.ID)
	}
	result.Stats.NodesCreated += len(nodes)

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

			for variable, node := range createdNodes {
				if strings.HasPrefix(item.expr, variable) || item.expr == variable {
					row[i] = e.resolveReturnItem(item, variable, node)
					break
				}
			}

			if row[i] == nil {
				if varName := extractVariableNameFromReturnItem(item.expr); varName != "" {
					if node, ok := createdNodes[varName]; ok {
						row[i] = e.resolveReturnItem(item, varName, node)
					}
				}
			}
		}
		result.Rows = [][]interface{}{row}
	}

	return result, nil, true
}

func (e *StorageExecutor) isEventualAsyncEligible(info *QueryInfo, cypher string) bool {
	if info == nil || !info.IsWriteQuery {
		return false
	}
	if info.HasSchema || info.IsSchemaQuery || isSystemCommandNoGraph(cypher) || isCreateProcedureCommand(cypher) {
		return false
	}
	if info.FirstClause != ClauseCreate || !info.HasCreate {
		return false
	}
	if info.HasMatch || info.HasOptionalMatch || info.HasMerge || info.HasDelete || info.HasDetachDelete ||
		info.HasSet || info.HasRemove || info.HasWith || info.HasUnwind || info.HasCall ||
		info.HasForeach || info.HasLoadCSV || info.HasUnion {
		return false
	}
	return true
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
		engines := e.resolveImplicitTxEngines()
		if engines.asyncEngine != nil {
			if result, err, handled := e.tryAsyncCreateNodeBatch(ctx, cypher); handled {
				return result, err
			}
			if e.isEventualAsyncEligible(info, cypher) {
				return e.executeWithoutTransaction(ctx, cypher, upperQuery)
			}
		}
		return e.executeWithImplicitTransaction(ctx, cypher, upperQuery)
	}

	// Read-only operations don't need transaction wrapping
	return e.executeWithoutTransaction(ctx, cypher, upperQuery)
}

// executeWithImplicitTransaction wraps a write query in an implicit transaction.
// If any part of the query fails, all changes are rolled back atomically.
// This prevents data corruption from partially executed queries.
func (e *StorageExecutor) executeWithImplicitTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	parsedCypher, inlineEmbeddingEnabled := stripWithEmbeddingSuffix(cypher)
	if inlineEmbeddingEnabled {
		cypher = parsedCypher
		upperQuery = strings.ToUpper(cypher)
	}

	// Try to get a transaction-capable engine and async wrapper (if present)
	engines := e.resolveImplicitTxEngines()
	if engines.namespace == "" {
		if dbName := strings.TrimSpace(GetUseDatabaseFromContext(ctx)); dbName != "" {
			engines.namespace = dbName
		} else if _, dbName := e.resolveWALAndDatabase(); strings.TrimSpace(dbName) != "" {
			engines.namespace = strings.TrimSpace(dbName)
		}
	}
	txEngine := engines.txEngine
	asyncEngine := engines.asyncEngine

	// If no transaction support, fall back to direct execution (legacy mode)
	// This is less safe but maintains backward compatibility
	if txEngine == nil {
		if inlineEmbeddingEnabled {
			return nil, fmt.Errorf("WITH EMBEDDING requires transaction-capable storage")
		}
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
	releaseAsyncFlushHold := func() {}
	if asyncEngine != nil {
		release := asyncEngine.HoldFlush()
		held := true
		releaseAsyncFlushHold = func() {
			if held {
				release()
				held = false
			}
		}
		defer releaseAsyncFlushHold()
	}

	// Start implicit transaction
	tx, err := txEngine.BeginTransaction()
	if err != nil {
		return nil, fmt.Errorf("failed to start implicit transaction: %w", err)
	}

	// Defer constraint validation to commit for implicit transactions.
	// This avoids duplicate per-operation checks and improves write throughput.
	if err := tx.SetDeferredConstraintValidation(true); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to configure implicit transaction: %w", err)
	}
	if err := tx.SetSkipCreateExistenceCheck(true); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to configure implicit transaction: %w", err)
	}

	// Optional WAL transaction markers for receipts.
	var wal *storage.WAL
	var walSeqStart uint64
	txID := tx.ID
	var dbName string
	if txID != "" {
		wal, dbName = e.resolveWALAndDatabase()
		if wal != nil {
			walSeqStart, err = wal.AppendTxBegin(dbName, txID, nil)
			if err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("failed to write WAL tx begin: %w", err)
			}
		}
	}

	// Create a transactional wrapper that routes writes through the transaction
	// CRITICAL: We pass the wrapper through context instead of modifying e.storage
	// because e.storage modification is NOT thread-safe for concurrent executions.
	separator := ":"
	if engines.namespace == "" {
		separator = ""
	}
	txWrapper := &transactionStorageWrapper{
		tx:             tx,
		underlying:     e.storage,
		namespace:      engines.namespace,
		separator:      separator,
		mutatedNodeIDs: make(map[string]struct{}),
	}

	// Execute with transaction wrapper via context
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)
	txExec := e.cloneWithStorage(txWrapper)

	// Execute the query
	result, execErr := txExec.executeWithoutTransaction(txCtx, cypher, upperQuery)

	// Handle result
	if execErr != nil {
		// Rollback on any error - prevents partial data corruption
		tx.Rollback()
		txExec.invalidateNodeLookupCache()
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, execErr.Error())
		}
		return nil, execErr
	}

	if inlineEmbeddingEnabled {
		if err := txExec.applyInlineEmbeddingMutations(txCtx, txWrapper.snapshotMutatedNodeIDs()); err != nil {
			tx.Rollback()
			txExec.invalidateNodeLookupCache()
			if wal != nil && walSeqStart > 0 {
				_, _ = wal.AppendTxAbort(dbName, txID, err.Error())
			}
			return nil, err
		}
	}

	// Commit successful transaction
	if err := tx.Commit(); err != nil {
		txExec.invalidateNodeLookupCache()
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, err.Error())
		}
		return nil, fmt.Errorf("failed to commit implicit transaction: %w", err)
	}

	// Attach receipt metadata if WAL markers were recorded.
	if wal != nil && walSeqStart > 0 {
		opCount := tx.OperationCount()
		if commitSeq, walErr := wal.AppendTxCommit(dbName, txID, opCount); walErr == nil {
			if receipt, recErr := storage.NewReceipt(txID, walSeqStart, commitSeq, dbName, time.Now().UTC()); recErr == nil {
				if result.Metadata == nil {
					result.Metadata = make(map[string]interface{})
				}
				result.Metadata["receipt"] = receipt
			}
		}
	}

	// Flush if needed for durability
	if !e.deferFlush && asyncEngine != nil {
		releaseAsyncFlushHold()
		asyncEngine.Flush()
	}

	return result, nil
}

// ctxKeyTxStorage is the context key for transaction storage wrapper.
type ctxKeyTxStorageType struct{}

var ctxKeyTxStorage = ctxKeyTxStorageType{}

func (e *StorageExecutor) applyInlineEmbeddingMutations(ctx context.Context, ids map[string]struct{}) error {
	if len(ids) == 0 {
		return nil
	}
	if e.embedder == nil {
		return fmt.Errorf("WITH EMBEDDING requires configured embedder")
	}
	store := e.getStorage(ctx)
	for id := range ids {
		node, err := store.GetNode(storage.NodeID(id))
		if err != nil {
			if err == storage.ErrNotFound {
				continue
			}
			return err
		}
		if node == nil {
			continue
		}
		text := embeddingutil.BuildText(node.Properties, node.Labels, e.inlineEmbeddingTextOptions)
		chunks, err := e.embedder.ChunkText(text, e.inlineEmbeddingChunkSize, e.inlineEmbeddingChunkOverlap)
		if err != nil {
			return fmt.Errorf("WITH EMBEDDING chunking failed for node %s: %w", id, err)
		}
		if len(chunks) == 0 {
			chunks = []string{text}
		}
		embeddings := make([][]float32, 0, len(chunks))
		for _, chunk := range chunks {
			emb, err := e.embedder.Embed(ctx, chunk)
			if err != nil {
				return fmt.Errorf("WITH EMBEDDING embed failed for node %s: %w", id, err)
			}
			if len(emb) == 0 {
				return fmt.Errorf("WITH EMBEDDING embed returned empty vector for node %s", id)
			}
			embeddings = append(embeddings, emb)
		}
		model := "inline-cypher"
		dimensions := len(embeddings[0])
		if named, ok := e.embedder.(interface{ Model() string }); ok {
			if v := strings.TrimSpace(named.Model()); v != "" {
				model = v
			}
		}
		if d, ok := e.embedder.(interface{ Dimensions() int }); ok {
			if v := d.Dimensions(); v > 0 {
				dimensions = v
			}
		}
		embeddingutil.ApplyManagedEmbedding(node, embeddings, model, dimensions, time.Now())
		if err := store.UpdateNode(node); err != nil {
			return err
		}
	}
	return nil
}

func stripWithEmbeddingSuffix(cypher string) (string, bool) {
	idx := findKeywordIndex(cypher, "WITH EMBEDDING")
	if idx < 0 {
		return cypher, false
	}
	before := strings.TrimSpace(cypher[:idx])
	after := strings.TrimSpace(cypher[idx+len("WITH EMBEDDING"):])
	if before == "" {
		return cypher, false
	}
	if after == "" {
		return before, true
	}
	return strings.TrimSpace(before + " " + after), true
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ctxKeyUseDatabase is the context key for :USE database switching.
// When :USE database_name is detected, the database name is stored in context
// so the server can switch to that database before executing the query.
type ctxKeyUseDatabaseType struct{}

var ctxKeyUseDatabase = ctxKeyUseDatabaseType{}

// ctxKeyAuthToken carries an Authorization header value for remote/OIDC forwarding.
type ctxKeyAuthTokenType struct{}

var ctxKeyAuthToken = ctxKeyAuthTokenType{}

// GetUseDatabaseFromContext extracts the database name from :USE command if present in context.
// Returns empty string if no :USE command was found.
func GetUseDatabaseFromContext(ctx context.Context) string {
	if dbName, ok := ctx.Value(ctxKeyUseDatabase).(string); ok {
		return dbName
	}
	return ""
}

// WithAuthToken stores an Authorization header token on context for execution paths
// that need to forward caller identity across remote constituents.
func WithAuthToken(ctx context.Context, authToken string) context.Context {
	if strings.TrimSpace(authToken) == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyAuthToken, authToken)
}

// GetAuthTokenFromContext extracts forwarded Authorization token from context.
func GetAuthTokenFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyAuthToken).(string); ok {
		return v
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

// resolveWALAndDatabase attempts to find a WAL instance and database name
// by unwrapping common storage wrappers (namespaced, async, WAL engines).
func (e *StorageExecutor) resolveWALAndDatabase() (*storage.WAL, string) {
	engine := e.storage
	var dbName string

	for engine != nil {
		if ns, ok := engine.(interface{ Namespace() string }); ok && dbName == "" {
			dbName = ns.Namespace()
		}
		if walProvider, ok := engine.(interface{ GetWAL() *storage.WAL }); ok {
			return walProvider.GetWAL(), dbName
		}
		switch wrapper := engine.(type) {
		case interface{ GetUnderlying() storage.Engine }:
			engine = wrapper.GetUnderlying()
		case interface{ GetEngine() storage.Engine }:
			engine = wrapper.GetEngine()
		case interface{ GetInnerEngine() storage.Engine }:
			engine = wrapper.GetInnerEngine()
		default:
			return nil, dbName
		}
	}

	return nil, dbName
}

// transactionStorageWrapper wraps a BadgerTransaction to implement storage.Engine
// for use in implicit transaction execution. It routes writes through the transaction
// (for atomicity/rollback) and reads through the underlying engine (for performance).
type transactionStorageWrapper struct {
	tx               *storage.BadgerTransaction
	underlying       storage.Engine // For read operations not supported by transaction
	namespace        string
	separator        string
	mutatedNodeIDs   map[string]struct{}
	mutatedNodeIDsMu sync.Mutex
}

func (w *transactionStorageWrapper) markMutatedNodeID(id storage.NodeID) {
	if id == "" || w.mutatedNodeIDs == nil {
		return
	}
	w.mutatedNodeIDsMu.Lock()
	w.mutatedNodeIDs[string(id)] = struct{}{}
	w.mutatedNodeIDsMu.Unlock()
}

func (w *transactionStorageWrapper) snapshotMutatedNodeIDs() map[string]struct{} {
	if len(w.mutatedNodeIDs) == 0 {
		return nil
	}
	w.mutatedNodeIDsMu.Lock()
	defer w.mutatedNodeIDsMu.Unlock()
	out := make(map[string]struct{}, len(w.mutatedNodeIDs))
	for id := range w.mutatedNodeIDs {
		out[id] = struct{}{}
	}
	return out
}

// Write operations - go through transaction for atomicity
func (w *transactionStorageWrapper) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if w.namespace == "" {
		id, err := w.tx.CreateNode(node)
		if err == nil {
			w.markMutatedNodeID(id)
		}
		return id, err
	}
	namespaced := storage.CopyNode(node)
	namespaced.ID = w.prefixNodeID(node.ID)
	actualID, err := w.tx.CreateNode(namespaced)
	if err != nil {
		return "", err
	}
	userID := w.unprefixNodeID(actualID)
	w.markMutatedNodeID(userID)
	return userID, nil
}

func (w *transactionStorageWrapper) UpdateNode(node *storage.Node) error {
	if w.namespace == "" {
		err := w.tx.UpdateNode(node)
		if err == nil {
			w.markMutatedNodeID(node.ID)
		}
		return err
	}
	namespaced := storage.CopyNode(node)
	namespaced.ID = w.prefixNodeID(node.ID)
	err := w.tx.UpdateNode(namespaced)
	if err == nil {
		w.markMutatedNodeID(node.ID)
	}
	return err
}

func (w *transactionStorageWrapper) DeleteNode(id storage.NodeID) error {
	return w.tx.DeleteNode(w.prefixNodeID(id))
}

func (w *transactionStorageWrapper) CreateEdge(edge *storage.Edge) error {
	if w.namespace == "" {
		return w.tx.CreateEdge(edge)
	}
	namespaced := storage.CopyEdge(edge)
	namespaced.ID = w.prefixEdgeID(edge.ID)
	namespaced.StartNode = w.prefixNodeID(edge.StartNode)
	namespaced.EndNode = w.prefixNodeID(edge.EndNode)
	return w.tx.CreateEdge(namespaced)
}

func (w *transactionStorageWrapper) DeleteEdge(id storage.EdgeID) error {
	return w.tx.DeleteEdge(w.prefixEdgeID(id))
}

// Read operations - transaction supports GetNode, forward others to underlying
func (w *transactionStorageWrapper) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := w.tx.GetNode(w.prefixNodeID(id))
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetEdge(id)
	}
	edge, err := w.tx.GetEdge(w.prefixEdgeID(id))
	if err != nil {
		return nil, err
	}
	return w.toUserEdge(edge), nil
}

func (w *transactionStorageWrapper) UpdateEdge(edge *storage.Edge) error {
	if w.namespace == "" {
		return w.tx.UpdateEdge(edge)
	}
	namespaced := storage.CopyEdge(edge)
	namespaced.ID = w.prefixEdgeID(edge.ID)
	namespaced.StartNode = w.prefixNodeID(edge.StartNode)
	namespaced.EndNode = w.prefixNodeID(edge.EndNode)
	return w.tx.UpdateEdge(namespaced)
}

func (w *transactionStorageWrapper) GetNodesByLabel(label string) ([]*storage.Node, error) {
	nodes, err := w.tx.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return nodes, nil
	}
	return w.toUserNodes(nodes), nil
}

func (w *transactionStorageWrapper) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	node, err := w.tx.GetFirstNodeByLabel(label)
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return node, nil
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) ForEachNodeIDByLabel(label string, visit func(storage.NodeID) bool) error {
	if visit == nil {
		return nil
	}
	if !w.tx.HasPendingNodeMutations() {
		if lookup, ok := w.underlying.(storage.LabelNodeIDLookupEngine); ok {
			return lookup.ForEachNodeIDByLabel(label, visit)
		}
	}

	nodes, err := w.GetNodesByLabel(label)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if !visit(node.ID) {
			return nil
		}
	}
	return nil
}

func (w *transactionStorageWrapper) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetOutgoingEdges(nodeID)
	}
	edges, err := w.tx.GetOutgoingEdges(w.prefixNodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetIncomingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetIncomingEdges(nodeID)
	}
	edges, err := w.tx.GetIncomingEdges(w.prefixNodeID(nodeID))
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetEdgesBetween(startID, endID storage.NodeID) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetEdgesBetween(startID, endID)
	}
	edges, err := w.tx.GetEdgesBetween(w.prefixNodeID(startID), w.prefixNodeID(endID))
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetEdgeBetween(startID, endID storage.NodeID, edgeType string) *storage.Edge {
	if w.namespace == "" {
		return w.tx.GetEdgeBetween(startID, endID, edgeType)
	}
	edge := w.tx.GetEdgeBetween(w.prefixNodeID(startID), w.prefixNodeID(endID), edgeType)
	if edge == nil {
		return nil
	}
	return w.toUserEdge(edge)
}

func (w *transactionStorageWrapper) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	if w.namespace == "" {
		return w.tx.GetEdgesByType(edgeType)
	}
	edges, err := w.tx.GetEdgesByType(edgeType)
	if err != nil {
		return nil, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetNodesByLabelVisibleAt(label string, version storage.MVCCVersion) ([]*storage.Node, error) {
	if provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetNodesByLabelVisibleAt(label, version)
	}
	return nil, storage.ErrNotImplemented
}

func (w *transactionStorageWrapper) GetEdgesByTypeVisibleAt(edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesByTypeVisibleAt(edgeType, version)
	}
	return nil, storage.ErrNotImplemented
}

func (w *transactionStorageWrapper) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	if provider, ok := w.underlying.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetEdgesBetweenVisibleAt(startID, endID, version)
	}
	return nil, storage.ErrNotImplemented
}

func (w *transactionStorageWrapper) AllNodes() ([]*storage.Node, error) {
	nodes, err := w.tx.AllNodes()
	if err != nil {
		return nil, err
	}
	if w.namespace == "" {
		return nodes, nil
	}
	return w.toUserNodes(nodes), nil
}

func (w *transactionStorageWrapper) AllEdges() ([]*storage.Edge, error) {
	return w.underlying.AllEdges()
}

func (w *transactionStorageWrapper) GetAllNodes() []*storage.Node {
	nodes := w.tx.GetAllNodes()
	if w.namespace == "" {
		return nodes
	}
	return w.toUserNodes(nodes)
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
		if w.namespace == "" {
			if _, err := w.tx.CreateNode(node); err != nil {
				return err
			}
			continue
		}
		namespaced := storage.CopyNode(node)
		namespaced.ID = w.prefixNodeID(node.ID)
		if _, err := w.tx.CreateNode(namespaced); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkCreateEdges(edges []*storage.Edge) error {
	for _, edge := range edges {
		if w.namespace == "" {
			if err := w.tx.CreateEdge(edge); err != nil {
				return err
			}
			continue
		}
		namespaced := storage.CopyEdge(edge)
		namespaced.ID = w.prefixEdgeID(edge.ID)
		namespaced.StartNode = w.prefixNodeID(edge.StartNode)
		namespaced.EndNode = w.prefixNodeID(edge.EndNode)
		if err := w.tx.CreateEdge(namespaced); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteNode(w.prefixNodeID(id)); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := w.tx.DeleteEdge(w.prefixEdgeID(id)); err != nil {
			return err
		}
	}
	return nil
}

func (w *transactionStorageWrapper) prefixNodeID(id storage.NodeID) storage.NodeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	if strings.HasPrefix(string(id), prefix) {
		return id
	}
	return storage.NodeID(w.namespace + w.separator + string(id))
}

func (w *transactionStorageWrapper) unprefixNodeID(id storage.NodeID) storage.NodeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return storage.NodeID(s[len(prefix):])
	}
	return id
}

func (w *transactionStorageWrapper) prefixEdgeID(id storage.EdgeID) storage.EdgeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	if strings.HasPrefix(string(id), prefix) {
		return id
	}
	return storage.EdgeID(w.namespace + w.separator + string(id))
}

func (w *transactionStorageWrapper) unprefixEdgeID(id storage.EdgeID) storage.EdgeID {
	if w.namespace == "" {
		return id
	}
	prefix := w.namespace + w.separator
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return storage.EdgeID(s[len(prefix):])
	}
	return id
}

func (w *transactionStorageWrapper) toUserNode(node *storage.Node) *storage.Node {
	if node == nil {
		return nil
	}
	out := storage.CopyNode(node)
	out.ID = w.unprefixNodeID(out.ID)
	return out
}

func (w *transactionStorageWrapper) toUserEdge(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	out := storage.CopyEdge(edge)
	out.ID = w.unprefixEdgeID(out.ID)
	out.StartNode = w.unprefixNodeID(out.StartNode)
	out.EndNode = w.unprefixNodeID(out.EndNode)
	return out
}

func (w *transactionStorageWrapper) toUserEdges(edges []*storage.Edge) []*storage.Edge {
	out := make([]*storage.Edge, 0, len(edges))
	for _, edge := range edges {
		out = append(out, w.toUserEdge(edge))
	}
	return out
}

func (w *transactionStorageWrapper) toUserNodes(nodes []*storage.Node) []*storage.Node {
	out := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, w.toUserNode(node))
	}
	return out
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
// using structured scanning rather than regex capture arrays.
// Returns (result, true) if handled, (nil, false) if the query should go through normal routing.
//
// Pattern: MATCH (a:Label), (b:Label) WITH a, b LIMIT 1 CREATE (a)-[r:Type]->(b) DELETE r
// This is a very common pattern in benchmarks and relationship tests.
func (e *StorageExecutor) tryFastPathCompoundQuery(ctx context.Context, cypher string) (*ExecuteResult, bool) {
	if match, ok := matchCompoundQueryShape(cypher); ok {
		switch match.Kind {
		case shapeKindCompoundCreateDeleteRel:
			e.markCompoundQueryFastPathUsed()
			return e.executeFastPathCreateDeleteRel(
				match.Captures.String("label1"),
				match.Captures.String("label2"),
				match.Captures.String("prop1"),
				match.Captures.Any("value1"),
				match.Captures.String("prop2"),
				match.Captures.Any("value2"),
				match.Captures.String("rel_type"),
			)
		case shapeKindCompoundPropCreateDeleteRel:
			e.markCompoundQueryFastPathUsed()
			return e.executeFastPathCreateDeleteRel(
				match.Captures.String("label1"),
				match.Captures.String("label2"),
				match.Captures.String("prop1"),
				match.Captures.Any("value1"),
				match.Captures.String("prop2"),
				match.Captures.Any("value2"),
				match.Captures.String("rel_type"),
			)
		case shapeKindCompoundPropCreateDeleteReturnCountRel:
			e.markCompoundQueryFastPathUsed()
			return e.executeFastPathCreateDeleteRelCount(
				match.Captures.String("label1"),
				match.Captures.String("label2"),
				match.Captures.String("prop1"),
				match.Captures.Any("value1"),
				match.Captures.String("prop2"),
				match.Captures.Any("value2"),
				match.Captures.String("rel_type"),
				match.Captures.String("rel_var"),
			)
		}
	}

	return nil, false
}

// executeFastPathCreateDeleteRel executes the fast-path for MATCH...CREATE...DELETE patterns.
// If prop1/prop2 are empty, uses GetFirstNodeByLabel. Otherwise uses property lookup.
func (e *StorageExecutor) executeFastPathCreateDeleteRel(label1, label2, prop1 string, val1 any, prop2 string, val2 any, relType string) (*ExecuteResult, bool) {
	var err error

	// Get node1
	if prop1 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label1)
	} else {
		node1 := e.findNodeByLabelAndProperty(label1, prop1, val1)
		if node1 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	// Get node2
	if prop2 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label2)
	} else {
		node2 := e.findNodeByLabelAndProperty(label2, prop2, val2)
		if node2 == nil {
			return nil, false
		}
	}
	if err != nil {
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
	var err error

	if prop1 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label1)
	} else {
		node1 := e.findNodeByLabelAndProperty(label1, prop1, val1)
		if node1 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	if prop2 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label2)
	} else {
		node2 := e.findNodeByLabelAndProperty(label2, prop2, val2)
		if node2 == nil {
			return nil, false
		}
	}
	if err != nil {
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
	e.ensureNodeLookupCache()

	// Try cache first (with proper locking)
	cacheKey := fmt.Sprintf("%s:{%s:%v}", label, prop, val)
	cacheMu := e.nodeLookupCacheLock()
	cacheMu.RLock()
	if cached, ok := e.nodeLookupCache[cacheKey]; ok {
		cacheMu.RUnlock()
		return cached
	}
	cacheMu.RUnlock()

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
				cacheMu.Lock()
				e.nodeLookupCache[cacheKey] = node
				cacheMu.Unlock()
				return node
			}
		}
	}

	return nil
}

// isSystemCommandNoGraph returns true for statements that operate on database metadata
// (CREATE/DROP DATABASE, SHOW DATABASES, etc.) and must not use the async engine or
// implicit transactions. These are routed to executeWithoutTransaction directly.
func isSystemCommandNoGraph(cypher string) bool {
	return findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "ALIAS") == 0 ||
		findMultiWordKeywordIndex(cypher, "SHOW", "DATABASES") == 0 ||
		findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE") == 0
}

func isShowConstraintContractsCommand(cypher string) bool {
	return findMultiWordKeywordIndex(cypher, "SHOW", "CONSTRAINT CONTRACTS") == 0
}

// executeWithoutTransaction executes query without transaction wrapping (original path).
func (e *StorageExecutor) executeWithoutTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// FAST PATH: Simple MATCH-return-limit reads should never be routed through
	// heavier compound planners. Keep this check first to avoid regressions when
	// adding complex routing rules.
	if result, handled := e.tryFastPathSimpleMatchReturnLimit(ctx, cypher, upperQuery); handled {
		return result, nil
	}

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

	// Correlated MATCH ... CALL { ... } must route before generic MATCH...CREATE/MERGE
	// detection, because CREATE/MERGE/SET tokens can legally appear inside the
	// CALL subquery body and should not trigger outer compound handlers.
	if startsWithMatch && hasSubqueryPattern(cypher, callSubqueryRe) {
		return e.executeMatchWithCallSubquery(ctx, cypher)
	}

	// MERGE queries get special handling - they have their own ON CREATE SET / ON MATCH SET logic
	if startsWithMerge {
		// Complex MERGE pipelines that include OPTIONAL MATCH / WITH / WHERE should
		// use the segment executor instead of the single-MERGE parser.
		if findKeywordIndexInContext(cypher, "OPTIONAL MATCH") > 0 ||
			findKeywordIndexInContext(cypher, "WITH") > 0 ||
			findKeywordIndexInContext(cypher, "WHERE") > 0 {
			return e.executeMultipleMerges(ctx, cypher)
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
		// Try the general pipeline executor first — it handles composite
		// queries like MATCH ... CREATE ... WITH ... UNWIND ... MATCH ...
		// CREATE ... which executeCompoundMatchCreate miscategorises
		// (treats everything after the first CREATE as CREATE patterns,
		// consuming intervening WITH/UNWIND/MATCH clauses as property map
		// garbage).
		if result, ok, err := e.executePipeline(ctx, cypher); ok {
			return result, err
		}
		return e.executeCompoundMatchCreate(ctx, cypher)
	}

	// Compound queries: CREATE ... WITH ... DELETE (create then delete in same statement)
	if startsWithCreate && withIdx > 0 && deleteIdx > 0 {
		return e.executeCompoundCreateWithDelete(ctx, cypher)
	}

	// UNWIND pipelines may contain trailing DELETE/SET/REMOVE keywords.
	// Route UNWIND before generic mutation keyword checks so
	// `UNWIND ... MATCH ... REMOVE ...` does not get misrouted to executeRemove.
	if findKeywordIndex(cypher, "UNWIND") == 0 {
		return e.executeUnwind(ctx, cypher)
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
	if startsWithCreate && !isCreateProcedureCommand(cypher) && hasSet && !hasOnCreateSet && !hasOnMatchSet &&
		findMultiWordKeywordIndex(cypher, "CREATE", "DECAY PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION POLICY") != 0 {
		return e.executeCreateSet(ctx, cypher)
	}

	// Check for ALTER DATABASE before generic SET (ALTER DATABASE SET LIMIT contains "SET")
	if findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE") == 0 {
		return e.executeAlterDatabase(ctx, cypher)
	}

	// Only route to executeSet if it's a MATCH ... SET or standalone SET
	if hasSet && !isCreateProcedureCommand(cypher) && !hasOnCreateSet && !hasOnMatchSet &&
		findMultiWordKeywordIndex(cypher, "CREATE", "DECAY PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION POLICY") != 0 &&
		findMultiWordKeywordIndex(cypher, "ALTER", "DECAY PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION POLICY") != 0 {
		if startsWithMatch || findKeywordIndex(cypher, "SET") == 0 {
			return e.executeSet(ctx, cypher)
		}
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
	case isCreateProcedureCommand(cypher):
		return e.executeCreateProcedure(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "DECAY PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION POLICY") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
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
		// Multi-MATCH chains have dedicated routing inside executeMatch (executeMultiMatch,
		// executeChainedMatchWithAggregations). Keep them on that path to preserve WHERE
		// semantics across MATCH boundaries.
		matchCount := countKeywordOccurrences(upperQuery, "MATCH")
		optionalMatchCount := countKeywordOccurrences(upperQuery, "OPTIONAL MATCH")
		isMultiMatch := matchCount-optionalMatchCount > 1
		if !isMultiMatch {
			// Check for optimizable patterns FIRST
			patternInfo := DetectQueryPattern(cypher)
			if patternInfo.IsOptimizable() {
				if result, ok := e.ExecuteOptimized(ctx, cypher, patternInfo); ok {
					return result, nil
				}
				// Fall through to generic on optimization failure
			}
		}
		return e.executeMatch(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "CONSTRAINT") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "RANGE INDEX") == 0,
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
	case findMultiWordKeywordIndex(cypher, "DROP", "CONSTRAINT") == 0:
		// Schema command: DROP CONSTRAINT (must not be treated as generic DROP no-op).
		// Must be at start (position 0) to be a standalone clause.
		return e.executeSchemaCommand(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "DECAY PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "DROP", "PROMOTION PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "DROP", "PROMOTION POLICY") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case isDropProcedureCommand(cypher):
		return e.executeDropProcedure(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "INDEX") == 0:
		// DROP INDEX — execute real drop against the target schema.
		return e.executeDropIndex(ctx, cypher)
	case findKeywordIndex(cypher, "DROP") == 0:
		// Other DROP variants (not INDEX, not CONSTRAINT, not ALIAS, not PROCEDURE)
		// treat as no-op for forward compatibility.
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
	case findMultiWordKeywordIndex(cypher, "SHOW", "FULLTEXT INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "FULLTEXT INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "RANGE INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "RANGE INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "VECTOR INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "VECTOR INDEX") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowIndexes(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "INDEX") == 0:
		// Must be at start (position 0) to be a standalone clause
		return e.executeShowIndexes(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "DECAY PROFILES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "PROMOTION PROFILES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "PROMOTION POLICIES") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
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
	case findMultiWordKeywordIndex(cypher, "ALTER", "DECAY PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION POLICY") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
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
	// Strip trailing modifiers from RETURN projection (single-row RETURN path).
	// This prevents "ORDER BY ..." from being treated as an additional projection item.
	if cut := firstTopLevelModifierIndex(returnClause); cut >= 0 {
		returnClause = strings.TrimSpace(returnClause[:cut])
	}

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

		// Fabric APPLY correlated bindings (e.g., RETURN textKey, textKey128, texts).
		if isValidIdentifier(part) {
			if v, ok := e.fabricRecordBindings[part]; ok {
				values = append(values, v)
				continue
			}
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

func firstTopLevelModifierIndex(clause string) int {
	cut := -1
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := topLevelKeywordIndex(clause, kw); idx >= 0 && (cut == -1 || idx < cut) {
			cut = idx
		}
	}
	return cut
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
