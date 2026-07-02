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
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
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
		return hasCallSubqueryPattern(query)
	case collectSubqueryRe:
		return hasKeywordFollowedByBrace(query, "COLLECT")
	}
	return false
}

func hasCallSubqueryPattern(query string) bool {
	for i := 0; i < len(query); i++ {
		if !matchKeywordAt(query, i, "CALL") {
			continue
		}
		j := skipSpaces(query, i+len("CALL"))
		if j < len(query) && query[j] == '{' {
			return true
		}
		if j >= len(query) || query[j] != '(' {
			continue
		}
		close := findMatchingCallParen(query, j)
		if close < 0 {
			continue
		}
		k := skipSpaces(query, close+1)
		if k < len(query) && query[k] == '{' {
			return true
		}
	}
	return false
}

func hasCallInTransactions(query string) bool {
	return hasCallSubqueryPattern(query) && findKeywordIndex(query, "IN TRANSACTIONS") >= 0
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
	// This dramatically speeds up repeated MATCH lookups for the same pattern.
	//
	// Transaction scoping: cloneWithStorage gives transactional clones a
	// FRESH cache + mutex so concurrent transactions can't see each other's
	// uncommitted MERGE node-ID mappings. Without that scoping, two writers
	// MERGE-ing on the same (label, prop, value) would each populate the
	// shared cache pre-commit; the peer would then probe its own
	// tx.badgerTx for the cached uncommitted node ID, taking the peer's
	// node key into its read set, and Badger SSI would reject the loser
	// with "Transaction Conflict" instead of the consumer-pinned
	// commit-time UNIQUE shape.
	nodeLookupCache   map[string]*storage.Node
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
	// upperQueryCache memoizes uppercase routing forms for exact query text
	// to avoid repeated strings.ToUpper allocations on hot query shapes.
	//
	// Initialized lazily via upperQueryCacheOnce so concurrent CALL { ... }
	// subqueries that share an executor pointer don't race on the lazy
	// install. See ensureUpperQueryCache for the matching helper.
	upperQueryCache     *upperQueryCache
	upperQueryCacheOnce sync.Once
	// syntaxValidationCache memoizes successful Nornic-parser syntax checks
	// for exact query text to avoid repeated bracket/string scans on hot loops.
	//
	// The cache pointer is lazily installed via syntaxValidationOnce so that
	// concurrent callers in parallel CALL { ... } / executeCallTailParallel
	// do not race on the pointer write — the goroutine fanout in call.go
	// previously triggered a data race detected by `go test -race`.
	syntaxValidationCache *syntaxValidationCache
	syntaxValidationOnce  sync.Once

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

type upperQueryCache struct {
	mu    sync.RWMutex
	cache map[string]string
	max   int
}

type syntaxValidationCache struct {
	mu    sync.RWMutex
	cache map[string]struct{}
	max   int
}

func (e *StorageExecutor) cloneWithStorage(override storage.Engine) *StorageExecutor {
	e.ensureNodeLookupCache()
	// Transactional clones use the lookup cache pinned to the
	// transactionStorageWrapper. Concurrent transactions hold distinct
	// wrappers and therefore distinct caches — that isolation prevents
	// a peer's uncommitted node-ID mapping from leaking into this
	// transaction's tx.badgerTx read set (and corrupting Badger SSI
	// conflict shapes). Re-entrant Execute calls within the same
	// transaction reuse the wrapper, so the in-tx cache survives
	// across clauses. On successful commit, callers promote the
	// wrapper-scoped entries back into the parent executor via
	// promoteNodeLookupCacheTo so subsequent Execute calls still
	// benefit from the cross-query speedup.
	lookupCache := e.nodeLookupCache
	lookupCacheMu := e.nodeLookupCacheLock()
	if txWrapper, isTxScoped := override.(*transactionStorageWrapper); isTxScoped {
		// Seed the wrapper-scoped cache from the parent's committed
		// entries on first clone so subsequent Execute calls retain
		// the cross-query speedup. Concurrent transactions still hold
		// distinct wrappers — the seeding is a one-shot copy, not a
		// live alias, so peer-tx writes after this point cannot leak.
		txWrapper.ensureNodeLookupCacheLocked(e)
		lookupCache = txWrapper.txNodeLookupCache
		lookupCacheMu = txWrapper.txNodeLookupCacheMu
	}
	return &StorageExecutor{
		parser:                      e.parser,
		storage:                     override,
		txContext:                   e.txContext,
		cache:                       e.cache,
		planCache:                   e.planCache,
		fabricPlanCache:             e.fabricPlanCache,
		analyzer:                    e.analyzer,
		nodeLookupCache:             lookupCache,
		nodeLookupCacheMu:           lookupCacheMu,
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
		upperQueryCache:             e.upperQueryCache,
		syntaxValidationCache:       e.syntaxValidationCache,
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

// notifyNodeMutated updates live search metadata and calls the onNodeMutated
// callback if set. Call after any node creation or mutation (CREATE, MERGE,
// SET, REMOVE) so search sees client-supplied vectors immediately and the
// embed queue can re-process.
func (e *StorageExecutor) notifyNodeMutated(nodeID string) {
	if e.searchService != nil && nodeID != "" {
		if node, err := e.storage.GetNode(storage.NodeID(nodeID)); err == nil && node != nil {
			_ = e.searchService.IndexNode(node)
		}
	}
	if e.onNodeMutated != nil {
		e.onNodeMutated(nodeID)
	}
}

// notifyEdgeMutated updates live search metadata after a relationship create or
// mutation so relationship vector queries can use client-supplied vectors before
// a full search warmup/build has run.
func (e *StorageExecutor) notifyEdgeMutated(edgeID string) {
	if e.searchService == nil || edgeID == "" {
		return
	}
	if edge, err := e.storage.GetEdge(storage.EdgeID(edgeID)); err == nil && edge != nil {
		e.indexMutatedEdge(edge)
	}
}

func (e *StorageExecutor) indexMutatedEdge(edge *storage.Edge) {
	if e.searchService != nil && edge != nil {
		_ = e.searchService.IndexEdge(edge)
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
	defer func() {
		if retErr == nil && result != nil {
			e.recordMaterializedResultAccess(result)
		}
	}()

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
	upperQuery := e.cachedUpperQuery(cypher)

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
	// Fast path: common ASCII-only Cypher text has no confusables to normalize.
	if isLikelyPlainASCIICypher(query) {
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

func isLikelyPlainASCIICypher(query string) bool {
	for i := 0; i < len(query); i++ {
		if query[i] >= 0x80 {
			return false
		}
	}
	return true
}

// ensureUpperQueryCache lazily installs the upper-query cache pointer with
// sync.Once so concurrent CALL { ... } subqueries cannot race on the
// pointer assignment. The cache itself is mutex-guarded for entry access.
func (e *StorageExecutor) ensureUpperQueryCache() *upperQueryCache {
	e.upperQueryCacheOnce.Do(func() {
		if e.upperQueryCache == nil {
			e.upperQueryCache = &upperQueryCache{
				cache: make(map[string]string, 1024),
				max:   4096,
			}
		}
	})
	return e.upperQueryCache
}

func (e *StorageExecutor) cachedUpperQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return ""
	}
	c := e.ensureUpperQueryCache()
	c.mu.RLock()
	if upper, ok := c.cache[trimmed]; ok {
		c.mu.RUnlock()
		return upper
	}
	c.mu.RUnlock()

	upper := strings.ToUpper(trimmed)
	c.mu.Lock()
	if len(c.cache) >= c.max {
		for k := range c.cache {
			delete(c.cache, k)
			break
		}
	}
	c.cache[trimmed] = upper
	c.mu.Unlock()
	return upper
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

	createClauses := SplitByCreate(createPart)
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
		nodePattern := e.parseNodePattern(ctx, nodePatternStr)

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
					row[i] = e.resolveReturnItem(ctx, item, variable, node)
					break
				}
			}

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
		if hasCallInTransactions(cypher) {
			return e.executeWithoutTransaction(ctx, cypher, upperQuery)
		}
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

// executeWithImplicitTransaction wraps a write query in a single implicit
// transaction. Commit-time conflicts are returned to the caller; retry-aware
// clients own any replay decision because NornicDB does not know whether a
// conflict is recoverable for the application.
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
	if engines.namespace != "" {
		if primer, ok := txEngine.(interface{ EnsureNamespaceMVCC(string) error }); ok {
			if err := primer.EnsureNamespaceMVCC(engines.namespace); err != nil {
				return nil, fmt.Errorf("failed to prime implicit transaction namespace: %w", err)
			}
		}
	}
	tx, err := txEngine.BeginTransaction()
	if err != nil {
		return nil, fmt.Errorf("failed to start implicit transaction: %w", err)
	}
	if engines.namespace != "" {
		if err := tx.SetNamespace(engines.namespace); err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("failed to pin implicit transaction namespace: %w", err)
		}
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
	// Skip the per-commit engine.Sync(). The Bolt session's end-of-session
	// Flush and the async engine's ticker-driven flush coalesce durability
	// for implicit writes; forcing an Msync per UNWIND batch turned every
	// batch into a 300µs syscall for no user-visible benefit.
	if err := tx.SetImplicit(true); err != nil {
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
		if info := e.analyzer.Analyze(cypher); IsRetrySafeMergeCommitQuery(info) {
			err = nornicerrors.MarkMergeCommitTimeUniqueConflict(err)
		}
		// Wire contract: substring "commit failed" is matched by downstream Bolt classifiers.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.1.
		// The implicit-autocommit path was historically wrapped with "failed to commit
		// implicit transaction: ..." which broke the consumer-pinned classifier; aligned
		// with pkg/cypher/transaction.go:181 so the explicit and implicit paths produce the
		// same wire shape.
		return nil, fmt.Errorf("commit failed: %w", err)
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

	// Promote the tx-scoped MERGE lookup cache into the parent so
	// subsequent Execute calls still benefit from the cross-query
	// speedup. Tx isolation is preserved because each in-flight tx had
	// its own clone; only post-commit entries graduate to the parent.
	txExec.promoteNodeLookupCacheTo(e)

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
