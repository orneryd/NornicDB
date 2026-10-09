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
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	apoccfg "github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
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
	return firstTopLevelCallSubquery(query) >= 0
}

// firstTopLevelCallSubquery returns the offset of the first CALL { } or
// CALL (vars) { } clause of query itself: outside strings, quoted names and
// brackets, so a CALL inside an EXISTS / COUNT / COLLECT subquery or another
// CALL body is not the statement's (#652). -1 when there is none.
func firstTopLevelCallSubquery(query string) int {
	depth := 0
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\'', '"', '`':
			i = skipQuotedSemanticText(query, i) - 1
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case 'C', 'c':
			if depth == 0 && callSubqueryAt(query, i) {
				return i
			}
		}
	}
	return -1
}

// callSubqueryAt reports whether a CALL { } or scoped CALL (vars) { }
// subquery clause starts at offset i of query.
func callSubqueryAt(query string, i int) bool {
	if !matchKeywordAt(query, i, "CALL") {
		return false
	}
	j := skipSpaces(query, i+len("CALL"))
	if j < len(query) && query[j] == '{' {
		return true
	}
	if j >= len(query) || query[j] != '(' {
		return false
	}
	close := findMatchingCallParen(query, j)
	if close < 0 {
		return false
	}
	k := skipSpaces(query, close+1)
	return k < len(query) && query[k] == '{'
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
	if i > 0 && isIdentByte(s[i-1]) {
		return false
	}
	if i+len(keyword) < len(s) && isIdentByte(s[i+len(keyword)]) {
		return false
	}
	return strings.EqualFold(s[i:i+len(keyword)], keyword)
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

// SettingsSnapshot contains configured and active values for SHOW SETTINGS.
type SettingsSnapshot struct {
	Configured map[string]string
	Active     map[string]string
}

// SettingsResolver returns the current database-scoped settings snapshot.
type SettingsResolver func() SettingsSnapshot

type StorageExecutor struct {
	parser    *Parser
	storage   storage.Engine
	txContext *TransactionContext // Active transaction context
	// sharedExecutor marks a cached per-database executor that every
	// auto-commit client of the database runs on (server cached executors,
	// the embedded DB's base executor). A statement must never leave such an
	// executor inside a transaction: bare transaction commands are rejected
	// there, and one-statement scripts run on a private clone. Protocol
	// transaction owners run on their own per-session executors, which are
	// never marked shared.
	sharedExecutor bool
	cache          *SmartQueryCache // Query result cache with label-aware invalidation
	// Query cache policy is immutable and scoped to this executor's database.
	queryCacheMaxEntries         int
	queryCacheTTL                time.Duration
	planCache                    *QueryPlanCache // Parsed query plan cache
	semanticValidationCache      *semanticValidationCache
	matchSemanticValidationCache *semanticValidationCache
	mergeSemanticValidationCache *semanticValidationCache
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

	// localizationRenderer renders descriptor-backed metadata using request context.
	localizationRenderer ProcedureMetadataRenderer
	settingsResolver     SettingsResolver
	queryStatistics      *queryStatisticsCollector

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
	// shellParams is the bucket for contexts without a caller identity;
	// shellParamsByToken holds one bucket per authenticated caller so one
	// client's parameters can never be read or overwritten by another.
	shellParams        map[string]interface{}
	shellParamsByToken map[string]map[string]interface{}
	shellParamsMu      sync.RWMutex

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
	log atomic.Pointer[slog.Logger]

	// slowQueryThreshold gates the D-04c slow-query emission path. Zero or
	// negative values disable slow-query logging entirely. Set via
	// SetSlowQueryThreshold so the configured cfg.Logging.SlowQueryThreshold
	// flows in from the bootstrap site without breaking the ctor.
	slowQueryThreshold time.Duration

	// allowLocalAPOCImportFileAccess gates non-HTTP APOC load/import file reads.
	// Default deny; enable explicitly via env-backed APOC config or setter.
	allowLocalAPOCImportFileAccess bool
	// allowLocalAPOCExportFileAccess gates non-HTTP APOC export file writes.
	// Default deny; enable explicitly via env-backed APOC config or setter.
	allowLocalAPOCExportFileAccess bool
	// allowRemoteAPOCURLAccess gates APOC HTTP(S) fetches. Default deny.
	allowRemoteAPOCURLAccess bool
	// apocRemoteURLAllowlist restricts APOC HTTP(S) fetches to explicitly
	// approved hosts or wildcard suffixes.
	apocRemoteURLAllowlist []string
	// apocRemoteHTTPClient is executor-scoped so tests and database-scoped
	// executors cannot race by replacing process-global HTTP transport state.
	apocRemoteHTTPClient *http.Client
	// apocRemoteHostResolver is executor-scoped for the same reason and keeps
	// URL-security tests independent from ambient DNS.
	apocRemoteHostResolver apocHostResolver
	// apocLocalFileAccessRoot mirrors Neo4j's import-directory behavior: when
	// set, local APOC file URLs are normalized and rebased under this root.
	apocLocalFileAccessRoot string

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
		parser:                         e.parser,
		storage:                        override,
		txContext:                      e.txContext,
		sharedExecutor:                 e.sharedExecutor,
		cache:                          e.cache,
		queryCacheMaxEntries:           e.queryCacheMaxEntries,
		queryCacheTTL:                  e.queryCacheTTL,
		planCache:                      e.planCache,
		semanticValidationCache:        e.semanticValidationCache,
		matchSemanticValidationCache:   e.matchSemanticValidationCache,
		mergeSemanticValidationCache:   e.mergeSemanticValidationCache,
		fabricPlanCache:                e.fabricPlanCache,
		analyzer:                       e.analyzer,
		nodeLookupCache:                lookupCache,
		nodeLookupCacheMu:              lookupCacheMu,
		deferFlush:                     e.deferFlush,
		embedder:                       e.embedder,
		searchService:                  e.searchService,
		inferenceManager:               e.inferenceManager,
		localizationRenderer:           e.localizationRenderer,
		settingsResolver:               e.settingsResolver,
		queryStatistics:                e.queryStatistics,
		onNodeMutated:                  e.onNodeMutated,
		inlineEmbeddingTextOptions:     e.inlineEmbeddingTextOptions,
		inlineEmbeddingChunkSize:       e.inlineEmbeddingChunkSize,
		inlineEmbeddingChunkOverlap:    e.inlineEmbeddingChunkOverlap,
		defaultEmbeddingDimensions:     e.defaultEmbeddingDimensions,
		dbManager:                      e.dbManager,
		shellParams:                    e.shellParams,
		shellParamsByToken:             cloneShellParamsByToken(e),
		vectorRegistry:                 e.vectorRegistry,
		vectorIndexSpaces:              e.vectorIndexSpaces,
		fabricRecordBindings:           e.fabricRecordBindings,
		hotPathTraceState:              e.hotPathTraceState,
		vectorQueryEmbedCache:          e.vectorQueryEmbedCache,
		vectorQueryEmbedInflight:       e.vectorQueryEmbedInflight,
		unwindMergeChainPlanCache:      e.unwindMergeChainPlanCache,
		upperQueryCache:                e.upperQueryCache,
		syntaxValidationCache:          e.syntaxValidationCache,
		allowLocalAPOCImportFileAccess: e.allowLocalAPOCImportFileAccess,
		allowLocalAPOCExportFileAccess: e.allowLocalAPOCExportFileAccess,
		allowRemoteAPOCURLAccess:       e.allowRemoteAPOCURLAccess,
		apocRemoteURLAllowlist:         append([]string(nil), e.apocRemoteURLAllowlist...),
		apocRemoteHTTPClient:           e.apocRemoteHTTPClient,
		apocRemoteHostResolver:         e.apocRemoteHostResolver,
		apocLocalFileAccessRoot:        e.apocLocalFileAccessRoot,
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

// DatabaseIdentityProvider optionally supplies persisted administrative identities.
// DatabaseIdentity returns a database UUID and its actual creation time;
// ServerIdentity returns the installation UUID and its creation time.
// Unknown identities use an empty string and unknown times use time.Time{}.
// SHOW DATABASES and dbms.info use this provider without requiring custom
// DatabaseManagerInterface implementations to support identity metadata.
type DatabaseIdentityProvider interface {
	DatabaseIdentity(name string) (string, time.Time)
	ServerIdentity() (string, time.Time)
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

type typedQueryEmbedder interface {
	EmbedWithInputType(ctx context.Context, text, inputType string) ([]float32, error)
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
	runtimeCfg := config.LoadFromEnv()
	maxEntries := runtimeCfg.Memory.QueryCacheSize
	if !runtimeCfg.Memory.QueryCacheEnabled {
		maxEntries = 0
	}
	return newStorageExecutor(store, runtimeCfg, maxEntries, runtimeCfg.Memory.QueryCacheTTL)
}

// NewStorageExecutorWithQueryCachePolicy creates an executor whose query-result
// cache capacity and lifetime are scoped to the executor's database. Zero max
// entries disables caching. A non-positive TTL uses the process default.
func NewStorageExecutorWithQueryCachePolicy(store storage.Engine, maxEntries int, queryCacheTTL time.Duration) *StorageExecutor {
	runtimeCfg := config.LoadFromEnv()
	return newStorageExecutor(store, runtimeCfg, maxEntries, queryCacheTTL)
}

// QueryCachePolicy returns the database-scoped query-result cache capacity and lifetime.
func (e *StorageExecutor) QueryCachePolicy() (maxEntries int, ttl time.Duration) {
	return e.queryCacheMaxEntries, e.queryCacheTTL
}

func newStorageExecutor(store storage.Engine, runtimeCfg *config.Config, maxEntries int, queryCacheTTL time.Duration) *StorageExecutor {
	if queryCacheTTL <= 0 {
		queryCacheTTL = runtimeCfg.Memory.QueryCacheTTL
	}
	if queryCacheTTL <= 0 {
		queryCacheTTL = 5 * time.Minute
	}
	apocCfg := apoccfg.LoadFromEnv()
	var queryCache *SmartQueryCache
	if maxEntries > 0 {
		queryCache = NewSmartQueryCache(maxEntries)
	}
	exec := &StorageExecutor{
		parser:                         NewParser(),
		storage:                        store,
		cache:                          queryCache,
		queryCacheMaxEntries:           maxEntries,
		queryCacheTTL:                  queryCacheTTL,
		planCache:                      NewQueryPlanCache(500), // Cache 500 parsed query plans
		semanticValidationCache:        newSemanticValidationCache(500),
		matchSemanticValidationCache:   newSemanticValidationCache(500),
		mergeSemanticValidationCache:   newSemanticValidationCache(500),
		fabricPlanCache:                fabric.NewPlanCache(500), // Cache 500 Fabric fragment plans
		analyzer:                       NewQueryAnalyzer(1000),   // Cache 1000 parsed query ASTs
		nodeLookupCache:                make(map[string]*storage.Node, 1000),
		nodeLookupCacheMu:              &sync.RWMutex{},
		shellParams:                    make(map[string]interface{}),
		searchService:                  nil, // Lazy initialization - will be set via SetSearchService() to reuse DB's cached service
		vectorRegistry:                 vectorspace.NewIndexRegistry(),
		vectorIndexSpaces:              make(map[string]vectorspace.VectorSpaceKey),
		hotPathTraceState:              &hotPathTraceState{},
		vectorQueryEmbedCache:          make(map[string][]float32, 512),
		vectorQueryEmbedInflight:       make(map[string]*vectorEmbedInflight, 64),
		unwindMergeChainPlanCache:      &unwindMergeChainPlanCache{plans: make(map[string]unwindMergeChainPlan, 128)},
		inlineEmbeddingTextOptions:     embeddingutil.EmbedTextOptionsFromConfig(runtimeCfg),
		inlineEmbeddingChunkSize:       maxInt(runtimeCfg.EmbeddingWorker.ChunkSize, 1),
		inlineEmbeddingChunkOverlap:    maxInt(runtimeCfg.EmbeddingWorker.ChunkOverlap, 0),
		allowLocalAPOCImportFileAccess: apocCfg.Security.AllowImportFileAccess || apocCfg.Security.AllowFileAccess,
		allowLocalAPOCExportFileAccess: apocCfg.Security.AllowExportFileAccess || apocCfg.Security.AllowFileAccess,
		allowRemoteAPOCURLAccess:       apocCfg.Security.AllowRemoteURLAccess,
		apocRemoteURLAllowlist:         normalizeAPOCRemoteURLAllowlist(apocCfg.Security.RemoteURLAllowlist),
		apocLocalFileAccessRoot:        strings.TrimSpace(apocCfg.Security.FileAccessRoot),
	}
	exec.queryStatistics = (&queryStatisticsRegistry{}).forDatabase(exec.currentDatabaseName())
	ensureBuiltInProceduresRegistered()
	_ = exec.loadPersistedProcedures()
	return exec
}

// SetLocalizationRenderer sets the immutable renderer used for procedure metadata.
func (e *StorageExecutor) SetLocalizationRenderer(renderer ProcedureMetadataRenderer) {
	e.localizationRenderer = renderer
}

// SetSettingsResolver configures database-scoped values for SHOW SETTINGS.
func (e *StorageExecutor) SetSettingsResolver(resolver SettingsResolver) {
	e.settingsResolver = resolver
}

// GetLocalizationRenderer returns the renderer inherited by scoped executors.
func (e *StorageExecutor) GetLocalizationRenderer() ProcedureMetadataRenderer {
	return e.localizationRenderer
}

// SetAllowLocalAPOCFileAccess enables or disables local APOC file reads/writes.
func (e *StorageExecutor) SetAllowLocalAPOCFileAccess(enabled bool) {
	e.allowLocalAPOCImportFileAccess = enabled
	e.allowLocalAPOCExportFileAccess = enabled
}

// SetAllowLocalAPOCImportFileAccess enables or disables local-file APOC loads/imports.
func (e *StorageExecutor) SetAllowLocalAPOCImportFileAccess(enabled bool) {
	e.allowLocalAPOCImportFileAccess = enabled
}

// SetAllowLocalAPOCExportFileAccess enables or disables local-file APOC exports.
func (e *StorageExecutor) SetAllowLocalAPOCExportFileAccess(enabled bool) {
	e.allowLocalAPOCExportFileAccess = enabled
}

// SetAllowRemoteAPOCURLAccess enables or disables APOC HTTP(S) fetches.
func (e *StorageExecutor) SetAllowRemoteAPOCURLAccess(enabled bool) {
	e.allowRemoteAPOCURLAccess = enabled
}

// SetAPOCRemoteURLAllowlist restricts APOC HTTP(S) fetches to approved hosts.
func (e *StorageExecutor) SetAPOCRemoteURLAllowlist(hosts []string) {
	e.apocRemoteURLAllowlist = normalizeAPOCRemoteURLAllowlist(hosts)
}

// SetAPOCLocalFileAccessRoot sets the local import root used for APOC file URLs.
func (e *StorageExecutor) SetAPOCLocalFileAccessRoot(root string) {
	e.apocLocalFileAccessRoot = strings.TrimSpace(root)
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
	if e.nodeLookupCache != nil {
		clear(e.nodeLookupCache)
	} else {
		e.nodeLookupCache = make(map[string]*storage.Node)
	}
	cacheMu.Unlock()
}

// InvalidateCommittedWriteCaches evicts cached graph state after a write was
// committed by another executor sharing the same storage engine. Transaction-
// scoped Bolt executors use this to keep the long-lived read executor coherent
// without discarding its immutable analysis and plan caches.
func (e *StorageExecutor) InvalidateCommittedWriteCaches() {
	if e.cache != nil {
		e.cache.Invalidate()
	}
	e.invalidateNodeLookupCache()
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
	e.log.Store(logger.With("component", "cypher"))
}

// SetSlowQueryThreshold configures the D-04c slow-query emission gate.
// Zero or negative durations disable slow-query logging entirely. Threaded
// from cfg.Logging.SlowQueryThreshold at the bootstrap site.
func (e *StorageExecutor) SetSlowQueryThreshold(d time.Duration) {
	e.slowQueryThreshold = d
}

// SetSharedExecutor marks an executor as the cached, per-database executor
// that every auto-commit client of its database runs on. A shared executor
// rejects bare BEGIN/COMMIT/ROLLBACK statements: a statement must never
// leave the cached executor inside a transaction. Protocol transaction
// owners (Bolt/HTTP sessions) use their own per-session executors and are
// never marked shared. See the sharedExecutor field.
func (e *StorageExecutor) SetSharedExecutor(shared bool) {
	e.sharedExecutor = shared
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
	if logger := e.log.Load(); logger != nil {
		return logger
	}
	fallback := slog.New(slog.NewTextHandler(io.Discard, nil)).With("component", "cypher")
	e.log.CompareAndSwap(nil, fallback)
	return e.log.Load()
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
//	plan_hash=<16-char hex; PlanHash(plan) when a plan tree is available,
//	           else StatementShapeHash of the redacted statement (NornicDB
//	           issue #563 defect 2 — see StatementShapeHash's doc comment: it is
//	           a statement-shape fingerprint, not a plan fingerprint)>
//	cypher.duration_ms=<int64 millisecond delta>
//	query=<RedactLiterals(query) truncated to 500 chars>
//
// Performance: PlanHash/StatementShapeHash + RedactLiterals only fire when
// this method is called, i.e., only when the executor's measured duration
// exceeded the configured threshold. The hot path (Execute fast return)
// never enters this method.
func (e *StorageExecutor) emitSlowQueryLog(query string, plan *ExecutionPlan, duration time.Duration) {
	if e.slowQueryThreshold <= 0 || duration < e.slowQueryThreshold {
		return
	}
	redacted := RedactLiterals(StripComments(query))
	hash := PlanHash(plan)
	if plan == nil || plan.Root == nil {
		// The normal (non-EXPLAIN/PROFILE) Execute() path never builds an
		// ExecutionPlan tree, so PlanHash would otherwise always collapse to
		// the zero placeholder here. Hash on statement shape instead so
		// distinct query shapes are still distinguishable in the log.
		hash = StatementShapeHash(redacted)
	}
	if len(redacted) > 500 {
		redacted = truncateRuneSafe(redacted, 500)
	}
	e.logEvent(slog.LevelWarn, localization.CypherSlowQueryEvent(
		hash,
		duration.Milliseconds(),
		redacted,
	))
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
	if strings.Contains(upperASCII(query), "DETACH DELETE") {
		return true
	}
	// Relationship pattern (has -[...]-> or <-[...]-) with CREATE+DELETE = relationship delete only
	if strings.Contains(query, "]->(") || strings.Contains(query, ")<-[") {
		return false
	}
	return true
}

// trimTrailingStatementDelimiters removes one optional trailing Cypher statement
// delimiter (';') and whitespace, leaving any additional semicolon for validation.
// This mirrors Neo4j-compatible client behavior where a final semicolon is optional.
func trimTrailingStatementDelimiters(query string) string {
	s := strings.TrimSpace(query)
	if strings.HasSuffix(s, ";") {
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}
	return s
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

	upper := upperASCII(trimmed)
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
		case storage.EngineUnwrapper:
			engine = wrapper.GetInnerEngine()
		default:
			engine = nil
		}
	}

	return out
}

func (e *StorageExecutor) tryAsyncCreateNodeBatch(ctx context.Context, cypher string) (*ExecuteResult, error, bool) {
	upper := upperASCII(strings.TrimSpace(cypher))
	if !strings.HasPrefix(upper, "CREATE") {
		return nil, nil, false
	}
	// System commands and schema commands must not be handled here — route to executeSchemaCommand instead
	if startsWithKeywords(cypher, "CREATE", "DATABASE") ||
		isCreateOrReplaceDatabaseQuery(cypher) ||
		startsWithKeywords(cypher, "CREATE", "COMPOSITE DATABASE") ||
		startsWithKeywords(cypher, "CREATE", "ALIAS") ||
		startsWithKeywords(cypher, "CREATE", "CONSTRAINT") ||
		startsWithKeywords(cypher, "CREATE", "INDEX") ||
		startsWithKeywords(cypher, "CREATE", "FULLTEXT") ||
		startsWithKeywords(cypher, "CREATE", "VECTOR") ||
		startsWithKeywords(cypher, "CREATE", "TEXT") ||
		startsWithKeywords(cypher, "CREATE", "POINT") ||
		startsWithKeywords(cypher, "CREATE", "RANGE") {
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

	returnIdx := findKeywordIndex(cypher, "RETURN")
	createPart := cypher
	if returnIdx > 0 {
		createPart = strings.TrimSpace(cypher[:returnIdx])
	}

	// Substitute parameters before parsing so (n:Label $props) becomes (n:Label { ... })
	// and the label is not mis-parsed as "Label $props".
	if params := getParamsFromContext(ctx); params != nil {
		createPart = e.substituteParams(createPart, params)
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
			if patternHasRelationship(pat) {
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
		nodePattern, err := e.prepareCreateNodePattern(ctx, nodePatternStr, createdNodes, nil)
		if err != nil {
			return nil, err, true
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

	labels := make([]string, 0, len(nodes))
	for _, node := range nodes {
		labels = append(labels, node.Labels...)
	}
	if e.writesAreChecked(labels, nil) {
		return nil, nil, false
	}

	if err := e.projectCreateReturn(ctx, &createOutcome{
		cypher:    cypher,
		returnIdx: returnIdx,
		nodes:     createdNodes,
		result:    result,
	}); err != nil {
		return nil, err, true
	}

	if err := e.applyCreatePlan(ctx, &createPlan{nodes: nodes}, result); err != nil {
		return nil, err, true
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

// writesAreChecked reports whether a constraint, property type, contract or
// relationship policy applies to writing nodes with these labels or
// relationships of these types. A CREATE that writes such an entity runs in
// an implicit transaction, checked and committed as one statement, instead of
// on the async write routes (tryAsyncCreateNodeBatch, the eventual CREATE
// route): a violation fails the statement and nothing it wrote is kept, as in
// Neo4j (#700). Other CREATEs keep the async routes.
func (e *StorageExecutor) writesAreChecked(labels []string, relTypes []string) bool {
	schema := e.storage.GetSchema()
	if !schema.HasWriteRules() {
		return false
	}
	if schema.NodeWriteChecked(labels) {
		return true
	}
	for _, relType := range relTypes {
		if schema.EdgeWriteChecked(relType) {
			return true
		}
	}
	return false
}

// executeImplicitAsync executes a single query using implicit transactions for writes.
// For write operations, wraps execution in an implicit transaction that can be
// rolled back on error, preventing partial data corruption from failed queries.
// For strict ACID guarantees with durability, use explicit BEGIN/COMMIT transactions.
func (e *StorageExecutor) executeImplicitAsync(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	// Check if this is a write operation using cached analysis
	info := e.analyzer.Analyze(cypher)
	needsTransaction := info.IsWriteQuery || (matchKeywordAt(cypher, 0, "CALL") && strings.EqualFold(extractProcedureName(cypher), "tx.setMetaData"))

	// For write operations, use implicit transaction for atomicity
	// This ensures partial writes are rolled back on error
	if needsTransaction {
		if hasCallInTransactions(cypher) {
			return e.executeWithoutTransaction(ctx, cypher, upperQuery)
		}
		engines := e.resolveImplicitTxEngines()
		// The async CREATE routes handle a single query; a top-level UNION
		// whose first branch is a CREATE goes to the UNION executor in the
		// implicit transaction below (#781).
		if _, union := topLevelUnion(cypher, upperQuery); engines.asyncEngine != nil && !union {
			if result, err, handled := e.tryAsyncCreateNodeBatch(ctx, cypher); handled {
				return result, err
			}
			if e.isEventualAsyncEligible(info, cypher) && !e.writesAreChecked(info.Labels, info.RelationshipTypes) &&
				!(strings.Contains(cypher, "$(") && e.storage.GetSchema().HasWriteRules()) {
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
	executionCypher, executionUpperQuery := cypher, upperQuery
	if parsedCypher, inlineEmbeddingEnabled := stripWithEmbeddingSuffix(cypher); inlineEmbeddingEnabled {
		executionCypher = parsedCypher
		executionUpperQuery = upperASCII(parsedCypher)
	}
	return e.executeWithImplicitTransactionCallback(ctx, cypher, upperQuery, func(txCtx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
		return txExec.executeWithoutTransaction(txCtx, executionCypher, executionUpperQuery)
	})
}

func (e *StorageExecutor) executeWithImplicitTransactionCallback(ctx context.Context, cypher string, upperQuery string, execute func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) {
	parsedCypher, inlineEmbeddingEnabled := stripWithEmbeddingSuffix(cypher)
	if inlineEmbeddingEnabled {
		cypher = parsedCypher
		upperQuery = upperASCII(cypher)
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
			return nil, localizedError(localization.CypherCoreEmbeddingTransactionStorageRequired(), nil)
		}
		result, err := execute(ctx, e)
		if err != nil {
			return result, err
		}
		// Flush if needed
		if !e.deferFlush {
			if asyncEngine != nil {
				asyncEngine.Flush()
			}
		}
		return result, nil
	}

	// Start implicit transaction
	if engines.namespace != "" {
		if primer, ok := txEngine.(interface{ EnsureNamespaceMVCC(string) error }); ok {
			if err := primer.EnsureNamespaceMVCC(engines.namespace); err != nil {
				return nil, localizedError(localization.CypherCoreImplicitTransactionPrimeFailed(err), err)
			}
		}
	}
	tx, err := beginTransactionSnapshot(asyncEngine, txEngine)
	if err != nil {
		return nil, localizedError(localization.CypherCoreImplicitTransactionStartFailed(err), err)
	}
	if engines.namespace != "" {
		if err := tx.SetNamespace(engines.namespace); err != nil {
			_ = tx.Rollback()
			return nil, localizedError(localization.CypherCoreImplicitTransactionPinFailed(err), err)
		}
	}

	// Defer constraint validation to commit for implicit transactions.
	// This avoids duplicate per-operation checks and improves write throughput.
	if err := tx.SetDeferredConstraintValidation(true); err != nil {
		_ = tx.Rollback()
		return nil, localizedError(localization.CypherCoreImplicitTransactionConfigureFailed(err), err)
	}
	if err := tx.SetSkipCreateExistenceCheck(true); err != nil {
		_ = tx.Rollback()
		return nil, localizedError(localization.CypherCoreImplicitTransactionConfigureFailed(err), err)
	}
	// Skip the per-commit engine.Sync(). The Bolt session's end-of-session
	// Flush and the async engine's ticker-driven flush coalesce durability
	// for implicit writes; forcing an Msync per UNWIND batch turned every
	// batch into a 300µs syscall for no user-visible benefit.
	if err := tx.SetImplicit(true); err != nil {
		_ = tx.Rollback()
		return nil, localizedError(localization.CypherCoreImplicitTransactionConfigureFailed(err), err)
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
				return nil, localizedError(localization.CypherCoreImplicitTransactionWALBeginFailed(err), err)
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
	result, execErr := execute(txCtx, txExec)
	// An expression error recorded while the statement ran is its error, so
	// nothing it wrote is committed.
	if execErr == nil {
		execErr = getExpressionFailure(txCtx)
	}

	// Handle result
	if execErr != nil {
		// Rollback on any error - prevents partial data corruption
		tx.Rollback()
		txExec.invalidateNodeLookupCache()
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, execErr.Error())
		}
		return result, execErr
	}

	// A write-shaped query can legitimately match no mutation targets. Committing
	// an empty Badger transaction still performs store-wide validation work, so
	// roll it back after the match has completed instead.
	if tx.OperationCount() == 0 && !tx.HasKnowledgePolicyChanges() {
		_ = tx.Rollback()
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, "no mutations")
		}
		return result, nil
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

	// A statement TERMINATE TRANSACTIONS ended doesn't commit (#718).
	if running, ok := ctx.Value(ctxKeyRunningTransaction{}).(*runningTransaction); ok && running.terminated.Load() {
		tx.Rollback()
		txExec.invalidateNodeLookupCache()
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, "terminated")
		}
		return nil, transactionTerminatedError()
	}

	// Commit successful transaction
	if err := tx.Commit(); err != nil {
		txExec.invalidateNodeLookupCache()
		if wal != nil && walSeqStart > 0 {
			_, _ = wal.AppendTxAbort(dbName, txID, err.Error())
		}
		if info := e.analyzer.Analyze(cypher); IsRetrySafeMergeCommitQuery(info) && MergeUniqueConflictIsRetrySafe([]CommitStatement{{Query: cypher, Params: getParamsFromContext(ctx)}}, err) {
			err = nornicerrors.MarkMergeCommitTimeUniqueConflict(err)
		}
		// Wire contract: substring "commit failed" is matched by downstream Bolt classifiers.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.1.
		// The implicit-autocommit path was historically wrapped with "failed to commit
		// implicit transaction: ..." which broke the consumer-pinned classifier; aligned
		// with pkg/cypher/transaction.go:181 so the explicit and implicit paths produce the
		// same wire shape.
		return nil, localizedError(localization.CypherCoreImplicitTransactionCommitFailed(err), err)
	}
	// Committed: a TERMINATE from now on doesn't fail this statement (#751).
	if running, ok := ctx.Value(ctxKeyRunningTransaction{}).(*runningTransaction); ok {
		running.committed.Store(true)
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
		return localizedError(localization.CypherCoreEmbeddingConfiguredRequired(), nil)
	}
	store := e.getStorage(ctx)
	typedEmbedder, hasTypedEmbedder := e.embedder.(typedQueryEmbedder)
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
			return localizedError(localization.CypherCoreEmbeddingChunkFailed(id, err), err)
		}
		if len(chunks) == 0 {
			chunks = []string{text}
		}
		embeddings := make([][]float32, 0, len(chunks))
		for _, chunk := range chunks {
			var emb []float32
			var err error
			if hasTypedEmbedder {
				emb, err = typedEmbedder.EmbedWithInputType(ctx, chunk, "document")
			} else {
				emb, err = e.embedder.Embed(ctx, chunk)
			}
			if err != nil {
				return localizedError(localization.CypherCoreEmbeddingNodeFailed(id, err), err)
			}
			if len(emb) == 0 {
				return localizedError(localization.CypherCoreEmbeddingEmptyVector(id), nil)
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

type ctxKeyAuthenticatedPrincipalType struct{}

var ctxKeyAuthenticatedPrincipal = ctxKeyAuthenticatedPrincipalType{}

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

// WithAuthenticatedPrincipal attaches an identity produced by successful authentication.
func WithAuthenticatedPrincipal(ctx context.Context, principal string) context.Context {
	if strings.TrimSpace(principal) == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyAuthenticatedPrincipal, principal)
}

// GetAuthenticatedPrincipalFromContext returns the trusted caller identity.
func GetAuthenticatedPrincipalFromContext(ctx context.Context) string {
	if value, ok := ctx.Value(ctxKeyAuthenticatedPrincipal).(string); ok {
		return value
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
	visited := make(map[storage.Engine]bool)

	for engine != nil && !visited[engine] {
		visited[engine] = true
		if ns, ok := engine.(interface{ Namespace() string }); ok && dbName == "" {
			dbName = ns.Namespace()
		}
		if walProvider, ok := engine.(interface{ GetWAL() *storage.WAL }); ok {
			return walProvider.GetWAL(), dbName
		}
		switch wrapper := engine.(type) {
		case storage.EngineUnwrapper:
			engine = wrapper.GetInnerEngine()
		default:
			return nil, dbName
		}
	}

	return nil, dbName
}

// statementAccessesGraph reports whether a statement reads or writes graph
// data: a pattern clause (MATCH, CREATE, MERGE, …), a write clause, a
// procedure call or LOAD CSV. A statement of only UNWIND / WITH / RETURN
// over values doesn't.
func statementAccessesGraph(info *QueryInfo) bool {
	return info.HasMatch || info.HasOptionalMatch || info.HasCreate || info.HasMerge ||
		info.HasDelete || info.HasDetachDelete || info.HasSet || info.HasRemove ||
		info.HasForeach || info.HasLoadCSV || info.HasShortestPath || info.HasCall ||
		info.HasShow || info.HasSchema
}

// statementAccessesGraphText conservatively reports whether a statement reads
// or writes graph data, scanning the text itself with the shared keyword
// scanner. The composite-root guard uses it instead of the caching analyzer:
// the analyzer's clause flags missed pattern comprehensions
// (`[(n)-->(m) | …]`) and pattern subqueries (`COUNT { }`, `EXISTS { }`,
// `EXISTS((n)-->(m))`), which read constituent data on a composite root. The
// direction is safe by construction: a false positive only forces the caller
// to target a constituent (and pass its authorization), while a false
// negative would read denied data.
func statementAccessesGraphText(cypher string) bool {
	opts := defaultKeywordScanOpts()
	for _, keyword := range []string{
		"MATCH", "OPTIONAL MATCH", "CREATE", "MERGE", "DELETE", "DETACH DELETE",
		"SET", "REMOVE", "FOREACH", "LOAD CSV", "CALL", "SHOW", "CREATE INDEX",
		"CREATE RANGE INDEX", "CREATE FULLTEXT INDEX", "CREATE VECTOR INDEX",
		"CREATE CONSTRAINT", "DROP INDEX", "DROP CONSTRAINT",
	} {
		if keywordIndexFrom(cypher, keyword, 0, opts) >= 0 {
			return true
		}
	}
	upper := upperASCII(cypher)
	if strings.Contains(upper, "SHORTESTPATH") || strings.Contains(upper, "ALLSHORTESTPATHS") {
		return true
	}
	// COUNT { … } and EXISTS { … } are pattern subqueries over the graph.
	for _, keyword := range []string{"COUNT", "EXISTS"} {
		for from := 0; ; {
			index := keywordIndexFrom(cypher, keyword, from, opts)
			if index < 0 {
				break
			}
			end := index + len(keyword)
			open := queryGapEnd(cypher, end)
			if open < len(cypher) && cypher[open] == '{' {
				return true
			}
			// EXISTS((n)-->(m)): a pattern between the parentheses.
			if keyword == "EXISTS" && open < len(cypher) && cypher[open] == '(' {
				if close := findMatchingDelimiter(cypher, open, '(', ')'); close > open {
					inner := strings.TrimSpace(cypher[open+1 : close])
					if strings.ContainsAny(inner, "-><") || strings.HasPrefix(inner, "(") {
						return true
					}
				}
			}
			from = end
		}
	}
	// A pattern comprehension starts with '[' followed (after gaps) by a
	// node or relationship pattern: '[(' … '|'. A subscript such as arr[(i)]
	// has the '[' glued directly to the expression atom it indexes, so it
	// does not qualify; whitespace before the '[' means the bracket starts a
	// new construct, which in '[' + '(' position is a pattern comprehension.
	for i := 0; i < len(cypher); i++ {
		switch cypher[i] {
		case '\'', '"', '`':
			i = skipCypherQuotedText(cypher, i, cypher[i]) - 1
			continue
		case '/':
			if end := queryCommentEnd(cypher, i); end >= 0 {
				i = end - 1
				continue
			}
		}
		if cypher[i] != '[' {
			continue
		}
		next := queryGapEnd(cypher, i+1)
		if next >= len(cypher) || cypher[next] != '(' {
			continue
		}
		if i > 0 && !isASCIISpace(cypher[i-1]) && (isIdentByte(cypher[i-1]) || cypher[i-1] == ']' || cypher[i-1] == ')' ||
			cypher[i-1] == '\'' || cypher[i-1] == '"' || cypher[i-1] == '`' || cypher[i-1] == '$') {
			continue
		}
		return true
	}
	return false
}

// statementParametersError is Neo4j's ParameterMissing for a statement that
// references a parameter it wasn't given ("Expected parameter(s): p"), or nil.
// Neo4j reports it after the statement compiles, so a statement that is also
// a syntax or semantic error reports that error (Execute calls this after
// validateSyntax and validateSemanticScopes; the Fabric route, which has no
// such validation, calls it first). EXPLAIN runs nothing and needs no
// values; in a procedure definition, $name is the procedure's argument.
// Parameter names are reported as the statement wrote them (#734).
func (e *StorageExecutor) statementParametersError(ctx context.Context, cypher string, params map[string]interface{}) error {
	if strings.IndexByte(cypher, '$') < 0 || (isExplainOrProfile(cypher) && !startsWithKeywordFold(strings.TrimSpace(cypher), "PROFILE")) || isCreateProcedureCommand(cypher) {
		return nil
	}
	shellParams := e.mergeShellParams(ctx, params)
	inherited := getParamsFromContext(ctx)
	missing := statementMissingParameters(cypher, func(name string) bool {
		if _, ok := shellParams[name]; ok {
			return true
		}
		_, ok := inherited[name]
		return ok
	})
	if len(missing) == 0 {
		return nil
	}
	names := quotedVariableNamesFor(ctx, cypher)
	for i, name := range missing {
		missing[i] = names.parameterName(name)
	}
	return parameterMissingError(missing)
}
