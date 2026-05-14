// Package storage provides storage engine implementations for NornicDB.
//
// BadgerEngine provides persistent disk-based storage using BadgerDB.
// It implements the Engine interface with full ACID transaction support.
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// Key prefixes for BadgerDB storage organization
// Using single-byte prefixes for efficiency
const (
	prefixNode              = byte(0x01) // nodes:nodeID -> Node
	prefixEdge              = byte(0x02) // edges:edgeID -> Edge
	prefixLabelIndex        = byte(0x03) // label:labelName:nodeID -> []byte{}
	prefixOutgoingIndex     = byte(0x04) // outgoing:nodeID:edgeID -> []byte{}
	prefixIncomingIndex     = byte(0x05) // incoming:nodeID:edgeID -> []byte{}
	prefixEdgeTypeIndex     = byte(0x06) // edgetype:type:edgeID -> []byte{} (for fast type lookups)
	prefixPendingEmbed      = byte(0x07) // pending_embed:nodeID -> []byte{} (nodes needing embedding)
	prefixEmbedding         = byte(0x08) // embedding:nodeID:chunkIndex -> []float32 (separate storage for large embeddings)
	prefixSchema            = byte(0x09) // schema:global -> JSON(SchemaDefinition)
	prefixTemporalIndex     = byte(0x0A) // temporal:namespace:label:keyprops:keyhash:valid_from:nodeID -> []byte{}
	prefixTemporalHead      = byte(0x0B) // temporal_current:namespace:label:keyprops:keyhash -> nodeID
	prefixMVCCNode          = byte(0x0C) // mvcc_node:nodeID:version -> Node version payload
	prefixMVCCEdge          = byte(0x0D) // mvcc_edge:edgeID:version -> Edge version payload
	prefixMVCCNodeHead      = byte(0x0E) // mvcc_node_head:nodeID -> MVCCHead
	prefixMVCCEdgeHead      = byte(0x0F) // mvcc_edge_head:edgeID -> MVCCHead
	prefixMVCCMeta          = byte(0x10) // mvcc_meta:* -> MVCC metadata (sequence, rebuild markers)
	prefixAccessMeta        = byte(0x11) // accessmeta:entityID -> msgpack(AccessMetaEntry)
	prefixIndexEntryCatalog = byte(0x12) // idxcat:entityID -> msgpack(IndexEntryCatalog)
	prefixDeindexWorkItem   = byte(0x13) // deindexwork:workItemID -> msgpack(DeindexWorkItem)
	prefixDecayProfile      = byte(0x14) // decayprofile:name -> msgpack(DecayProfileDef)
	prefixPromotionProfile  = byte(0x15) // promoprofile:name -> msgpack(PromotionProfileDef)
	prefixPromotionPolicy   = byte(0x16) // promopolicy:name -> msgpack(PromotionPolicyDef)
	prefixIndexTombstone    = byte(0x17) // idxtomb:<original-index-key> -> []byte{} (presence marker)
	prefixEdgeBetweenIndex  = byte(0x18) // edgebetween_set:start:end:type:edgeID -> []byte{} (all exact relationship lookups)
	prefixEdgeBetweenHead   = byte(0x19) // edgebetween_head:start:end:type -> edgeID (fast single relationship lookup)
)

// prefixMVCCMeta subkeys reserved by storage metadata records.
const (
	prefixMVCCMetaSchemaVersion         = byte(0x02)
	prefixMVCCMetaEdgeBetweenIndexReady = byte(0x03)
)

// maxNodeSize is the maximum size for a node to be stored inline (50KB to leave room for BadgerDB overhead)
// Nodes exceeding this will have embeddings stored separately
const maxNodeSize = 50 * 1024 // 50KB

const (
	defaultBadgerNodeCacheMaxEntries   = 10000
	defaultBadgerEdgeTypeCacheMaxTypes = 50
	defaultBadgerLabelFirstCacheMax    = 1000
)

// BadgerEngine provides persistent storage using BadgerDB.
//
// Features:
//   - ACID transactions for all operations
//   - Persistent storage to disk
//   - Secondary indexes for efficient queries
//   - Thread-safe concurrent access
//   - Automatic crash recovery
//
// Key Structure:
//   - Nodes: 0x01 + nodeID -> JSON(Node)
//   - Edges: 0x02 + edgeID -> JSON(Edge)
//   - Label Index: 0x03 + label + 0x00 + nodeID -> empty
//   - Outgoing Index: 0x04 + nodeID + 0x00 + edgeID -> empty
//   - Incoming Index: 0x05 + nodeID + 0x00 + edgeID -> empty
//   - Edge-Between Set: 0x18 + startID + 0x00 + endID + 0x00 + type + 0x00 + edgeID -> empty
//   - Edge-Between Head: 0x19 + startID + 0x00 + endID + 0x00 + type -> edgeID
//
// Example:
//
//	engine, err := storage.NewBadgerEngine("/path/to/data")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer engine.Close()
//
//	node := &storage.Node{
//		ID:     "user-123",
//		Labels: []string{"User"},
//		Properties: map[string]any{"name": "Alice"},
//	}
//	engine.CreateNode(node)
type BadgerEngine struct {
	db       *badger.DB
	mu       sync.RWMutex // Protects lifecycle state (e.g., Close) and any coarse-grained engine invariants
	closed   bool
	inMemory bool   // True if running in memory-only mode (testing)
	dataDir  string // Captured from BadgerOptions.DataDir; used by migration logging.

	// migrationDidRun is set by RunOnStartMigrations when at least one
	// migration arm rewrote bodies. The engine open path uses it to
	// decide whether to pay the close+reopen+Flatten cost: most opens
	// don't migrate, and only an actual migration leaves enough stale
	// keys behind to make synchronous compaction worthwhile.
	migrationDidRun bool

	// Per-namespace schema (Neo4j-compatible: each database has its own schema).
	schemasMu sync.RWMutex
	schemas   map[string]*SchemaManager // namespace -> schema

	// Hot node cache for frequently accessed nodes
	// Dramatically speeds up repeated MATCH lookups
	nodeCache   map[NodeID]*Node
	nodeCacheMu sync.RWMutex
	cacheHits   int64
	cacheMisses int64

	// Edge type cache for mutual relationship queries
	// Caches edges by type for O(1) lookup
	edgeTypeCache   map[string][]*Edge // edgeType -> edges of that type
	edgeTypeCacheMu sync.RWMutex

	// Fast label lookup cache: label -> first node ID
	// Used by ID-only label scans to avoid repeated index iteration.
	labelFirstNodeCache   map[string]NodeID
	labelFirstNodeCacheMu sync.RWMutex

	// Cache sizing (tunable via config/options).
	// Used by the cache invalidation helpers to preserve invariants.
	nodeCacheMaxEntries   int
	edgeTypeCacheMaxTypes int
	labelFirstCacheMax    int

	// Cached counts for O(1) stats lookups (updated on create/delete)
	// Eliminates expensive full table scans for node/edge counts
	nodeCount atomic.Int64
	edgeCount atomic.Int64
	mvccSeq   atomic.Uint64

	retentionPolicy           RetentionPolicy
	activeMVCCSnapshotReaders atomic.Int64
	lifecycleController       MVCCLifecycleController

	// Cached per-namespace counts for O(1) multi-database stats.
	// Keys are namespace prefixes like "nornic:".
	namespaceCountsMu   sync.RWMutex
	namespaceNodeCounts map[string]int64
	namespaceEdgeCounts map[string]int64

	edgeBetweenIndexBackfillMu     sync.Mutex
	edgeBetweenIndexBackfillCancel context.CancelFunc
	edgeBetweenIndexBackfillDone   chan struct{}

	// idDict assigns compact 8-byte numeric IDs to string node/edge IDs
	// so secondary indexes (edge-between, outgoing/incoming, edge-type,
	// label, MVCC heads) can encode references with 8 bytes instead of
	// 40–50-byte UUID strings. See id_dictionary.go for the full layout.
	idDict *idDictionary

	// propKeyDict tokenizes property-key NAMES (per-namespace) to varint
	// IDs so node and edge bodies can store keys as 1-2 byte integers
	// instead of repeating the string in every record. See
	// property_key_dictionary.go for the full layout.
	propKeyDict *propertyKeyDictionary

	// storageVersion is the on-disk schema version after migrations have
	// run during engine open. Written once during NewBadgerEngineWithOptions
	// and read-only thereafter. The encode path picks codecs deterministically
	// from this value; the decode path dispatches on per-record format bytes
	// because old bodies coexist with new ones until rewritten.
	storageVersion int

	// Event callbacks for external coordination (search indexes, caches, etc.)
	// These are fired AFTER storage operations succeed
	onNodeCreated NodeEventCallback
	onNodeUpdated NodeEventCallback
	onNodeDeleted NodeDeleteCallback
	onEdgeCreated EdgeEventCallback
	onEdgeUpdated EdgeEventCallback
	onEdgeDeleted EdgeDeleteCallback
	callbackMu    sync.RWMutex

	// Knowledge-layer decay scoring (Phase 4).
	decayEnabled  bool
	accumulator   accessIncrementor
	revealAll     atomic.Bool
	revealQueryMu sync.RWMutex

	// embeddingsEnabled gates the pending-embed index write on node creates.
	// When false, new nodes skip the pendingEmbed marker since no embed worker
	// will consume it — saves one Badger Set per user node on hot-path writes.
	embeddingsEnabled atomic.Bool

	// log is the structured *slog.Logger for storage subsystem emissions.
	// Tagged at construction with component=storage, engine=badger.
	// D-01 logger DI; D-07 storage internals use e.log directly. Discard
	// fallback installed in NewBadgerEngineWithOptions when opts.Logger == nil.
	log *slog.Logger

	// Plan 04-04-06: pre-bound storage observers cached at AttachMetrics
	// time (MET-25). Hot-path observation funnels through these — zero
	// WithLabelValues lookup per Get/Put/Delete/Scan call. Nil-safe: when
	// opObserve is the zero value (no metrics attached) the deferred
	// observation in observeStorageOp is a no-op.
	storageMetrics  *observability.StorageMetrics
	mvccMetrics     *observability.MVCCMetrics
	opDurGet        observability.BoundLatencyObserver
	opDurPut        observability.BoundLatencyObserver
	opDurDelete     observability.BoundLatencyObserver
	opDurScan       observability.BoundLatencyObserver
	metricsAttached atomic.Bool
}

// IsInMemory returns true if the engine is running in memory-only mode.
// In-memory mode is used for testing - there's no disk to fsync to.
func (b *BadgerEngine) IsInMemory() bool {
	return b.inMemory
}

// DB returns the underlying *badger.DB handle. Used by Plan 04-04-04
// bytes_metrics_sweeper to call EstimateSize(prefix). Read-only handle —
// callers must not invoke lifecycle methods (Close/DropAll) on the
// returned pointer; the engine owns lifecycle.
func (b *BadgerEngine) DB() *badger.DB {
	return b.db
}

// OnNodeCreated sets a callback to be invoked when nodes are created.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnNodeCreated(callback NodeEventCallback) {
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onNodeCreated = callback
}

// OnNodeUpdated sets a callback to be invoked when nodes are updated.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnNodeUpdated(callback NodeEventCallback) {
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onNodeUpdated = callback
}

// OnNodeDeleted sets a callback to be invoked when nodes are deleted.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onNodeDeleted = callback
}

// OnEdgeCreated sets a callback to be invoked when edges are created.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnEdgeCreated(callback EdgeEventCallback) {
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onEdgeCreated = callback
}

// OnEdgeUpdated sets a callback to be invoked when edges are updated.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onEdgeUpdated = callback
}

// OnEdgeDeleted sets a callback to be invoked when edges are deleted.
// Implements StorageEventNotifier interface.
func (b *BadgerEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onEdgeDeleted = callback
}

// notifyNodeCreated calls the registered callback if set.
func (b *BadgerEngine) notifyNodeCreated(node *Node) {
	b.callbackMu.RLock()
	callback := b.onNodeCreated
	b.callbackMu.RUnlock()

	if callback != nil {
		callback(node)
	}
}

// notifyNodeUpdated calls the registered callback if set.
func (b *BadgerEngine) notifyNodeUpdated(node *Node) {
	b.callbackMu.RLock()
	callback := b.onNodeUpdated
	b.callbackMu.RUnlock()

	if callback != nil {
		callback(node)
	}
}

// notifyNodeDeleted calls the registered callback if set.
func (b *BadgerEngine) notifyNodeDeleted(nodeID NodeID) {
	b.callbackMu.RLock()
	callback := b.onNodeDeleted
	b.callbackMu.RUnlock()

	if callback != nil {
		callback(nodeID)
	}
}

// notifyEdgeCreated calls the registered callback if set.
func (b *BadgerEngine) notifyEdgeCreated(edge *Edge) {
	b.callbackMu.RLock()
	callback := b.onEdgeCreated
	b.callbackMu.RUnlock()

	if callback != nil {
		callback(edge)
	}
}

// notifyEdgeUpdated calls the registered callback if set.
func (b *BadgerEngine) notifyEdgeUpdated(edge *Edge) {
	b.callbackMu.RLock()
	callback := b.onEdgeUpdated
	b.callbackMu.RUnlock()

	if callback != nil {
		callback(edge)
	}
}

// notifyEdgeDeleted calls the registered callback if set.
func (b *BadgerEngine) notifyEdgeDeleted(edgeID EdgeID) {
	b.callbackMu.RLock()
	callback := b.onEdgeDeleted
	b.callbackMu.RUnlock()

	if callback != nil {
		callback(edgeID)
	}
}

// BadgerOptions configures the BadgerDB engine.
type BadgerOptions struct {
	// DataDir is the directory for storing data files.
	// Required.
	DataDir string

	// InMemory runs BadgerDB in memory-only mode.
	// Useful for testing. Data is not persisted.
	InMemory bool

	// SyncWrites forces fsync after each write.
	// Slower but more durable.
	SyncWrites bool

	// BadgerInternalLogger is the logger handed to BadgerDB itself for its
	// own internal logging (compaction, value-log GC, etc.). If nil, BadgerDB's
	// quiet/default logger is used. This field replaces the previous
	// BadgerOptions.Logger which collided with the new structured *slog.Logger
	// field below; rename is internal-only (no external setter ever existed
	// outside doc comments — verified 2026-05-01).
	BadgerInternalLogger badger.Logger

	// Logger is the structured *slog.Logger threaded into the storage engine.
	// D-01 logger DI: optional; nil falls back to a discard handler at ctor
	// entry per D-01a so existing callers (and tests) compile unchanged.
	// Once set, BadgerEngine emits all operational diagnostics through this
	// logger tagged with component=storage, engine=badger.
	Logger *slog.Logger

	// LowMemory enables memory-constrained settings.
	// Reduces MemTableSize and other buffers to use less RAM.
	LowMemory bool

	// HighPerformance enables aggressive caching and larger buffers.
	// Uses more RAM but significantly faster writes/reads.
	HighPerformance bool

	// EncryptionKey is the 16, 24, or 32 byte key for AES encryption.
	// If provided, all data will be encrypted at rest using AES-CTR.
	// WARNING: If you lose this key, your data is irrecoverable!
	// Leave empty to disable encryption.
	EncryptionKey []byte

	// AllowStorageUpgrade authorizes the engine to advance the on-disk
	// storage version through whichever migration arms this binary
	// understands. Without it, a binary that opens a data directory
	// older than its own version refuses to start. The upgrade is
	// one-way; operators should back up before passing this flag.
	AllowStorageUpgrade bool

	// NodeCacheMaxEntries is the maximum number of nodes held in the in-process
	// hot node cache (used by GetNode). When exceeded, the cache is cleared.
	// Set to 0 to use the default.
	NodeCacheMaxEntries int

	// EdgeTypeCacheMaxTypes is the maximum number of distinct edge types cached
	// for GetEdgesByType. When exceeded, the cache is cleared.
	// Set to 0 to use the default.
	EdgeTypeCacheMaxTypes int

	// LabelFirstNodeCacheMaxEntries is the maximum number of labels cached for
	// fast label-first lookups. When exceeded, the cache is cleared.
	// Set to 0 to use the default.
	LabelFirstNodeCacheMaxEntries int

	// EngineOptions holds engine-wide MVCC retention defaults and similar policies.
	EngineOptions EngineOptions
}

// NewBadgerEngine creates a new persistent storage engine with default settings.
//
// This is the simplest way to create a storage engine. The engine uses BadgerDB
// for persistent disk storage with ACID transaction guarantees. All data is
// stored in the specified directory and persists across restarts.
//
// Parameters:
//   - dataDir: Directory path for storing data files. Created if it doesn't exist.
//
// Returns:
//   - *BadgerEngine on success
//   - error if database cannot be opened (e.g., permissions, disk space)
//
// Example 1 - Basic Usage:
//
//	engine, err := storage.NewBadgerEngine("./data/nornicdb")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer engine.Close()
//
//	// Engine is ready - create nodes
//	node := &storage.Node{
//		ID:     "user-1",
//		Labels: []string{"User"},
//		Properties: map[string]any{"name": "Alice"},
//	}
//	engine.CreateNode(node)
//
// Example 2 - Production Application:
//
//	// Use absolute path for production
//	dataDir := filepath.Join(os.Getenv("APP_HOME"), "data", "nornicdb")
//	engine, err := storage.NewBadgerEngine(dataDir)
//	if err != nil {
//		return fmt.Errorf("failed to open database: %w", err)
//	}
//	defer engine.Close()
//
// Example 3 - Multiple Databases:
//
//	// Main application database
//	mainDB, _ := storage.NewBadgerEngine("./data/main")
//	defer mainDB.Close()
//
//	// Test database
//	testDB, _ := storage.NewBadgerEngine("./data/test")
//	defer testDB.Close()
//
//	// Cache database
//	cacheDB, _ := storage.NewBadgerEngine("./data/cache")
//	defer cacheDB.Close()
//
// ELI12:
//
// Think of NewBadgerEngine like setting up a filing cabinet in your room.
// You tell it "put the cabinet here" (the dataDir), and it creates folders
// and organizes everything. Even if you turn off your computer, the cabinet
// stays there with all your files inside. Next time you start up, all your
// data is still there!
//
// Disk Usage:
//   - Approximately 2-3x the size of your actual data
//   - Includes write-ahead log and compaction overhead
//
// Thread Safety:
//
//	Safe for concurrent use from multiple goroutines.
func NewBadgerEngine(dataDir string) (*BadgerEngine, error) {
	return NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: dataDir,
	})
}

// NewBadgerEngineWithOptions creates a BadgerEngine with custom configuration.
//
// Use this function when you need fine-grained control over the storage engine
// behavior, such as enabling in-memory mode for testing, forcing synchronous
// writes for maximum durability, or reducing memory usage.
//
// Parameters:
//   - opts: BadgerOptions struct with configuration settings
//
// Returns:
//   - *BadgerEngine on success
//   - error if database cannot be opened
//
// Example 1 - In-Memory Database for Testing:
//
//	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
//		DataDir:  "./test", // Still needs a path but won't be used
//		InMemory: true,     // All data in RAM, lost on shutdown
//	})
//	defer engine.Close()
//
//	// Perfect for unit tests - fast and clean
//	testCreateNodes(engine)
//
// Example 2 - Maximum Durability for Financial Data:
//
//	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
//		DataDir:    "./data/transactions",
//		SyncWrites: true, // Force fsync after each write (slower but safer)
//	})
//	// Guaranteed data persistence even if power fails
//
// Example 3 - Low Memory Mode for Embedded Devices:
//
//	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
//		DataDir:   "./data/nornicdb",
//		LowMemory: true, // Reduces RAM usage by 50-70%
//	})
//	// Uses ~50MB instead of ~150MB for typical workloads
//
// Example 4 - Custom Logger Integration:
//
//	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
//	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
//		DataDir: "./data/nornicdb",
//		Logger:  &BadgerLogger{zlog: logger}, // Custom logging
//	})
//
// ELI12:
//
// NewBadgerEngine is like getting a basic backpack for school.
// NewBadgerEngineWithOptions is like customizing your backpack - you can:
//   - Make it waterproof (SyncWrites = true)
//   - Make it lighter but less storage (LowMemory = true)
//   - Use it as a temporary bag (InMemory = true)
//   - Add custom labels (Logger)
//
// Configuration Trade-offs:
//   - SyncWrites=true: Slower writes (2-5x) but maximum safety
//   - LowMemory=true: Less RAM but slightly slower
//   - InMemory=true: Fastest but data lost on shutdown
//
// Thread Safety:
//
//	Safe for concurrent use from multiple goroutines.
func NewBadgerEngineWithOptions(opts BadgerOptions) (*BadgerEngine, error) {
	retentionPolicy := normalizeRetentionPolicy(opts.EngineOptions.RetentionPolicy)
	badgerOpts := badger.DefaultOptions(opts.DataDir)

	if opts.InMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	}

	if opts.SyncWrites {
		badgerOpts = badgerOpts.WithSyncWrites(true)
	}

	if opts.BadgerInternalLogger != nil {
		badgerOpts = badgerOpts.WithLogger(opts.BadgerInternalLogger)
	} else {
		// Use a quiet logger by default
		badgerOpts = badgerOpts.WithLogger(nil)
	}

	// D-01a discard fallback: structured *slog.Logger is optional. When the
	// caller does not supply one (e.g., test paths, scripts/perf_direct), a
	// discard logger keeps the LOG-01 invariant that no storage code path
	// reaches stdlib log printers even when no observability stack exists.
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	storageLog := opts.Logger.With("component", "storage", "engine", "badger")

	// Enable encryption at rest if key is provided
	if len(opts.EncryptionKey) > 0 {
		// Validate key length (must be 16, 24, or 32 bytes for AES-128/192/256)
		keyLen := len(opts.EncryptionKey)
		if keyLen != 16 && keyLen != 24 && keyLen != 32 {
			return nil, fmt.Errorf("encryption key must be 16, 24, or 32 bytes (got %d bytes)", keyLen)
		}
		badgerOpts = badgerOpts.WithEncryptionKey(opts.EncryptionKey)
	}

	if opts.HighPerformance {
		// HIGH PERFORMANCE MODE: Maximize speed, use more RAM
		// Target: Get close to in-memory performance for small-medium datasets
		badgerOpts = badgerOpts.
			WithMemTableSize(128 << 20).     // 128MB memtable (8x default) - fewer flushes
			WithValueLogFileSize(256 << 20). // 256MB value log files
			WithNumMemtables(5).             // 5 memtables for write buffering
			WithNumLevelZeroTables(10).      // More L0 tables before compaction
			WithNumLevelZeroTablesStall(20). // Higher stall threshold
			WithValueThreshold(1 << 20).     // 1MB - keep most values in LSM tree
			WithBlockCacheSize(256 << 20).   // 256MB block cache
			WithIndexCacheSize(128 << 20).   // 128MB index cache
			WithNumCompactors(4).            // More parallel compaction
			WithCompactL0OnClose(false).     // Don't compact on close (faster shutdown)
			WithDetectConflicts(false)       // Skip conflict detection (we handle it)
	} else if opts.LowMemory {
		// LOW MEMORY MODE: Minimize RAM usage
		badgerOpts = badgerOpts.
			WithMemTableSize(8 << 20).      // 8MB memtable
			WithValueLogFileSize(32 << 20). // 32MB value log
			WithNumMemtables(1).            // Single memtable
			WithNumLevelZeroTables(1).      // Aggressive compaction
			WithNumLevelZeroTablesStall(2).
			WithValueThreshold(512).     // Small values in LSM
			WithBlockCacheSize(8 << 20). // 8MB block cache
			WithIndexCacheSize(4 << 20)  // 4MB index cache
	} else {
		// DEFAULT: Balanced settings
		badgerOpts = badgerOpts.
			WithMemTableSize(64 << 20).      // 64MB memtable (default)
			WithValueLogFileSize(128 << 20). // 128MB value log
			WithNumMemtables(3).             // 3 memtables
			WithNumLevelZeroTables(5).       // Default L0 tables
			WithNumLevelZeroTablesStall(10).
			WithValueThreshold(64 << 10). // 64KB threshold - allows larger property values
			WithBlockCacheSize(64 << 20). // 64MB block cache
			WithIndexCacheSize(32 << 20)  // 32MB index cache
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	// Warn (but don't fail) if the data directory still holds legacy gob
	// bodies. The decoder handles them, but operators should run
	// MigrateBadgerToMsgpack to upgrade in place.
	if detected, hasData, derr := detectStoredSerializer(db); derr != nil {
		db.Close()
		return nil, fmt.Errorf("detecting storage serializer: %w", derr)
	} else if hasData && detected == detectedGob {
		storageLog.Warn("legacy gob-encoded bodies detected; run MigrateBadgerToMsgpack to upgrade in place",
			"data_dir", opts.DataDir,
		)
	}

	engine := &BadgerEngine{
		db:              db,
		inMemory:        opts.InMemory,
		dataDir:         opts.DataDir,
		schemas:         make(map[string]*SchemaManager),
		retentionPolicy: retentionPolicy,

		nodeCacheMaxEntries:   opts.NodeCacheMaxEntries,
		edgeTypeCacheMaxTypes: opts.EdgeTypeCacheMaxTypes,
		labelFirstCacheMax:    opts.LabelFirstNodeCacheMaxEntries,

		log: storageLog,
	}

	if engine.nodeCacheMaxEntries <= 0 {
		engine.nodeCacheMaxEntries = defaultBadgerNodeCacheMaxEntries
	}
	if engine.edgeTypeCacheMaxTypes <= 0 {
		engine.edgeTypeCacheMaxTypes = defaultBadgerEdgeTypeCacheMaxTypes
	}
	if engine.labelFirstCacheMax <= 0 {
		engine.labelFirstCacheMax = defaultBadgerLabelFirstCacheMax
	}

	engine.nodeCache = make(map[NodeID]*Node, engine.nodeCacheMaxEntries)
	engine.edgeTypeCache = make(map[string][]*Edge, engine.edgeTypeCacheMaxTypes)
	engine.labelFirstNodeCache = make(map[string]NodeID, engine.labelFirstCacheMax)
	engine.namespaceNodeCounts = make(map[string]int64)
	engine.namespaceEdgeCounts = make(map[string]int64)

	engine.idDict = newIDDictionary()
	engine.idDict.setFreelistTTL(opts.EngineOptions.IDFreelistTTL)
	if err := engine.idDict.loadFromBadger(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load id dictionary: %w", err)
	}

	engine.propKeyDict = newPropertyKeyDictionary()
	if err := engine.propKeyDict.loadFromBadger(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load property key dictionary: %w", err)
	}

	// Initialize cached counts by scanning existing data (one-time cost)
	// This enables O(1) stats lookups instead of O(N) scans on every request
	if err := engine.initializeCounts(); err != nil {
		db.Close() // Clean up on error
		return nil, fmt.Errorf("failed to initialize counts: %w", err)
	}

	// Run schema migrations BEFORE starting any background work. The
	// V1→V2 migration rewrites every node and edge body; if the
	// edge-between index backfill ran concurrently, Badger's
	// DropPrefix would block migration writes ("Writes are blocked,
	// possibly due to DropAll or Close").
	if err := engine.loadPersistedSchemas(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load persisted schema: %w", err)
	}

	if err := engine.initializeMVCCSequence(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize mvcc sequence: %w", err)
	}

	if err := engine.RunOnStartMigrations(opts.AllowStorageUpgrade); err != nil {
		db.Close()
		return nil, err
	}

	// If a migration arm rewrote bodies, the v2 versions of every
	// node and edge are still in memtables. Closing the DB drains
	// memtables to SSTs; reopening gives Flatten a tree where the v1
	// originals and v2 rewrites are both on disk so it can collapse
	// them. Without this, Flatten only sees the untouched v1 SSTs and
	// reclaims a few percent of disk instead of unwinding the upgrade
	// bloat.
	if engine.migrationDidRun && !opts.InMemory {
		newDB, err := engine.reopenForPostMigrationCompaction(badgerOpts)
		if err != nil {
			return nil, fmt.Errorf("post-migration reopen: %w", err)
		}
		engine.db = newDB
		db = newDB
		engine.compactAfterMigration()
	}

	// Edge-between index backfill runs only after migrations have
	// settled the body format. Failure here is non-fatal at open
	// time — the index self-heals from the outgoing index on read.
	if err := engine.ensureEdgeBetweenIndex(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize edge-between index: %w", err)
	}

	return engine, nil
}

// reopenForPostMigrationCompaction closes the current Badger handle
// (forcing memtable flush to SSTs) and opens a fresh one with the same
// options, then re-loads the in-memory state that depends on the live
// DB pointer. Returns the new *badger.DB so the caller can swap it on
// the engine.
//
// The state we restore matches the order of the original open path:
// id dictionary, property-key dictionary, persisted schemas, MVCC
// sequence, cached counts. Migrations may have added entries to the
// dictionaries, so re-loading is correctness-required, not just
// hygiene.
func (b *BadgerEngine) reopenForPostMigrationCompaction(badgerOpts badger.Options) (*badger.DB, error) {
	if b.log != nil {
		b.log.Info("post-migration DB reopen starting")
	}
	if err := b.db.Close(); err != nil {
		return nil, fmt.Errorf("close pre-compaction DB: %w", err)
	}
	newDB, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("reopen DB: %w", err)
	}
	if err := b.idDict.loadFromBadger(newDB); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reload id dictionary: %w", err)
	}
	if err := b.propKeyDict.loadFromBadger(newDB); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reload property-key dictionary: %w", err)
	}
	// loadPersistedSchemas + initializeMVCCSequence + initializeCounts
	// all read off b.db, so we have to swap it before calling them.
	b.db = newDB
	if err := b.loadPersistedSchemas(); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reload persisted schemas: %w", err)
	}
	if err := b.initializeMVCCSequence(); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reinitialize mvcc sequence: %w", err)
	}
	if err := b.initializeCounts(); err != nil {
		newDB.Close()
		return nil, fmt.Errorf("reinitialize counts: %w", err)
	}
	if b.log != nil {
		b.log.Info("post-migration DB reopen complete")
	}
	return newDB, nil
}

// NewBadgerEngineInMemory creates an in-memory BadgerDB for testing.
//
// Data is not persisted and is lost when the engine is closed.
// Useful for unit tests that need persistent storage semantics
// without actual disk I/O.
//
// Example:
//
//	engine, err := storage.NewBadgerEngineInMemory()
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer engine.Close()
//
//	// Use engine for testing...
func NewBadgerEngineInMemory() (*BadgerEngine, error) {
	return NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
	})
}

func (b *BadgerEngine) initializeMVCCSequence() error {
	seq, err := b.loadPersistedMVCCSequence()
	if err != nil {
		return err
	}
	b.mvccSeq.Store(seq)
	return nil
}

func (b *BadgerEngine) loadPersistedMVCCSequence() (uint64, error) {
	var seq uint64
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(mvccSequenceKey())
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid mvcc sequence length: %d", len(val))
			}
			seq = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (b *BadgerEngine) allocateMVCCVersion(txn *badger.Txn, commitTime time.Time) (MVCCVersion, error) {
	seq := b.mvccSeq.Add(1)
	encodedSeq := make([]byte, 8)
	binary.BigEndian.PutUint64(encodedSeq, seq)
	if err := txn.Set(mvccSequenceKey(), encodedSeq); err != nil {
		return MVCCVersion{}, err
	}
	return MVCCVersion{
		CommitTimestamp: commitTime.UTC(),
		CommitSequence:  seq,
	}, nil
}

func (b *BadgerEngine) readSchemaVersion() (int, error) {
	var version int
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(mvccSchemaVersionKey())
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) == 1 && val[0] == 1 {
				// Compatibility: a broken startup build wrote the edge-between
				// ready marker to the schema-version key. Treat that one-byte
				// value as "version unset" so migrations can restore the proper
				// 8-byte schema version on open.
				version = 0
				return nil
			}
			if len(val) != 8 {
				return fmt.Errorf("invalid schema version length: %d", len(val))
			}
			version = int(binary.BigEndian.Uint64(val))
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (b *BadgerEngine) writeSchemaVersion(version int) error {
	return b.db.Update(func(txn *badger.Txn) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(version))
		return txn.Set(mvccSchemaVersionKey(), buf)
	})
}

// ============================================================================
