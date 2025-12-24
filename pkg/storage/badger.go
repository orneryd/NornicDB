// Package storage provides storage engine implementations for NornicDB.
//
// BadgerEngine provides persistent disk-based storage using BadgerDB.
// It implements the Engine interface with full ACID transaction support.
package storage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// Key prefixes for BadgerDB storage organization
// Using single-byte prefixes for efficiency
const (
	prefixNode          = byte(0x01) // nodes:nodeID -> Node
	prefixEdge          = byte(0x02) // edges:edgeID -> Edge
	prefixLabelIndex    = byte(0x03) // label:labelName:nodeID -> []byte{}
	prefixOutgoingIndex = byte(0x04) // outgoing:nodeID:edgeID -> []byte{}
	prefixIncomingIndex = byte(0x05) // incoming:nodeID:edgeID -> []byte{}
	prefixEdgeTypeIndex = byte(0x06) // edgetype:type:edgeID -> []byte{} (for fast type lookups)
	prefixPendingEmbed  = byte(0x07) // pending_embed:nodeID -> []byte{} (nodes needing embedding)
	prefixEmbedding     = byte(0x08) // embedding:nodeID:chunkIndex -> []float32 (separate storage for large embeddings)
)

// maxNodeSize is the maximum size for a node to be stored inline (50KB to leave room for BadgerDB overhead)
// Nodes exceeding this will have embeddings stored separately
const maxNodeSize = 50 * 1024 // 50KB

const (
	defaultBadgerNodeCacheMaxEntries   = 10000
	defaultBadgerEdgeTypeCacheMaxTypes = 50
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
	schema   *SchemaManager
	mu       sync.RWMutex // Protects schema operations
	closed   bool
	inMemory bool // True if running in memory-only mode (testing)

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

	// Cache sizing (tunable via config/options).
	// Used by the cache invalidation helpers to preserve invariants.
	nodeCacheMaxEntries   int
	edgeTypeCacheMaxTypes int

	// Cached counts for O(1) stats lookups (updated on create/delete)
	// Eliminates expensive full table scans for node/edge counts
	nodeCount atomic.Int64
	edgeCount atomic.Int64

	// Event callbacks for external coordination (search indexes, caches, etc.)
	// These are fired AFTER storage operations succeed
	onNodeCreated NodeEventCallback
	onNodeUpdated NodeEventCallback
	onNodeDeleted NodeDeleteCallback
	onEdgeCreated EdgeEventCallback
	onEdgeUpdated EdgeEventCallback
	onEdgeDeleted EdgeDeleteCallback
	callbackMu    sync.RWMutex
}

// IsInMemory returns true if the engine is running in memory-only mode.
// In-memory mode is used for testing - there's no disk to fsync to.
func (b *BadgerEngine) IsInMemory() bool {
	return b.inMemory
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

	// Logger for BadgerDB internal logging.
	// If nil, BadgerDB's default logger is used.
	Logger badger.Logger

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

	// NodeCacheMaxEntries is the maximum number of nodes held in the in-process
	// hot node cache (used by GetNode). When exceeded, the cache is cleared.
	// Set to 0 to use the default.
	NodeCacheMaxEntries int

	// EdgeTypeCacheMaxTypes is the maximum number of distinct edge types cached
	// for GetEdgesByType. When exceeded, the cache is cleared.
	// Set to 0 to use the default.
	EdgeTypeCacheMaxTypes int
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
	badgerOpts := badger.DefaultOptions(opts.DataDir)

	if opts.InMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	}

	if opts.SyncWrites {
		badgerOpts = badgerOpts.WithSyncWrites(true)
	}

	if opts.Logger != nil {
		badgerOpts = badgerOpts.WithLogger(opts.Logger)
	} else {
		// Use a quiet logger by default
		badgerOpts = badgerOpts.WithLogger(nil)
	}

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

	engine := &BadgerEngine{
		db:       db,
		schema:   NewSchemaManager(),
		inMemory: opts.InMemory,

		nodeCacheMaxEntries:   opts.NodeCacheMaxEntries,
		edgeTypeCacheMaxTypes: opts.EdgeTypeCacheMaxTypes,
	}

	if engine.nodeCacheMaxEntries <= 0 {
		engine.nodeCacheMaxEntries = defaultBadgerNodeCacheMaxEntries
	}
	if engine.edgeTypeCacheMaxTypes <= 0 {
		engine.edgeTypeCacheMaxTypes = defaultBadgerEdgeTypeCacheMaxTypes
	}

	engine.nodeCache = make(map[NodeID]*Node, engine.nodeCacheMaxEntries)
	engine.edgeTypeCache = make(map[string][]*Edge, engine.edgeTypeCacheMaxTypes)

	// Initialize cached counts by scanning existing data (one-time cost)
	// This enables O(1) stats lookups instead of O(N) scans on every request
	if err := engine.initializeCounts(); err != nil {
		db.Close() // Clean up on error
		return nil, fmt.Errorf("failed to initialize counts: %w", err)
	}

	return engine, nil
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

// ============================================================================
