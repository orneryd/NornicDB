// Package storage provides storage engine implementations for NornicDB.
//
// BadgerEngine provides persistent disk-based storage using BadgerDB.
// It implements the Engine interface with full ACID transaction support.
package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"strings"
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
		db:            db,
		schema:        NewSchemaManager(),
		inMemory:      opts.InMemory,
		nodeCache:     make(map[NodeID]*Node, 10000), // Cache up to 10K hot nodes
		edgeTypeCache: make(map[string][]*Edge, 100), // Cache edges by type for mutual queries
	}

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
// Key encoding helpers
// ============================================================================

// nodeKey creates a key for storing a node.
func nodeKey(id NodeID) []byte {
	return append([]byte{prefixNode}, []byte(id)...)
}

// edgeKey creates a key for storing an edge.
func edgeKey(id EdgeID) []byte {
	return append([]byte{prefixEdge}, []byte(id)...)
}

// labelIndexKey creates a key for the label index.
// Format: prefix + label (lowercase) + 0x00 + nodeID
// Labels are normalized to lowercase for case-insensitive matching (Neo4j compatible)
func labelIndexKey(label string, nodeID NodeID) []byte {
	normalizedLabel := strings.ToLower(label)
	key := make([]byte, 0, 1+len(normalizedLabel)+1+len(nodeID))
	key = append(key, prefixLabelIndex)
	key = append(key, []byte(normalizedLabel)...)
	key = append(key, 0x00) // Separator
	key = append(key, []byte(nodeID)...)
	return key
}

// labelIndexPrefix returns the prefix for scanning all nodes with a label.
// Labels are normalized to lowercase for case-insensitive matching (Neo4j compatible)
func labelIndexPrefix(label string) []byte {
	normalizedLabel := strings.ToLower(label)
	key := make([]byte, 0, 1+len(normalizedLabel)+1)
	key = append(key, prefixLabelIndex)
	key = append(key, []byte(normalizedLabel)...)
	key = append(key, 0x00)
	return key
}

// outgoingIndexKey creates a key for the outgoing edge index.
func outgoingIndexKey(nodeID NodeID, edgeID EdgeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1+len(edgeID))
	key = append(key, prefixOutgoingIndex)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	key = append(key, []byte(edgeID)...)
	return key
}

// outgoingIndexPrefix returns the prefix for scanning outgoing edges.
func outgoingIndexPrefix(nodeID NodeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1)
	key = append(key, prefixOutgoingIndex)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	return key
}

// incomingIndexKey creates a key for the incoming edge index.
func incomingIndexKey(nodeID NodeID, edgeID EdgeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1+len(edgeID))
	key = append(key, prefixIncomingIndex)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	key = append(key, []byte(edgeID)...)
	return key
}

// incomingIndexPrefix returns the prefix for scanning incoming edges.
func incomingIndexPrefix(nodeID NodeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1)
	key = append(key, prefixIncomingIndex)
	key = append(key, []byte(nodeID)...)
	key = append(key, 0x00)
	return key
}

// edgeTypeIndexKey creates a key for the edge type index.
// Format: prefix + edgeType (lowercase) + 0x00 + edgeID
func edgeTypeIndexKey(edgeType string, edgeID EdgeID) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+len(normalizedType)+1+len(edgeID))
	key = append(key, prefixEdgeTypeIndex)
	key = append(key, []byte(normalizedType)...)
	key = append(key, 0x00) // Separator
	key = append(key, []byte(edgeID)...)
	return key
}

// edgeTypeIndexPrefix returns the prefix for scanning all edges of a type.
// Edge types are normalized to lowercase for case-insensitive matching (Neo4j compatible)
func edgeTypeIndexPrefix(edgeType string) []byte {
	normalizedType := strings.ToLower(edgeType)
	key := make([]byte, 0, 1+len(normalizedType)+1)
	key = append(key, prefixEdgeTypeIndex)
	key = append(key, []byte(normalizedType)...)
	key = append(key, 0x00)
	return key
}

// pendingEmbedKey creates a key for the pending embeddings index.
// Format: prefix + nodeID
func pendingEmbedKey(nodeID NodeID) []byte {
	return append([]byte{prefixPendingEmbed}, []byte(nodeID)...)
}

// extractEdgeIDFromIndexKey extracts the edgeID from an index key.
// Format: prefix + nodeID + 0x00 + edgeID
func extractEdgeIDFromIndexKey(key []byte) EdgeID {
	// Find the separator (0x00)
	for i := 1; i < len(key); i++ {
		if key[i] == 0x00 {
			return EdgeID(key[i+1:])
		}
	}
	return ""
}

// extractNodeIDFromLabelIndex extracts the nodeID from a label index key.
// Format: prefix + label + 0x00 + nodeID
func extractNodeIDFromLabelIndex(key []byte, labelLen int) NodeID {
	// Skip prefix (1) + label (labelLen) + separator (1)
	offset := 1 + labelLen + 1
	if offset >= len(key) {
		return ""
	}
	return NodeID(key[offset:])
}

// ============================================================================
// Serialization helpers
// ============================================================================

// encodeNode serializes a Node using gob (preserves Go types like int64).
func encodeNode(n *Node) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(n); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeNode deserializes a Node from gob.
func decodeNode(data []byte) (*Node, error) {
	var node Node
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&node); err != nil {
		return nil, err
	}
	return &node, nil
}

// encodeEdge serializes an Edge using gob (preserves Go types).
func encodeEdge(e *Edge) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeEdge deserializes an Edge from gob.
func decodeEdge(data []byte) (*Edge, error) {
	var edge Edge
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&edge); err != nil {
		return nil, err
	}
	return &edge, nil
}

// ============================================================================
// Node Operations
// ============================================================================

// CreateNode creates a new node in persistent storage.
func (b *BadgerEngine) CreateNode(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	// Check unique constraints for all labels and properties
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			if err := b.schema.CheckUniqueConstraint(label, propName, propValue, ""); err != nil {
				return fmt.Errorf("constraint violation: %w", err)
			}
		}
	}

	err := b.db.Update(func(txn *badger.Txn) error {
		// Check if node already exists
		key := nodeKey(node.ID)
		_, err := txn.Get(key)
		if err == nil {
			return ErrAlreadyExists
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// Serialize node
		data, err := encodeNode(node)
		if err != nil {
			return fmt.Errorf("failed to encode node: %w", err)
		}

		// Store node
		if err := txn.Set(key, data); err != nil {
			return err
		}

		// Create label indexes
		for _, label := range node.Labels {
			labelKey := labelIndexKey(label, node.ID)
			if err := txn.Set(labelKey, []byte{}); err != nil {
				return err
			}
		}

		// Add to pending embeddings index if it needs embedding (atomic with node creation)
		if len(node.Embedding) == 0 && NodeNeedsEmbedding(node) {
			if err := txn.Set(pendingEmbedKey(node.ID), []byte{}); err != nil {
				return err
			}
		}

		return nil
	})

	// On successful create, update cache and register unique constraint values
	if err == nil {
		// Store deep copy in cache to prevent external mutation
		b.nodeCacheMu.Lock()
		b.nodeCache[node.ID] = copyNode(node)
		b.nodeCacheMu.Unlock()

		// Register unique constraint values
		for _, label := range node.Labels {
			for propName, propValue := range node.Properties {
				b.schema.RegisterUniqueValue(label, propName, propValue, node.ID)
			}
		}

		// Increment cached node count for O(1) stats lookups
		b.nodeCount.Add(1)

		// Notify listeners (e.g., search service) to index the new node
		b.notifyNodeCreated(node)
	}

	return err
}

// GetNode retrieves a node by ID.
func (b *BadgerEngine) GetNode(id NodeID) (*Node, error) {
	if id == "" {
		return nil, ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	// Check cache first
	b.nodeCacheMu.RLock()
	if cached, ok := b.nodeCache[id]; ok {
		b.nodeCacheMu.RUnlock()
		atomic.AddInt64(&b.cacheHits, 1)
		// Return copy to prevent external mutation of cache
		return copyNode(cached), nil
	}
	b.nodeCacheMu.RUnlock()
	atomic.AddInt64(&b.cacheMisses, 1)

	var node *Node
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			var decodeErr error
			node, decodeErr = decodeNode(val)
			return decodeErr
		})
	})

	// Cache the result on successful fetch
	if err == nil && node != nil {
		b.nodeCacheMu.Lock()
		// Simple eviction: if cache is too large, clear it
		if len(b.nodeCache) > 10000 {
			b.nodeCache = make(map[NodeID]*Node, 10000)
		}
		b.nodeCache[id] = copyNode(node)
		b.nodeCacheMu.Unlock()
	}

	return node, err
}

// UpdateNode updates an existing node or creates it if it doesn't exist (upsert).
func (b *BadgerEngine) UpdateNode(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	// Track if this is an insert (new node) or update (existing node)
	wasInsert := false

	err := b.db.Update(func(txn *badger.Txn) error {
		key := nodeKey(node.ID)

		// Get existing node for label index updates (if exists)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			// Node doesn't exist - do an insert (upsert behavior)
			wasInsert = true
			data, err := encodeNode(node)
			if err != nil {
				return fmt.Errorf("failed to encode node: %w", err)
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}
			// Create label indexes
			for _, label := range node.Labels {
				if err := txn.Set(labelIndexKey(label, node.ID), []byte{}); err != nil {
					return err
				}
			}
			// Add to pending embeddings index if needed (same as CreateNode)
			if len(node.Embedding) == 0 && NodeNeedsEmbedding(node) {
				if err := txn.Set(pendingEmbedKey(node.ID), []byte{}); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			return err
		}

		// Node exists - update it
		var existing *Node
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			existing, decodeErr = decodeNode(val)
			return decodeErr
		}); err != nil {
			return err
		}

		// Remove old label indexes
		for _, label := range existing.Labels {
			if err := txn.Delete(labelIndexKey(label, node.ID)); err != nil {
				return err
			}
		}

		// Serialize and store updated node
		data, err := encodeNode(node)
		if err != nil {
			return fmt.Errorf("failed to encode node: %w", err)
		}

		if err := txn.Set(key, data); err != nil {
			return err
		}

		// Create new label indexes
		for _, label := range node.Labels {
			if err := txn.Set(labelIndexKey(label, node.ID), []byte{}); err != nil {
				return err
			}
		}

		// Manage pending embeddings index atomically
		if len(node.Embedding) > 0 {
			// Node has embedding - remove from pending index
			txn.Delete(pendingEmbedKey(node.ID))
		} else if NodeNeedsEmbedding(node) {
			// Node needs embedding - ensure it's in pending index
			txn.Set(pendingEmbedKey(node.ID), []byte{})
		}

		return nil
	})

	// Update cache on successful operation
	if err == nil {
		b.nodeCacheMu.Lock()
		b.nodeCache[node.ID] = node
		b.nodeCacheMu.Unlock()

		if wasInsert {
			// Increment count for new nodes (upsert as insert)
			b.nodeCount.Add(1)
			// Notify listeners about the new node
			b.notifyNodeCreated(node)
		} else {
			// Notify listeners to re-index the updated node
			b.notifyNodeUpdated(node)
		}
	}

	return err
}

// DeleteNode removes a node and all its edges.
func (b *BadgerEngine) DeleteNode(id NodeID) error {
	if id == "" {
		return ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	// Track edge deletions for counter update after transaction
	var totalEdgesDeleted int64
	var deletedEdgeIDs []EdgeID

	err := b.db.Update(func(txn *badger.Txn) error {
		key := nodeKey(id)

		// Get node for label cleanup
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		var node *Node
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			node, decodeErr = decodeNode(val)
			return decodeErr
		}); err != nil {
			return err
		}

		// Delete label indexes
		for _, label := range node.Labels {
			if err := txn.Delete(labelIndexKey(label, id)); err != nil {
				return err
			}
		}

		// Delete outgoing edges (and track count)
		outPrefix := outgoingIndexPrefix(id)
		outCount, outIDs, err := b.deleteEdgesWithPrefix(txn, outPrefix)
		if err != nil {
			return err
		}
		totalEdgesDeleted += outCount
		deletedEdgeIDs = append(deletedEdgeIDs, outIDs...)

		// Delete incoming edges (and track count)
		inPrefix := incomingIndexPrefix(id)
		inCount, inIDs, err := b.deleteEdgesWithPrefix(txn, inPrefix)
		if err != nil {
			return err
		}
		totalEdgesDeleted += inCount
		deletedEdgeIDs = append(deletedEdgeIDs, inIDs...)

		// Remove from pending embeddings index (if present)
		txn.Delete(pendingEmbedKey(id))

		// Delete the node
		return txn.Delete(key)
	})

	// Invalidate cache on successful delete
	if err == nil {
		b.nodeCacheMu.Lock()
		delete(b.nodeCache, id)
		b.nodeCacheMu.Unlock()

		// Decrement cached node count for O(1) stats lookups
		b.nodeCount.Add(-1)

		// Decrement cached edge count for edges deleted with this node
		if totalEdgesDeleted > 0 {
			b.edgeCount.Add(-totalEdgesDeleted)
			// Invalidate edge type cache since we deleted edges
			b.InvalidateEdgeTypeCache()
			// Notify listeners about deleted edges
			for _, edgeID := range deletedEdgeIDs {
				b.notifyEdgeDeleted(edgeID)
			}
		}

		// Notify listeners (e.g., search service) to remove from indexes
		b.notifyNodeDeleted(id)
	}

	return err
}

// deleteEdgesWithPrefix deletes all edges matching a prefix (helper for DeleteNode).
// deleteEdgesWithPrefix deletes all edges matching the given prefix.
// Returns the count of edges actually deleted for accurate stats tracking.
// IMPORTANT: The returned count MUST be used to decrement edgeCount after txn commits.
func (b *BadgerEngine) deleteEdgesWithPrefix(txn *badger.Txn, prefix []byte) (int64, []EdgeID, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	var edgeIDs []EdgeID
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
		edgeIDs = append(edgeIDs, edgeID)
	}

	var deletedCount int64
	var deletedIDs []EdgeID
	for _, edgeID := range edgeIDs {
		err := b.deleteEdgeInTxn(txn, edgeID)
		if err == nil {
			deletedCount++
			deletedIDs = append(deletedIDs, edgeID)
		} else if err != ErrNotFound {
			return 0, nil, err
		}
	}

	return deletedCount, deletedIDs, nil
}

// ============================================================================
// Edge Operations
// ============================================================================

// CreateEdge creates a new edge between two nodes.
func (b *BadgerEngine) CreateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	err := b.db.Update(func(txn *badger.Txn) error {
		// Check if edge already exists
		key := edgeKey(edge.ID)
		_, err := txn.Get(key)
		if err == nil {
			return ErrAlreadyExists
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// Verify start node exists
		_, err = txn.Get(nodeKey(edge.StartNode))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// Verify end node exists
		_, err = txn.Get(nodeKey(edge.EndNode))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// Serialize edge
		data, err := encodeEdge(edge)
		if err != nil {
			return fmt.Errorf("failed to encode edge: %w", err)
		}

		// Store edge
		if err := txn.Set(key, data); err != nil {
			return err
		}

		// Create outgoing index
		outKey := outgoingIndexKey(edge.StartNode, edge.ID)
		if err := txn.Set(outKey, []byte{}); err != nil {
			return err
		}

		// Create incoming index
		inKey := incomingIndexKey(edge.EndNode, edge.ID)
		if err := txn.Set(inKey, []byte{}); err != nil {
			return err
		}

		// Create edge type index
		edgeTypeKey := edgeTypeIndexKey(edge.Type, edge.ID)
		if err := txn.Set(edgeTypeKey, []byte{}); err != nil {
			return err
		}

		return nil
	})

	// Invalidate only this edge type (not entire cache)
	if err == nil {
		b.InvalidateEdgeTypeCacheForType(edge.Type)

		// Increment cached edge count for O(1) stats lookups
		b.edgeCount.Add(1)

		// Notify listeners (e.g., graph analyzers) about the new edge
		b.notifyEdgeCreated(edge)
	}

	return err
}

// GetEdge retrieves an edge by ID.
func (b *BadgerEngine) GetEdge(id EdgeID) (*Edge, error) {
	if id == "" {
		return nil, ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	var edge *Edge
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(edgeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			var decodeErr error
			edge, decodeErr = decodeEdge(val)
			return decodeErr
		})
	})

	return edge, err
}

// UpdateEdge updates an existing edge.
func (b *BadgerEngine) UpdateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	err := b.db.Update(func(txn *badger.Txn) error {
		key := edgeKey(edge.ID)

		// Get existing edge
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		var existing *Edge
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			existing, decodeErr = decodeEdge(val)
			return decodeErr
		}); err != nil {
			return err
		}

		// If endpoints changed, update indexes
		if existing.StartNode != edge.StartNode || existing.EndNode != edge.EndNode {
			// Verify new endpoints exist
			if _, err := txn.Get(nodeKey(edge.StartNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			if _, err := txn.Get(nodeKey(edge.EndNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}

			// Remove old indexes
			if err := txn.Delete(outgoingIndexKey(existing.StartNode, edge.ID)); err != nil {
				return err
			}
			if err := txn.Delete(incomingIndexKey(existing.EndNode, edge.ID)); err != nil {
				return err
			}

			// Add new indexes
			if err := txn.Set(outgoingIndexKey(edge.StartNode, edge.ID), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(incomingIndexKey(edge.EndNode, edge.ID), []byte{}); err != nil {
				return err
			}
		}

		// Store updated edge
		data, err := encodeEdge(edge)
		if err != nil {
			return fmt.Errorf("failed to encode edge: %w", err)
		}

		return txn.Set(key, data)
	})

	// Notify listeners on successful update
	if err == nil {
		b.notifyEdgeUpdated(edge)
	}

	return err
}

// DeleteEdge removes an edge.
func (b *BadgerEngine) DeleteEdge(id EdgeID) error {
	if id == "" {
		return ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	// Get edge type before deletion for selective cache invalidation
	edge, _ := b.GetEdge(id)
	var edgeType string
	if edge != nil {
		edgeType = edge.Type
	}

	err := b.db.Update(func(txn *badger.Txn) error {
		return b.deleteEdgeInTxn(txn, id)
	})

	// Invalidate only this edge type (not entire cache)
	if err == nil && edgeType != "" {
		b.InvalidateEdgeTypeCacheForType(edgeType)

		// Decrement cached edge count for O(1) stats lookups
		b.edgeCount.Add(-1)

		// Notify listeners (e.g., graph analyzers) about the deleted edge
		b.notifyEdgeDeleted(id)
	}

	return err
}

// deleteEdgeInTxn is the internal helper for deleting an edge within a transaction.
func (b *BadgerEngine) deleteEdgeInTxn(txn *badger.Txn, id EdgeID) error {
	key := edgeKey(id)

	// Get edge for index cleanup
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = decodeEdge(val)
		return decodeErr
	}); err != nil {
		return err
	}

	// Delete indexes
	if err := txn.Delete(outgoingIndexKey(edge.StartNode, id)); err != nil {
		return err
	}
	if err := txn.Delete(incomingIndexKey(edge.EndNode, id)); err != nil {
		return err
	}
	if err := txn.Delete(edgeTypeIndexKey(edge.Type, id)); err != nil {
		return err
	}

	// Delete edge
	return txn.Delete(key)
}

// deleteNodeInTxn is the internal helper for deleting a node within a transaction.
// Returns the count of edges that were deleted along with the node (for stats tracking).
func (b *BadgerEngine) deleteNodeInTxn(txn *badger.Txn, id NodeID) (edgesDeleted int64, deletedEdgeIDs []EdgeID, err error) {
	key := nodeKey(id)

	// Get node for label cleanup
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil, ErrNotFound
	}
	if err != nil {
		return 0, nil, err
	}

	var node *Node
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		node, decodeErr = decodeNode(val)
		return decodeErr
	}); err != nil {
		return 0, nil, err
	}

	// Delete label indexes
	for _, label := range node.Labels {
		if err := txn.Delete(labelIndexKey(label, id)); err != nil {
			return 0, nil, err
		}
	}

	// Delete outgoing edges (and track count)
	outPrefix := outgoingIndexPrefix(id)
	outCount, outIDs, err := b.deleteEdgesWithPrefix(txn, outPrefix)
	if err != nil {
		return 0, nil, err
	}
	edgesDeleted += outCount
	deletedEdgeIDs = append(deletedEdgeIDs, outIDs...)

	// Delete incoming edges (and track count)
	inPrefix := incomingIndexPrefix(id)
	inCount, inIDs, err := b.deleteEdgesWithPrefix(txn, inPrefix)
	if err != nil {
		return 0, nil, err
	}
	edgesDeleted += inCount
	deletedEdgeIDs = append(deletedEdgeIDs, inIDs...)

	// Delete the node
	return edgesDeleted, deletedEdgeIDs, txn.Delete(key)
}

// BulkDeleteNodes removes multiple nodes in a single transaction.
// This is much faster than calling DeleteNode repeatedly.
// IMPORTANT: This also deletes all edges connected to the deleted nodes and updates edge counts.
func (b *BadgerEngine) BulkDeleteNodes(ids []NodeID) error {
	if len(ids) == 0 {
		return nil
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	// Track which nodes were actually deleted for accurate counting
	deletedNodeCount := int64(0)
	deletedNodeIDs := make([]NodeID, 0, len(ids))
	// Track edges deleted along with nodes
	totalEdgesDeleted := int64(0)
	deletedEdgeIDs := make([]EdgeID, 0)

	err := b.db.Update(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue // Skip invalid IDs
			}
			edgesDeleted, edgeIDs, err := b.deleteNodeInTxn(txn, id)
			if err == nil {
				deletedNodeCount++                          // Successfully deleted
				deletedNodeIDs = append(deletedNodeIDs, id) // Track for callbacks
				totalEdgesDeleted += edgesDeleted
				deletedEdgeIDs = append(deletedEdgeIDs, edgeIDs...)
			} else if err != ErrNotFound {
				return err // Actual error, abort transaction
			}
			// ErrNotFound is ignored (node didn't exist, no count change)
		}
		return nil
	})

	// Invalidate cache for deleted nodes and update counts
	if err == nil {
		b.nodeCacheMu.Lock()
		for _, id := range ids {
			delete(b.nodeCache, id)
		}
		b.nodeCacheMu.Unlock()

		// Decrement cached node count by actual number deleted (accurate)
		if deletedNodeCount > 0 {
			b.nodeCount.Add(-deletedNodeCount)
		}

		// Decrement cached edge count by edges deleted along with nodes
		if totalEdgesDeleted > 0 {
			b.edgeCount.Add(-totalEdgesDeleted)
			// Invalidate edge type cache since we deleted edges
			b.InvalidateEdgeTypeCache()
			// Notify listeners about deleted edges
			for _, edgeID := range deletedEdgeIDs {
				b.notifyEdgeDeleted(edgeID)
			}
		}

		// Notify listeners (e.g., search service) for each deleted node
		for _, id := range deletedNodeIDs {
			b.notifyNodeDeleted(id)
		}
	}

	return err
}

// BulkDeleteEdges removes multiple edges in a single transaction.
// This is much faster than calling DeleteEdge repeatedly.
func (b *BadgerEngine) BulkDeleteEdges(ids []EdgeID) error {
	if len(ids) == 0 {
		return nil
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	// Track which edges were actually deleted for accurate counting
	deletedCount := int64(0)
	deletedIDs := make([]EdgeID, 0, len(ids))
	err := b.db.Update(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue // Skip invalid IDs
			}
			err := b.deleteEdgeInTxn(txn, id)
			if err == nil {
				deletedCount++                      // Successfully deleted
				deletedIDs = append(deletedIDs, id) // Track for callbacks
			} else if err != ErrNotFound {
				return err // Actual error, abort transaction
			}
			// ErrNotFound is ignored (edge didn't exist, no count change)
		}
		return nil
	})

	// Invalidate edge type cache on successful bulk delete and update count
	if err == nil && deletedCount > 0 {
		b.InvalidateEdgeTypeCache()

		// Decrement cached edge count by actual number deleted (accurate)
		b.edgeCount.Add(-deletedCount)

		// Notify listeners (e.g., graph analyzers) for each deleted edge
		for _, id := range deletedIDs {
			b.notifyEdgeDeleted(id)
		}
	}

	return err
}

// ============================================================================
// Query Operations
// ============================================================================

// GetFirstNodeByLabel returns the first node with the specified label.
// This is optimized for MATCH...LIMIT 1 patterns - stops after first match.
func (b *BadgerEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	var node *Node
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := labelIndexPrefix(label)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			nodeID := extractNodeIDFromLabelIndex(it.Item().Key(), len(label))
			if nodeID == "" {
				continue
			}

			item, err := txn.Get(nodeKey(nodeID))
			if err != nil {
				continue
			}

			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = decodeNode(val)
				return decodeErr
			}); err != nil {
				continue
			}

			return nil // Found first node, stop
		}
		return nil
	})

	return node, err
}

// GetNodesByLabel returns all nodes with the specified label.
func (b *BadgerEngine) GetNodesByLabel(label string) ([]*Node, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	// Single-pass: iterate label index and fetch nodes in same transaction
	// This reduces transaction overhead compared to two-phase approach
	var nodes []*Node
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := labelIndexPrefix(label)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Don't need values from index keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			nodeID := extractNodeIDFromLabelIndex(it.Item().Key(), len(label))
			if nodeID == "" {
				continue
			}

			// Fetch node data in same transaction
			item, err := txn.Get(nodeKey(nodeID))
			if err != nil {
				continue // Skip if node was deleted
			}

			var node *Node
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = decodeNode(val)
				return decodeErr
			}); err != nil {
				continue
			}

			nodes = append(nodes, node)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return nodes, nil
}

// GetAllNodes returns all nodes in the storage.
func (b *BadgerEngine) GetAllNodes() []*Node {
	nodes, _ := b.AllNodes()
	return nodes
}

// AllNodes returns all nodes (implements Engine interface).
func (b *BadgerEngine) AllNodes() ([]*Node, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	var nodes []*Node
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var node *Node
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = decodeNode(val)
				return decodeErr
			}); err != nil {
				continue
			}

			nodes = append(nodes, node)
		}

		return nil
	})

	return nodes, err
}

// AllEdges returns all edges (implements Engine interface).
func (b *BadgerEngine) AllEdges() ([]*Edge, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	var edges []*Edge
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte{prefixEdge}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var edge *Edge
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	return edges, err
}

// GetEdgesByType returns all edges of a specific type using the edge type index.
// This is MUCH faster than AllEdges() for queries like mutual follows.
// Edge types are matched case-insensitively (Neo4j compatible).
// Results are cached per type to speed up repeated queries.
func (b *BadgerEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	if edgeType == "" {
		return b.AllEdges() // No type filter = all edges
	}

	normalizedType := strings.ToLower(edgeType)

	// Check cache first
	b.edgeTypeCacheMu.RLock()
	if cached, ok := b.edgeTypeCache[normalizedType]; ok {
		b.edgeTypeCacheMu.RUnlock()
		return cached, nil
	}
	b.edgeTypeCacheMu.RUnlock()

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	var edges []*Edge
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := edgeTypeIndexPrefix(edgeType)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need the key to get edgeID
		it := txn.NewIterator(opts)
		defer it.Close()

		// Collect edge IDs from index
		var edgeIDs []EdgeID
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			// Extract edgeID from key: prefix + type + 0x00 + edgeID
			sepIdx := bytes.LastIndexByte(key, 0x00)
			if sepIdx >= 0 && sepIdx < len(key)-1 {
				edgeIDs = append(edgeIDs, EdgeID(key[sepIdx+1:]))
			}
		}

		// Batch fetch edges
		edges = make([]*Edge, 0, len(edgeIDs))
		for _, edgeID := range edgeIDs {
			item, err := txn.Get(edgeKey(edgeID))
			if err != nil {
				continue
			}

			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Cache the result (simple LRU-style: clear if too many types)
	b.edgeTypeCacheMu.Lock()
	if len(b.edgeTypeCache) > 50 {
		b.edgeTypeCache = make(map[string][]*Edge, 50)
	}
	b.edgeTypeCache[normalizedType] = edges
	b.edgeTypeCacheMu.Unlock()

	return edges, nil
}

// InvalidateEdgeTypeCache clears the entire edge type cache.
// Called after bulk edge mutations to ensure cache consistency.
func (b *BadgerEngine) InvalidateEdgeTypeCache() {
	b.edgeTypeCacheMu.Lock()
	b.edgeTypeCache = make(map[string][]*Edge, 50)
	b.edgeTypeCacheMu.Unlock()
}

// InvalidateEdgeTypeCacheForType removes only the specified edge type from cache.
// Much faster than full invalidation for single-edge operations.
func (b *BadgerEngine) InvalidateEdgeTypeCacheForType(edgeType string) {
	if edgeType == "" {
		return
	}
	normalizedType := strings.ToLower(edgeType)
	b.edgeTypeCacheMu.Lock()
	delete(b.edgeTypeCache, normalizedType)
	b.edgeTypeCacheMu.Unlock()
}

// BatchGetNodes fetches multiple nodes in a single transaction.
// Returns a map for O(1) lookup by ID. Missing nodes are not included in the result.
// This is optimized for traversal operations that need to fetch many nodes.
func (b *BadgerEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	if len(ids) == 0 {
		return make(map[NodeID]*Node), nil
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	result := make(map[NodeID]*Node, len(ids))
	err := b.db.View(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue
			}

			item, err := txn.Get(nodeKey(id))
			if err != nil {
				continue // Skip missing nodes
			}

			var node *Node
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = decodeNode(val)
				return decodeErr
			}); err != nil {
				continue
			}

			result[id] = node
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetOutgoingEdges returns all edges where the given node is the source.
func (b *BadgerEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	var edges []*Edge
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := outgoingIndexPrefix(nodeID)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
			if edgeID == "" {
				continue
			}

			// Get the edge
			item, err := txn.Get(edgeKey(edgeID))
			if err != nil {
				continue
			}

			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return edges, nil
}

// GetIncomingEdges returns all edges where the given node is the target.
func (b *BadgerEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	var edges []*Edge
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := incomingIndexPrefix(nodeID)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
			if edgeID == "" {
				continue
			}

			// Get the edge
			item, err := txn.Get(edgeKey(edgeID))
			if err != nil {
				continue
			}

			var edge *Edge
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
				continue
			}

			edges = append(edges, edge)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return edges, nil
}

// GetEdgesBetween returns all edges between two nodes.
func (b *BadgerEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	outgoing, err := b.GetOutgoingEdges(startID)
	if err != nil {
		return nil, err
	}

	var result []*Edge
	for _, edge := range outgoing {
		if edge.EndNode == endID {
			result = append(result, edge)
		}
	}

	return result, nil
}

// GetEdgeBetween returns an edge between two nodes with the given type.
func (b *BadgerEngine) GetEdgeBetween(source, target NodeID, edgeType string) *Edge {
	edges, err := b.GetEdgesBetween(source, target)
	if err != nil {
		return nil
	}

	for _, edge := range edges {
		if edgeType == "" || edge.Type == edgeType {
			return edge
		}
	}

	return nil
}

// ============================================================================
// Bulk Operations
// ============================================================================

// BulkCreateNodes creates multiple nodes in a single transaction.
func (b *BadgerEngine) BulkCreateNodes(nodes []*Node) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	// Validate all nodes first
	for _, node := range nodes {
		if node == nil {
			return ErrInvalidData
		}
		if node.ID == "" {
			return ErrInvalidID
		}
	}

	// Check unique constraints for all nodes BEFORE inserting any
	for _, node := range nodes {
		for _, label := range node.Labels {
			for propName, propValue := range node.Properties {
				if err := b.schema.CheckUniqueConstraint(label, propName, propValue, ""); err != nil {
					return fmt.Errorf("constraint violation: %w", err)
				}
			}
		}
	}

	err := b.db.Update(func(txn *badger.Txn) error {
		// Check for duplicates
		for _, node := range nodes {
			_, err := txn.Get(nodeKey(node.ID))
			if err == nil {
				return ErrAlreadyExists
			}
			if err != badger.ErrKeyNotFound {
				return err
			}
		}

		// Insert all nodes
		for _, node := range nodes {
			data, err := encodeNode(node)
			if err != nil {
				return fmt.Errorf("failed to encode node: %w", err)
			}

			if err := txn.Set(nodeKey(node.ID), data); err != nil {
				return err
			}

			for _, label := range node.Labels {
				if err := txn.Set(labelIndexKey(label, node.ID), []byte{}); err != nil {
					return err
				}
			}
		}

		return nil
	})

	// Register unique constraint values after successful bulk insert
	if err == nil {
		for _, node := range nodes {
			for _, label := range node.Labels {
				for propName, propValue := range node.Properties {
					b.schema.RegisterUniqueValue(label, propName, propValue, node.ID)
				}
			}
		}

		// Increment cached node count for O(1) stats lookups
		b.nodeCount.Add(int64(len(nodes)))

		// Notify listeners (e.g., search service) to index all new nodes
		for _, node := range nodes {
			b.notifyNodeCreated(node)
		}
	}

	return err
}

// BulkCreateEdges creates multiple edges in a single transaction.
func (b *BadgerEngine) BulkCreateEdges(edges []*Edge) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	// Validate all edges first
	for _, edge := range edges {
		if edge == nil {
			return ErrInvalidData
		}
		if edge.ID == "" {
			return ErrInvalidID
		}
	}

	err := b.db.Update(func(txn *badger.Txn) error {
		// Validate all edges
		for _, edge := range edges {
			// Check edge doesn't exist
			_, err := txn.Get(edgeKey(edge.ID))
			if err == nil {
				return ErrAlreadyExists
			}
			if err != badger.ErrKeyNotFound {
				return err
			}

			// Verify nodes exist
			if _, err := txn.Get(nodeKey(edge.StartNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			if _, err := txn.Get(nodeKey(edge.EndNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
		}

		// Insert all edges
		for _, edge := range edges {
			data, err := encodeEdge(edge)
			if err != nil {
				return fmt.Errorf("failed to encode edge: %w", err)
			}

			if err := txn.Set(edgeKey(edge.ID), data); err != nil {
				return err
			}

			if err := txn.Set(outgoingIndexKey(edge.StartNode, edge.ID), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(incomingIndexKey(edge.EndNode, edge.ID), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(edgeTypeIndexKey(edge.Type, edge.ID), []byte{}); err != nil {
				return err
			}
		}

		return nil
	})

	// Invalidate edge type cache on successful bulk create
	if err == nil && len(edges) > 0 {
		b.InvalidateEdgeTypeCache()

		// Increment cached edge count for O(1) stats lookups
		b.edgeCount.Add(int64(len(edges)))

		// Notify listeners (e.g., graph analyzers) for all new edges
		for _, edge := range edges {
			b.notifyEdgeCreated(edge)
		}
	}

	return err
}

// ============================================================================
// Degree Functions
// ============================================================================

// GetInDegree returns the number of incoming edges to a node.
func (b *BadgerEngine) GetInDegree(nodeID NodeID) int {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0
	}
	b.mu.RUnlock()

	count := 0
	_ = b.db.View(func(txn *badger.Txn) error {
		prefix := incomingIndexPrefix(nodeID)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// GetOutDegree returns the number of outgoing edges from a node.
func (b *BadgerEngine) GetOutDegree(nodeID NodeID) int {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0
	}
	b.mu.RUnlock()

	count := 0
	_ = b.db.View(func(txn *badger.Txn) error {
		prefix := outgoingIndexPrefix(nodeID)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// ============================================================================
// Stats and Lifecycle
// ============================================================================

// NodeCount returns the total number of valid, decodable nodes.
// This is consistent with AllNodes() - only counts nodes that can be successfully decoded.
// Corrupted or incompatible node entries are not counted.
// initializeCounts scans the database once to initialize the cached node and edge counts.
// This is called only on engine startup to enable O(1) stats lookups.
func (b *BadgerEngine) initializeCounts() error {
	var nodeCount, edgeCount int64

	err := b.db.View(func(txn *badger.Txn) error {
		// Count nodes
		nodeOpts := badger.DefaultIteratorOptions
		nodeOpts.PrefetchValues = false // Only need keys for counting
		nodeIt := txn.NewIterator(nodeOpts)
		defer nodeIt.Close()

		nodePrefix := []byte{prefixNode}
		for nodeIt.Seek(nodePrefix); nodeIt.ValidForPrefix(nodePrefix); nodeIt.Next() {
			nodeCount++
		}

		// Count edges
		edgeOpts := badger.DefaultIteratorOptions
		edgeOpts.PrefetchValues = false // Only need keys for counting
		edgeIt := txn.NewIterator(edgeOpts)
		defer edgeIt.Close()

		edgePrefix := []byte{prefixEdge}
		for edgeIt.Seek(edgePrefix); edgeIt.ValidForPrefix(edgePrefix); edgeIt.Next() {
			edgeCount++
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Initialize atomic counters
	b.nodeCount.Store(nodeCount)
	b.edgeCount.Store(edgeCount)

	return nil
}

func (b *BadgerEngine) NodeCount() (int64, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, ErrStorageClosed
	}
	b.mu.RUnlock()

	// Return cached count for O(1) performance
	// The counter is updated atomically on create/delete operations
	count := b.nodeCount.Load()

	// Clamp to zero if negative (should never happen, log for debugging)
	if count < 0 {
		log.Printf("⚠️ [COUNT BUG] BadgerEngine.NodeCount went negative: %d (clamping to 0)", count)
		return 0, nil
	}
	return count, nil
}

// EdgeCount returns the total number of valid, decodable edges.
// This is consistent with AllEdges() - only counts edges that can be successfully decoded.
func (b *BadgerEngine) EdgeCount() (int64, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, ErrStorageClosed
	}
	b.mu.RUnlock()

	// Return cached count for O(1) performance
	// The counter is updated atomically on create/delete operations
	count := b.edgeCount.Load()

	// Clamp to zero if negative (should never happen, log for debugging)
	if count < 0 {
		log.Printf("⚠️ [COUNT BUG] BadgerEngine.EdgeCount went negative: %d (clamping to 0)", count)
		return 0, nil
	}
	return count, nil
}

// GetSchema returns the schema manager.
func (b *BadgerEngine) GetSchema() *SchemaManager {
	return b.schema
}

// Close closes the BadgerDB database.
func (b *BadgerEngine) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.closed = true
	return b.db.Close()
}

// Sync forces a sync of all data to disk.
// This is useful for ensuring durability before a crash.
func (b *BadgerEngine) Sync() error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	return b.db.Sync()
}

// RunGC runs garbage collection on the BadgerDB value log.
// Should be called periodically for long-running applications.
func (b *BadgerEngine) RunGC() error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	return b.db.RunValueLogGC(0.5)
}

// Size returns the approximate size of the database in bytes.
func (b *BadgerEngine) Size() (lsm, vlog int64) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, 0
	}
	b.mu.RUnlock()

	return b.db.Size()
}

// FindNodeNeedingEmbedding returns a node that needs embedding.
// Uses Badger's secondary index (prefixPendingEmbed) for O(1) lookup.
//
// This is highly optimized:
// - O(1) to find next node (just seek to prefix and get first key)
// - No in-memory index needed
// - Persistent across restarts
// - Atomic with node operations
func (b *BadgerEngine) FindNodeNeedingEmbedding() *Node {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil
	}
	b.mu.RUnlock()

	var nodeID NodeID

	// Find first node in pending embeddings index - O(1)
	b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Keys only
		opts.PrefetchSize = 1
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{prefixPendingEmbed}
		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			// Extract nodeID from key (skip prefix byte)
			key := it.Item().Key()
			if len(key) > 1 {
				nodeID = NodeID(key[1:])
			}
		}
		return nil
	})

	if nodeID == "" {
		return nil // No nodes need embedding
	}

	// Load the full node
	node, err := b.GetNode(nodeID)
	if err != nil || node == nil {
		// Node was deleted - remove from pending index
		b.MarkNodeEmbedded(nodeID)
		// Try again with next node
		return b.FindNodeNeedingEmbedding()
	}

	// Double-check it still needs embedding (may have been updated externally)
	if len(node.Embedding) > 0 || !NodeNeedsEmbedding(node) {
		b.MarkNodeEmbedded(nodeID)
		// Try again with next node
		return b.FindNodeNeedingEmbedding()
	}

	return node
}

// MarkNodeEmbedded removes a node from the pending embeddings index.
// Call this after successfully embedding a node.
func (b *BadgerEngine) MarkNodeEmbedded(nodeID NodeID) {
	b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(pendingEmbedKey(nodeID))
	})
}

// AddToPendingEmbeddings adds a node to the pending embeddings index.
// Call this when creating a node that needs embedding.
func (b *BadgerEngine) AddToPendingEmbeddings(nodeID NodeID) {
	b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(pendingEmbedKey(nodeID), []byte{})
	})
}

// PendingEmbeddingsCount returns the number of nodes waiting for embedding.
// Note: This requires a scan of the pending index, so use sparingly.
func (b *BadgerEngine) PendingEmbeddingsCount() int {
	count := 0
	b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Keys only - fast
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{prefixPendingEmbed}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	return count
}

// InvalidatePendingEmbeddingsIndex is a no-op for Badger index.
// The index is persistent and doesn't need invalidation.
func (b *BadgerEngine) InvalidatePendingEmbeddingsIndex() {
	// No-op - Badger index is persistent and self-maintaining
}

// RefreshPendingEmbeddingsIndex rebuilds the pending embeddings index.
// This scans all nodes and adds any missing ones to the index.
// Use this on startup or after bulk imports.
func (b *BadgerEngine) RefreshPendingEmbeddingsIndex() int {
	added := 0

	b.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{prefixNode}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			item.Value(func(val []byte) error {
				node, err := decodeNode(val)
				if err != nil {
					return nil
				}

				// Skip internal nodes
				for _, label := range node.Labels {
					if len(label) > 0 && label[0] == '_' {
						return nil
					}
				}

				// Check if needs embedding and not already in index
				if len(node.Embedding) == 0 && NodeNeedsEmbedding(node) {
					// Check if already in pending index
					_, err := txn.Get(pendingEmbedKey(node.ID))
					if err == badger.ErrKeyNotFound {
						txn.Set(pendingEmbedKey(node.ID), []byte{})
						added++
					}
				}
				return nil
			})
		}
		return nil
	})

	if added > 0 {
		fmt.Printf("📊 Pending embeddings index refreshed: added %d nodes\n", added)
	}
	return added
}

// IterateNodes iterates through all nodes one at a time without loading all into memory.
// The callback returns true to continue, false to stop.
func (b *BadgerEngine) IterateNodes(fn func(*Node) bool) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{prefixNode}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			var node *Node
			err := item.Value(func(val []byte) error {
				var decErr error
				node, decErr = decodeNode(val)
				return decErr
			})
			if err != nil {
				continue // Skip invalid nodes
			}
			if !fn(node) {
				break // Callback requested stop
			}
		}
		return nil
	})
}

// StreamNodes implements StreamingEngine.StreamNodes for memory-efficient iteration.
// Iterates through all nodes one at a time without loading all into memory.
func (b *BadgerEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{prefixNode}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			var node *Node
			err := item.Value(func(val []byte) error {
				var decErr error
				node, decErr = decodeNode(val)
				return decErr
			})
			if err != nil {
				continue // Skip invalid nodes
			}
			if err := fn(node); err != nil {
				if err == ErrIterationStopped {
					return nil // Normal stop
				}
				return err
			}
		}
		return nil
	})
}

// StreamEdges implements StreamingEngine.StreamEdges for memory-efficient iteration.
// Iterates through all edges one at a time without loading all into memory.
func (b *BadgerEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{prefixEdge}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			var edge *Edge
			err := item.Value(func(val []byte) error {
				var decErr error
				edge, decErr = decodeEdge(val)
				return decErr
			})
			if err != nil {
				continue // Skip invalid edges
			}
			if err := fn(edge); err != nil {
				if err == ErrIterationStopped {
					return nil // Normal stop
				}
				return err
			}
		}
		return nil
	})
}

// StreamNodeChunks implements StreamingEngine.StreamNodeChunks for batch processing.
// Iterates through nodes in chunks, more efficient for batch operations.
func (b *BadgerEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrStorageClosed
	}
	b.mu.RUnlock()

	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = min(chunkSize, 100)
		it := txn.NewIterator(opts)
		defer it.Close()

		chunk := make([]*Node, 0, chunkSize)
		prefix := []byte{prefixNode}

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			var node *Node
			err := item.Value(func(val []byte) error {
				var decErr error
				node, decErr = decodeNode(val)
				return decErr
			})
			if err != nil {
				continue // Skip invalid nodes
			}

			chunk = append(chunk, node)

			if len(chunk) >= chunkSize {
				if err := fn(chunk); err != nil {
					return err
				}
				// Reset chunk, reuse capacity
				chunk = chunk[:0]
			}
		}

		// Process remaining nodes
		if len(chunk) > 0 {
			if err := fn(chunk); err != nil {
				return err
			}
		}

		return nil
	})
}

// ============================================================================
// Utility functions for compatibility
// ============================================================================

// HasPrefix checks if a byte slice has the given prefix.
func hasPrefix(s, prefix []byte) bool {
	return len(s) >= len(prefix) && bytes.Equal(s[:len(prefix)], prefix)
}

// ClearAllEmbeddings removes embeddings from all nodes, allowing them to be regenerated.
// Returns the number of nodes that had their embeddings cleared.
func (b *BadgerEngine) ClearAllEmbeddings() (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, ErrStorageClosed
	}
	b.mu.Unlock()

	cleared := 0

	// First, collect all node IDs that have embeddings
	var nodeIDs []NodeID
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{prefixNode}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				node, err := decodeNode(val)
				if err != nil {
					return nil // Skip invalid nodes
				}
				if len(node.Embedding) > 0 {
					nodeIDs = append(nodeIDs, node.ID)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("error scanning nodes: %w", err)
	}

	// Now update each node to clear its embedding
	for _, id := range nodeIDs {
		node, err := b.GetNode(id)
		if err != nil {
			continue // Skip if node no longer exists
		}
		node.Embedding = nil
		if err := b.UpdateNode(node); err != nil {
			log.Printf("Warning: failed to clear embedding for node %s: %v", id, err)
			continue
		}
		cleared++
	}

	log.Printf("✓ Cleared embeddings from %d nodes", cleared)
	return cleared, nil
}

// Backup creates a backup of the database to the specified file path.
// Uses BadgerDB's streaming backup which creates a consistent snapshot.
// The backup file is a self-contained, portable copy of the database.
func (b *BadgerEngine) Backup(path string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrStorageClosed
	}

	// Create backup file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer f.Close()

	// Use BufferedWriter for better performance
	buf := bufio.NewWriterSize(f, 16*1024*1024) // 16MB buffer

	// Stream backup (since=0 means full backup)
	_, err = b.db.Backup(buf, 0)
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}

	// Flush buffer
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("failed to flush backup: %w", err)
	}

	// Sync to disk
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync backup: %w", err)
	}

	return nil
}

// Verify BadgerEngine implements Engine interface
var _ Engine = (*BadgerEngine)(nil)
