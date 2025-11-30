// Package storage provides storage engine implementations for NornicDB.
//
// BadgerEngine provides persistent disk-based storage using BadgerDB.
// It implements the Engine interface with full ACID transaction support.
package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

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
	db     *badger.DB
	schema *SchemaManager
	mu     sync.RWMutex // Protects schema operations
	closed bool
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

	// Apply low memory settings to reduce RAM usage
	// These settings are always applied for containerized environments
	badgerOpts = badgerOpts.
		WithMemTableSize(16 << 20).     // 16MB instead of 64MB
		WithValueLogFileSize(64 << 20). // 64MB instead of 1GB
		WithNumMemtables(2).            // 2 instead of 5
		WithNumLevelZeroTables(2).      // 2 instead of 5
		WithNumLevelZeroTablesStall(4). // 4 instead of 15
		WithValueThreshold(1024).       // Store values > 1KB in value log
		WithBlockCacheSize(32 << 20).   // 32MB block cache
		WithIndexCacheSize(16 << 20)    // 16MB index cache

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	return &BadgerEngine{
		db:     db,
		schema: NewSchemaManager(),
	}, nil
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

// serializableNode is the JSON-serializable form of a Node.
type serializableNode struct {
	ID           string         `json:"id"`
	Labels       []string       `json:"labels"`
	Properties   map[string]any `json:"properties"`
	CreatedAt    int64          `json:"createdAt"`
	UpdatedAt    int64          `json:"updatedAt"`
	DecayScore   float64        `json:"decayScore"`
	LastAccessed int64          `json:"lastAccessed"`
	AccessCount  int64          `json:"accessCount"`
	Embedding    []float32      `json:"embedding,omitempty"`
}

// serializableEdge is the JSON-serializable form of an Edge.
type serializableEdge struct {
	ID            string         `json:"id"`
	StartNode     string         `json:"startNode"`
	EndNode       string         `json:"endNode"`
	Type          string         `json:"type"`
	Properties    map[string]any `json:"properties"`
	CreatedAt     int64          `json:"createdAt"`
	Confidence    float64        `json:"confidence"`
	AutoGenerated bool           `json:"autoGenerated"`
}

// encodeNode serializes a Node to JSON.
func encodeNode(n *Node) ([]byte, error) {
	sn := serializableNode{
		ID:           string(n.ID),
		Labels:       n.Labels,
		Properties:   n.Properties,
		CreatedAt:    n.CreatedAt.Unix(),
		UpdatedAt:    n.UpdatedAt.Unix(),
		DecayScore:   n.DecayScore,
		LastAccessed: n.LastAccessed.Unix(),
		AccessCount:  n.AccessCount,
		Embedding:    n.Embedding,
	}
	return json.Marshal(sn)
}

// decodeNode deserializes a Node from JSON.
func decodeNode(data []byte) (*Node, error) {
	var sn serializableNode
	if err := json.Unmarshal(data, &sn); err != nil {
		return nil, err
	}

	return &Node{
		ID:           NodeID(sn.ID),
		Labels:       sn.Labels,
		Properties:   sn.Properties,
		CreatedAt:    unixToTime(sn.CreatedAt),
		UpdatedAt:    unixToTime(sn.UpdatedAt),
		DecayScore:   sn.DecayScore,
		LastAccessed: unixToTime(sn.LastAccessed),
		AccessCount:  sn.AccessCount,
		Embedding:    sn.Embedding,
	}, nil
}

// encodeEdge serializes an Edge to JSON.
func encodeEdge(e *Edge) ([]byte, error) {
	se := serializableEdge{
		ID:            string(e.ID),
		StartNode:     string(e.StartNode),
		EndNode:       string(e.EndNode),
		Type:          e.Type,
		Properties:    e.Properties,
		CreatedAt:     e.CreatedAt.Unix(),
		Confidence:    e.Confidence,
		AutoGenerated: e.AutoGenerated,
	}
	return json.Marshal(se)
}

// decodeEdge deserializes an Edge from JSON.
func decodeEdge(data []byte) (*Edge, error) {
	var se serializableEdge
	if err := json.Unmarshal(data, &se); err != nil {
		return nil, err
	}

	return &Edge{
		ID:            EdgeID(se.ID),
		StartNode:     NodeID(se.StartNode),
		EndNode:       NodeID(se.EndNode),
		Type:          se.Type,
		Properties:    se.Properties,
		CreatedAt:     unixToTime(se.CreatedAt),
		Confidence:    se.Confidence,
		AutoGenerated: se.AutoGenerated,
	}, nil
}

// unixToTime converts Unix timestamp to time.Time.
func unixToTime(unix int64) time.Time {
	if unix <= 0 {
		return time.Time{}
	}
	return time.Unix(unix, 0)
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

	return b.db.Update(func(txn *badger.Txn) error {
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

		return nil
	})
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

	return node, err
}

// UpdateNode updates an existing node.
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

	return b.db.Update(func(txn *badger.Txn) error {
		key := nodeKey(node.ID)

		// Get existing node for label index updates
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

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

		return nil
	})
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

	return b.db.Update(func(txn *badger.Txn) error {
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

		// Delete outgoing edges
		outPrefix := outgoingIndexPrefix(id)
		if err := b.deleteEdgesWithPrefix(txn, outPrefix); err != nil {
			return err
		}

		// Delete incoming edges
		inPrefix := incomingIndexPrefix(id)
		if err := b.deleteEdgesWithPrefix(txn, inPrefix); err != nil {
			return err
		}

		// Delete the node
		return txn.Delete(key)
	})
}

// deleteEdgesWithPrefix deletes all edges matching a prefix (helper for DeleteNode).
func (b *BadgerEngine) deleteEdgesWithPrefix(txn *badger.Txn, prefix []byte) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	var edgeIDs []EdgeID
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
		edgeIDs = append(edgeIDs, edgeID)
	}

	for _, edgeID := range edgeIDs {
		if err := b.deleteEdgeInTxn(txn, edgeID); err != nil && err != ErrNotFound {
			return err
		}
	}

	return nil
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

	return b.db.Update(func(txn *badger.Txn) error {
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

		return nil
	})
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

	return b.db.Update(func(txn *badger.Txn) error {
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

	return b.db.Update(func(txn *badger.Txn) error {
		return b.deleteEdgeInTxn(txn, id)
	})
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

	// Delete edge
	return txn.Delete(key)
}

// deleteNodeInTxn is the internal helper for deleting a node within a transaction.
func (b *BadgerEngine) deleteNodeInTxn(txn *badger.Txn, id NodeID) error {
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

	// Delete outgoing edges
	outPrefix := outgoingIndexPrefix(id)
	if err := b.deleteEdgesWithPrefix(txn, outPrefix); err != nil {
		return err
	}

	// Delete incoming edges
	inPrefix := incomingIndexPrefix(id)
	if err := b.deleteEdgesWithPrefix(txn, inPrefix); err != nil {
		return err
	}

	// Delete the node
	return txn.Delete(key)
}

// BulkDeleteNodes removes multiple nodes in a single transaction.
// This is much faster than calling DeleteNode repeatedly.
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

	return b.db.Update(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue // Skip invalid IDs
			}
			// Best effort - continue on ErrNotFound
			if err := b.deleteNodeInTxn(txn, id); err != nil && err != ErrNotFound {
				return err
			}
		}
		return nil
	})
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

	return b.db.Update(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue // Skip invalid IDs
			}
			// Best effort - continue on ErrNotFound
			if err := b.deleteEdgeInTxn(txn, id); err != nil && err != ErrNotFound {
				return err
			}
		}
		return nil
	})
}

// ============================================================================
// Query Operations
// ============================================================================

// GetNodesByLabel returns all nodes with the specified label.
func (b *BadgerEngine) GetNodesByLabel(label string) ([]*Node, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrStorageClosed
	}
	b.mu.RUnlock()

	// OPTIMIZATION: Two-phase approach - collect IDs first, then batch fetch
	// This enables better prefetching and reduces transaction overhead
	var nodeIDs []NodeID
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := labelIndexPrefix(label)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Don't need values from index
		it := txn.NewIterator(opts)
		defer it.Close()

		// Phase 1: Collect all node IDs from label index
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			nodeID := extractNodeIDFromLabelIndex(it.Item().Key(), len(label))
			if nodeID != "" {
				nodeIDs = append(nodeIDs, nodeID)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return []*Node{}, nil
	}

	// Phase 2: Batch fetch all nodes in single transaction with prefetch
	nodes := make([]*Node, 0, len(nodeIDs))
	err = b.db.View(func(txn *badger.Txn) error {
		for _, nodeID := range nodeIDs {
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

	return b.db.Update(func(txn *badger.Txn) error {
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

	return b.db.Update(func(txn *badger.Txn) error {
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
		}

		return nil
	})
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

// NodeCount returns the total number of nodes.
func (b *BadgerEngine) NodeCount() (int64, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, ErrStorageClosed
	}
	b.mu.RUnlock()

	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	return count, err
}

// EdgeCount returns the total number of edges.
func (b *BadgerEngine) EdgeCount() (int64, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0, ErrStorageClosed
	}
	b.mu.RUnlock()

	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte{prefixEdge}
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	return count, err
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

// FindNodeNeedingEmbedding iterates through nodes and returns the first one
// without an embedding. Uses Badger's iterator to avoid loading all nodes.
func (b *BadgerEngine) FindNodeNeedingEmbedding() *Node {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil
	}
	b.mu.RUnlock()

	var result *Node
	scanned := 0
	withEmbed := 0
	internal := 0

	b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 10 // Small batch
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte{prefixNode}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			scanned++
			item := it.Item()
			err := item.Value(func(val []byte) error {
				node, err := decodeNode(val)
				if err != nil {
					return nil // Skip invalid nodes
				}

				// Skip internal nodes for stats
				for _, label := range node.Labels {
					if len(label) > 0 && label[0] == '_' {
						internal++
						return nil
					}
				}

				// Track nodes with embeddings for stats
				if len(node.Embedding) > 0 {
					withEmbed++
					return nil
				}

				// Use helper to check if node needs embedding
				if !NodeNeedsEmbedding(node) {
					return nil
				}

				// Found one that needs embedding
				result = node
				return ErrIterationStopped // Custom error to break iteration
			})
			if err == ErrIterationStopped {
				break
			}
		}
		return nil
	})

	fmt.Printf("🔍 Scanned %d nodes: %d with embeddings, %d internal, found: %v\n",
		scanned, withEmbed, internal, result != nil)

	return result
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

// Verify BadgerEngine implements Engine interface
var _ Engine = (*BadgerEngine)(nil)
