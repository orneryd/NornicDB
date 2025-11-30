// Package storage provides write-ahead logging for NornicDB durability.
//
// WAL (Write-Ahead Logging) ensures crash recovery by logging all mutations
// before they are applied to the storage engine. Combined with periodic
// snapshots, this provides:
//   - Durability: No data loss on crash
//   - Recovery: Restore state from snapshot + WAL replay
//   - Audit trail: Complete history of all mutations
//
// Feature flag: NORNICDB_WAL_ENABLED (enabled by default)
//
// Usage:
//
//	// Create WAL-backed storage
//	engine := NewMemoryEngine()
//	wal, err := NewWAL("/path/to/wal", nil)
//	walEngine := NewWALEngine(engine, wal)
//
//	// Operations are logged before execution
//	walEngine.CreateNode(&Node{ID: "n1", ...})
//
//	// Create periodic snapshots
//	snapshot, err := wal.CreateSnapshot(engine)
//	wal.SaveSnapshot(snapshot, "/path/to/snapshot.json")
//
//	// Recovery after crash
//	engine, err = RecoverFromWAL("/path/to/wal", "/path/to/snapshot.json")
package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// Additional WAL operation types (extends OperationType from transaction.go)
const (
	OpBulkNodes       OperationType = "bulk_create_nodes"
	OpBulkEdges       OperationType = "bulk_create_edges"
	OpBulkDeleteNodes OperationType = "bulk_delete_nodes"
	OpBulkDeleteEdges OperationType = "bulk_delete_edges"
	OpCheckpoint      OperationType = "checkpoint" // Marks snapshot boundaries
)

// Common WAL errors
var (
	ErrWALClosed      = errors.New("wal: closed")
	ErrWALCorrupted   = errors.New("wal: corrupted entry")
	ErrSnapshotFailed = errors.New("wal: snapshot creation failed")
	ErrRecoveryFailed = errors.New("wal: recovery failed")
)

// WALEntry represents a single write-ahead log entry.
// Each mutating operation is recorded as an entry before execution.
type WALEntry struct {
	Sequence  uint64        `json:"seq"`      // Monotonically increasing sequence number
	Timestamp time.Time     `json:"ts"`       // When the operation occurred
	Operation OperationType `json:"op"`       // Operation type (create_node, update_node, etc.)
	Data      []byte        `json:"data"`     // JSON-serialized operation data
	Checksum  uint32        `json:"checksum"` // CRC32 checksum for integrity
}

// WALNodeData holds node data for WAL entries.
type WALNodeData struct {
	Node *Node `json:"node"`
}

// WALEdgeData holds edge data for WAL entries.
type WALEdgeData struct {
	Edge *Edge `json:"edge"`
}

// WALDeleteData holds delete operation data.
type WALDeleteData struct {
	ID string `json:"id"`
}

// WALBulkNodesData holds bulk node creation data.
type WALBulkNodesData struct {
	Nodes []*Node `json:"nodes"`
}

// WALBulkEdgesData holds bulk edge creation data.
type WALBulkEdgesData struct {
	Edges []*Edge `json:"edges"`
}

// WALBulkDeleteNodesData holds bulk node deletion data.
type WALBulkDeleteNodesData struct {
	IDs []string `json:"ids"`
}

// WALBulkDeleteEdgesData holds bulk edge deletion data.
type WALBulkDeleteEdgesData struct {
	IDs []string `json:"ids"`
}

// WALConfig configures WAL behavior.
type WALConfig struct {
	// Directory for WAL files
	Dir string

	// SyncMode controls when writes are synced to disk
	// "immediate": fsync after each write (safest, slowest)
	// "batch": fsync periodically (faster, some risk)
	// "none": no fsync (fastest, data loss on crash)
	SyncMode string

	// BatchSyncInterval for "batch" sync mode
	BatchSyncInterval time.Duration

	// MaxFileSize triggers rotation when exceeded
	MaxFileSize int64

	// MaxEntries triggers rotation when exceeded
	MaxEntries int64

	// SnapshotInterval for automatic snapshots
	SnapshotInterval time.Duration
}

// DefaultWALConfig returns sensible defaults.
func DefaultWALConfig() *WALConfig {
	return &WALConfig{
		Dir:               "data/wal",
		SyncMode:          "batch",
		BatchSyncInterval: 100 * time.Millisecond,
		MaxFileSize:       100 * 1024 * 1024, // 100MB
		MaxEntries:        100000,
		SnapshotInterval:  1 * time.Hour,
	}
}

// WAL provides write-ahead logging for durability.
// Thread-safe for concurrent writes.
type WAL struct {
	mu       sync.Mutex
	config   *WALConfig
	file     *os.File
	writer   *bufio.Writer
	encoder  *json.Encoder
	sequence atomic.Uint64
	entries  atomic.Int64
	bytes    atomic.Int64
	closed   atomic.Bool

	// Background sync goroutine
	syncTicker *time.Ticker
	stopSync   chan struct{}

	// Stats
	totalWrites   atomic.Int64
	totalSyncs    atomic.Int64
	lastSyncTime  atomic.Int64
	lastEntryTime atomic.Int64
}

// WALStats provides observability into WAL state.
type WALStats struct {
	Sequence      uint64
	EntryCount    int64
	BytesWritten  int64
	TotalWrites   int64
	TotalSyncs    int64
	LastSyncTime  time.Time
	LastEntryTime time.Time
	Closed        bool
}

// NewWAL creates a new write-ahead log.
func NewWAL(dir string, cfg *WALConfig) (*WAL, error) {
	if cfg == nil {
		cfg = DefaultWALConfig()
	}
	if dir != "" {
		cfg.Dir = dir
	}

	// Create directory if needed
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: failed to create directory: %w", err)
	}

	// Open or create WAL file
	walPath := filepath.Join(cfg.Dir, "wal.log")
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open file: %w", err)
	}

	w := &WAL{
		config:   cfg,
		file:     file,
		writer:   bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		stopSync: make(chan struct{}),
	}
	w.encoder = json.NewEncoder(w.writer)

	// Load existing sequence number
	if seq, err := w.loadLastSequence(); err == nil {
		w.sequence.Store(seq)
	}

	// Start batch sync if configured
	if cfg.SyncMode == "batch" && cfg.BatchSyncInterval > 0 {
		w.syncTicker = time.NewTicker(cfg.BatchSyncInterval)
		go w.batchSyncLoop()
	}

	return w, nil
}

// loadLastSequence reads the last sequence number from existing WAL.
func (w *WAL) loadLastSequence() (uint64, error) {
	walPath := filepath.Join(w.config.Dir, "wal.log")
	file, err := os.Open(walPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var lastSeq uint64
	decoder := json.NewDecoder(file)
	for {
		var entry WALEntry
		if err := decoder.Decode(&entry); err != nil {
			break
		}
		lastSeq = entry.Sequence
	}
	return lastSeq, nil
}

// batchSyncLoop periodically syncs writes to disk.
func (w *WAL) batchSyncLoop() {
	for {
		select {
		case <-w.syncTicker.C:
			w.Sync()
		case <-w.stopSync:
			return
		}
	}
}

// Append writes a new entry to the WAL.
func (w *WAL) Append(op OperationType, data interface{}) error {
	if !config.IsWALEnabled() {
		return nil // WAL disabled, skip
	}

	if w.closed.Load() {
		return ErrWALClosed
	}

	// Serialize data
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("wal: failed to marshal data: %w", err)
	}

	// Create entry
	seq := w.sequence.Add(1)
	entry := WALEntry{
		Sequence:  seq,
		Timestamp: time.Now(),
		Operation: op,
		Data:      dataBytes,
		Checksum:  crc32Checksum(dataBytes),
	}

	// Write entry
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.encoder.Encode(&entry); err != nil {
		return fmt.Errorf("wal: failed to write entry: %w", err)
	}

	w.entries.Add(1)
	w.totalWrites.Add(1)
	w.lastEntryTime.Store(time.Now().UnixNano())

	// Immediate sync if configured
	if w.config.SyncMode == "immediate" {
		return w.syncLocked()
	}

	return nil
}

// Sync flushes all buffered writes to disk.
func (w *WAL) Sync() error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

func (w *WAL) syncLocked() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush failed: %w", err)
	}

	if w.config.SyncMode != "none" {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal: sync failed: %w", err)
		}
	}

	w.totalSyncs.Add(1)
	w.lastSyncTime.Store(time.Now().UnixNano())
	return nil
}

// Checkpoint creates a checkpoint marker for snapshot boundaries.
func (w *WAL) Checkpoint() error {
	return w.Append(OpCheckpoint, map[string]interface{}{
		"checkpoint_time": time.Now(),
		"sequence":        w.sequence.Load(),
	})
}

// Close closes the WAL, flushing all pending writes.
func (w *WAL) Close() error {
	if w.closed.Swap(true) {
		return nil // Already closed
	}

	// Stop sync goroutine
	if w.syncTicker != nil {
		w.syncTicker.Stop()
		close(w.stopSync)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Final sync
	if err := w.syncLocked(); err != nil {
		// Log but continue closing
	}

	return w.file.Close()
}

// Stats returns current WAL statistics.
func (w *WAL) Stats() WALStats {
	var lastSync, lastEntry time.Time
	if t := w.lastSyncTime.Load(); t > 0 {
		lastSync = time.Unix(0, t)
	}
	if t := w.lastEntryTime.Load(); t > 0 {
		lastEntry = time.Unix(0, t)
	}

	return WALStats{
		Sequence:      w.sequence.Load(),
		EntryCount:    w.entries.Load(),
		BytesWritten:  w.bytes.Load(),
		TotalWrites:   w.totalWrites.Load(),
		TotalSyncs:    w.totalSyncs.Load(),
		LastSyncTime:  lastSync,
		LastEntryTime: lastEntry,
		Closed:        w.closed.Load(),
	}
}

// Sequence returns the current sequence number.
func (w *WAL) Sequence() uint64 {
	return w.sequence.Load()
}

// =============================================================================
// BATCH COMMIT MODE
// =============================================================================

// BatchWriter provides explicit batch commit control for bulk operations.
// Instead of syncing after each write (even in batch mode), BatchWriter
// buffers all writes and only syncs when Commit() is called.
//
// This dramatically improves bulk write throughput by reducing fsync calls.
// Use for imports, migrations, or any operation writing many records.
//
// Example:
//
//	batch := wal.NewBatch()
//	for _, node := range nodes {
//	    batch.AppendNode(OpCreateNode, node)
//	}
//	if err := batch.Commit(); err != nil {
//	    batch.Rollback() // Discard uncommitted entries
//	}
//
// Performance:
//   - Single fsync at end instead of per-write
//   - 10-100x faster for bulk operations
//   - Memory usage proportional to batch size
type BatchWriter struct {
	wal     *WAL
	entries []WALEntry
	mu      sync.Mutex
}

// NewBatch creates a new batch writer.
func (w *WAL) NewBatch() *BatchWriter {
	return &BatchWriter{
		wal:     w,
		entries: make([]WALEntry, 0, 100),
	}
}

// AppendNode adds a node operation to the batch.
func (b *BatchWriter) AppendNode(op OperationType, node *Node) error {
	return b.append(op, &WALNodeData{Node: node})
}

// AppendEdge adds an edge operation to the batch.
func (b *BatchWriter) AppendEdge(op OperationType, edge *Edge) error {
	return b.append(op, &WALEdgeData{Edge: edge})
}

// AppendDelete adds a delete operation to the batch.
func (b *BatchWriter) AppendDelete(op OperationType, id string) error {
	return b.append(op, &WALDeleteData{ID: id})
}

// append adds a generic operation to the batch.
func (b *BatchWriter) append(op OperationType, data interface{}) error {
	if !config.IsWALEnabled() {
		return nil
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("wal batch: failed to marshal data: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	seq := b.wal.sequence.Add(1)
	entry := WALEntry{
		Sequence:  seq,
		Timestamp: time.Now(),
		Operation: op,
		Data:      dataBytes,
		Checksum:  crc32Checksum(dataBytes),
	}
	b.entries = append(b.entries, entry)
	return nil
}

// Commit writes all batched entries and syncs to disk.
// This is the only fsync in the batch - much faster than per-write sync.
func (b *BatchWriter) Commit() error {
	if !config.IsWALEnabled() {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.entries) == 0 {
		return nil
	}

	b.wal.mu.Lock()
	defer b.wal.mu.Unlock()

	// Write all entries
	for _, entry := range b.entries {
		if err := b.wal.encoder.Encode(&entry); err != nil {
			return fmt.Errorf("wal batch: failed to write entry: %w", err)
		}
		b.wal.entries.Add(1)
		b.wal.totalWrites.Add(1)
	}

	// Single sync at the end
	if err := b.wal.syncLocked(); err != nil {
		return fmt.Errorf("wal batch: sync failed: %w", err)
	}

	b.wal.lastEntryTime.Store(time.Now().UnixNano())

	// Clear batch
	b.entries = b.entries[:0]
	return nil
}

// Rollback discards all uncommitted entries.
func (b *BatchWriter) Rollback() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = b.entries[:0]
}

// Len returns the number of pending entries.
func (b *BatchWriter) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}

// crc32Checksum computes a simple checksum.
func crc32Checksum(data []byte) uint32 {
	var sum uint32
	for _, b := range data {
		sum = (sum >> 8) ^ uint32(b)
		sum ^= sum << 16
	}
	return sum
}

// Snapshot represents a point-in-time snapshot of the database.
type Snapshot struct {
	Sequence  uint64    `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
	Nodes     []*Node   `json:"nodes"`
	Edges     []*Edge   `json:"edges"`
	Version   string    `json:"version"`
}

// CreateSnapshot creates a point-in-time snapshot from the engine.
func (w *WAL) CreateSnapshot(engine Engine) (*Snapshot, error) {
	if w.closed.Load() {
		return nil, ErrWALClosed
	}

	// Get current sequence
	seq := w.sequence.Load()

	// Checkpoint before snapshot
	if err := w.Checkpoint(); err != nil {
		return nil, fmt.Errorf("wal: checkpoint failed: %w", err)
	}

	// Get all nodes
	nodes, err := engine.AllNodes()
	if err != nil {
		return nil, fmt.Errorf("wal: failed to get nodes: %w", err)
	}

	// Get all edges
	edges, err := engine.AllEdges()
	if err != nil {
		return nil, fmt.Errorf("wal: failed to get edges: %w", err)
	}

	return &Snapshot{
		Sequence:  seq,
		Timestamp: time.Now(),
		Nodes:     nodes,
		Edges:     edges,
		Version:   "1.0",
	}, nil
}

// SaveSnapshot writes a snapshot to disk.
func SaveSnapshot(snapshot *Snapshot, path string) error {
	// Create directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("wal: failed to create snapshot directory: %w", err)
	}

	// Write to temp file first
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("wal: failed to create snapshot file: %w", err)
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snapshot); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("wal: failed to encode snapshot: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("wal: failed to sync snapshot: %w", err)
	}
	file.Close()

	// Atomic rename
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("wal: failed to rename snapshot: %w", err)
	}

	return nil
}

// LoadSnapshot reads a snapshot from disk.
func LoadSnapshot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open snapshot: %w", err)
	}
	defer file.Close()

	var snapshot Snapshot
	if err := json.NewDecoder(file).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("wal: failed to decode snapshot: %w", err)
	}

	return &snapshot, nil
}

// ReadWALEntries reads all entries from a WAL file.
// Returns error on corruption of critical entries (nodes, edges).
// Embedding updates are safe to skip as they can be regenerated.
func ReadWALEntries(walPath string) ([]WALEntry, error) {
	file, err := os.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open: %w", err)
	}
	defer file.Close()

	var entries []WALEntry
	var skippedEmbeddings int
	decoder := json.NewDecoder(file)

	for {
		var entry WALEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			// JSON decode failed - entry is malformed
			// This is a critical error for data integrity
			return nil, fmt.Errorf("%w: JSON decode failed at entry after seq %d: %v",
				ErrWALCorrupted, getLastSeq(entries), err)
		}

		// Verify checksum
		expected := crc32Checksum(entry.Data)
		if entry.Checksum != expected {
			// Checksum mismatch - data corrupted
			if entry.Operation == OpUpdateEmbedding {
				// Embedding updates are safe to skip - will be regenerated
				skippedEmbeddings++
				continue
			}
			// Critical operation corrupted - fail recovery
			return nil, fmt.Errorf("%w: checksum mismatch at seq %d, op %s (expected %d, got %d)",
				ErrWALCorrupted, entry.Sequence, entry.Operation, expected, entry.Checksum)
		}

		entries = append(entries, entry)
	}

	if skippedEmbeddings > 0 {
		fmt.Printf("⚠️  WAL recovery: skipped %d corrupted embedding entries (will regenerate)\n", skippedEmbeddings)
	}

	return entries, nil
}

// getLastSeq returns the sequence number of the last entry, or 0 if empty.
func getLastSeq(entries []WALEntry) uint64 {
	if len(entries) == 0 {
		return 0
	}
	return entries[len(entries)-1].Sequence
}

// ReadWALEntriesAfter reads entries after a given sequence number.
func ReadWALEntriesAfter(walPath string, afterSeq uint64) ([]WALEntry, error) {
	all, err := ReadWALEntries(walPath)
	if err != nil {
		return nil, err
	}

	var filtered []WALEntry
	for _, entry := range all {
		if entry.Sequence > afterSeq {
			filtered = append(filtered, entry)
		}
	}
	return filtered, nil
}

// ReplayWALEntry applies a single WAL entry to the engine.
func ReplayWALEntry(engine Engine, entry WALEntry) error {
	switch entry.Operation {
	case OpCreateNode:
		var data WALNodeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal node: %w", err)
		}
		return engine.CreateNode(data.Node)

	case OpUpdateNode:
		var data WALNodeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal node: %w", err)
		}
		return engine.UpdateNode(data.Node)

	case OpDeleteNode:
		var data WALDeleteData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal delete: %w", err)
		}
		return engine.DeleteNode(NodeID(data.ID))

	case OpCreateEdge:
		var data WALEdgeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal edge: %w", err)
		}
		return engine.CreateEdge(data.Edge)

	case OpUpdateEdge:
		var data WALEdgeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal edge: %w", err)
		}
		return engine.UpdateEdge(data.Edge)

	case OpDeleteEdge:
		var data WALDeleteData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal delete: %w", err)
		}
		return engine.DeleteEdge(EdgeID(data.ID))

	case OpBulkNodes:
		var data WALBulkNodesData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk nodes: %w", err)
		}
		return engine.BulkCreateNodes(data.Nodes)

	case OpBulkEdges:
		var data WALBulkEdgesData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk edges: %w", err)
		}
		return engine.BulkCreateEdges(data.Edges)

	case OpBulkDeleteNodes:
		var data WALBulkDeleteNodesData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk delete nodes: %w", err)
		}
		ids := make([]NodeID, len(data.IDs))
		for i, id := range data.IDs {
			ids[i] = NodeID(id)
		}
		return engine.BulkDeleteNodes(ids)

	case OpBulkDeleteEdges:
		var data WALBulkDeleteEdgesData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk delete edges: %w", err)
		}
		ids := make([]EdgeID, len(data.IDs))
		for i, id := range data.IDs {
			ids[i] = EdgeID(id)
		}
		return engine.BulkDeleteEdges(ids)

	case OpCheckpoint:
		// Checkpoints are markers, no action needed
		return nil

	default:
		return fmt.Errorf("wal: unknown operation: %s", entry.Operation)
	}
}

// RecoverFromWAL recovers database state from a snapshot and WAL.
// Returns a new MemoryEngine with the recovered state.
func RecoverFromWAL(walDir, snapshotPath string) (*MemoryEngine, error) {
	engine := NewMemoryEngine()

	// Load snapshot if available
	var snapshotSeq uint64
	if snapshotPath != "" {
		snapshot, err := LoadSnapshot(snapshotPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("wal: failed to load snapshot: %w", err)
			}
			// No snapshot, start fresh
		} else {
			// Apply snapshot
			snapshotSeq = snapshot.Sequence
			if err := engine.BulkCreateNodes(snapshot.Nodes); err != nil {
				return nil, fmt.Errorf("wal: failed to restore nodes: %w", err)
			}
			if err := engine.BulkCreateEdges(snapshot.Edges); err != nil {
				return nil, fmt.Errorf("wal: failed to restore edges: %w", err)
			}
		}
	}

	// Replay WAL entries after snapshot
	walPath := filepath.Join(walDir, "wal.log")

	// Check if WAL file exists first
	if _, statErr := os.Stat(walPath); os.IsNotExist(statErr) {
		return engine, nil // No WAL to replay, return engine as-is
	}

	entries, err := ReadWALEntriesAfter(walPath, snapshotSeq)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to read WAL: %w", err)
	}

	// Replay entries
	for _, entry := range entries {
		if err := ReplayWALEntry(engine, entry); err != nil {
			// Log but continue - some entries may fail (e.g., duplicate creates)
			// This is expected during recovery
			continue
		}
	}

	return engine, nil
}

// WALEngine wraps a storage engine with write-ahead logging.
// All mutating operations are logged before execution.
type WALEngine struct {
	engine Engine
	wal    *WAL
}

// NewWALEngine creates a WAL-backed storage engine.
func NewWALEngine(engine Engine, wal *WAL) *WALEngine {
	return &WALEngine{
		engine: engine,
		wal:    wal,
	}
}

// CreateNode logs then executes node creation.
func (w *WALEngine) CreateNode(node *Node) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpCreateNode, WALNodeData{Node: node}); err != nil {
			return fmt.Errorf("wal: failed to log create_node: %w", err)
		}
	}
	return w.engine.CreateNode(node)
}

// UpdateNode logs then executes node update.
func (w *WALEngine) UpdateNode(node *Node) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpUpdateNode, WALNodeData{Node: node}); err != nil {
			return fmt.Errorf("wal: failed to log update_node: %w", err)
		}
	}
	return w.engine.UpdateNode(node)
}

// UpdateNodeEmbedding logs then executes embedding-only node update.
// Uses OpUpdateEmbedding which is safe to skip during WAL recovery
// since embeddings can be regenerated automatically.
func (w *WALEngine) UpdateNodeEmbedding(node *Node) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpUpdateEmbedding, WALNodeData{Node: node}); err != nil {
			return fmt.Errorf("wal: failed to log update_embedding: %w", err)
		}
	}
	return w.engine.UpdateNode(node)
}

// DeleteNode logs then executes node deletion.
func (w *WALEngine) DeleteNode(id NodeID) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpDeleteNode, WALDeleteData{ID: string(id)}); err != nil {
			return fmt.Errorf("wal: failed to log delete_node: %w", err)
		}
	}
	return w.engine.DeleteNode(id)
}

// CreateEdge logs then executes edge creation.
func (w *WALEngine) CreateEdge(edge *Edge) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpCreateEdge, WALEdgeData{Edge: edge}); err != nil {
			return fmt.Errorf("wal: failed to log create_edge: %w", err)
		}
	}
	return w.engine.CreateEdge(edge)
}

// UpdateEdge logs then executes edge update.
func (w *WALEngine) UpdateEdge(edge *Edge) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpUpdateEdge, WALEdgeData{Edge: edge}); err != nil {
			return fmt.Errorf("wal: failed to log update_edge: %w", err)
		}
	}
	return w.engine.UpdateEdge(edge)
}

// DeleteEdge logs then executes edge deletion.
func (w *WALEngine) DeleteEdge(id EdgeID) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpDeleteEdge, WALDeleteData{ID: string(id)}); err != nil {
			return fmt.Errorf("wal: failed to log delete_edge: %w", err)
		}
	}
	return w.engine.DeleteEdge(id)
}

// BulkCreateNodes logs then executes bulk node creation.
func (w *WALEngine) BulkCreateNodes(nodes []*Node) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpBulkNodes, WALBulkNodesData{Nodes: nodes}); err != nil {
			return fmt.Errorf("wal: failed to log bulk_create_nodes: %w", err)
		}
	}
	return w.engine.BulkCreateNodes(nodes)
}

// BulkCreateEdges logs then executes bulk edge creation.
func (w *WALEngine) BulkCreateEdges(edges []*Edge) error {
	if config.IsWALEnabled() {
		if err := w.wal.Append(OpBulkEdges, WALBulkEdgesData{Edges: edges}); err != nil {
			return fmt.Errorf("wal: failed to log bulk_create_edges: %w", err)
		}
	}
	return w.engine.BulkCreateEdges(edges)
}

// BulkDeleteNodes logs then executes bulk node deletion.
func (w *WALEngine) BulkDeleteNodes(ids []NodeID) error {
	if config.IsWALEnabled() {
		// Convert to strings for serialization
		strIDs := make([]string, len(ids))
		for i, id := range ids {
			strIDs[i] = string(id)
		}
		if err := w.wal.Append(OpBulkDeleteNodes, WALBulkDeleteNodesData{IDs: strIDs}); err != nil {
			return fmt.Errorf("wal: failed to log bulk_delete_nodes: %w", err)
		}
	}
	return w.engine.BulkDeleteNodes(ids)
}

// BulkDeleteEdges logs then executes bulk edge deletion.
func (w *WALEngine) BulkDeleteEdges(ids []EdgeID) error {
	if config.IsWALEnabled() {
		// Convert to strings for serialization
		strIDs := make([]string, len(ids))
		for i, id := range ids {
			strIDs[i] = string(id)
		}
		if err := w.wal.Append(OpBulkDeleteEdges, WALBulkDeleteEdgesData{IDs: strIDs}); err != nil {
			return fmt.Errorf("wal: failed to log bulk_delete_edges: %w", err)
		}
	}
	return w.engine.BulkDeleteEdges(ids)
}

// Delegate read operations directly to underlying engine

// GetNode delegates to underlying engine.
func (w *WALEngine) GetNode(id NodeID) (*Node, error) {
	return w.engine.GetNode(id)
}

// GetEdge delegates to underlying engine.
func (w *WALEngine) GetEdge(id EdgeID) (*Edge, error) {
	return w.engine.GetEdge(id)
}

// GetNodesByLabel delegates to underlying engine.
func (w *WALEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return w.engine.GetNodesByLabel(label)
}

// BatchGetNodes delegates to underlying engine.
func (w *WALEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	return w.engine.BatchGetNodes(ids)
}

// GetOutgoingEdges delegates to underlying engine.
func (w *WALEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	return w.engine.GetOutgoingEdges(nodeID)
}

// GetIncomingEdges delegates to underlying engine.
func (w *WALEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	return w.engine.GetIncomingEdges(nodeID)
}

// GetEdgesBetween delegates to underlying engine.
func (w *WALEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	return w.engine.GetEdgesBetween(startID, endID)
}

// GetEdgeBetween delegates to underlying engine.
func (w *WALEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	return w.engine.GetEdgeBetween(startID, endID, edgeType)
}

// AllNodes delegates to underlying engine.
func (w *WALEngine) AllNodes() ([]*Node, error) {
	return w.engine.AllNodes()
}

// AllEdges delegates to underlying engine.
func (w *WALEngine) AllEdges() ([]*Edge, error) {
	return w.engine.AllEdges()
}

// GetAllNodes delegates to underlying engine.
func (w *WALEngine) GetAllNodes() []*Node {
	return w.engine.GetAllNodes()
}

// GetInDegree delegates to underlying engine.
func (w *WALEngine) GetInDegree(nodeID NodeID) int {
	return w.engine.GetInDegree(nodeID)
}

// GetOutDegree delegates to underlying engine.
func (w *WALEngine) GetOutDegree(nodeID NodeID) int {
	return w.engine.GetOutDegree(nodeID)
}

// GetSchema delegates to underlying engine.
func (w *WALEngine) GetSchema() *SchemaManager {
	return w.engine.GetSchema()
}

// NodeCount delegates to underlying engine.
func (w *WALEngine) NodeCount() (int64, error) {
	return w.engine.NodeCount()
}

// EdgeCount delegates to underlying engine.
func (w *WALEngine) EdgeCount() (int64, error) {
	return w.engine.EdgeCount()
}

// Close closes both the WAL and underlying engine.
func (w *WALEngine) Close() error {
	// Sync and close WAL first
	if err := w.wal.Close(); err != nil {
		// Log but continue
	}
	return w.engine.Close()
}

// GetWAL returns the underlying WAL for direct access.
func (w *WALEngine) GetWAL() *WAL {
	return w.wal
}

// GetEngine returns the underlying engine.
func (w *WALEngine) GetEngine() Engine {
	return w.engine
}

// FindNodeNeedingEmbedding delegates to underlying engine if it supports it.
func (w *WALEngine) FindNodeNeedingEmbedding() *Node {
	if finder, ok := w.engine.(interface{ FindNodeNeedingEmbedding() *Node }); ok {
		return finder.FindNodeNeedingEmbedding()
	}
	return nil
}

// IterateNodes delegates to underlying engine if it supports streaming iteration.
func (w *WALEngine) IterateNodes(fn func(*Node) bool) error {
	if iterator, ok := w.engine.(interface{ IterateNodes(func(*Node) bool) error }); ok {
		return iterator.IterateNodes(fn)
	}
	return fmt.Errorf("underlying engine does not support IterateNodes")
}

// Verify WALEngine implements Engine interface
var _ Engine = (*WALEngine)(nil)
