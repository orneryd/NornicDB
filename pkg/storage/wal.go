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
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
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

	// Transaction boundary markers for ACID compliance
	OpTxBegin  OperationType = "tx_begin"  // Marks transaction start
	OpTxCommit OperationType = "tx_commit" // Marks successful transaction completion
	OpTxAbort  OperationType = "tx_abort"  // Marks explicit transaction rollback
)

// Common WAL errors
var (
	ErrWALClosed          = errors.New("wal: closed")
	ErrWALCorrupted       = errors.New("wal: corrupted entry")
	ErrWALPartialWrite    = errors.New("wal: partial write detected")
	ErrWALChecksumFailed  = errors.New("wal: checksum verification failed")
	ErrWALMissingTrailer  = errors.New("wal: missing or invalid trailer (incomplete write)")
	ErrSnapshotFailed     = errors.New("wal: snapshot creation failed")
	ErrRecoveryFailed     = errors.New("wal: recovery failed")
)

// WAL format constants for atomic writes
const (
	// walMagic identifies the start of an atomic WAL entry (length-prefixed format)
	// "WALE" = WAL Entry
	walMagic uint32 = 0x454C4157 // "WALE" in little-endian

	// walFormatVersion for future format changes
	// v1: original format (magic + version + length + payload + crc)
	// v2: corruption-proof format with trailer canary and 8-byte alignment
	walFormatVersion uint8 = 2

	// walTrailer is written after every record to detect incomplete writes.
	// If a crash occurs mid-write, the trailer will be missing or corrupted.
	// This pattern (0xDEADBEEFFEEDFACE) is:
	// - Unlikely to appear in real data
	// - Easy to spot in hex dumps
	// - A well-known debug/sentinel pattern
	// Inspired by etcd bug #6191 where torn writes corrupted the WAL.
	walTrailer uint64 = 0xDEADBEEFFEEDFACE

	// walAlignment ensures record headers never straddle sector boundaries.
	// Since disk sectors (512B) and pages (4KB) are multiples of 8,
	// an 8-byte aligned header cannot be torn across physical boundaries.
	// This makes "torn headers" mathematically impossible.
	walAlignment int64 = 8

	// Maximum entry size (16MB) to prevent memory exhaustion on corrupt data
	walMaxEntrySize uint32 = 16 * 1024 * 1024
)

// alignUp rounds n up to the nearest multiple of walAlignment (8 bytes).
// This ensures WAL records start at aligned offsets, preventing torn headers.
func alignUp(n int64) int64 {
	return (n + walAlignment - 1) &^ (walAlignment - 1)
}

// WALEntry represents a single write-ahead log entry.
// Each mutating operation is recorded as an entry before execution.
type WALEntry struct {
	Sequence  uint64        `json:"seq"`      // Monotonically increasing sequence number
	Timestamp time.Time     `json:"ts"`       // When the operation occurred
	Operation OperationType `json:"op"`       // Operation type (create_node, update_node, etc.)
	Data      []byte        `json:"data"`     // JSON-serialized operation data
	Checksum  uint32        `json:"checksum"` // CRC32 checksum for integrity
}

// WALNodeData holds node data for WAL entries with optional undo support.
// For update/delete operations, OldNode contains the "before image" for rollback.
type WALNodeData struct {
	Node    *Node  `json:"node"`               // New state (redo)
	OldNode *Node  `json:"old_node,omitempty"` // Previous state (undo) - for updates
	TxID    string `json:"tx_id,omitempty"`    // Transaction ID for grouping
}

// WALEdgeData holds edge data for WAL entries with optional undo support.
// For update/delete operations, OldEdge contains the "before image" for rollback.
type WALEdgeData struct {
	Edge    *Edge  `json:"edge"`               // New state (redo)
	OldEdge *Edge  `json:"old_edge,omitempty"` // Previous state (undo) - for updates
	TxID    string `json:"tx_id,omitempty"`    // Transaction ID for grouping
}

// WALDeleteData holds delete operation data with undo support.
// For proper undo, we store the complete entity being deleted.
type WALDeleteData struct {
	ID      string `json:"id"`
	OldNode *Node  `json:"old_node,omitempty"` // Complete node being deleted (undo)
	OldEdge *Edge  `json:"old_edge,omitempty"` // Complete edge being deleted (undo)
	TxID    string `json:"tx_id,omitempty"`    // Transaction ID for grouping
}

// WALBulkNodesData holds bulk node creation data.
type WALBulkNodesData struct {
	Nodes []*Node `json:"nodes"`
	TxID  string  `json:"tx_id,omitempty"` // Transaction ID for grouping
}

// WALBulkEdgesData holds bulk edge creation data.
type WALBulkEdgesData struct {
	Edges []*Edge `json:"edges"`
	TxID  string  `json:"tx_id,omitempty"` // Transaction ID for grouping
}

// WALBulkDeleteNodesData holds bulk node deletion data with undo support.
type WALBulkDeleteNodesData struct {
	IDs      []string `json:"ids"`
	OldNodes []*Node  `json:"old_nodes,omitempty"` // Complete nodes being deleted (undo)
	TxID     string   `json:"tx_id,omitempty"`     // Transaction ID for grouping
}

// WALBulkDeleteEdgesData holds bulk edge deletion data with undo support.
type WALBulkDeleteEdgesData struct {
	IDs      []string `json:"ids"`
	OldEdges []*Edge  `json:"old_edges,omitempty"` // Complete edges being deleted (undo)
	TxID     string   `json:"tx_id,omitempty"`     // Transaction ID for grouping
}

// WALTxData holds transaction boundary data.
type WALTxData struct {
	TxID     string            `json:"tx_id"`              // Transaction identifier
	Metadata map[string]string `json:"metadata,omitempty"` // Optional transaction metadata
	Reason   string            `json:"reason,omitempty"`   // For abort: why was it aborted
	OpCount  int               `json:"op_count,omitempty"` // For commit: number of operations
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

	// Sync directory to ensure file creation is durable
	if err := syncDir(cfg.Dir); err != nil {
		file.Close()
		return nil, err
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
// Handles both legacy JSON format and new atomic format automatically.
func (w *WAL) loadLastSequence() (uint64, error) {
	walPath := filepath.Join(w.config.Dir, "wal.log")

	// Use ReadWALEntries which handles both formats
	entries, err := ReadWALEntries(walPath)
	if err != nil {
		// For corrupted or empty WAL, start fresh
		if os.IsNotExist(err) {
			return 0, err
		}
		// Log corruption but return 0 to start fresh
		return 0, nil
	}

	if len(entries) == 0 {
		return 0, nil
	}

	return entries[len(entries)-1].Sequence, nil
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

// Append writes a new entry to the WAL using atomic write format.
//
// The atomic format ensures partial writes are detectable:
//
//	[4 bytes: magic "WALE"]
//	[1 byte: format version]
//	[4 bytes: payload length]
//	[N bytes: JSON-encoded entry]
//	[4 bytes: CRC32 of payload]
//
// If crash occurs mid-write:
//   - Missing magic: entry doesn't exist
//   - Missing/truncated payload: length mismatch detected
//   - Missing checksum: CRC verification fails
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

	// Serialize the entire entry to bytes first
	entryBytes, err := json.Marshal(&entry)
	if err != nil {
		return fmt.Errorf("wal: failed to serialize entry: %w", err)
	}

	// Calculate CRC of the entire entry (not just data)
	entryCRC := crc32Checksum(entryBytes)

	// Write atomically: magic + version + length + payload + crc + trailer + padding
	// Format v2 adds trailer canary and 8-byte alignment for corruption-proof writes.
	w.mu.Lock()
	defer w.mu.Unlock()

	// Calculate record size with trailer and alignment padding
	// Header: magic(4) + version(1) + length(4) = 9 bytes
	// Body: payload(N) + crc(4) + trailer(8)
	headerSize := 4 + 1 + 4
	bodySize := len(entryBytes) + 4 + 8
	rawRecordLen := int64(headerSize + bodySize)
	alignedRecordLen := alignUp(rawRecordLen)
	paddingLen := alignedRecordLen - rawRecordLen

	// Build the complete record in memory first
	record := make([]byte, alignedRecordLen)

	offset := 0
	// Magic bytes
	binary.LittleEndian.PutUint32(record[offset:], walMagic)
	offset += 4
	// Format version
	record[offset] = walFormatVersion
	offset += 1
	// Payload length
	binary.LittleEndian.PutUint32(record[offset:], uint32(len(entryBytes)))
	offset += 4
	// Payload
	copy(record[offset:], entryBytes)
	offset += len(entryBytes)
	// CRC of payload
	binary.LittleEndian.PutUint32(record[offset:], entryCRC)
	offset += 4
	// Trailer canary - marks record as fully written
	binary.LittleEndian.PutUint64(record[offset:], walTrailer)
	offset += 8
	// Zero-fill padding for alignment (already zeroed by make, but explicit)
	for i := int64(0); i < paddingLen; i++ {
		record[offset] = 0
		offset++
	}

	// Write the complete record in one call
	if _, err := w.writer.Write(record); err != nil {
		return fmt.Errorf("wal: failed to write entry: %w", err)
	}

	w.entries.Add(1)
	w.bytes.Add(alignedRecordLen)
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
//
// IMPORTANT: Sequence numbers are assigned at commit time, not append time.
// This ensures entries are written in sequence order, preventing replay issues
// when mixing batch and non-batch operations.
type BatchWriter struct {
	wal     *WAL
	pending []pendingEntry // Operations without sequence numbers
	mu      sync.Mutex
}

// pendingEntry stores an operation before sequence number assignment.
type pendingEntry struct {
	Operation OperationType
	Data      []byte
	Checksum  uint32
	Timestamp time.Time
}

// NewBatch creates a new batch writer.
func (w *WAL) NewBatch() *BatchWriter {
	return &BatchWriter{
		wal:     w,
		pending: make([]pendingEntry, 0, 100),
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
// NOTE: Sequence numbers are NOT assigned here - they are assigned at commit time.
// This ensures proper ordering when mixing batch and non-batch operations.
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

	// Store operation WITHOUT sequence number - will be assigned at commit
	entry := pendingEntry{
		Operation: op,
		Data:      dataBytes,
		Checksum:  crc32Checksum(dataBytes),
		Timestamp: time.Now(),
	}
	b.pending = append(b.pending, entry)
	return nil
}

// Commit writes all batched entries and syncs to disk.
// Sequence numbers are assigned here to ensure proper ordering.
// This is the only fsync in the batch - much faster than per-write sync.
func (b *BatchWriter) Commit() error {
	if !config.IsWALEnabled() {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pending) == 0 {
		return nil
	}

	b.wal.mu.Lock()
	defer b.wal.mu.Unlock()

	// Assign sequence numbers and write all entries using atomic format
	var totalBytes int64
	for _, pending := range b.pending {
		// Assign sequence number NOW, at commit time
		seq := b.wal.sequence.Add(1)

		entry := WALEntry{
			Sequence:  seq,
			Timestamp: pending.Timestamp,
			Operation: pending.Operation,
			Data:      pending.Data,
			Checksum:  pending.Checksum,
		}

		// Serialize entry
		entryBytes, err := json.Marshal(&entry)
		if err != nil {
			return fmt.Errorf("wal batch: failed to serialize entry seq %d: %w", seq, err)
		}

		// Calculate CRC of entire entry
		entryCRC := crc32Checksum(entryBytes)

		// Build atomic record: magic + version + length + payload + crc + trailer + padding
		// Format v2 with corruption-proof trailer and 8-byte alignment
		headerSize := 4 + 1 + 4
		bodySize := len(entryBytes) + 4 + 8
		rawRecordLen := int64(headerSize + bodySize)
		alignedRecordLen := alignUp(rawRecordLen)
		paddingLen := alignedRecordLen - rawRecordLen

		record := make([]byte, alignedRecordLen)

		offset := 0
		binary.LittleEndian.PutUint32(record[offset:], walMagic)
		offset += 4
		record[offset] = walFormatVersion
		offset += 1
		binary.LittleEndian.PutUint32(record[offset:], uint32(len(entryBytes)))
		offset += 4
		copy(record[offset:], entryBytes)
		offset += len(entryBytes)
		binary.LittleEndian.PutUint32(record[offset:], entryCRC)
		offset += 4
		// Trailer canary - marks record as fully written
		binary.LittleEndian.PutUint64(record[offset:], walTrailer)
		offset += 8
		// Zero-fill padding for alignment
		for i := int64(0); i < paddingLen; i++ {
			record[offset] = 0
			offset++
		}

		// Write complete record
		if _, err := b.wal.writer.Write(record); err != nil {
			return fmt.Errorf("wal batch: failed to write entry seq %d: %w", seq, err)
		}

		b.wal.entries.Add(1)
		b.wal.totalWrites.Add(1)
		totalBytes += alignedRecordLen
	}

	// Single sync at the end
	if err := b.wal.syncLocked(); err != nil {
		return fmt.Errorf("wal batch: sync failed: %w", err)
	}

	b.wal.bytes.Add(totalBytes)
	b.wal.lastEntryTime.Store(time.Now().UnixNano())

	// Clear batch
	b.pending = b.pending[:0]
	return nil
}

// Rollback discards all uncommitted entries.
func (b *BatchWriter) Rollback() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = b.pending[:0]
}

// Len returns the number of pending entries.
func (b *BatchWriter) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// crc32Table uses Castagnoli polynomial for hardware acceleration on modern CPUs.
// This provides proper CRC32 checksumming with strong collision resistance.
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// crc32Checksum computes a proper CRC32-C checksum.
// Hardware-accelerated on modern AMD64/ARM64 CPUs via SSE4.2/NEON.
func crc32Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32Table)
}

// verifyCRC32 verifies the checksum of data matches expected.
func verifyCRC32(data []byte, expected uint32) bool {
	return crc32Checksum(data) == expected
}

// syncDir is implemented in platform-specific files:
// - wal_sync_unix.go: Unix/Linux/macOS (uses os.Open + Sync)
// - wal_sync_windows.go: Windows (no-op, NTFS handles this automatically)

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

// SaveSnapshot writes a snapshot to disk with full durability guarantees.
// Uses write-to-temp + atomic-rename pattern for crash safety.
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

	// Sync directory to ensure rename is durable
	// Without this, the rename may not survive a crash on some filesystems
	if err := syncDir(dir); err != nil {
		// Log but don't fail - snapshot data is already safe
		// The rename may just need to be redone on recovery
		return nil
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

// TruncateAfterSnapshot truncates the WAL after a successful snapshot.
// This removes all entries up to and including the snapshot sequence number,
// preventing unbounded WAL growth. Call this after SaveSnapshot succeeds.
//
// The process is crash-safe:
//  1. Close current WAL file
//  2. Read entries after snapshot sequence
//  3. Write new WAL with only post-snapshot entries
//  4. Atomically rename new WAL over old
//  5. Reopen WAL for appends
//
// If the system crashes during truncation:
//   - Old WAL remains intact (rename is atomic)
//   - Recovery will replay full WAL (safe, just slower)
//   - Retry truncation on next snapshot
//
// Example:
//
//	snapshot, _ := wal.CreateSnapshot(engine)
//	SaveSnapshot(snapshot, "data/snapshot.json")
//	wal.TruncateAfterSnapshot(snapshot.Sequence) // Reclaim disk space
func (w *WAL) TruncateAfterSnapshot(snapshotSeq uint64) error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush pending writes before truncation
	if err := w.syncLocked(); err != nil {
		return fmt.Errorf("wal: failed to flush before truncate: %w", err)
	}

	// Close current WAL file
	walPath := filepath.Join(w.config.Dir, "wal.log")
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close for truncate: %w", err)
	}

	// Read all entries from current WAL
	allEntries, err := ReadWALEntries(walPath)
	if err != nil {
		// Reopen WAL even on error to keep it functional
		w.reopenWAL()
		return fmt.Errorf("wal: failed to read entries for truncate: %w", err)
	}

	// Filter entries AFTER snapshot sequence
	var keptEntries []WALEntry
	for _, entry := range allEntries {
		if entry.Sequence > snapshotSeq {
			keptEntries = append(keptEntries, entry)
		}
	}

	// Write new WAL with only kept entries
	tmpPath := walPath + ".truncate.tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		w.reopenWAL()
		return fmt.Errorf("wal: failed to create temp WAL: %w", err)
	}

	tmpWriter := bufio.NewWriterSize(tmpFile, 64*1024)
	var bytesWritten int64

	// Write kept entries using atomic format (v2: with trailer canary and 8-byte alignment)
	for _, entry := range keptEntries {
		// Serialize entry
		entryBytes, err := json.Marshal(&entry)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			w.reopenWAL()
			return fmt.Errorf("wal: failed to serialize entry seq %d: %w", entry.Sequence, err)
		}

		// Calculate CRC of entire entry
		entryCRC := crc32Checksum(entryBytes)

		// Build atomic record: magic + version + length + payload + crc + trailer + padding
		// Calculate aligned record size for 8-byte alignment
		headerSize := 4 + 1 + 4                     // magic + version + length
		bodySize := len(entryBytes) + 4 + 8        // payload + crc + trailer
		rawRecordLen := int64(headerSize + bodySize)
		alignedRecordLen := alignUp(rawRecordLen)
		paddingLen := alignedRecordLen - rawRecordLen
		record := make([]byte, alignedRecordLen)

		offset := 0
		binary.LittleEndian.PutUint32(record[offset:], walMagic)
		offset += 4
		record[offset] = walFormatVersion
		offset += 1
		binary.LittleEndian.PutUint32(record[offset:], uint32(len(entryBytes)))
		offset += 4
		copy(record[offset:], entryBytes)
		offset += len(entryBytes)
		binary.LittleEndian.PutUint32(record[offset:], entryCRC)
		offset += 4
		// Write trailer canary to detect incomplete writes
		binary.LittleEndian.PutUint64(record[offset:], walTrailer)
		offset += 8
		// Zero-fill alignment padding (already zeroed by make, but explicit for clarity)
		for i := int64(0); i < paddingLen; i++ {
			record[offset] = 0
			offset++
		}

		if _, err := tmpWriter.Write(record); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			w.reopenWAL()
			return fmt.Errorf("wal: failed to write entry seq %d: %w", entry.Sequence, err)
		}
		bytesWritten += alignedRecordLen
	}

	// Flush and sync temp WAL
	if err := tmpWriter.Flush(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		w.reopenWAL()
		return fmt.Errorf("wal: failed to flush temp WAL: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		w.reopenWAL()
		return fmt.Errorf("wal: failed to sync temp WAL: %w", err)
	}
	tmpFile.Close()

	// Atomically rename temp WAL over old WAL
	if err := os.Rename(tmpPath, walPath); err != nil {
		os.Remove(tmpPath)
		w.reopenWAL()
		return fmt.Errorf("wal: failed to rename truncated WAL: %w", err)
	}

	// Sync directory to ensure rename is durable
	if err := syncDir(w.config.Dir); err != nil {
		w.reopenWAL()
		return fmt.Errorf("wal: failed to sync directory: %w", err)
	}

	// Reopen WAL for appends
	if err := w.reopenWAL(); err != nil {
		return fmt.Errorf("wal: failed to reopen after truncate: %w", err)
	}

	// Update stats
	w.entries.Store(int64(len(keptEntries)))
	w.bytes.Store(bytesWritten)

	return nil
}

// reopenWAL reopens the WAL file for appending.
// Called after truncation or other operations that close the file.
func (w *WAL) reopenWAL() error {
	walPath := filepath.Join(w.config.Dir, "wal.log")
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("wal: failed to reopen: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriterSize(file, 64*1024)
	w.encoder = json.NewEncoder(w.writer)

	return nil
}

// ReadWALEntries reads all entries from a WAL file.
// Supports both legacy JSON format and new atomic format with automatic detection.
// Returns error on corruption of critical entries (nodes, edges).
// Embedding updates are safe to skip as they can be regenerated.
func ReadWALEntries(walPath string) ([]WALEntry, error) {
	file, err := os.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open: %w", err)
	}
	defer file.Close()

	// Peek at first 4 bytes to detect format
	header := make([]byte, 4)
	n, err := file.Read(header)
	if err == io.EOF || n == 0 {
		return []WALEntry{}, nil // Empty file
	}
	if err != nil {
		return nil, fmt.Errorf("wal: failed to read header: %w", err)
	}

	// Reset to beginning
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("wal: failed to seek: %w", err)
	}

	// Detect format: new format starts with magic "WALE", legacy starts with '{'
	magic := binary.LittleEndian.Uint32(header)
	if magic == walMagic {
		return readAtomicWALEntries(file)
	}

	// Legacy JSON format (for backward compatibility)
	return readLegacyWALEntries(file)
}

// readAtomicWALEntries reads entries in the new atomic format.
// Format v1: [magic:4][version:1][length:4][payload:N][crc:4]
// Format v2: [magic:4][version:1][length:4][payload:N][crc:4][trailer:8][padding:0-7]
func readAtomicWALEntries(file *os.File) ([]WALEntry, error) {
	var entries []WALEntry
	var skippedEmbeddings int
	var partialWriteDetected bool

	reader := bufio.NewReader(file)
	headerBuf := make([]byte, 9) // magic(4) + version(1) + length(4)

	for {
		// Read header
		n, err := io.ReadFull(reader, headerBuf)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			// Partial header = incomplete write at end of file
			partialWriteDetected = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("wal: failed to read entry header: %w", err)
		}
		if n != 9 {
			partialWriteDetected = true
			break
		}

		// Verify magic
		magic := binary.LittleEndian.Uint32(headerBuf[0:4])
		if magic != walMagic {
			return nil, fmt.Errorf("%w: invalid magic at offset, expected WALE", ErrWALCorrupted)
		}

		// Read version (for future compatibility)
		version := headerBuf[4]
		if version > walFormatVersion {
			return nil, fmt.Errorf("%w: unsupported WAL version %d (max supported: %d)",
				ErrWALCorrupted, version, walFormatVersion)
		}

		// Read payload length
		payloadLen := binary.LittleEndian.Uint32(headerBuf[5:9])
		if payloadLen > walMaxEntrySize {
			return nil, fmt.Errorf("%w: entry size %d exceeds maximum %d",
				ErrWALCorrupted, payloadLen, walMaxEntrySize)
		}

		// Read payload
		payload := make([]byte, payloadLen)
		n, err = io.ReadFull(reader, payload)
		if err == io.ErrUnexpectedEOF || uint32(n) != payloadLen {
			// Partial payload = incomplete write
			partialWriteDetected = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("wal: failed to read payload: %w", err)
		}

		// Read CRC
		crcBuf := make([]byte, 4)
		n, err = io.ReadFull(reader, crcBuf)
		if err == io.ErrUnexpectedEOF || n != 4 {
			// Missing CRC = incomplete write
			partialWriteDetected = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("wal: failed to read CRC: %w", err)
		}

		// Verify CRC
		storedCRC := binary.LittleEndian.Uint32(crcBuf)
		computedCRC := crc32Checksum(payload)
		if storedCRC != computedCRC {
			// CRC mismatch - but check if it's an embedding we can skip
			var entry WALEntry
			if err := json.Unmarshal(payload, &entry); err == nil {
				if entry.Operation == OpUpdateEmbedding {
					skippedEmbeddings++
					continue
				}
			}
			return nil, fmt.Errorf("%w: CRC mismatch (stored=%x, computed=%x) after seq %d",
				ErrWALChecksumFailed, storedCRC, computedCRC, getLastSeq(entries))
		}

		// Version 2+: Read and verify trailer canary
		if version >= 2 {
			trailerBuf := make([]byte, 8)
			n, err = io.ReadFull(reader, trailerBuf)
			if err == io.ErrUnexpectedEOF || n != 8 {
				// Missing trailer = incomplete write
				partialWriteDetected = true
				break
			}
			if err != nil {
				return nil, fmt.Errorf("wal: failed to read trailer: %w", err)
			}

			// Verify trailer canary
			storedTrailer := binary.LittleEndian.Uint64(trailerBuf)
			if storedTrailer != walTrailer {
				// Trailer mismatch indicates incomplete/corrupted write
				partialWriteDetected = true
				break
			}

			// Skip alignment padding
			// Record size so far: header(9) + payload(N) + crc(4) + trailer(8)
			rawRecordLen := int64(9 + payloadLen + 4 + 8)
			alignedRecordLen := alignUp(rawRecordLen)
			paddingLen := alignedRecordLen - rawRecordLen
			if paddingLen > 0 {
				paddingBuf := make([]byte, paddingLen)
				_, err = io.ReadFull(reader, paddingBuf)
				if err == io.ErrUnexpectedEOF {
					// Missing padding = incomplete write (rare edge case)
					partialWriteDetected = true
					break
				}
				if err != nil {
					return nil, fmt.Errorf("wal: failed to read padding: %w", err)
				}
			}
		}

		// Decode entry
		var entry WALEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			return nil, fmt.Errorf("%w: failed to decode entry after seq %d: %v",
				ErrWALCorrupted, getLastSeq(entries), err)
		}

		// Also verify the inner data checksum
		expectedDataCRC := crc32Checksum(entry.Data)
		if entry.Checksum != expectedDataCRC {
			if entry.Operation == OpUpdateEmbedding {
				skippedEmbeddings++
				continue
			}
			return nil, fmt.Errorf("%w: data checksum mismatch at seq %d",
				ErrWALChecksumFailed, entry.Sequence)
		}

		entries = append(entries, entry)
	}

	if partialWriteDetected {
		fmt.Printf("⚠️  WAL recovery: detected incomplete write at end (crash recovery)\n")
	}
	if skippedEmbeddings > 0 {
		fmt.Printf("⚠️  WAL recovery: skipped %d corrupted embedding entries (will regenerate)\n", skippedEmbeddings)
	}

	return entries, nil
}

// readLegacyWALEntries reads entries in the legacy JSON-per-line format.
// This is for backward compatibility with existing WAL files.
func readLegacyWALEntries(file *os.File) ([]WALEntry, error) {
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
			// This could be a partial write from a crash
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

// ReplayResult tracks the outcome of WAL replay for observability.
type ReplayResult struct {
	Applied int           // Successfully applied entries
	Skipped int           // Expected skips (duplicates, checkpoints)
	Failed  int           // Unexpected failures
	Errors  []ReplayError // Detailed error information
}

// ReplayError captures details about a failed replay entry.
type ReplayError struct {
	Sequence  uint64
	Operation OperationType
	Error     error
}

// HasCriticalErrors returns true if there were unexpected failures.
func (r ReplayResult) HasCriticalErrors() bool {
	return r.Failed > 0
}

// Summary returns a human-readable summary of replay results.
func (r ReplayResult) Summary() string {
	return fmt.Sprintf("applied=%d skipped=%d failed=%d", r.Applied, r.Skipped, r.Failed)
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

	case OpTxBegin, OpTxCommit, OpTxAbort:
		// Transaction boundaries are markers handled at a higher level
		// Individual replay doesn't need to process them
		return nil

	default:
		return fmt.Errorf("wal: unknown operation: %s", entry.Operation)
	}
}

// UndoWALEntry reverses a WAL entry using its stored "before image".
// This is used for transaction rollback on crash recovery.
// Returns ErrNoUndoData if the entry lacks undo information.
var ErrNoUndoData = errors.New("wal: entry has no undo data")

func UndoWALEntry(engine Engine, entry WALEntry) error {
	switch entry.Operation {
	case OpCreateNode:
		// Undo create = delete
		var data WALNodeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal node for undo: %w", err)
		}
		if data.Node == nil {
			return ErrNoUndoData
		}
		return engine.DeleteNode(data.Node.ID)

	case OpUpdateNode:
		// Undo update = restore old node
		var data WALNodeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal node for undo: %w", err)
		}
		if data.OldNode == nil {
			return ErrNoUndoData
		}
		return engine.UpdateNode(data.OldNode)

	case OpDeleteNode:
		// Undo delete = recreate old node
		var data WALDeleteData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal delete for undo: %w", err)
		}
		if data.OldNode == nil {
			return ErrNoUndoData
		}
		return engine.CreateNode(data.OldNode)

	case OpCreateEdge:
		// Undo create = delete
		var data WALEdgeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal edge for undo: %w", err)
		}
		if data.Edge == nil {
			return ErrNoUndoData
		}
		return engine.DeleteEdge(data.Edge.ID)

	case OpUpdateEdge:
		// Undo update = restore old edge
		var data WALEdgeData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal edge for undo: %w", err)
		}
		if data.OldEdge == nil {
			return ErrNoUndoData
		}
		return engine.UpdateEdge(data.OldEdge)

	case OpDeleteEdge:
		// Undo delete = recreate old edge
		var data WALDeleteData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal delete for undo: %w", err)
		}
		if data.OldEdge == nil {
			return ErrNoUndoData
		}
		return engine.CreateEdge(data.OldEdge)

	case OpBulkNodes:
		// Undo bulk create = delete all
		var data WALBulkNodesData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk nodes for undo: %w", err)
		}
		ids := make([]NodeID, len(data.Nodes))
		for i, n := range data.Nodes {
			ids[i] = n.ID
		}
		return engine.BulkDeleteNodes(ids)

	case OpBulkEdges:
		// Undo bulk create = delete all
		var data WALBulkEdgesData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk edges for undo: %w", err)
		}
		ids := make([]EdgeID, len(data.Edges))
		for i, e := range data.Edges {
			ids[i] = e.ID
		}
		return engine.BulkDeleteEdges(ids)

	case OpBulkDeleteNodes:
		// Undo bulk delete = recreate all
		var data WALBulkDeleteNodesData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk delete nodes for undo: %w", err)
		}
		if len(data.OldNodes) == 0 {
			return ErrNoUndoData
		}
		return engine.BulkCreateNodes(data.OldNodes)

	case OpBulkDeleteEdges:
		// Undo bulk delete = recreate all
		var data WALBulkDeleteEdgesData
		if err := json.Unmarshal(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk delete edges for undo: %w", err)
		}
		if len(data.OldEdges) == 0 {
			return ErrNoUndoData
		}
		return engine.BulkCreateEdges(data.OldEdges)

	case OpCheckpoint, OpTxBegin, OpTxCommit, OpTxAbort, OpUpdateEmbedding:
		// These don't need undo
		return nil

	default:
		return fmt.Errorf("wal: unknown operation for undo: %s", entry.Operation)
	}
}

// GetEntryTxID extracts the transaction ID from a WAL entry, if present.
func GetEntryTxID(entry WALEntry) string {
	// Try each data type that might have a TxID
	var txID string

	switch entry.Operation {
	case OpCreateNode, OpUpdateNode:
		var data WALNodeData
		if json.Unmarshal(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpCreateEdge, OpUpdateEdge:
		var data WALEdgeData
		if json.Unmarshal(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpDeleteNode, OpDeleteEdge:
		var data WALDeleteData
		if json.Unmarshal(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBulkNodes:
		var data WALBulkNodesData
		if json.Unmarshal(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBulkEdges:
		var data WALBulkEdgesData
		if json.Unmarshal(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBulkDeleteNodes:
		var data WALBulkDeleteNodesData
		if json.Unmarshal(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBulkDeleteEdges:
		var data WALBulkDeleteEdgesData
		if json.Unmarshal(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpTxBegin, OpTxCommit, OpTxAbort:
		var data WALTxData
		if json.Unmarshal(entry.Data, &data) == nil {
			txID = data.TxID
		}
	}

	return txID
}

// TransactionState tracks the state of an in-progress transaction during recovery.
type TransactionState struct {
	TxID    string
	Entries []WALEntry // All entries in this transaction (in order)
	Started bool       // True if we saw TxBegin
	Done    bool       // True if we saw TxCommit or TxAbort
	Aborted bool       // True if explicitly aborted
}

// RecoverWithTransactions performs transaction-aware WAL recovery.
// Incomplete transactions (no commit/abort) are rolled back using undo data.
// Returns the engine state and recovery statistics.
func RecoverWithTransactions(walDir, snapshotPath string) (*MemoryEngine, *TransactionRecoveryResult, error) {
	engine := NewMemoryEngine()
	result := &TransactionRecoveryResult{
		Transactions: make(map[string]*TransactionState),
	}

	// Load snapshot if available
	if snapshotPath != "" {
		snapshot, err := LoadSnapshot(snapshotPath)
		if err != nil && !os.IsNotExist(err) {
			return nil, result, fmt.Errorf("wal: failed to load snapshot: %w", err)
		}
		if snapshot != nil {
			result.SnapshotSeq = snapshot.Sequence
			if err := engine.BulkCreateNodes(snapshot.Nodes); err != nil {
				return nil, result, fmt.Errorf("wal: failed to restore nodes: %w", err)
			}
			if err := engine.BulkCreateEdges(snapshot.Edges); err != nil {
				return nil, result, fmt.Errorf("wal: failed to restore edges: %w", err)
			}
		}
	}

	// Read all WAL entries
	walPath := filepath.Join(walDir, "wal.log")
	entries, err := ReadWALEntries(walPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, result, fmt.Errorf("wal: failed to read WAL: %w", err)
	}

	// Phase 1: Categorize entries by transaction
	nonTxEntries := []WALEntry{}
	for _, entry := range entries {
		if entry.Sequence <= result.SnapshotSeq {
			continue
		}

		txID := GetEntryTxID(entry)

		switch entry.Operation {
		case OpTxBegin:
			var data WALTxData
			json.Unmarshal(entry.Data, &data)
			result.Transactions[data.TxID] = &TransactionState{
				TxID:    data.TxID,
				Entries: []WALEntry{},
				Started: true,
			}

		case OpTxCommit:
			var data WALTxData
			json.Unmarshal(entry.Data, &data)
			if tx, ok := result.Transactions[data.TxID]; ok {
				tx.Done = true
			}

		case OpTxAbort:
			var data WALTxData
			json.Unmarshal(entry.Data, &data)
			if tx, ok := result.Transactions[data.TxID]; ok {
				tx.Done = true
				tx.Aborted = true
			}

		default:
			if txID != "" {
				// Entry belongs to a transaction
				if tx, ok := result.Transactions[txID]; ok {
					tx.Entries = append(tx.Entries, entry)
				} else {
					// Entry references unknown transaction - treat as non-transactional
					nonTxEntries = append(nonTxEntries, entry)
				}
			} else {
				// Non-transactional entry
				nonTxEntries = append(nonTxEntries, entry)
			}
		}
	}

	// Phase 2: Apply committed transactions
	for txID, tx := range result.Transactions {
		if tx.Done && !tx.Aborted {
			// Transaction committed - apply all its entries
			for _, entry := range tx.Entries {
				if err := ReplayWALEntry(engine, entry); err != nil {
					if !errors.Is(err, ErrAlreadyExists) {
						result.FailedTransactions = append(result.FailedTransactions, txID)
					}
				}
			}
			result.CommittedTransactions++
		}
	}

	// Phase 3: Rollback incomplete transactions
	for txID, tx := range result.Transactions {
		if !tx.Done {
			// Transaction incomplete - needs rollback
			// First apply forward (in case partially applied before crash)
			for _, entry := range tx.Entries {
				ReplayWALEntry(engine, entry) // Ignore errors - might already exist
			}
			// Then undo in reverse order
			for i := len(tx.Entries) - 1; i >= 0; i-- {
				entry := tx.Entries[i]
				if err := UndoWALEntry(engine, entry); err != nil {
					if !errors.Is(err, ErrNoUndoData) {
						result.UndoErrors = append(result.UndoErrors, fmt.Sprintf("tx %s: %v", txID, err))
					}
				}
			}
			result.RolledBackTransactions++
		} else if tx.Aborted {
			result.AbortedTransactions++
		}
	}

	// Phase 4: Apply non-transactional entries
	for _, entry := range nonTxEntries {
		if err := ReplayWALEntry(engine, entry); err != nil {
			if !errors.Is(err, ErrAlreadyExists) {
				result.NonTxErrors = append(result.NonTxErrors, err.Error())
			}
		}
		result.NonTxApplied++
	}

	return engine, result, nil
}

// TransactionRecoveryResult contains detailed statistics from transaction-aware recovery.
type TransactionRecoveryResult struct {
	SnapshotSeq            uint64
	Transactions           map[string]*TransactionState
	CommittedTransactions  int
	RolledBackTransactions int
	AbortedTransactions    int
	FailedTransactions     []string
	UndoErrors             []string
	NonTxApplied           int
	NonTxErrors            []string
}

// Summary returns a human-readable summary of the recovery.
func (r *TransactionRecoveryResult) Summary() string {
	return fmt.Sprintf("committed=%d rolledback=%d aborted=%d non-tx=%d errors=%d",
		r.CommittedTransactions, r.RolledBackTransactions, r.AbortedTransactions,
		r.NonTxApplied, len(r.UndoErrors)+len(r.NonTxErrors))
}

// HasErrors returns true if there were any errors during recovery.
func (r *TransactionRecoveryResult) HasErrors() bool {
	return len(r.UndoErrors) > 0 || len(r.NonTxErrors) > 0 || len(r.FailedTransactions) > 0
}

// RecoverFromWAL recovers database state from a snapshot and WAL.
// Returns a new MemoryEngine with the recovered state.
func RecoverFromWAL(walDir, snapshotPath string) (*MemoryEngine, error) {
	engine, result, err := RecoverFromWALWithResult(walDir, snapshotPath)
	if err != nil {
		return nil, err
	}

	// Log recovery results
	if result.Failed > 0 {
		fmt.Printf("⚠️  WAL recovery completed with errors: %s\n", result.Summary())
		for _, e := range result.Errors {
			fmt.Printf("   - seq %d op %s: %v\n", e.Sequence, e.Operation, e.Error)
		}
	}

	return engine, nil
}

// RecoverFromWALWithResult recovers database state and returns detailed results.
// Use this for programmatic access to replay statistics and errors.
func RecoverFromWALWithResult(walDir, snapshotPath string) (*MemoryEngine, ReplayResult, error) {
	engine := NewMemoryEngine()
	result := ReplayResult{}

	// Load snapshot if available
	var snapshotSeq uint64
	if snapshotPath != "" {
		snapshot, err := LoadSnapshot(snapshotPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, result, fmt.Errorf("wal: failed to load snapshot: %w", err)
			}
			// No snapshot, start fresh
		} else {
			// Apply snapshot
			snapshotSeq = snapshot.Sequence
			if err := engine.BulkCreateNodes(snapshot.Nodes); err != nil {
				return nil, result, fmt.Errorf("wal: failed to restore nodes: %w", err)
			}
			if err := engine.BulkCreateEdges(snapshot.Edges); err != nil {
				return nil, result, fmt.Errorf("wal: failed to restore edges: %w", err)
			}
		}
	}

	// Replay WAL entries after snapshot
	walPath := filepath.Join(walDir, "wal.log")

	// Check if WAL file exists first
	if _, statErr := os.Stat(walPath); os.IsNotExist(statErr) {
		return engine, result, nil // No WAL to replay, return engine as-is
	}

	entries, err := ReadWALEntriesAfter(walPath, snapshotSeq)
	if err != nil {
		return nil, result, fmt.Errorf("wal: failed to read WAL: %w", err)
	}

	// Replay entries with proper error tracking
	result = ReplayWALEntries(engine, entries)

	return engine, result, nil
}

// ReplayWALEntries replays multiple entries and tracks results.
// Expected errors (duplicates, checkpoints) are counted as skipped.
// Unexpected errors (corruption, constraint violations) are counted as failed.
func ReplayWALEntries(engine Engine, entries []WALEntry) ReplayResult {
	result := ReplayResult{
		Errors: make([]ReplayError, 0),
	}

	for _, entry := range entries {
		// Checkpoints are markers only - skip them (no-op, don't count as applied)
		if entry.Operation == OpCheckpoint {
			result.Skipped++
			continue
		}

		err := ReplayWALEntry(engine, entry)
		if err == nil {
			result.Applied++
			continue
		}

		// Classify the error
		if errors.Is(err, ErrAlreadyExists) {
			// Duplicate during replay - expected (idempotency)
			result.Skipped++
		} else {
			// Unexpected error - track it
			result.Failed++
			result.Errors = append(result.Errors, ReplayError{
				Sequence:  entry.Sequence,
				Operation: entry.Operation,
				Error:     err,
			})
		}
	}

	return result
}

// WALEngine wraps a storage engine with write-ahead logging.
// All mutating operations are logged before execution.
type WALEngine struct {
	engine Engine
	wal    *WAL

	// Automatic snapshot and compaction
	snapshotDir      string
	snapshotMu       sync.RWMutex // Protects snapshotTicker and stopSnapshot
	snapshotTicker   *time.Ticker
	stopSnapshot     chan struct{}
	lastSnapshotTime atomic.Int64
	totalSnapshots   atomic.Int64
}

// NewWALEngine creates a WAL-backed storage engine.
func NewWALEngine(engine Engine, wal *WAL) *WALEngine {
	return &WALEngine{
		engine: engine,
		wal:    wal,
	}
}

// EnableAutoCompaction starts automatic snapshot creation and WAL truncation.
// Snapshots are created at the configured SnapshotInterval, and the WAL is
// truncated after each successful snapshot to prevent unbounded growth.
//
// Snapshots are saved to snapshotDir/snapshot-<timestamp>.json
//
// This solves the "WAL grows forever" problem by automatically removing old
// entries that are already captured in snapshots.
//
// Example:
//
//	walEngine.EnableAutoCompaction("data/snapshots")
//	// WAL will now be automatically truncated every SnapshotInterval
func (w *WALEngine) EnableAutoCompaction(snapshotDir string) error {
	w.snapshotMu.Lock()
	defer w.snapshotMu.Unlock()

	if w.stopSnapshot != nil {
		return fmt.Errorf("wal: auto-compaction already enabled")
	}

	// Create snapshot directory
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("wal: failed to create snapshot directory: %w", err)
	}

	w.snapshotDir = snapshotDir

	interval := w.wal.config.SnapshotInterval
	if interval <= 0 {
		interval = 1 * time.Hour // Default if not configured
	}

	// Create ticker and channel BEFORE starting goroutine to avoid race
	w.snapshotTicker = time.NewTicker(interval)
	w.stopSnapshot = make(chan struct{})

	// Now start goroutine - ticker is already initialized and protected by lock
	go w.autoSnapshotLoop()

	return nil
}

// DisableAutoCompaction stops automatic snapshot creation and WAL truncation.
func (w *WALEngine) DisableAutoCompaction() {
	w.snapshotMu.Lock()
	defer w.snapshotMu.Unlock()

	if w.stopSnapshot != nil {
		close(w.stopSnapshot)
		w.stopSnapshot = nil
	}
	if w.snapshotTicker != nil {
		w.snapshotTicker.Stop()
		w.snapshotTicker = nil
	}
}

// autoSnapshotLoop runs in background, creating periodic snapshots and truncating WAL.
func (w *WALEngine) autoSnapshotLoop() {
	for {
		// Get local copies of channels under lock to avoid races
		w.snapshotMu.RLock()
		ticker := w.snapshotTicker
		stopCh := w.stopSnapshot
		w.snapshotMu.RUnlock()

		// Check if shutdown was requested
		if ticker == nil || stopCh == nil {
			return
		}

		// Select on local channel copies (safe to do without lock)
		select {
		case <-ticker.C:
			if err := w.createSnapshotAndCompact(); err != nil {
				// Log error but continue - don't crash on snapshot failure
				fmt.Printf("WAL auto-compaction failed: %v\n", err)
			}
		case <-stopCh:
			return
		}
	}
}

// createSnapshotAndCompact creates a snapshot and truncates the WAL.
// This is called automatically by the background goroutine.
func (w *WALEngine) createSnapshotAndCompact() error {
	// Create snapshot from current engine state
	snapshot, err := w.wal.CreateSnapshot(w.engine)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Save snapshot with timestamp
	timestamp := time.Now().Format("20060102-150405")
	snapshotPath := filepath.Join(w.snapshotDir, fmt.Sprintf("snapshot-%s.json", timestamp))

	if err := SaveSnapshot(snapshot, snapshotPath); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	// Truncate WAL to remove entries before snapshot
	// This is the key compaction step that prevents unbounded growth
	if err := w.wal.TruncateAfterSnapshot(snapshot.Sequence); err != nil {
		// Log error but don't fail - snapshot is still valid
		// Next compaction will try again
		return fmt.Errorf("failed to truncate WAL (snapshot saved): %w", err)
	}

	// Update stats
	w.totalSnapshots.Add(1)
	w.lastSnapshotTime.Store(time.Now().UnixNano())

	return nil
}

// GetSnapshotStats returns statistics about automatic snapshots.
func (w *WALEngine) GetSnapshotStats() (totalSnapshots int64, lastSnapshotTime time.Time) {
	total := w.totalSnapshots.Load()
	lastTS := w.lastSnapshotTime.Load()

	var lastTime time.Time
	if lastTS > 0 {
		lastTime = time.Unix(0, lastTS)
	}

	return total, lastTime
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

// GetFirstNodeByLabel delegates to underlying engine.
func (w *WALEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	return w.engine.GetFirstNodeByLabel(label)
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

// GetEdgesByType delegates to underlying engine.
func (w *WALEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return w.engine.GetEdgesByType(edgeType)
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
	// Stop auto-compaction if enabled
	w.DisableAutoCompaction()

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

// ============================================================================
// StreamingEngine Implementation
// ============================================================================

// StreamNodes implements StreamingEngine.StreamNodes by delegating to the underlying engine.
func (w *WALEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	if streamer, ok := w.engine.(StreamingEngine); ok {
		return streamer.StreamNodes(ctx, fn)
	}
	// Fallback: load all nodes
	nodes, err := w.engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := fn(node); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// StreamEdges implements StreamingEngine.StreamEdges by delegating to the underlying engine.
func (w *WALEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	if streamer, ok := w.engine.(StreamingEngine); ok {
		return streamer.StreamEdges(ctx, fn)
	}
	// Fallback: load all edges
	edges, err := w.engine.AllEdges()
	if err != nil {
		return err
	}
	for _, edge := range edges {
		if err := fn(edge); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// StreamNodeChunks implements StreamingEngine.StreamNodeChunks by delegating to the underlying engine.
func (w *WALEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if streamer, ok := w.engine.(StreamingEngine); ok {
		return streamer.StreamNodeChunks(ctx, chunkSize, fn)
	}
	// Fallback: use StreamNodes to build chunks
	chunk := make([]*Node, 0, chunkSize)
	err := w.StreamNodes(ctx, func(node *Node) error {
		chunk = append(chunk, node)
		if len(chunk) >= chunkSize {
			if err := fn(chunk); err != nil {
				return err
			}
			chunk = make([]*Node, 0, chunkSize)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(chunk) > 0 {
		return fn(chunk)
	}
	return nil
}

// Verify WALEngine implements Engine interface
var _ Engine = (*WALEngine)(nil)

// Verify WALEngine implements StreamingEngine interface
var _ StreamingEngine = (*WALEngine)(nil)
