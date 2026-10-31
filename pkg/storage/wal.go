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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/localization"
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

	// OpBufferedCommit records the full payload of an acknowledged
	// write-behind commit. It is appended to the WAL before the ACK so a
	// process crash that loses the in-memory buffer can be recovered by
	// replaying these entries into Badger at open (see
	// RecoverBufferedCommits). The entry is written to the kernel page
	// cache before the ACK (write(2)), so it survives kill -9; power-loss
	// durability still requires the durable synchronous commit path.
	OpBufferedCommit OperationType = "buffered_commit"
)

// Common WAL errors
var (
	ErrWALClosed         = errors.New("wal: closed")
	ErrWALCorrupted      = errors.New("wal: corrupted entry")
	ErrWALPartialWrite   = errors.New("wal: partial write detected")
	ErrWALChecksumFailed = errors.New("wal: checksum verification failed")
	ErrWALMissingTrailer = errors.New("wal: missing or invalid trailer (incomplete write)")
	ErrSnapshotFailed    = errors.New("wal: snapshot creation failed")
	ErrRecoveryFailed    = errors.New("wal: recovery failed")
)

// CorruptionDiagnostics captures detailed information about WAL corruption
// to help diagnose root causes (disk failure, split-brain, bugs, etc.)
type CorruptionDiagnostics struct {
	Timestamp      time.Time `json:"timestamp"`
	WALPath        string    `json:"wal_path"`
	CorruptedSeq   uint64    `json:"corrupted_seq"`
	Operation      string    `json:"operation"`
	ExpectedCRC    uint32    `json:"expected_crc"`
	ActualCRC      uint32    `json:"actual_crc"`
	FileSize       int64     `json:"file_size"`
	LastGoodSeq    uint64    `json:"last_good_seq"`
	SuspectedCause string    `json:"suspected_cause"`
	BackupPath     string    `json:"backup_path,omitempty"`
	RecoveryAction string    `json:"recovery_action"`
}

// diagnoseCause analyzes corruption patterns to suggest likely causes
func (d *CorruptionDiagnostics) diagnoseCause() {
	// Heuristics for common corruption causes:
	// 1. Seq 1 corruption often indicates stale WAL from previous run or split-brain
	// 2. High-sequence corruption usually indicates crash during write
	// 3. CRC mismatch with valid JSON suggests bit-flip (disk/memory error)
	// 4. Multiple corrupted entries suggest catastrophic failure

	if d.CorruptedSeq == 1 {
		d.SuspectedCause = "stale_wal_or_split_brain: corruption at seq 1 suggests " +
			"either a leftover WAL from a previous instance, or split-brain " +
			"where two nodes wrote to the same WAL. Check for duplicate primaries."
	} else if d.CorruptedSeq > 0 && d.LastGoodSeq == d.CorruptedSeq-1 {
		d.SuspectedCause = "crash_during_write: corruption at entry boundary suggests " +
			"crash or power loss during write. This is expected and recoverable."
	} else {
		d.SuspectedCause = "unknown: possible disk error, memory corruption, or bug. " +
			"Check system logs for I/O errors. Backup preserved for forensics."
	}
}

// WAL format constants for atomic writes
const (
	// walMagic identifies the start of an atomic WAL entry (length-prefixed format)
	// "WALE" = WAL Entry
	walMagic uint32 = 0x454C4157 // "WALE" in little-endian

	// walFormatVersionCurrent identifies the v1.4.4 WAL record contract.
	// Older frames are converted only by the authorized V4 startup migration.
	walFormatVersionCurrent uint8 = 6

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

// WALEntry represents a single write-ahead log entry.
// Each mutating operation is recorded as an entry before execution.
type WALEntry struct {
	Sequence  uint64        `json:"seq"`                // Monotonically increasing sequence number
	Timestamp time.Time     `json:"ts"`                 // When the operation occurred
	Operation OperationType `json:"op"`                 // Operation type (create_node, update_node, etc.)
	Data      []byte        `json:"data"`               // JSON-serialized operation data
	Checksum  uint32        `json:"checksum"`           // CRC32 checksum for integrity
	Database  string        `json:"database,omitempty"` // Database/namespace name (for multi-database support)
	// Readers attach immutable file authority; JSON transport preserves it.
	// Native writers leave this nil and encode authority once at epoch boundaries.
	PropertyEpoch      *WALPropertyEpoch `json:"property_epoch,omitempty"`
	framingVersion     uint8
	recordEnd          uint64
	propertyScopeReset bool
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

// WALPropKeyEntry binds a source-store property token to its namespace and
// name. Replay resolves that name through the destination dictionary before
// applying the physical body; source numeric IDs are never installed there.
type WALPropKeyEntry struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	ID        uint64 `json:"id"`
}

// WALBufferedCommitData is the durable payload of one acknowledged
// write-behind commit. It carries everything applyBufferedCommits needs to
// replay the commit into Badger after a crash loses the in-memory buffer.
//
// The count deltas are deliberately not serialized: they are rebuilt from
// committed entities after replay (crash recovery is rare, so the full
// rebuild is acceptable).
type WALBufferedCommitData struct {
	LogicalTxID string            `json:"logical_tx_id,omitempty"`
	TxID        string            `json:"tx_id"`
	Namespace   string            `json:"namespace,omitempty"`
	Ops         []Operation       `json:"ops"`
	Writes      map[string][]byte `json:"writes,omitempty"`
	Deletes     []string          `json:"deletes,omitempty"`
	Version     MVCCVersion       `json:"version"`
	NodeMax     uint64            `json:"node_max,omitempty"`
	EdgeMax     uint64            `json:"edge_max,omitempty"`
	HasCounters bool              `json:"has_counters,omitempty"`
	// NumericBindings identifies source-local node and edge IDs used by
	// physical keys and compact bodies. Foreign replay resolves their names
	// against the destination dictionary before applying those bytes.
	NumericBindings []byte `json:"numeric_bindings,omitempty"`

	// PropertyBindings binds every property token referenced by Writes,
	// including persisted names. Import remaps tokens into the destination.
	PropertyBindings             []byte `json:"property_bindings,omitempty"`
	PropertyDictionaryID         string `json:"property_dictionary_id,omitempty"`
	namedPropertiesAuthoritative bool
	propertyEpoch                *walPropertyEpochState
	// numericAliases exists only during the original store's historical WAL replay.
	numericAliases map[walNumericToken]string

	// Historical writers carried only newly staged keys and source counters.
	PropKeys        []WALPropKeyEntry `json:"prop_keys,omitempty"`
	PropKeyCounters map[string]uint64 `json:"prop_key_counters,omitempty"`
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

	// DeferAppendFlush skips the per-append userspace flush in non-immediate
	// modes: records accumulate in the bufio writer and reach the kernel
	// only when the background Sync (batchSyncLoop tick) or Close drains
	// them. Per-op appends stop paying a write(2) syscall each; in exchange
	// up to BatchSyncInterval of records are lost on process crash (power
	// loss behavior is unchanged: batch mode never fsyncs per record).
	// Intended for engines that already accept a bounded loss window per
	// commit (write-behind buffering), where per-append flush syscalls
	// otherwise dominate ingest CPU.
	DeferAppendFlush bool

	// BatchSyncInterval for "batch" sync mode
	BatchSyncInterval time.Duration

	// MaxFileSize triggers rotation when exceeded
	MaxFileSize int64

	// MaxEntries triggers rotation when exceeded
	MaxEntries int64

	// SnapshotInterval for automatic snapshots
	SnapshotInterval time.Duration

	// RetentionMaxSegments keeps at most N sealed segments (0 = unlimited).
	RetentionMaxSegments int

	// RetentionMaxAge keeps segments newer than this duration (0 = unlimited).
	RetentionMaxAge time.Duration

	// SnapshotRetentionMaxCount is the maximum number of snapshot files to keep in the
	// snapshot directory (0 = keep all). Oldest snapshots are deleted after each new one.
	// Recommended 3–5 so disk space stays bounded while keeping recovery options.
	SnapshotRetentionMaxCount int

	// SnapshotRetentionMaxAge is the maximum age of snapshot files to keep (0 = unlimited).
	// Snapshots older than this are deleted even if under MaxCount.
	SnapshotRetentionMaxAge time.Duration

	// Logger receives WAL diagnostics events (optional, legacy structured
	// channel implemented by pkg/storage's WALLogger interface). If nil,
	// a default slog-backed logger is installed at ctor entry — the
	// previous stdlib-printer-backed default has been removed per LOG-01.
	Logger WALLogger

	// SlogLogger is the structured *slog.Logger used by D-07 WAL recovery
	// emissions (subsystem=wal, subsystem=wal_recovery). Optional; nil
	// falls back to a discard handler at NewWAL entry per D-01a.
	SlogLogger *slog.Logger

	// OnCorruption is called when WAL corruption diagnostics are produced (optional).
	// This allows the server layer to surface "WAL degraded" health state without
	// parsing logs. The callback MUST be fast and non-blocking.
	OnCorruption func(diag *CorruptionDiagnostics, cause error)
}

// DefaultWALConfig returns sensible defaults.
func DefaultWALConfig() *WALConfig {
	return &WALConfig{
		Dir:                       "data/wal",
		SyncMode:                  "batch",
		BatchSyncInterval:         100 * time.Millisecond,
		MaxFileSize:               100 * 1024 * 1024, // 100MB
		MaxEntries:                100000,
		SnapshotInterval:          1 * time.Hour,
		SnapshotRetentionMaxCount: 3, // Keep last 3 snapshots so disk doesn't grow unbounded
		SnapshotRetentionMaxAge:   0, // No age limit by default
	}
}

// WAL provides write-ahead logging for durability.
// Thread-safe for concurrent writes.
type WAL struct {
	mu                         trackedRWMutex
	propertyDictionary         *propertyKeyDictionary
	propertyEpoch              *WALPropertyEpoch
	propertyEpochRevision      uint64
	propertyEpochError         error
	propertyEpochKnown         bool // physical stream state; never writer provenance
	propertyEpochFormatChecked bool
	// propertyEpochUnsealed mirrors the durable marker: the log may hold v4
	// controls. Clean close and compaction convert only while it is set.
	propertyEpochUnsealed bool
	// syncMu serializes fsync syscalls without blocking Append. w.mu
	// only guards Go-side state (bufio writer, encoder, segment
	// bookkeeping) — the kernel-level fsync on w.file is independent
	// once the bufio buffer has been drained. Holding w.mu across
	// fsync was the top source of mutex contention during bulk seed
	// workloads (see perf investigation notes), because batchSyncLoop
	// ticks every 100ms and fsync can take tens of ms, blocking every
	// Append waiting on w.mu for that duration.
	syncMu   sync.Mutex
	config   *WALConfig
	file     *os.File
	writer   *bufio.Writer
	encoder  *jsontext.Encoder
	sequence atomic.Uint64
	entries  atomic.Int64
	bytes    atomic.Int64
	// work is the WAL's one owner: the batch sync loop is its accepted work,
	// its seal is the closed state, and Close is its one stop.
	work lifecycle.WorkLifetime
	// handleReleased (mu) records that the owner's dispose released the
	// append handle. Admission is the owner's seal; this is only the handle.
	handleReleased bool

	segmentFirstSeq  uint64
	segmentEntries   int64
	segmentBytes     int64
	segmentCreatedAt time.Time
	// Per-instance fault injection for the compaction replacement boundary.
	truncateReplace func(string, string) error
	// Per-instance synchronization for the snapshot/close race regression.
	beforeSnapshotLock func()

	// Background sync goroutine
	syncTicker *time.Ticker

	// Stats
	totalWrites   atomic.Int64
	totalSyncs    atomic.Int64
	lastSyncTime  atomic.Int64
	lastEntryTime atomic.Int64

	// Degraded state (set when corruption is detected / recovery actions taken)
	degraded       atomic.Bool
	lastCorruption atomic.Value // *CorruptionDiagnostics

	// walLog is the pre-bound subsystem=wal *slog.Logger derived from
	// cfg.SlogLogger at NewWAL entry. D-07 single-allocation: every WAL
	// instance pays one .With(...) cost; recovery / truncate / corruption
	// paths reuse this child logger.
	walLog *slog.Logger
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

// Config returns the WAL configuration (read-only access).
func (w *WAL) Config() *WALConfig {
	if w == nil {
		return nil
	}
	return w.config
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
	if err := os.MkdirAll(walSegmentsDir(cfg.Dir), 0755); err != nil {
		return nil, fmt.Errorf("wal: failed to create segments directory: %w", err)
	}

	// D-01a discard fallback for the structured *slog.Logger before the
	// existing WALLogger fallback so the latter can adopt a slog backing.
	if cfg.SlogLogger == nil {
		cfg.SlogLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	// Ensure logger is available early (used by startup repair diagnostics).
	// The default WALLogger now writes into the structured slog channel
	// instead of stdlib log printers (LOG-01 storage clean).
	if cfg.Logger == nil {
		cfg.Logger = newSlogWALLogger(cfg.SlogLogger)
	}

	// Startup repair: if the previous process crashed mid-write, the WAL can end with a
	// partial record. If we append after that, the WAL becomes permanently unreadable.
	//
	// This truncates only the *incomplete tail* (or the first corrupted tail record)
	// back to the last known-good record boundary.
	walPath := walActivePath(cfg.Dir)
	if err := ValidateWALFormat(cfg.Dir, WALValidateCurrent); err != nil {
		return nil, err
	}
	if repaired, diag, err := repairWALTailIfNeeded(walPath, cfg.Logger); err != nil {
		return nil, err
	} else if repaired && diag != nil {
		// Best-effort structured log; the diagnostic JSON artifact is already written.
		cfg.Logger.Log("warn", "wal startup repair applied", map[string]any{
			"wal_path":          diag.WALPath,
			"file_size":         diag.FileSize,
			"corrupted_seq":     diag.CorruptedSeq,
			"last_good_seq":     diag.LastGoodSeq,
			"recovery_action":   diag.RecoveryAction,
			"suspected_cause":   diag.SuspectedCause,
			"backup_path":       diag.BackupPath,
			"timestamp_rfc3339": diag.Timestamp.Format(time.RFC3339),
		})
	}

	// Open or create WAL file
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
		config: cfg,
		file:   file,
		writer: bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		// D-07 subsystem-tag once: walLog inherits cfg.SlogLogger with
		// subsystem=wal. Recovery, truncation, and corruption paths reuse it.
		walLog: cfg.SlogLogger.With("subsystem", "wal"),
	}
	// cfg.Logger is guaranteed non-nil above.
	w.encoder = jsontext.NewEncoder(w.writer)
	// An unreadable marker is treated as present: conversion is then attempted.
	if _, err := os.Stat(walPropertyEpochUnsealedPath(cfg.Dir)); !os.IsNotExist(err) {
		w.propertyEpochUnsealed = true
	}

	// Load existing sequence number
	seq, err := w.loadLastSequence()
	if err != nil {
		file.Close()
		return nil, err
	}
	w.sequence.Store(seq)
	w.loadActiveSegmentState()

	// Start batch sync if configured
	if cfg.SyncMode == "batch" && cfg.BatchSyncInterval > 0 {
		w.syncTicker = time.NewTicker(cfg.BatchSyncInterval)
		if w.work.Enter() {
			go w.batchSyncLoop()
		}
	}

	return w, nil
}

// loadLastSequence reads the last sequence number from the current-format WAL.
func (w *WAL) loadLastSequence() (uint64, error) {
	entries, err := ReadWALEntriesFromDir(w.config.Dir)
	if err != nil {
		return 0, err
	}
	manifest, err := accessWALManifest(w.config.Dir, walReadManifest, nil)
	if err != nil {
		return 0, err
	}
	last := manifest.RetiredThrough
	if len(entries) > 0 && entries[len(entries)-1].Sequence > last {
		last = entries[len(entries)-1].Sequence
	}
	return last, nil
}

func (w *WAL) loadActiveSegmentState() {
	activePath := walActivePath(w.config.Dir)
	entries, err := ReadWALEntries(activePath, nil)
	if err != nil || len(entries) == 0 {
		return
	}

	info, err := os.Stat(activePath)
	if err == nil {
		w.segmentBytes = info.Size()
	}
	w.segmentEntries = int64(len(entries))
	w.segmentFirstSeq = entries[0].Sequence
	w.segmentCreatedAt = entries[0].Timestamp
}

// batchSyncLoop periodically syncs writes to disk.
func (w *WAL) batchSyncLoop() {
	defer w.work.Leave()
	stop := w.work.Context().Done()
	for {
		select {
		case <-w.syncTicker.C:
			w.Sync()
		case <-stop:
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
	_, err := w.AppendWithDatabaseReturningSeq(op, data, "")
	return err
}

// AppendReturningSeq writes a new entry to the WAL and returns its sequence number.
func (w *WAL) AppendReturningSeq(op OperationType, data interface{}) (uint64, error) {
	return w.AppendWithDatabaseReturningSeq(op, data, "")
}

// AppendWithDatabase writes a new entry to the WAL with database/namespace information.
func (w *WAL) AppendWithDatabase(op OperationType, data interface{}, database string) error {
	_, err := w.AppendWithDatabaseReturningSeq(op, data, database)
	return err
}

// AppendWithDatabaseReturningSeq writes a new entry to the WAL with database/namespace information
// and returns the assigned sequence number.
func (w *WAL) AppendWithDatabaseReturningSeq(op OperationType, data interface{}, database string) (uint64, error) {
	if !config.IsWALEnabled() {
		return 0, nil // WAL disabled, skip
	}

	if w.work.Closing() {
		return 0, ErrWALClosed
	}

	dataBuf := walJSONBufPool.Get().(*bytes.Buffer)
	defer walJSONBufPool.Put(dataBuf)
	entryBuf := walJSONBufPool.Get().(*bytes.Buffer)
	defer walJSONBufPool.Put(entryBuf)

	// Entity-bearing records use their single lossless recovery representation.
	dataBytes, err := marshalWALRecoveryPayload(dataBuf, data)
	if err != nil {
		return 0, fmt.Errorf("wal: failed to marshal data: %w", err)
	}

	// Create entry
	seq := w.sequence.Add(1)
	entry := WALEntry{
		Sequence:  seq,
		Timestamp: time.Now(),
		Operation: op,
		Data:      dataBytes,
		Checksum:  crc32Checksum(dataBytes),
		Database:  database, // Store database name for proper recovery
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.prepareUnboundWALRecordLocked(op); err != nil {
		return 0, err
	}

	entryBytes, err := processWALEntryPayload(walPayloadEncode, entryBuf, &entry, nil, 0)
	if err != nil {
		return 0, fmt.Errorf("wal: failed to serialize entry: %w", err)
	}

	// Write atomically: full record in one Write when using bufio so the last record
	// is never split across a buffer flush (avoids partial-write on recovery after close).
	alignedRecordLen, err := writeAtomicRecordBufio(w.writer, entryBytes, walFormatVersionCurrent)
	if err != nil {
		return 0, fmt.Errorf("wal: failed to write entry: %w", err)
	}

	w.entries.Add(1)
	w.bytes.Add(alignedRecordLen)
	w.totalWrites.Add(1)
	w.lastEntryTime.Store(time.Now().UnixNano())

	if w.segmentEntries == 0 {
		w.segmentFirstSeq = seq
		w.segmentCreatedAt = entry.Timestamp
	}
	w.segmentEntries++
	w.segmentBytes += alignedRecordLen

	// Immediate sync if configured
	if w.config.SyncMode == "immediate" {
		if err := w.syncLocked(); err != nil {
			return 0, err
		}
		if err := w.maybeRotateLocked(seq); err != nil {
			return 0, err
		}
		return seq, nil
	}

	// For non-immediate modes, flush the userspace buffer so the WAL is
	// readable (and crash-recovery can find entries) even when we aren't
	// fsync'ing. DeferAppendFlush skips this per-append write(2); the
	// background Sync drains the buffer on its tick instead.
	if !w.config.DeferAppendFlush {
		if err := w.writer.Flush(); err != nil {
			return 0, fmt.Errorf("wal: flush failed: %w", err)
		}
	}

	if err := w.maybeRotateLocked(seq); err != nil {
		return 0, err
	}

	return seq, nil
}

// AppendBufferedCommit writes the full payload of an acknowledged
// write-behind commit to the WAL and pushes it to the kernel page cache
// (write(2)) before returning, so the ACK never outruns the record that
// survives kill -9. It deliberately skips fsync: power-loss durability
// remains the durable synchronous path's contract.
func (w *WAL) AppendBufferedCommit(c *bufferedCommit, dictionary *propertyKeyDictionary) error {
	if w == nil || c == nil {
		return nil
	}
	data := walBufferedCommitPayload{
		LogicalTxID: c.logicalTxID,
		TxID:        c.txID,
		Namespace:   c.namespace,
		Ops:         c.ops,
		Writes:      c.writes,
		Version:     c.version,
		NodeMax:     c.counterNodeMax,
		EdgeMax:     c.counterEdgeMax,
		HasCounters: c.hasCounters,
	}
	if c.walIDs != nil {
		var err error
		data.NumericBindings, err = captureWALNumericBindings(c.walIDs, c.writes, c.deletes, nil)
		if err != nil {
			return fmt.Errorf("wal: failed to bind buffered numeric IDs: %w", err)
		}
	}
	if len(c.deletes) > 0 {
		data.Deletes = make([]string, 0, len(c.deletes))
		for key := range c.deletes {
			data.Deletes = append(data.Deletes, key)
		}
	}
	if !c.propKeyDrain.empty() {
		if len(c.propKeyDrain.pending) > 0 {
			data.PropKeys = make([]WALPropKeyEntry, 0, len(c.propKeyDrain.pending))
			for _, entry := range c.propKeyDrain.pending {
				data.PropKeys = append(data.PropKeys, WALPropKeyEntry{Namespace: entry.namespace, Name: entry.name, ID: entry.id})
			}
		}
		data.PropKeyCounters = c.propKeyDrain.counters
	}
	if _, err := w.appendWithDatabaseForcedFlush(OpBufferedCommit, &data, c.namespace, dictionary); err != nil {
		return fmt.Errorf("wal: failed to log buffered commit %q: %w", c.txID, err)
	}
	return nil
}

// appendWithDatabaseForcedFlush is AppendWithDatabaseReturningSeq with one
// difference: it flushes the userspace buffer to the kernel after the write,
// regardless of DeferAppendFlush, so a crash cannot lose the record from the
// userspace buffer before the ACK.
func (w *WAL) appendWithDatabaseForcedFlush(op OperationType, data interface{}, database string, dictionaries ...*propertyKeyDictionary) (uint64, error) {
	if !config.IsWALEnabled() {
		return 0, nil
	}
	if w.work.Closing() {
		return 0, ErrWALClosed
	}
	if op != OpBufferedCommit {
		return w.AppendWithDatabaseReturningSeq(op, data, database)
	}

	entryBuf := walJSONBufPool.Get().(*bytes.Buffer)
	defer walJSONBufPool.Put(entryBuf)

	// Physical Badger keys are arbitrary bytes, not UTF-8 text. The typed
	// recovery codec preserves those keys and the operation images exactly.
	// Keep the encoder's buffer until the entry has been written: compression
	// can return its input unchanged for a small record.
	payload, ok := data.(*walBufferedCommitPayload)
	if !ok || payload == nil {
		return 0, fmt.Errorf("wal: buffered commit requires its recovery payload")
	}
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	dataBytes, err := encoder.recovery(payload)
	if err != nil {
		return 0, fmt.Errorf("wal: failed to marshal data: %w", err)
	}
	if len(dataBytes) > maxBufferedWALPayloadBytes {
		return 0, fmt.Errorf("wal: buffered commit exceeds decoded payload limit")
	}
	// The buffered-commit payload is the dominant WAL cost (it carries every
	// write for kill -9 recovery); losslessly compress it so the WAL stays
	// proportional to the data instead of several times larger.
	dataBytes = compressWALBufferedCommitPayload(dataBytes)

	seq := w.sequence.Add(1)
	entry := WALEntry{
		Sequence:  seq,
		Timestamp: time.Now(),
		Operation: op,
		Data:      dataBytes,
		Checksum:  crc32Checksum(dataBytes),
		Database:  database,
	}

	w.mu.Lock()
	if len(dictionaries) != 0 {
		if err := w.ensurePropertyEpochLocked(dictionaries[0], false); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	} else if err := w.prepareUnboundWALRecordLocked(op); err != nil {
		w.mu.Unlock()
		return 0, err
	}
	entryBytes, err := processWALEntryPayload(walPayloadEncode, entryBuf, &entry, nil, 0)
	if err != nil {
		w.mu.Unlock()
		return 0, fmt.Errorf("wal: failed to serialize entry: %w", err)
	}
	alignedRecordLen, err := writeAtomicRecordBufio(w.writer, entryBytes, walFormatVersionCurrent)
	if err != nil {
		w.mu.Unlock()
		return 0, fmt.Errorf("wal: failed to write entry: %w", err)
	}
	w.entries.Add(1)
	w.bytes.Add(alignedRecordLen)
	w.totalWrites.Add(1)
	w.lastEntryTime.Store(time.Now().UnixNano())
	if w.segmentEntries == 0 {
		w.segmentFirstSeq = seq
		w.segmentCreatedAt = entry.Timestamp
	}
	w.segmentEntries++
	w.segmentBytes += alignedRecordLen

	// Forced write(2): the record must be in the kernel page cache before
	// the caller acknowledges, even when DeferAppendFlush is set.
	if flushErr := w.writer.Flush(); flushErr != nil {
		w.mu.Unlock()
		return 0, fmt.Errorf("wal: flush failed: %w", flushErr)
	}

	if rotateErr := w.maybeRotateLocked(seq); rotateErr != nil {
		w.mu.Unlock()
		return 0, rotateErr
	}
	w.mu.Unlock()
	return seq, nil
}

// Sync flushes all buffered writes to disk.
//
// Lock discipline: we acquire w.mu ONLY to drain the bufio userspace
// buffer to the kernel (a short, bounded memcpy+write), then release
// it BEFORE the kernel fsync. The fsync is serialized instead through
// syncMu so concurrent Sync() callers don't issue duplicate fsyncs
// while still letting Append() progress against w.mu in parallel.
// This eliminated the dominant mutex-contention source seen during
// bulk seed: batchSyncLoop's 100ms ticks used to block every Append
// for the duration of each fsync.
func (w *WAL) Sync() error {
	if w.work.Closing() {
		return ErrWALClosed
	}

	// Step 1: drain the userspace buffer under w.mu.
	w.mu.Lock()
	if err := w.writer.Flush(); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("wal: flush failed: %w", err)
	}
	file := w.file
	syncMode := w.config.SyncMode
	w.mu.Unlock()

	// Step 2: fsync without w.mu, so concurrent Appends don't block.
	// syncMu serializes fsyncs themselves so we don't pile them up.
	if syncMode != "none" {
		w.syncMu.Lock()
		err := file.Sync()
		w.syncMu.Unlock()
		if err != nil {
			return fmt.Errorf("wal: sync failed: %w", err)
		}
	}

	w.totalSyncs.Add(1)
	w.lastSyncTime.Store(time.Now().UnixNano())
	return nil
}

// syncLocked performs the full flush+fsync while the caller already
// holds w.mu. Used by rotation and immediate-mode Append where write
// and fsync must be atomic relative to the writer state. Hot-path
// Sync() callers MUST go through Sync() instead so they can release
// w.mu across the fsync.
func (w *WAL) syncLocked() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush failed: %w", err)
	}

	if w.config.SyncMode != "none" {
		// Even under w.mu, coordinate with concurrent Sync() callers
		// on syncMu so we don't double-fsync.
		w.syncMu.Lock()
		err := w.file.Sync()
		w.syncMu.Unlock()
		if err != nil {
			return fmt.Errorf("wal: sync failed: %w", err)
		}
	}

	w.totalSyncs.Add(1)
	w.lastSyncTime.Store(time.Now().UnixNano())
	return nil
}

func (w *WAL) maybeRotateLocked(lastSeq uint64) error {
	if w.segmentEntries == 0 {
		return nil
	}

	if w.config.MaxFileSize > 0 && w.segmentBytes >= w.config.MaxFileSize {
		return w.rotateSegmentLocked(lastSeq)
	}
	if w.config.MaxEntries > 0 && w.segmentEntries >= w.config.MaxEntries {
		return w.rotateSegmentLocked(lastSeq)
	}
	return nil
}

func (w *WAL) rotateSegmentLocked(lastSeq uint64) error {
	if w.segmentEntries == 0 {
		return nil
	}

	if err := w.syncLocked(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	segmentDir := walSegmentsDir(w.config.Dir)
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return err
	}

	segmentName := fmt.Sprintf("seg-%020d-%020d.wal", w.segmentFirstSeq, lastSeq)
	activePath := walActivePath(w.config.Dir)
	segmentPath := filepath.Join(segmentDir, segmentName)
	if err := os.Rename(activePath, segmentPath); err != nil {
		return err
	}
	_ = syncDir(segmentDir)

	manifest, err := accessWALManifest(w.config.Dir, walReadManifest, nil)
	if err != nil {
		return err
	}
	info, err := os.Stat(segmentPath)
	sizeBytes := int64(0)
	if err == nil {
		sizeBytes = info.Size()
	}
	segment := WALSegment{
		FirstSeq:  w.segmentFirstSeq,
		LastSeq:   lastSeq,
		SizeBytes: sizeBytes,
		CreatedAt: w.segmentCreatedAt,
		Path:      segmentName,
	}
	listed := false
	for i := range manifest.Segments {
		if manifest.Segments[i].Path == segmentName {
			manifest.Segments[i] = segment
			listed = true
			break
		}
	}
	if !listed {
		manifest.Segments = append(manifest.Segments, segment)
	}
	sort.Slice(manifest.Segments, func(i, j int) bool {
		return manifest.Segments[i].FirstSeq < manifest.Segments[j].FirstSeq
	})
	if _, err := accessWALManifest(w.config.Dir, walPublishManifest, manifest); err != nil {
		return err
	}

	file, err := os.OpenFile(activePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if err := syncDir(w.config.Dir); err != nil {
		file.Close()
		return err
	}

	w.file = file
	w.writer = bufio.NewWriterSize(file, 64*1024)
	w.encoder = jsontext.NewEncoder(w.writer)
	w.segmentFirstSeq = 0
	w.segmentEntries = 0
	w.segmentBytes = 0
	w.segmentCreatedAt = time.Time{}

	w.propertyEpochKnown, w.propertyEpochFormatChecked = true, true
	if w.propertyEpoch != nil || w.propertyDictionary != nil {
		if err := w.writePropertyEpochLocked(w.propertyEpoch); err != nil {
			return err
		}
		return w.writer.Flush()
	}
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
	// The seal and the final handle release share the file owner. Epoch
	// sealing also owns mu and must never mistake a Close waiting here for
	// an already flushed and closed handle.
	w.mu.Lock()
	if w.work.CloseAdmission() && w.syncTicker != nil {
		w.syncTicker.Stop()
	}
	w.mu.Unlock()
	// Every caller, first or repeated, joins the one stop: the sync loop
	// returns, then the final sync and handle release run once.
	return w.work.Shutdown(context.Background(), w.closeFile)
}

// closeFile is the WAL owner's dispose, after the sync loop returned.
func (w *WAL) closeFile() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	syncErr := w.syncLocked()
	closeErr := w.file.Close()
	w.handleReleased = true
	return compose.Outcomes(syncErr, closeErr)
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
		Closed:        w.work.Closing(),
	}
}

// Sequence returns the current sequence number.
func (w *WAL) Sequence() uint64 {
	return w.sequence.Load()
}

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

// WALIntegrityReport provides detailed integrity check results
type WALIntegrityReport struct {
	Healthy           bool                    `json:"healthy"`
	TotalEntries      int                     `json:"total_entries"`
	ValidEntries      int                     `json:"valid_entries"`
	CorruptedEntries  int                     `json:"corrupted_entries"`
	SkippedEmbeddings int                     `json:"skipped_embeddings"`
	FirstSeq          uint64                  `json:"first_seq"`
	LastSeq           uint64                  `json:"last_seq"`
	FileSize          int64                   `json:"file_size"`
	Format            string                  `json:"format"` // "atomic" or "legacy"
	Errors            []string                `json:"errors,omitempty"`
	CorruptionDetails []CorruptionDiagnostics `json:"corruption_details,omitempty"`
}

// CheckWALIntegrity performs a comprehensive integrity check on a WAL file.
// This can be used for health checks, startup validation, or manual diagnostics.
// It does NOT modify the WAL - read-only operation.
func CheckWALIntegrity(walPath string) (*WALIntegrityReport, error) {
	report := &WALIntegrityReport{
		Healthy: true,
		Format:  "unknown",
	}

	// Check file exists and get size
	fi, err := os.Stat(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			report.Healthy = true // No WAL is healthy (fresh start)
			return report, nil
		}
		return nil, fmt.Errorf("failed to stat WAL: %w", err)
	}
	report.FileSize = fi.Size()

	if report.FileSize == 0 {
		report.Healthy = true
		return report, nil
	}

	// Open and detect format
	file, err := os.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	defer file.Close()

	header := make([]byte, 4)
	n, err := file.Read(header)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	if n == 0 {
		report.Healthy = true
		return report, nil
	}

	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header)
	if magic == walMagic {
		report.Format = "atomic"
		return checkAtomicWALIntegrity(file, report)
	}

	return nil, fmt.Errorf("%w: unrecognized WAL header in %s", ErrWALUnsupportedVersion, walPath)
}

func checkAtomicWALIntegrity(file *os.File, report *WALIntegrityReport) (*WALIntegrityReport, error) {
	reader := bufio.NewReader(file)
	headerBuf := make([]byte, 9)
	var lastGoodSeq uint64

	for {
		n, err := io.ReadFull(reader, headerBuf)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF || n != 9 {
			report.Errors = append(report.Errors, "partial header at end of file")
			break
		}
		if err != nil {
			report.Healthy = false
			report.Errors = append(report.Errors, fmt.Sprintf("read error: %v", err))
			break
		}

		magic := binary.LittleEndian.Uint32(headerBuf[0:4])
		if magic != walMagic {
			report.Healthy = false
			report.CorruptedEntries++
			report.Errors = append(report.Errors,
				fmt.Sprintf("invalid magic after seq %d", lastGoodSeq))
			break
		}

		version := headerBuf[4]
		if version != walFormatVersionCurrent {
			return nil, unsupportedWALVersion(version)
		}
		payloadLen := binary.LittleEndian.Uint32(headerBuf[5:9])

		if payloadLen > walMaxEntrySize {
			report.Healthy = false
			report.CorruptedEntries++
			report.Errors = append(report.Errors,
				fmt.Sprintf("invalid payload size %d after seq %d", payloadLen, lastGoodSeq))
			break
		}

		// Read payload
		payload := make([]byte, payloadLen)
		n, err = io.ReadFull(reader, payload)
		if err != nil || uint32(n) != payloadLen {
			report.Errors = append(report.Errors, "truncated payload")
			break
		}

		// Read CRC
		crcBuf := make([]byte, 4)
		n, err = io.ReadFull(reader, crcBuf)
		if err != nil || n != 4 {
			report.Errors = append(report.Errors, "missing CRC")
			break
		}

		storedCRC := binary.LittleEndian.Uint32(crcBuf)
		computedCRC := crc32Checksum(payload)

		if storedCRC != computedCRC {
			report.Healthy = false
			report.CorruptedEntries++
			diag := CorruptionDiagnostics{
				LastGoodSeq: lastGoodSeq,
				ExpectedCRC: computedCRC,
				ActualCRC:   storedCRC,
			}
			diag.diagnoseCause()
			report.CorruptionDetails = append(report.CorruptionDetails, diag)
			report.Errors = append(report.Errors,
				fmt.Sprintf("CRC mismatch after seq %d", lastGoodSeq))
			// Continue checking to find all corrupted entries
		}

		// Read trailer for v2+
		if version >= 2 {
			trailerBuf := make([]byte, 8)
			n, err = io.ReadFull(reader, trailerBuf)
			if err != nil || n != 8 {
				report.Errors = append(report.Errors, "missing trailer")
				break
			}

			storedTrailer := binary.LittleEndian.Uint64(trailerBuf)
			if storedTrailer != walTrailer {
				report.Errors = append(report.Errors,
					fmt.Sprintf("invalid trailer after seq %d", lastGoodSeq))
			}

			// Skip padding
			rawRecordLen := int64(9 + payloadLen + 4 + 8)
			alignedRecordLen := alignUp(rawRecordLen)
			paddingLen := alignedRecordLen - rawRecordLen
			if paddingLen > 0 {
				paddingBuf := make([]byte, paddingLen)
				io.ReadFull(reader, paddingBuf)
			}
		}

		// Decode entry for sequence tracking
		entry, err := WALEntry{}, error(nil)
		_, err = processWALEntryPayload(walPayloadDecode, nil, &entry, payload, version)
		if err != nil {
			if storedCRC == computedCRC {
				report.CorruptedEntries++
			}
			report.Healthy = false
			report.Errors = append(report.Errors, fmt.Sprintf("cannot decode entry after seq %d: %v", lastGoodSeq, err))
			continue
		}

		report.TotalEntries++
		if storedCRC == computedCRC {
			report.ValidEntries++
			lastGoodSeq = entry.Sequence

			if report.FirstSeq == 0 {
				report.FirstSeq = entry.Sequence
			}
			report.LastSeq = entry.Sequence
		}
	}

	return report, nil
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
	if w.work.Closing() {
		return nil, ErrWALClosed
	}

	// Get current sequence
	seq := w.sequence.Load()

	// Checkpoint before snapshot
	if err := w.Checkpoint(); err != nil {
		return nil, fmt.Errorf("wal: checkpoint failed: %w", err)
	}

	// Every stored node and edge: the ones the visibility layer hides from
	// reads are stored data too.
	nodes, err := CollectStoredNodes(context.Background(), engine)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to get nodes: %w", err)
	}

	edges, err := CollectStoredEdges(context.Background(), engine)
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
// It writes lossless NDS2 v2 frames using write-to-temp + atomic rename.
// Older binaries reject this format; retain a pre-upgrade backup for rollback.
func SaveSnapshot(snapshot *Snapshot, path string) error {
	return saveSnapshot(snapshot, path, syncDir)
}

func saveSnapshot(snapshot *Snapshot, path string, syncDirectory func(string) error) error {
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

	// Use the versioned lossless framed transport; legacy JSON remains readable.
	if err := writeSnapshotCollection(file, snapshot); err != nil {
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
	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf("wal: failed to sync snapshot directory: %w", err)
	}

	return nil
}

// LoadSnapshot reads legacy JSON or NDS2 v1/v2 framed snapshots from disk.
func LoadSnapshot(path string) (*Snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open snapshot: %w", err)
	}
	defer file.Close()

	streaming, err := IsStreamingSnapshot(path)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to inspect snapshot: %w", err)
	}
	var snapshot Snapshot
	if streaming {
		snapshot.Version = "1.0"
		metadata, err := readSnapshot(file, snapshotCollectionVisitor{snapshot: &snapshot}, &snapshot, materializedSnapshotRecordSize)
		if err != nil {
			return nil, fmt.Errorf("wal: failed to decode snapshot: %w", err)
		}
		snapshot.Sequence = metadata.Sequence
		snapshot.Timestamp = metadata.Timestamp
		return &snapshot, nil
	}
	if err := decodeLegacyRecoveryJSON(file, &snapshot); err != nil {
		return nil, fmt.Errorf("wal: failed to decode snapshot: %w", err)
	}

	return &snapshot, nil
}

// snapshotFileInfo holds path and mod time for retention pruning.
type snapshotFileInfo struct {
	path    string
	modTime time.Time
}

// PruneOldSnapshotFiles removes old snapshot files in dir according to cfg retention.
// Keeps at most SnapshotRetentionMaxCount snapshots (newest by mtime) and deletes
// any older than SnapshotRetentionMaxAge. Idempotent; safe to call after each save.
func PruneOldSnapshotFiles(dir string, cfg *WALConfig) error {
	if cfg == nil || (cfg.SnapshotRetentionMaxCount <= 0 && cfg.SnapshotRetentionMaxAge <= 0) {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("wal: list snapshot dir: %w", err)
	}
	var files []snapshotFileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		files = append(files, snapshotFileInfo{path: path, modTime: info.ModTime()})
	}
	if len(files) == 0 {
		return nil
	}
	now := time.Now()
	if cfg.SnapshotRetentionMaxAge > 0 {
		cutoff := now.Add(-cfg.SnapshotRetentionMaxAge)
		var kept []snapshotFileInfo
		for _, f := range files {
			if f.modTime.After(cutoff) {
				kept = append(kept, f)
			} else {
				_ = os.Remove(f.path)
			}
		}
		files = kept
	}
	if cfg.SnapshotRetentionMaxCount <= 0 || len(files) <= cfg.SnapshotRetentionMaxCount {
		return nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modTime.After(files[j].modTime) })
	for i := cfg.SnapshotRetentionMaxCount; i < len(files); i++ {
		_ = os.Remove(files[i].path)
	}
	return nil
}

// WALSnapshotMode selects the post-snapshot WAL work ManageSnapshot performs.
type WALSnapshotMode uint8

const (
	// WALSnapshotTruncate reclaims the entries a snapshot covers. With WAL
	// retention configured (RetentionMaxAge or RetentionMaxSegments, e.g.
	// NORNICDB_WAL_RETENTION_MAX_AGE) entries are not dropped: the active WAL is
	// sealed into a segment and retention removes only the sealed segments the
	// snapshot covers that are past their age or count. Those entries stay
	// readable (db.txlog.entries, replication, audit) for the retention period;
	// recovery replays only entries after the snapshot. Without retention the
	// covered entries are dropped.
	WALSnapshotTruncate WALSnapshotMode = iota
	// WALSnapshotRetention applies segment retention to sealed segments only. It
	// never rotates or rewrites the active WAL and never seals property epochs.
	WALSnapshotRetention
)

// String returns the stable mode name used in logs and test subtests.
func (m WALSnapshotMode) String() string {
	switch m {
	case WALSnapshotTruncate:
		return "truncate"
	case WALSnapshotRetention:
		return "retention"
	default:
		return "unknown"
	}
}

// ManageSnapshot is the single owner of post-snapshot WAL management. Call it
// after SaveSnapshot succeeds. Both modes take w.mu once and report ErrWALClosed
// on a closed WAL.
//
// WALSnapshotTruncate validates the WAL first. It is crash-safe:
//  1. Validate and read the current WAL before closing it
//  2. Write and sync a new WAL with only post-snapshot entries
//  3. Atomically rename the new WAL over the old file
//  4. Reopen the WAL for appends
//
// If the system crashes during truncation:
//   - Old WAL remains intact (rename is atomic)
//   - Recovery will replay full WAL (safe, just slower)
//   - Retry truncation on next snapshot
//
// WALSnapshotRetention deletes sealed segments a snapshot covers that retention
// no longer keeps. A zero snapshot sequence is a no-op.
//
// Example:
//
//	snapshot, _ := wal.CreateSnapshot(engine)
//	SaveSnapshot(snapshot, "data/snapshot.json")
//	wal.ManageSnapshot(snapshot.Sequence, WALSnapshotTruncate)  // Reclaim disk space
//	wal.ManageSnapshot(snapshot.Sequence, WALSnapshotRetention) // Retention only
func (w *WAL) ManageSnapshot(snapshotSeq uint64, mode WALSnapshotMode) error {
	if w.work.Closing() {
		return ErrWALClosed
	}
	if mode != WALSnapshotTruncate && mode != WALSnapshotRetention {
		return fmt.Errorf("wal: invalid WAL snapshot mode %d", mode)
	}
	if w.beforeSnapshotLock != nil {
		w.beforeSnapshotLock()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	// Close publishes the closed state while holding the same lock. It may
	// have won after the optimistic check above and before this acquisition.
	if w.work.Closing() {
		return ErrWALClosed
	}

	if mode != WALSnapshotTruncate {
		return w.retainSealedSegmentsLocked(snapshotSeq)
	}

	// A missing or unreadable segment, unsupported format, or malformed manifest
	// is not a corrupt tail that can be safely discarded after a snapshot.
	// Check before closing the active file or changing any retained WAL state.
	if err := ValidateWALFormat(w.config.Dir, WALValidateCurrent); err != nil {
		return fmt.Errorf("wal: cannot truncate before validating WAL: %w", err)
	}

	if w.retainsSegments() {
		if err := w.rotateSegmentLocked(w.sequence.Load()); err != nil {
			return fmt.Errorf("wal: failed to seal segment before retention: %w", err)
		}
		if err := w.retainSealedSegmentsLocked(snapshotSeq); err != nil {
			return err
		}
		return w.sealPropertyEpochsLocked()
	}

	// Flush pending writes before truncation
	if err := w.syncLocked(); err != nil {
		return fmt.Errorf("wal: failed to flush before truncate: %w", err)
	}

	walPath := walActivePath(w.config.Dir)
	// A failed read may include entries newer than the snapshot. Rewriting the
	// file from a best-effort subset would turn an observable failure into data
	// loss, so leave the active handle and WAL intact for recovery.
	allEntries, readErr := ReadWALEntriesFromDir(w.config.Dir)
	if readErr != nil {
		if errors.Is(readErr, ErrWALCorrupted) || errors.Is(readErr, ErrWALChecksumFailed) {
			diag := &CorruptionDiagnostics{
				Timestamp:      time.Now(),
				WALPath:        walPath,
				BackupPath:     w.backupCorruptedWAL(walPath),
				RecoveryAction: "truncate_refused",
			}
			if fi, err := os.Stat(walPath); err == nil {
				diag.FileSize = fi.Size()
			}
			diag.diagnoseCause()
			w.reportCorruption(diag, readErr)
		}
		return fmt.Errorf("wal: cannot truncate unreadable WAL: %w", readErr)
	}
	activeEntries, err := ReadWALEntries(walPath, nil)
	if err != nil {
		return fmt.Errorf("wal: cannot truncate unreadable active WAL: %w", err)
	}
	manifest, err := accessWALManifest(w.config.Dir, walReadManifest, nil)
	if err != nil {
		return fmt.Errorf("wal: cannot load segments for truncation: %w", err)
	}
	var retainedSegments, coveredSegments []WALSegment
	var retainedSegmentBytes int64
	for _, segment := range manifest.Segments {
		covered := segment.LastSeq <= snapshotSeq
		if covered {
			// Never discard a segment based solely on inaccurate range metadata.
			path, pathErr := resolveWALSegmentPath(w.config.Dir, segment.Path)
			if pathErr != nil {
				return pathErr
			}
			entries, segmentErr := ReadWALEntries(path, nil)
			if segmentErr != nil {
				return fmt.Errorf("wal: cannot inspect segment for truncation: %w", segmentErr)
			}
			for _, entry := range entries {
				if entry.Sequence > snapshotSeq {
					covered = false
					break
				}
			}
		}
		if covered {
			coveredSegments = append(coveredSegments, segment)
		} else {
			retainedSegments = append(retainedSegments, segment)
			retainedSegmentBytes += segment.SizeBytes
		}
	}

	// Close current WAL file only after all entries have been read safely.
	tmpPath := walPath + ".truncate.tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("wal: failed to create temp WAL: %w", err)
	}
	if err := w.file.Close(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("wal: failed to close for truncate: %w", err)
	}

	// Filter entries AFTER snapshot sequence
	var keptEntries []WALEntry
	// Retained segments stay in place. Folding them into the new active file
	// would duplicate their records if the process stopped before cleanup.
	for _, entry := range activeEntries {
		if entry.Sequence > snapshotSeq {
			keptEntries = append(keptEntries, entry)
		}
	}

	// Write new WAL with only kept entries
	tmpWriter := bufio.NewWriterSize(tmpFile, 64*1024)
	var bytesWritten int64

	// Write kept entries using the current atomic format (v3 raw-Data payload)
	entryBuf := walJSONBufPool.Get().(*bytes.Buffer)
	defer walJSONBufPool.Put(entryBuf)

	var retainedEpoch *WALPropertyEpoch
	for _, entry := range keptEntries {
		headerBytes, err := w.writeRetainedPropertyEpochLocked(tmpWriter, &retainedEpoch, entry)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			w.reopenWAL()
			return fmt.Errorf("wal: retain property epoch: %w", err)
		}
		bytesWritten += headerBytes
		// Serialize entry
		entryBytes, err := processWALEntryPayload(walPayloadEncode, entryBuf, &entry, nil, 0)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			w.reopenWAL()
			return fmt.Errorf("wal: failed to serialize entry seq %d: %w", entry.Sequence, err)
		}
		alignedRecordLen, err := writeAtomicRecordBufio(tmpWriter, entryBytes, walFormatVersionCurrent)
		if err != nil {
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
	replace := os.Rename
	if w.truncateReplace != nil {
		replace = w.truncateReplace
	}
	if err := replace(tmpPath, walPath); err != nil {
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

	// Publish the retained segment set only after the active replacement is
	// durable. Removed segments contain no entries newer than the snapshot.
	manifest.Segments = retainedSegments
	if snapshotSeq > manifest.RetiredThrough {
		manifest.RetiredThrough = snapshotSeq
	}
	if _, err := accessWALManifest(w.config.Dir, walPublishManifest, manifest); err != nil {
		return fmt.Errorf("wal: failed to publish truncated segments: %w", err)
	}
	for _, segment := range coveredSegments {
		path, pathErr := resolveWALSegmentPath(w.config.Dir, segment.Path)
		if pathErr != nil {
			return pathErr
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wal: failed to remove covered segment: %w", err)
		}
	}
	if len(coveredSegments) > 0 {
		if err := syncDir(walSegmentsDir(w.config.Dir)); err != nil {
			return fmt.Errorf("wal: failed to sync truncated segments: %w", err)
		}
	}

	w.segmentFirstSeq = 0
	w.segmentEntries = 0
	w.segmentBytes = 0
	w.segmentCreatedAt = time.Time{}
	w.loadActiveSegmentState()

	// Update stats
	var retainedCount int64
	for _, entry := range allEntries {
		if entry.Sequence > snapshotSeq {
			retainedCount++
		}
	}
	w.entries.Store(retainedCount)
	w.bytes.Store(bytesWritten + retainedSegmentBytes)

	w.propertyEpoch, w.propertyEpochKnown, w.propertyEpochFormatChecked = retainedEpoch, true, true
	return w.sealPropertyEpochsLocked()
}

// retainsSegments reports whether WAL retention is configured: sealed
// segments are then kept for their age or count after a snapshot instead of
// being dropped by WALSnapshotTruncate.
func (w *WAL) retainsSegments() bool {
	return w.config.RetentionMaxAge > 0 || w.config.RetentionMaxSegments > 0
}

// retainSealedSegmentsLocked deletes sealed segments that are safe to drop
// after a snapshot. Segments are only deleted if their last sequence is
// <= snapshotSeq, and the manifest is published before their files are removed.
// Only segments already listed in the manifest are touched: the active WAL is
// neither rotated nor rewritten and no property epoch is sealed. The caller
// owns w.mu.
func (w *WAL) retainSealedSegmentsLocked(snapshotSeq uint64) error {
	if snapshotSeq == 0 {
		return nil
	}

	manifest, err := accessWALManifest(w.config.Dir, walReadManifest, nil)
	if err != nil {
		return err
	}
	if len(manifest.Segments) == 0 {
		return nil
	}

	now := time.Now()
	cutoff := now.Add(-w.config.RetentionMaxAge)

	var candidates []WALSegment
	var keep []WALSegment
	for _, seg := range manifest.Segments {
		if seg.LastSeq <= snapshotSeq {
			candidates = append(candidates, seg)
		} else {
			keep = append(keep, seg)
		}
	}

	if w.config.RetentionMaxAge > 0 {
		var remaining []WALSegment
		for _, seg := range candidates {
			if seg.CreatedAt.After(cutoff) {
				keep = append(keep, seg)
			} else {
				remaining = append(remaining, seg)
			}
		}
		candidates = remaining
	}

	if w.config.RetentionMaxSegments > 0 && len(candidates) > w.config.RetentionMaxSegments {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].LastSeq < candidates[j].LastSeq
		})
		keepCount := w.config.RetentionMaxSegments
		keep = append(keep, candidates[len(candidates)-keepCount:]...)
		candidates = candidates[:len(candidates)-keepCount]
	}

	manifest.Segments = keep
	for _, segment := range candidates {
		if segment.LastSeq > manifest.RetiredThrough {
			manifest.RetiredThrough = segment.LastSeq
		}
	}
	sort.Slice(manifest.Segments, func(i, j int) bool {
		return manifest.Segments[i].FirstSeq < manifest.Segments[j].FirstSeq
	})

	if _, err := accessWALManifest(w.config.Dir, walPublishManifest, manifest); err != nil {
		return err
	}
	for _, seg := range candidates {
		if err := os.Remove(filepath.Join(walSegmentsDir(w.config.Dir), seg.Path)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wal: remove retained segment: %w", err)
		}
	}
	if len(candidates) > 0 {
		return syncDir(walSegmentsDir(w.config.Dir))
	}
	return nil
}

// reopenWAL reopens the WAL file for appending.
// Called after truncation or other operations that close the file.
func (w *WAL) reopenWAL() error {
	walPath := walActivePath(w.config.Dir)
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("wal: failed to reopen: %w", err)
	}

	w.propertyEpochKnown, w.propertyEpochFormatChecked = false, false
	w.file = file
	w.writer = bufio.NewWriterSize(file, 64*1024)
	w.encoder = jsontext.NewEncoder(w.writer)
	w.segmentFirstSeq = 0
	w.segmentEntries = 0
	w.segmentBytes = 0
	w.segmentCreatedAt = time.Time{}
	w.loadActiveSegmentState()

	return nil
}

// ReadWALEntries reads all entries from a WAL file.
// Reads the current atomic WAL format; older WAL formats are rejected.
// Returns error on corruption of critical entries (nodes, edges).
// Embedding updates are safe to skip as they can be regenerated.
//
// A nil logger discards recovery diagnostics.
func ReadWALEntries(walPath string, logger *slog.Logger) ([]WALEntry, error) {
	entries := make([]WALEntry, 0)
	err := visitWALEntriesWithLogger(walPath, logger, func(entry WALEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

func visitWALEntriesWithLogger(walPath string, logger *slog.Logger, visit func(WALEntry) error) error {
	if logger == nil {
		logger = discardWALSlog()
	}
	file, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("wal: failed to open: %w", err)
	}
	defer file.Close()

	// Peek at first 4 bytes to detect format
	header := make([]byte, 4)
	n, err := file.Read(header)
	if err == io.EOF {
		return nil // Empty file
	}
	if err != nil {
		return fmt.Errorf("wal: failed to read header: %w", err)
	}
	if n == 0 {
		return nil
	}

	// Reset to beginning
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("wal: failed to seek: %w", err)
	}

	// The current atomic format starts with magic "WALE".
	magic := binary.LittleEndian.Uint32(header)
	if magic == walMagic {
		return visitAtomicWALEntries(file, logger, visit)
	}

	return fmt.Errorf("%w: unrecognized WAL header in %s", ErrWALUnsupportedVersion, walPath)
}

func visitAtomicWALEntries(file *os.File, logger *slog.Logger, visit func(WALEntry) error) error {
	visit = withWALPropertyEpochs(visit)
	if logger == nil {
		logger = discardWALSlog()
	}
	var skippedEmbeddings int
	var partialWriteDetected bool
	var lastGoodSeq uint64
	var offset uint64

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
			return fmt.Errorf("wal: failed to read entry header: %w", err)
		}
		if n != 9 {
			partialWriteDetected = true
			break
		}

		// Verify magic
		magic := binary.LittleEndian.Uint32(headerBuf[0:4])
		if magic != walMagic {
			return fmt.Errorf("%w: invalid magic at offset, expected WALE", ErrWALCorrupted)
		}

		// Read version (for future compatibility)
		version := headerBuf[4]
		if version != walFormatVersionCurrent {
			return unsupportedWALVersion(version)
		}

		// Read payload length
		payloadLen := binary.LittleEndian.Uint32(headerBuf[5:9])
		if payloadLen > walMaxEntrySize {
			return fmt.Errorf("%w: entry size %d exceeds maximum %d",
				ErrWALCorrupted, payloadLen, walMaxEntrySize)
		}

		recordLen := int64(9 + payloadLen + 4)
		if version >= 2 {
			recordLen = alignUp(recordLen + 8)
		}
		offset += uint64(recordLen)
		// Read payload
		payload := make([]byte, payloadLen)
		n, err = io.ReadFull(reader, payload)
		if err == io.ErrUnexpectedEOF || uint32(n) != payloadLen {
			// Partial payload = incomplete write
			partialWriteDetected = true
			break
		}
		if err != nil {
			return fmt.Errorf("wal: failed to read payload: %w", err)
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
			return fmt.Errorf("wal: failed to read CRC: %w", err)
		}

		// If a regenerable embedding is corrupt, consume the entire frame before
		// advancing; its trailer and alignment padding are not the next header.
		storedCRC := binary.LittleEndian.Uint32(crcBuf)
		computedCRC := crc32Checksum(payload)
		skipEmbedding := false
		if storedCRC != computedCRC {
			entry, decodeErr := WALEntry{}, error(nil)
			_, decodeErr = processWALEntryPayload(walPayloadDecode, nil, &entry, payload, version)
			if decodeErr == nil && entry.Operation == OpUpdateEmbedding {
				skipEmbedding = true
			} else {
				return fmt.Errorf("%w: CRC mismatch (stored=%x, computed=%x) after seq %d",
					ErrWALChecksumFailed, storedCRC, computedCRC, lastGoodSeq)
			}
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
				return fmt.Errorf("wal: failed to read trailer: %w", err)
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
					return fmt.Errorf("wal: failed to read padding: %w", err)
				}
			}
		}

		if skipEmbedding {
			skippedEmbeddings++
			continue
		}

		// Decode entry
		entry, err := WALEntry{}, error(nil)
		_, err = processWALEntryPayload(walPayloadDecode, nil, &entry, payload, version)
		if err != nil {
			return fmt.Errorf("%w: failed to decode entry after seq %d: %v",
				ErrWALCorrupted, lastGoodSeq, err)
		}

		entry.recordEnd = offset
		// Also verify the inner data checksum
		expectedDataCRC := crc32Checksum(entry.Data)
		if entry.Checksum != expectedDataCRC {
			if entry.Operation == OpUpdateEmbedding {
				skippedEmbeddings++
				continue
			}
			return fmt.Errorf("%w: data checksum mismatch at seq %d",
				ErrWALChecksumFailed, entry.Sequence)
		}

		if err := visit(entry); err != nil {
			return err
		}
		lastGoodSeq = entry.Sequence
	}

	if partialWriteDetected {
		logWALRecoveryEvent(logger, localization.StorageWALIncompleteWriteEvent())
	}
	if skippedEmbeddings > 0 {
		logWALRecoveryEvent(logger, localization.StorageWALCorruptedEmbeddingsSkippedEvent(skippedEmbeddings, "atomic"))
	}

	return nil
}

// readWALEntriesForTruncation does a best-effort read of WAL entries for truncation.
// Unlike ReadWALEntries, this function:
// 1. Ignores corrupted entries (they're being discarded anyway)
// 2. Only returns entries with sequence > afterSeq
// 3. Returns whatever entries could be salvaged
// This is safe because truncation only happens AFTER a snapshot is saved,
// so corrupted entries before the snapshot sequence are no longer needed.
func readWALEntriesForTruncation(walPath string, afterSeq uint64) ([]WALEntry, error) {
	file, err := os.Open(walPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Peek at first 4 bytes to detect format
	header := make([]byte, 4)
	n, err := file.Read(header)
	if err == io.EOF || n == 0 {
		return []WALEntry{}, nil
	}
	if err != nil {
		return nil, err
	}

	if _, err := file.Seek(0, 0); err != nil {
		return nil, err
	}

	magic := binary.LittleEndian.Uint32(header)
	if magic == walMagic {
		return readAtomicWALEntriesForTruncation(file, afterSeq)
	}
	return nil, fmt.Errorf("%w: unrecognized WAL header in %s", ErrWALUnsupportedVersion, walPath)
}

// readAtomicWALEntriesForTruncation reads atomic format entries, skipping corrupted ones.
func readAtomicWALEntriesForTruncation(file *os.File, afterSeq uint64) ([]WALEntry, error) {
	var entries []WALEntry
	var offset uint64
	visit := withWALPropertyEpochs(func(entry WALEntry) error {
		if entry.Sequence > afterSeq {
			entries = append(entries, entry)
		}
		return nil
	})
	reader := bufio.NewReader(file)
	headerBuf := make([]byte, 9)

	for {
		n, err := io.ReadFull(reader, headerBuf)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF || n != 9 {
			break // Partial header, stop reading
		}
		if err != nil {
			break
		}

		magic := binary.LittleEndian.Uint32(headerBuf[0:4])
		if magic != walMagic {
			break // Invalid magic, corrupted
		}

		version := headerBuf[4]
		if version != walFormatVersionCurrent {
			return entries, unsupportedWALVersion(version)
		}
		payloadLen := binary.LittleEndian.Uint32(headerBuf[5:9])
		if payloadLen > walMaxEntrySize {
			break // Invalid size
		}

		recordLen := int64(9 + payloadLen + 4)
		if version >= 2 {
			recordLen = alignUp(recordLen + 8)
		}
		offset += uint64(recordLen)
		payload := make([]byte, payloadLen)
		n, err = io.ReadFull(reader, payload)
		if err != nil || uint32(n) != payloadLen {
			break
		}

		crcBuf := make([]byte, 4)
		n, err = io.ReadFull(reader, crcBuf)
		if err != nil || n != 4 {
			break
		}

		outerValid := binary.LittleEndian.Uint32(crcBuf) == crc32Checksum(payload)
		trailerValid := true

		// Version 2+: Read trailer
		if version >= 2 {
			trailerBuf := make([]byte, 8)
			n, err = io.ReadFull(reader, trailerBuf)
			if err != nil || n != 8 {
				break
			}
			trailerValid = binary.LittleEndian.Uint64(trailerBuf) == walTrailer
			// Skip alignment padding
			rawRecordLen := int64(9 + payloadLen + 4 + 8)
			alignedRecordLen := alignUp(rawRecordLen)
			paddingLen := alignedRecordLen - rawRecordLen
			if paddingLen > 0 {
				paddingBuf := make([]byte, paddingLen)
				_, err = io.ReadFull(reader, paddingBuf)
				if err != nil {
					break
				}
			}
		}

		entry, err := WALEntry{}, error(nil)
		_, err = processWALEntryPayload(walPayloadDecode, nil, &entry, payload, version)
		if err != nil {
			_ = visit(WALEntry{propertyScopeReset: true})
			continue // Unknown record cannot carry dictionary authority across the gap
		}

		entry.recordEnd = offset
		if isWALPropertyEpoch(entry) && (!outerValid || !trailerValid || entry.Checksum != crc32Checksum(entry.Data)) {
			return entries, fmt.Errorf("%w: invalid retained property epoch", ErrWALCorrupted)
		}
		if !outerValid || !trailerValid {
			_ = visit(WALEntry{propertyScopeReset: true})
			entry.PropertyEpoch = nil
		}
		if err := visit(entry); err != nil {
			return entries, err
		}
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

// WALReadScope selects whether a WAL read covers one file or its complete directory.
type WALReadScope uint8

const (
	// WALReadFile reads only the named WAL file.
	WALReadFile WALReadScope = iota
	// WALReadDirectory reads the active WAL and retained segments in order.
	WALReadDirectory
)

// ReadWALEntriesAfter reads entries after a sequence from the selected WAL scope.
func ReadWALEntriesAfter(path string, afterSeq uint64, scope WALReadScope) ([]WALEntry, error) {
	var all []WALEntry
	var err error
	switch scope {
	case WALReadFile:
		all, err = ReadWALEntries(path, nil)
	case WALReadDirectory:
		all, err = ReadWALEntriesFromDir(path)
	default:
		return nil, fmt.Errorf("wal: invalid read scope %d", scope)
	}
	if err != nil {
		return nil, err
	}
	filtered := make([]WALEntry, 0, len(all))
	for _, entry := range all {
		if entry.Sequence > afterSeq {
			filtered = append(filtered, entry)
		}
	}
	return filtered, nil
}

// ReplayWALEntry applies a single WAL entry to the engine.
// Uses the database name from the entry if present, otherwise wraps with default database.
func ReplayWALEntry(engine Engine, entry WALEntry) error {
	if entry.Operation != OpCheckpoint && entry.Operation != OpTxBegin && entry.Operation != OpTxCommit && entry.Operation != OpTxAbort {
		// Retain engine state through provenance, import and physical apply.
		// A lifetime lease permits delegated CRUD to acquire its own write
		// barrier; holding that barrier here would risk recursive-read deadlock.
		if b, ok := UnwrapEngine(engine).(*BadgerEngine); ok {
			_, err := b.beginHelperTxn()
			if err != nil {
				return err
			}
			defer b.work.Leave()
		}
		if err := markRecoveryPropertyForeign(engine); err != nil {
			return err
		}
	}
	// Determine database name: use entry's database if present, otherwise infer from engine/config.
	dbName := entry.Database
	if dbName == "" {
		if namespaced, ok := engine.(*NamespacedEngine); ok {
			dbName = namespaced.Namespace()
		} else {
			globalConfig := config.LoadFromEnv()
			dbName = globalConfig.Database.DefaultDatabase
			if dbName == "" {
				dbName = "nornic" // Fallback
			}
		}
	}

	// Unwrap NamespacedEngine if present to avoid double-wrapping.
	baseEngine := engine
	if namespaced, ok := engine.(*NamespacedEngine); ok {
		baseEngine = namespaced.GetInnerEngine()
	}

	// Wrap base engine with NamespacedEngine for the entry's database.
	namespacedEngine := NewNamespacedEngine(baseEngine, dbName)

	switch entry.Operation {
	case OpEntityCommit:
		var data walEntityCommitData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return err
		}
		return ApplyAcceptedEntityTransaction(baseEngine, data.Payload)
	case OpCreateNode:
		var data WALNodeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal node: %w", err)
		}
		_, err := namespacedEngine.CreateNode(data.Node)
		return err

	case OpUpdateNode:
		var data WALNodeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal node: %w", err)
		}
		return namespacedEngine.UpdateNode(NodeUpdateRequest{After: data.Node})

	case OpDeleteNode:
		var data WALDeleteData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal delete: %w", err)
		}
		return namespacedEngine.DeleteNode(NodeID(data.ID))

	case OpCreateEdge:
		var data WALEdgeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal edge: %w", err)
		}
		return namespacedEngine.CreateEdge(data.Edge)

	case OpUpdateEdge:
		var data WALEdgeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal edge: %w", err)
		}
		return namespacedEngine.UpdateEdge(EdgeUpdateRequest{After: data.Edge})

	case OpDeleteEdge:
		var data WALDeleteData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal delete: %w", err)
		}
		return namespacedEngine.DeleteEdge(EdgeID(data.ID))

	case OpBulkNodes:
		var data WALBulkNodesData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk nodes: %w", err)
		}
		return namespacedEngine.BulkCreateNodes(data.Nodes)

	case OpBulkEdges:
		var data WALBulkEdgesData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk edges: %w", err)
		}
		return namespacedEngine.BulkCreateEdges(data.Edges)

	case OpBulkDeleteNodes:
		var data WALBulkDeleteNodesData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk delete nodes: %w", err)
		}
		ids := make([]NodeID, len(data.IDs))
		for i, id := range data.IDs {
			ids[i] = NodeID(id)
		}
		return namespacedEngine.BulkDeleteNodes(ids)

	case OpBulkDeleteEdges:
		var data WALBulkDeleteEdgesData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk delete edges: %w", err)
		}
		ids := make([]EdgeID, len(data.IDs))
		for i, id := range data.IDs {
			ids[i] = EdgeID(id)
		}
		return namespacedEngine.BulkDeleteEdges(ids)

	case OpBufferedCommit:
		// A write-behind commit's full payload. Replay it through the same
		// commit application path the flusher uses, so MVCC heads, ID
		// counters, indexes and caches all land identically. Only
		// BadgerEngine destinations support this; anything else skips.
		data, err := decodeBufferedWALRecord(entry)
		if err != nil {
			return err
		}
		b, ok := baseEngine.(*BadgerEngine)
		if !ok || b == nil {
			return fmt.Errorf("wal: buffered commit replay requires a BadgerEngine destination, got %T", baseEngine)
		}
		if len(data.Ops) == 0 && len(data.Writes) == 0 && len(data.Deletes) == 0 {
			return nil
		}
		c, err := b.importBufferedWALRecord(&data, walForeignImport)
		if err != nil {
			return err
		}
		return b.applyBufferedCommits([]bufferedCommit{c})

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
	// Determine database name: use entry's database if present, otherwise infer from engine/config.
	dbName := entry.Database
	if dbName == "" {
		if namespaced, ok := engine.(*NamespacedEngine); ok {
			dbName = namespaced.Namespace()
		} else {
			globalConfig := config.LoadFromEnv()
			dbName = globalConfig.Database.DefaultDatabase
			if dbName == "" {
				dbName = "nornic" // Fallback
			}
		}
	}

	// Unwrap NamespacedEngine if present to avoid double-wrapping.
	baseEngine := engine
	if namespaced, ok := engine.(*NamespacedEngine); ok {
		baseEngine = namespaced.GetInnerEngine()
	}

	// Wrap base engine with NamespacedEngine for the entry's database.
	namespacedEngine := NewNamespacedEngine(baseEngine, dbName)

	switch entry.Operation {
	case OpCreateNode:
		// Undo create = delete
		var data WALNodeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal node for undo: %w", err)
		}
		if data.Node == nil {
			return ErrNoUndoData
		}
		return namespacedEngine.DeleteNode(data.Node.ID)

	case OpUpdateNode:
		// Undo update = restore old node
		var data WALNodeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal node for undo: %w", err)
		}
		if data.OldNode == nil {
			return ErrNoUndoData
		}
		return namespacedEngine.UpdateNode(NodeUpdateRequest{After: data.OldNode})

	case OpDeleteNode:
		// Undo delete = recreate old node
		var data WALDeleteData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal delete for undo: %w", err)
		}
		if data.OldNode == nil {
			return ErrNoUndoData
		}
		_, err := namespacedEngine.CreateNode(data.OldNode)
		return err

	case OpCreateEdge:
		// Undo create = delete
		var data WALEdgeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal edge for undo: %w", err)
		}
		if data.Edge == nil {
			return ErrNoUndoData
		}
		return namespacedEngine.DeleteEdge(data.Edge.ID)

	case OpUpdateEdge:
		// Undo update = restore old edge
		var data WALEdgeData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal edge for undo: %w", err)
		}
		if data.OldEdge == nil {
			return ErrNoUndoData
		}
		return namespacedEngine.UpdateEdge(EdgeUpdateRequest{After: data.OldEdge})

	case OpDeleteEdge:
		// Undo delete = recreate old edge
		var data WALDeleteData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal delete for undo: %w", err)
		}
		if data.OldEdge == nil {
			return ErrNoUndoData
		}
		return namespacedEngine.CreateEdge(data.OldEdge)

	case OpBulkNodes:
		// Undo bulk create = delete all
		var data WALBulkNodesData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
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
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
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
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk delete nodes for undo: %w", err)
		}
		if len(data.OldNodes) == 0 {
			return ErrNoUndoData
		}
		return namespacedEngine.BulkCreateNodes(data.OldNodes)

	case OpBulkDeleteEdges:
		// Undo bulk delete = recreate all
		var data WALBulkDeleteEdgesData
		if err := decodeRecoveryPayload(entry.Data, &data); err != nil {
			return fmt.Errorf("wal: failed to unmarshal bulk delete edges for undo: %w", err)
		}
		if len(data.OldEdges) == 0 {
			return ErrNoUndoData
		}
		return namespacedEngine.BulkCreateEdges(data.OldEdges)

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
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpCreateEdge, OpUpdateEdge:
		var data WALEdgeData
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpDeleteNode, OpDeleteEdge:
		var data WALDeleteData
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBulkNodes:
		var data WALBulkNodesData
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBulkEdges:
		var data WALBulkEdgesData
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBulkDeleteNodes:
		var data WALBulkDeleteNodesData
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBulkDeleteEdges:
		var data WALBulkDeleteEdgesData
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpTxBegin, OpTxCommit, OpTxAbort:
		var data WALTxData
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			txID = data.TxID
		}
	case OpBufferedCommit:
		if data, err := decodeBufferedWALRecord(entry); err == nil {
			txID = data.TxID
		}
	case OpEntityCommit:
		var data walEntityCommitData
		if decodeRecoveryPayload(entry.Data, &data) == nil {
			if envelope, _, err := DecodeEntityTransaction(data.Payload); err == nil {
				txID = envelope.LogicalTxID
			}
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
	if err := ValidateWALFormat(walDir, WALValidateCurrent); err != nil {
		return nil, nil, fmt.Errorf("wal: failed to read WAL: %w", err)
	}
	store := newRecoveredStore()
	defer store.close()
	engine := store.engine
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
			// Get database name from config (WAL entries have unprefixed IDs)
			globalConfig := config.LoadFromEnv()
			dbName := globalConfig.Database.DefaultDatabase
			if dbName == "" {
				dbName = "nornic" // Fallback to "nornic" if not configured
			}
			// Wrap engine with NamespacedEngine for snapshot restore (snapshot nodes have unprefixed IDs)
			namespacedEngine := NewNamespacedEngine(engine, dbName)
			if err := namespacedEngine.BulkCreateNodes(snapshot.Nodes); err != nil {
				return nil, result, fmt.Errorf("wal: failed to restore nodes: %w", err)
			}
			if err := namespacedEngine.BulkCreateEdges(snapshot.Edges); err != nil {
				return nil, result, fmt.Errorf("wal: failed to restore edges: %w", err)
			}
		}
	}

	// Read all WAL entries
	entries, err := ReadWALEntriesFromDir(walDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, result, fmt.Errorf("wal: failed to read WAL: %w", err)
	}

	// Phase 1: Categorize entries by transaction
	nonTxEntries := []WALEntry{}
	for _, entry := range entries {
		if entry.Sequence <= result.SnapshotSeq {
			continue
		}
		// These are already accepted, self-contained commits. Their IDs are
		// useful for inspection but do not make them children of a matching
		// begin/commit marker in the transaction recovery stream.
		if entry.Operation == OpBufferedCommit || entry.Operation == OpEntityCommit {
			nonTxEntries = append(nonTxEntries, entry)
			continue
		}

		txID := GetEntryTxID(entry)

		switch entry.Operation {
		case OpTxBegin:
			var data WALTxData
			decodeRecoveryPayload(entry.Data, &data)
			result.Transactions[data.TxID] = &TransactionState{
				TxID:    data.TxID,
				Entries: []WALEntry{},
				Started: true,
			}

		case OpTxCommit:
			var data WALTxData
			decodeRecoveryPayload(entry.Data, &data)
			if tx, ok := result.Transactions[data.TxID]; ok {
				tx.Done = true
			}

		case OpTxAbort:
			var data WALTxData
			decodeRecoveryPayload(entry.Data, &data)
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
	// Each entry will use its own database name (stored in entry.Database)

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

	// Roll back incomplete transactions.
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

	return store.release(), result, nil
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
//
// Uses a discard *slog.Logger for diagnostics. Callers wanting structured
// recovery visibility should use RecoverFromWALWithLogger.
func RecoverFromWAL(walDir, snapshotPath string) (*MemoryEngine, error) {
	return RecoverFromWALWithLogger(walDir, snapshotPath, discardWALSlog())
}

// RecoverFromWALWithLogger is the slog-aware variant of RecoverFromWAL.
// D-07: pass a child logger tagged subsystem=wal_recovery so completion-
// with-errors records land in the structured stream. Operator-actionable
// fields: failed, errors, summary; per-error attributes seq, operation, error.
func RecoverFromWALWithLogger(walDir, snapshotPath string, logger *slog.Logger) (*MemoryEngine, error) {
	if logger == nil {
		logger = discardWALSlog()
	}
	engine, result, err := RecoverFromWALWithResult(walDir, snapshotPath)
	if err != nil {
		return nil, err
	}

	// D-07 single record summarising recovery completion-with-errors. The
	// per-error detail block below is debug-level so production volume is
	// bounded by the (rare) failure rate but operators can grep for
	// `recovery_error` when triaging.
	if result.Failed > 0 {
		recoveryLog := logger.With("subsystem", "wal_recovery")
		recoveryLog.Warn("wal recovery completed with errors",
			"failed", result.Failed,
			"summary", result.Summary(),
		)
		for _, e := range result.Errors {
			recoveryLog.Debug("wal recovery error",
				"seq", e.Sequence,
				"operation", e.Operation,
			)
		}
	}

	return engine, nil
}

// RecoverFromWALWithResult recovers database state and returns detailed results.
// Use this for programmatic access to replay statistics and errors.
func RecoverFromWALWithResult(walDir, snapshotPath string) (*MemoryEngine, ReplayResult, error) {
	if err := ValidateWALFormat(walDir, WALValidateCurrent); err != nil {
		return nil, ReplayResult{}, fmt.Errorf("wal: failed to read WAL: %w", err)
	}
	store := newRecoveredStore()
	defer store.close()
	engine := store.engine
	result := ReplayResult{}

	// Load snapshot if available
	var snapshotSeq uint64
	if snapshotPath != "" {
		streaming, detectErr := IsStreamingSnapshot(snapshotPath)
		if detectErr != nil && !os.IsNotExist(detectErr) {
			return nil, result, fmt.Errorf("wal: failed to inspect snapshot: %w", detectErr)
		}
		if streaming {
			file, err := os.Open(snapshotPath)
			if err != nil {
				return nil, result, fmt.Errorf("wal: failed to open snapshot: %w", err)
			}
			metadata, readErr := readSnapshot(file, engineSnapshotVisitor{engine: engine}, nil, materializedSnapshotRecordSize)
			closeErr := file.Close()
			if readErr != nil {
				return nil, result, fmt.Errorf("wal: failed to load snapshot: %w", readErr)
			}
			if closeErr != nil {
				return nil, result, fmt.Errorf("wal: failed to close snapshot: %w", closeErr)
			}
			snapshotSeq = metadata.Sequence
		} else {
			snapshot, err := LoadSnapshot(snapshotPath)
			if err != nil {
				if !os.IsNotExist(err) {
					return nil, result, fmt.Errorf("wal: failed to load snapshot: %w", err)
				}
				// No snapshot, start fresh
			} else {
				// Apply snapshot
				snapshotSeq = snapshot.Sequence

				// Determine database name from snapshot contents or config.
				globalConfig := config.LoadFromEnv()
				dbName := globalConfig.Database.DefaultDatabase
				if dbName == "" {
					dbName = "nornic" // Fallback to "nornic" if not configured
				}

				if len(snapshot.Nodes) > 0 {
					if parsedDB, _, ok := ParseDatabasePrefix(string(snapshot.Nodes[0].ID)); ok {
						dbName = parsedDB
					}
				} else if len(snapshot.Edges) > 0 {
					if parsedDB, _, ok := ParseDatabasePrefix(string(snapshot.Edges[0].ID)); ok {
						dbName = parsedDB
					}
				}

				// Unprefix snapshot data for the selected database before restoring via NamespacedEngine.
				for _, node := range snapshot.Nodes {
					node.ID = NodeID(StripDatabasePrefix(dbName, string(node.ID)))
				}
				for _, edge := range snapshot.Edges {
					edge.ID = EdgeID(StripDatabasePrefix(dbName, string(edge.ID)))
					edge.StartNode = NodeID(StripDatabasePrefix(dbName, string(edge.StartNode)))
					edge.EndNode = NodeID(StripDatabasePrefix(dbName, string(edge.EndNode)))
				}

				namespacedEngine := NewNamespacedEngine(engine, dbName)
				if err := namespacedEngine.BulkCreateNodes(snapshot.Nodes); err != nil {
					return nil, result, fmt.Errorf("wal: failed to restore nodes: %w", err)
				}
				if err := namespacedEngine.BulkCreateEdges(snapshot.Edges); err != nil {
					return nil, result, fmt.Errorf("wal: failed to restore edges: %w", err)
				}
			}
		}
	}

	// Replay WAL entries after snapshot
	activePath := walActivePath(walDir)
	manifest, err := accessWALManifest(walDir, walReadManifest, nil)
	if err != nil {
		return nil, result, fmt.Errorf("wal: failed to load recovery manifest: %w", err)
	}
	if _, statErr := os.Stat(activePath); os.IsNotExist(statErr) && len(manifest.Segments) == 0 {
		return store.release(), result, nil // No WAL to replay, return engine as-is
	}

	// Ensure we don't have a torn tail record before reading for recovery.
	// This is critical when recovery is invoked because the main server never started,
	// so NewWAL() (which also repairs tails) was never called.
	if _, _, repairErr := repairWALTailIfNeeded(activePath, defaultWALLogger{}); repairErr != nil {
		return nil, result, fmt.Errorf("wal: failed to inspect recovery log: %w", repairErr)
	}

	entries, err := ReadWALEntriesAfter(walDir, snapshotSeq, WALReadDirectory)
	if err != nil {
		return nil, result, fmt.Errorf("wal: failed to read WAL: %w", err)
	}

	// Replay entries with proper error tracking
	// Each entry will use its own database name (stored in entry.Database)
	result.Errors = make([]ReplayError, 0)
	for _, entry := range entries {
		result.Record(entry, ReplayWALEntry(engine, entry))
	}

	return store.release(), result, nil
}

type engineSnapshotVisitor struct {
	engine Engine
}

func (v engineSnapshotVisitor) VisitNode(node *Node) error {
	_, err := v.engine.CreateNode(qualifyRecoverySnapshotNode(node))
	if err != nil {
		return fmt.Errorf("wal: failed to restore nodes: %w", err)
	}
	return nil
}

func (v engineSnapshotVisitor) VisitEdge(edge *Edge) error {
	if err := v.engine.CreateEdge(qualifyRecoverySnapshotEdge(edge)); err != nil {
		return fmt.Errorf("wal: failed to restore edges: %w", err)
	}
	return nil
}

// Record accumulates the actual outcome of ReplayWALEntry. Checkpoints and
// duplicate creates are expected skips; every other error retains its original
// sequence, operation and cause. For example, a caller replaying a slice uses:
//
//	for _, entry := range entries {
//		result.Record(entry, ReplayWALEntry(engine, entry))
//	}
func (r *ReplayResult) Record(entry WALEntry, err error) {
	if entry.Operation == OpCheckpoint || errors.Is(err, ErrAlreadyExists) {
		r.Skipped++
	} else if err == nil {
		r.Applied++
	} else {
		r.Failed++
		r.Errors = append(r.Errors, ReplayError{Sequence: entry.Sequence, Operation: entry.Operation, Error: err})
	}
}
