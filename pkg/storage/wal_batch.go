// Package storage provides write-ahead logging for NornicDB durability.
package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

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
	entryBuf := walJSONBufPool.Get().(*bytes.Buffer)
	defer walJSONBufPool.Put(entryBuf)

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

		// Serialize entry into reusable buffer (reduces allocs vs json.Marshal).
		entryBytes, err := marshalJSONCompact(entryBuf, &entry)
		if err != nil {
			return fmt.Errorf("wal batch: failed to serialize entry seq %d: %w", seq, err)
		}

		alignedRecordLen, err := writeAtomicRecordV2(b.wal.writer, entryBytes)
		if err != nil {
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
