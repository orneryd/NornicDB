// Package replication provides distributed replication for NornicDB.
package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// StorageAdapter bridges the replication.Storage interface to storage.Engine.
// It translates replication commands into storage operations and maintains WAL state.
type StorageAdapter struct {
	engine storage.Engine

	// Persistent WAL for replication commands
	wal         *storage.WAL
	walDir      string
	walMu       sync.RWMutex // Protects wal and walPosition
	walPosition atomic.Uint64
	// Author: NOR-288 lane (Codex), 2026-10-27. Nil in production; tests
	// fail acknowledgement after a real synchronous append and durable Sync.
	walAppendAckHook func() error // guarded by walMu

	// In-memory WAL for fast streaming (avoids re-reading wal.log continuously).
	memWALMu sync.RWMutex
	memWAL   []*WALEntry // sorted by Position asc

	// Async WAL writing for performance
	walQueue   chan *walWriteRequest
	walBatch   []*walWriteRequest
	walBatchMu sync.Mutex
	// work is the adapter's one owner: the async WAL writer is its accepted
	// work, its cancellation is the writer's stop, and Close is its one stop.
	work lifecycle.WorkLifetime
}

// walWriteRequest represents a pending WAL write.
type walWriteRequest struct {
	record replicationWALRecord
	posCh  chan uint64 // Barrier completion position
	errCh  chan error  // Channel to receive any error

	// barrier marks a sentinel request. flushWALBatch does not append a
	// barrier to the WAL but does signal posCh once the surrounding batch
	// completes. Used by FlushWAL to deterministically wait for every
	// previously-queued request to drain without sleeping.
	barrier bool
}

type replicationWALRecord struct {
	// Timestamp when the entry was created.
	Timestamp int64 `json:"ts"`

	// Command is the replicated command.
	Command *Command `json:"cmd"`
}

// NewStorageAdapter creates a new storage adapter wrapping the given engine.
// The WAL directory defaults to "data/replication/wal" if not specified.
func NewStorageAdapter(engine storage.Engine) (*StorageAdapter, error) {
	return NewStorageAdapterWithWAL(engine, "")
}

// NewStorageAdapterWithWAL creates a new storage adapter with a custom WAL directory.
// If walDir is empty, defaults to "data/replication/wal".
func NewStorageAdapterWithWAL(engine storage.Engine, walDir string) (*StorageAdapter, error) {
	if walDir == "" {
		walDir = "data/replication/wal"
	}

	// Create WAL directory if needed
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, localizedError(localization.ReplicationStorageWALDirectoryCreateFailed(err), err)
	}

	// Create persistent WAL
	walConfig := storage.DefaultWALConfig()
	walConfig.Dir = walDir
	walConfig.SyncMode = "batch" // Batch sync for performance
	walConfig.BatchSyncInterval = 100 * time.Millisecond

	wal, err := storage.NewWAL(walDir, walConfig)
	if err != nil {
		return nil, localizedError(localization.ReplicationStorageWALCreateFailed(err), err)
	}

	adapter := &StorageAdapter{
		engine:   engine,
		wal:      wal,
		walDir:   walDir,
		walQueue: make(chan *walWriteRequest, 1000), // Buffered channel for async WAL writes
		walBatch: make([]*walWriteRequest, 0, 100),  // Pre-allocated batch buffer
	}

	// Load existing WAL position
	_ = adapter.loadWALPosition()

	// Start async WAL writer
	if adapter.work.Enter() {
		go adapter.walWriterLoop()
	}

	return adapter, nil
}

// loadWALPosition loads the last WAL position from persistent storage.
func (a *StorageAdapter) loadWALPosition() error {
	// storage.WAL already recovers its own sequence on startup.
	// Use that as the authoritative replication WAL position.
	if a.wal != nil {
		a.walPosition.Store(a.wal.Sequence())
	}
	return nil
}

// ApplyCommand applies cmd; ctx is the caller's work or request context, which
// an accepted command's native submission observes.
func (a *StorageAdapter) ApplyCommand(ctx context.Context, cmd *Command) error {
	if cmd == nil {
		return localizedError(localization.ReplicationStorageCommandRequired(), nil)
	}

	if err := retiredCommandError(cmd.Type); err != nil {
		return err
	}

	// Observed client requests are admission input, never accepted state-machine
	// records. Only the leader preparer may normalize them into a validated
	// CmdCommitTransaction before any replication/WAL publication.
	if cmd.Type == CmdUpdateNodeRequest || cmd.Type == CmdUpdateEdgeRequest {
		return storage.ErrInvalidData
	}

	// Decode once before publishing to the replication WAL. An unreadable
	// command must never be replayed to a peer after its caller saw rejection.
	var envelope storage.EntityTransactionEnvelope
	var operations []storage.Operation
	var prefix string
	var err error
	switch cmd.Type {
	case CmdCommitTransaction:
		if !cmd.accepted.AppliesTo(a.engine) {
			envelope, operations, err = storage.DecodeEntityTransaction(cmd.Data)
		}
	case CmdCreateNode, CmdUpdateNode, CmdDeleteNode, CmdCreateEdge, CmdUpdateEdge,
		CmdDeleteEdge, CmdBulkCreateNodes, CmdBulkCreateEdges, CmdBulkDeleteNodes, CmdBulkDeleteEdges:
		operations, _, err = decodeEntityCommandOperations(cmd.Type, cmd.Data)
	case CmdDeleteByPrefix:
		prefix, err = decodeDeleteByPrefix(cmd.Data)
	default:
		return localizedError(localization.ReplicationStorageUnknownCommandType(uint8(cmd.Type)), nil)
	}
	if err != nil {
		return err
	}

	// Record in persistent WAL (write-ahead logging)
	record := replicationWALRecord{
		Timestamp: cmd.Timestamp.UnixNano(),
		Command:   cmd,
	}

	// Queue async WAL write (non-blocking)
	req := &walWriteRequest{
		record: record,
		errCh:  make(chan error, 1),
	}

	// Try to queue async write
	select {
	case a.walQueue <- req:
		// Successfully queued, continue
	default:
		// Channel full - fall back to synchronous write to avoid blocking
		// This should be rare with a 1000-item buffer
		a.walMu.Lock()
		err := a.wal.Append(storage.OperationType("replication_command"), record)
		if err == nil && a.walAppendAckHook != nil {
			err = a.walAppendAckHook()
		}
		if err != nil {
			a.walMu.Unlock()
			return localizedError(localization.ReplicationStorageWALAppendFailed(err), err)
		}
		pos := a.wal.Sequence()
		a.walPosition.Store(pos)
		a.walMu.Unlock()
		a.appendToMemWAL(pos, record, cmd)
	}

	// Execute the command immediately (don't wait for WAL write)
	// This allows storage operations to proceed in parallel with WAL I/O
	var execErr error
	switch cmd.Type {
	case CmdCommitTransaction:
		if cmd.accepted.AppliesTo(a.engine) {
			execErr = cmd.accepted.ApplyContext(ctx, cmd.Data)
			break
		}
		receipt, err := storage.SubmitAcceptedEntityTransaction(ctx, a.engine, envelope, operations, nil)
		if err != nil {
			execErr = err
		} else {
			// Author: NOR-288 lane (Codex), 2026-10-27. Foreign command
			// callers observe inline search enqueue and broker publication at
			// return. Join the existing full receipt, including native listeners.
			// Local accepted handles above have their own outer full join.
			// The wait outlives this request: it takes the applying engine's
			// lifetime, whose Close ends it as recovery-pending.
			if workCtx, ctxErr := storage.AcceptedWorkContext(a.engine); ctxErr != nil {
				execErr = ctxErr
			} else {
				execErr = receipt.WaitOutcome(workCtx).Err
			}
		}
	case CmdCreateNode, CmdUpdateNode, CmdDeleteNode, CmdCreateEdge, CmdUpdateEdge,
		CmdDeleteEdge, CmdBulkCreateNodes, CmdBulkCreateEdges, CmdBulkDeleteNodes, CmdBulkDeleteEdges:
		execErr = applyDirectEntityCommand(a.engine, cmd.Type, operations)
	case CmdDeleteByPrefix:
		_, _, execErr = a.engine.DeleteByPrefix(prefix)
	}

	// The WAL writer owns queued position and memory publication exactly once.
	// Preserve the existing best-effort asynchronous error observation here.
	select {
	case err := <-req.errCh:
		if err != nil {
			// Log error but don't fail the operation (WAL is for durability, not correctness)
			// The command was already applied to storage
			logReplicationPrintf(context.Background(), slog.LevelError, "[WAL] Async write error (non-fatal): %v", err)
		}
	default:
		// The writer will finish publication or report its asynchronous error.
	}

	return execErr
}

// appendToMemWAL appends an entry to the in-memory WAL for fast streaming.
func (a *StorageAdapter) appendToMemWAL(pos uint64, record replicationWALRecord, cmd *Command) {
	a.memWALMu.Lock()
	a.memWAL = append(a.memWAL, &WALEntry{
		Position:  pos,
		Timestamp: record.Timestamp,
		Command:   cmd,
	})
	a.memWALMu.Unlock()
}

// walWriterLoop processes WAL writes asynchronously in batches.
func (a *StorageAdapter) walWriterLoop() {
	defer a.work.Leave()

	ticker := time.NewTicker(10 * time.Millisecond) // Batch interval
	defer ticker.Stop()

	for {
		select {
		case <-a.work.Context().Done():
			// Flush remaining writes
			a.flushWALBatch()
			return
		case req := <-a.walQueue:
			// Add to batch
			a.walBatchMu.Lock()
			a.walBatch = append(a.walBatch, req)
			batchSize := len(a.walBatch)
			a.walBatchMu.Unlock()

			// Flush if batch is large enough (100 items)
			if batchSize >= 100 {
				a.flushWALBatch()
			}
		case <-ticker.C:
			// Periodic flush
			a.flushWALBatch()
		}
	}
}

// flushWALBatch writes all pending WAL entries.
// Uses individual appends but processes them in a batch to reduce lock contention.
func (a *StorageAdapter) flushWALBatch() {
	a.walBatchMu.Lock()
	if len(a.walBatch) == 0 {
		a.walBatchMu.Unlock()
		return
	}
	batch := a.walBatch
	// The batch belongs to this worker through replay and barrier delivery.
	defer clear(batch)
	a.walBatch = a.walBatch[:0] // Clear batch
	a.walBatchMu.Unlock()

	// Write each entry to WAL in a single lock to reduce contention
	// The WAL's internal buffering will batch the actual file writes
	a.walMu.Lock()
	positions := make([]uint64, 0, len(batch))
	for _, req := range batch {
		if req.barrier {
			// Sentinel for FlushWAL — no WAL append. The post position
			// signal happens below so callers are blocked until every
			// real append in this batch has completed.
			positions = append(positions, 0)
			continue
		}
		if err := a.wal.Append(storage.OperationType("replication_command"), req.record); err != nil {
			// Send error to waiting request
			select {
			case req.errCh <- localizedError(localization.ReplicationStorageWALAppendFailed(err), err):
			default:
			}
			positions = append(positions, 0) // Placeholder for failed entry
			continue
		}
		// Get position after append (sequence is incremented atomically by WAL)
		pos := a.wal.Sequence()
		positions = append(positions, pos)

	}
	// Update global position
	if len(batch) > 0 {
		a.walPosition.Store(a.wal.Sequence())
	}
	currentPos := a.wal.Sequence()
	a.walMu.Unlock()

	// Update in-memory WAL outside the lock to avoid deadlock
	for i, req := range batch {
		if positions[i] > 0 {
			a.appendToMemWAL(positions[i], req.record, req.record.Command)
		}
	}

	// Release any barrier waiters now that every real entry in the
	// batch has been appended and walPosition has been updated.
	for _, req := range batch {
		if !req.barrier {
			continue
		}
		select {
		case req.posCh <- currentPos:
		default:
		}
	}
}

func decodeDeleteByPrefix(data []byte) (string, error) {
	var req struct {
		Prefix string
	}
	if err := storage.DecodeCommandPayload(data, &req); err != nil {
		return "", localizedError(localization.ReplicationStorageDecodeDeletePrefixFailed(err), err)
	}
	if req.Prefix == "" {
		return "", localizedError(localization.ReplicationStoragePrefixRequired(), nil)
	}
	return req.Prefix, nil
}

func (a *StorageAdapter) FlushWAL() error {
	// Build a sentinel write request. barrier=true tells flushWALBatch
	// to skip the WAL append for this entry but still ack posCh after
	// the surrounding batch completes — so the writer goroutine drives
	// the synchronization without us touching its channels directly.
	barrier := &walWriteRequest{
		posCh:   make(chan uint64, 1),
		errCh:   make(chan error, 1),
		barrier: true,
	}

	// Push the barrier behind everything already queued.
	select {
	case a.walQueue <- barrier:
	case <-a.work.Context().Done():
		// Writer is shutting down; fall through to a best-effort sync.
		a.walMu.Lock()
		defer a.walMu.Unlock()
		if a.wal != nil {
			return a.wal.Sync()
		}
		return nil
	}

	// Wait for the writer goroutine to flush the batch containing the
	// barrier. posCh is signaled inside flushWALBatch after the WAL
	// append completes, so by the time we read from it every prior
	// request is also persisted.
	select {
	case <-barrier.posCh:
	case err := <-barrier.errCh:
		if err != nil {
			return localizedError(localization.ReplicationStorageFlushWALFailed(err), err)
		}
	case <-a.work.Context().Done():
		// Adapter closing; drop barrier wait.
	}

	// Sync the WAL to ensure all writes are on disk.
	a.walMu.Lock()
	defer a.walMu.Unlock()
	if a.wal != nil {
		return a.wal.Sync()
	}
	return nil
}

// stopWALWriterForTest halts the async WAL writer goroutine without closing
// the WAL itself. Tests that want to mutate walQueue (to force the
// "channel full" sync-fallback branch in ApplyCommand) call this first so
// the swap doesn't race with walWriterLoop's channel read. Idempotent.
//
// Production callers should use Close instead — this helper exists only
// for tests that need to swap the queue mid-life.
func (a *StorageAdapter) stopWALWriterForTest() {
	<-a.work.BeginStop(nil)
}

// Close releases replication resources (WAL file handles/background goroutines).
func (a *StorageAdapter) Close() error {
	// The owner's stop: the writer flushes and returns, then the WAL closes
	// once. Every caller joins the same stop and outcome.
	return a.work.Shutdown(context.Background(), a.closeWAL)
}

// closeWAL is the adapter owner's dispose, after the WAL writer returned.
func (a *StorageAdapter) closeWAL() error {
	a.walMu.Lock()
	defer a.walMu.Unlock()
	if a.wal != nil {
		err := a.wal.Close()
		a.wal = nil
		return err
	}
	return nil
}

// GetWALPosition returns the current WAL position.
func (a *StorageAdapter) GetWALPosition() (uint64, error) {
	return a.walPosition.Load(), nil
}

// GetWALEntries returns WAL entries after the given position. A zero limit
// returns an empty window; a negative limit is invalid.
func (a *StorageAdapter) GetWALEntries(fromPosition uint64, maxEntries int) ([]*WALEntry, error) {
	if maxEntries < 0 {
		return nil, fmt.Errorf("replication WAL entry limit must not be negative: %d", maxEntries)
	}
	if maxEntries == 0 {
		return []*WALEntry{}, nil
	}
	// Keep the shared backing array locked until the result window is copied:
	// pruning compacts and clears it in place.
	a.memWALMu.RLock()
	mem := a.memWAL

	if len(mem) > 0 && fromPosition >= mem[0].Position {
		// Binary search first entry with Position > fromPosition
		lo, hi := 0, len(mem)
		for lo < hi {
			mid := (lo + hi) / 2
			if mem[mid].Position <= fromPosition {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo >= len(mem) {
			a.memWALMu.RUnlock()
			return []*WALEntry{}, nil
		}
		end := len(mem)
		if maxEntries < len(mem)-lo {
			end = lo + maxEntries
		}
		entries := make([]*WALEntry, end-lo)
		copy(entries, mem[lo:end])
		a.memWALMu.RUnlock()
		return entries, nil
	}
	a.memWALMu.RUnlock()

	// Read from persistent WAL
	a.walMu.RLock()
	defer a.walMu.RUnlock()

	storageEntries, err := storage.ReadWALEntriesFromDir(a.walDir)
	if err != nil {
		// Handle missing WAL file gracefully (may not exist yet)
		if os.IsNotExist(err) {
			return []*WALEntry{}, nil
		}
		// Check if error message indicates file not found (storage.ReadWALEntries may wrap the error)
		errStr := err.Error()
		if strings.Contains(errStr, "no such file") || strings.Contains(errStr, "not found") {
			return []*WALEntry{}, nil
		}
		return nil, localizedError(localization.ReplicationStorageReadWALEntriesFailed(err), err)
	}

	var entries []*WALEntry
	for _, storageEntry := range storageEntries {
		// Only process replication_command entries
		if storageEntry.Operation != storage.OperationType("replication_command") {
			continue
		}

		pos := storageEntry.Sequence
		if pos <= fromPosition {
			continue
		}
		// Never return a later position after dropping an unreadable command:
		// the receiver could acknowledge a window with a missing write.
		var rec replicationWALRecord
		if err := json.Unmarshal(storageEntry.Data, &rec); err != nil {
			err = fmt.Errorf("replication WAL entry %d: %w", pos, err)
			return nil, localizedError(localization.ReplicationStorageReadWALEntriesFailed(err), err)
		}
		if rec.Command == nil {
			err := fmt.Errorf("replication WAL entry %d has no command", pos)
			return nil, localizedError(localization.ReplicationStorageReadWALEntriesFailed(err), err)
		}
		entries = append(entries, &WALEntry{
			Position:  pos,
			Timestamp: rec.Timestamp,
			Command:   rec.Command,
		})
		if len(entries) >= maxEntries {
			break
		}
	}

	return entries, nil
}

// PruneWALEntries drops in-memory WAL entries up to (and including) uptoPosition.
// This keeps memory bounded while streaming and does not affect the persistent WAL.
func (a *StorageAdapter) PruneWALEntries(uptoPosition uint64) {
	a.memWALMu.Lock()
	defer a.memWALMu.Unlock()
	if len(a.memWAL) == 0 {
		return
	}
	// Find first entry with Position > uptoPosition.
	lo, hi := 0, len(a.memWAL)
	for lo < hi {
		mid := (lo + hi) / 2
		if a.memWAL[mid].Position <= uptoPosition {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return
	}
	// Drop prefix in-place to avoid allocating a new backing array.
	remaining := copy(a.memWAL, a.memWAL[lo:])
	clear(a.memWAL[remaining:])
	a.memWAL = a.memWAL[:remaining]
}

// WriteSnapshot writes a full snapshot to the given writer.
func (a *StorageAdapter) WriteSnapshot(w SnapshotWriter) error {
	// Every stored node and edge: the ones the visibility layer hides from
	// reads are replicated data too.
	nodes, err := storage.CollectStoredNodes(context.Background(), a.engine)
	if err != nil {
		return localizedError(localization.ReplicationStorageGetAllNodesFailed(err), err)
	}

	edges, err := storage.CollectStoredEdges(context.Background(), a.engine)
	if err != nil {
		return localizedError(localization.ReplicationStorageGetAllEdgesFailed(err), err)
	}

	snapshot := storage.ReplicationSnapshot{
		WALPosition: a.walPosition.Load(),
		Nodes:       nodes,
		Edges:       edges,
	}

	data, err := storage.EncodeCommandPayload(snapshot)
	if err != nil {
		return localizedError(localization.ReplicationStorageEncodeSnapshotFailed(err), err)
	}

	_, err = w.Write(data)
	return err
}

// RestoreSnapshot restores state from a snapshot.
func (a *StorageAdapter) RestoreSnapshot(r SnapshotReader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return localizedError(localization.ReplicationStorageReadSnapshotFailed(err), err)
	}

	var snapshot storage.ReplicationSnapshot

	if err := storage.DecodeCommandPayload(data, &snapshot); err != nil {
		return localizedError(localization.ReplicationStorageDecodeSnapshotFailed(err), err)
	}

	// Restore nodes
	for _, node := range snapshot.Nodes {
		if _, err := a.engine.CreateNode(node); err != nil {
			return localizedError(localization.ReplicationStorageRestoreNodeFailed(err), err)
		}
	}

	// Restore edges
	for _, edge := range snapshot.Edges {
		if err := a.engine.CreateEdge(edge); err != nil {
			return localizedError(localization.ReplicationStorageRestoreEdgeFailed(err), err)
		}
	}

	// Restore WAL position
	a.walPosition.Store(snapshot.WALPosition)

	return nil
}

// Engine returns the underlying storage engine.
func (a *StorageAdapter) Engine() storage.Engine {
	return a.engine
}

// Verify StorageAdapter implements Storage interface.
var _ Storage = (*StorageAdapter)(nil)
