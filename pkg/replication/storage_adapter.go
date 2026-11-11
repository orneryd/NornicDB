// Package replication provides distributed replication for NornicDB.
package replication

import (
	"context"
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
	// calls seals WAL producer/barrier ingress before the writer stop. Close waits
	// owned queue/I/O admissions, never foreign native application/listeners. The worker
	// never takes this lock, so admitted barriers can finish before sealing.
	calls  sync.RWMutex
	sealed bool

	engine storage.Engine

	// Persistent WAL for replication commands
	wal         *storage.WAL
	walDir      string
	walMu       sync.RWMutex // Protects wal and walPosition
	walPosition atomic.Uint64
	// projectedIDs maps each retained accepted command identity (its database and
	// logical ID) to its journal position. It is rebuilt from the retained journal
	// and protected by walMu.
	projectedIDs      map[projectionIdentity]uint64
	projectionLoadErr error

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

// walCompletion is the sole writer's typed outcome for one queued request.
// position is the physical WAL sequence that request's own record received and
// is zero when the record was not published; err is the append failure that
// prevented publication.
type walCompletion struct {
	position uint64
	err      error
}

// walWriteRequest represents a pending WAL write.
type walWriteRequest struct {
	record   replicationWALRecord
	identity projectionIdentity // resolved before native application or queue transfer
	// done carries the writer's typed completion. Every request allocates one
	// buffered outcome channel, replacing the former record error channel and
	// barrier position/error channels; the writer sends exactly once.
	done chan walCompletion

	// barrier marks a sentinel request. flushWALBatch does not append a
	// barrier to the WAL but does signal done once the surrounding batch
	// completes. Used by FlushWAL to deterministically wait for every
	// previously-queued request to drain without sleeping.
	barrier bool
}

// complete delivers this request's single typed outcome without blocking the
// writer's batch loop.
func (req *walWriteRequest) complete(outcome walCompletion) {
	select {
	case req.done <- outcome:
	default:
	}
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
	// The journal is shipped to peers entry by entry, in batches the
	// transport carries as one message: it keeps every entry in one frame,
	// as before records could span frames (NOR-412).
	walConfig.SingleFrameRecords = true

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
	adapter.projectionLoadErr = adapter.loadWALPosition()

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
	entries, err := storage.ReadWALEntriesFromDir(a.work.Context(), a.walDir)
	if err != nil {
		return localizedError(localization.ReplicationStorageReadWALEntriesFailed(err), err)
	}
	a.projectedIDs = make(map[projectionIdentity]uint64)
	for _, entry := range entries {
		if entry.Operation != storage.OperationType("replication_command") {
			continue
		}
		record, err := decodeProjectionRecord(entry)
		if err != nil {
			return localizedError(localization.ReplicationStorageReadWALEntriesFailed(err), err)
		}
		var envelope storage.EntityTransactionEnvelope
		if record.Command.Type == CmdCommitTransaction {
			var err error
			envelope, _, err = storage.DecodeEntityTransaction(record.Command.Data)
			if err != nil {
				return localizedError(localization.ReplicationStorageReadWALEntriesFailed(err), err)
			}
		}
		identity := projectionCommandIdentity(record.Command, envelope)
		if identity.logicalID != "" {
			if _, exists := a.projectedIDs[identity]; !exists {
				a.projectedIDs[identity] = entry.Sequence
			}
		}
	}
	return nil
}

// ApplyCommand applies a replicated command and reports the publication of its
// own replication WAL record.
//
// waitCtx, publication and cmd are validated before any queue or native side
// effect. Valid intent is transferred to the bounded FIFO WAL writer before the
// native command is applied, and the native result stays the primary contract.
// waitCtx bounds only this caller's publication wait: it never retracts accepted
// intent, suppresses native application, removes a queued record or implies a
// rollback.
func (a *StorageAdapter) ApplyCommand(waitCtx context.Context, cmd *Command, publication WALPublicationMode) (CommandPublication, error) {
	if cmd == nil {
		return CommandPublication{}, localizedError(localization.ReplicationStorageCommandRequired(), nil)
	}
	if publication != WALQueued && publication != WALPublished {
		return CommandPublication{}, fmt.Errorf("replication: unknown WAL publication mode %d", publication)
	}

	if err := retiredCommandError(cmd.Type); err != nil {
		return CommandPublication{}, err
	}
	if a.projectionLoadErr != nil {
		return CommandPublication{}, a.projectionLoadErr
	}

	// Observed client requests are admission input, never accepted state-machine
	// records. Only the leader preparer may normalize them into a validated
	// CmdCommitTransaction before any replication/WAL publication.
	if cmd.Type == CmdUpdateNodeRequest || cmd.Type == CmdUpdateEdgeRequest {
		return CommandPublication{}, storage.ErrInvalidData
	}

	// Decode once before publishing to the replication WAL. An unreadable
	// command must never be replayed to a peer after its caller saw rejection.
	var envelope storage.EntityTransactionEnvelope
	var operations []storage.Operation
	var prefix string
	var err error
	switch cmd.Type {
	case CmdCommitTransaction:
		if cmd.accepted.AppliesTo(a.engine) {
			envelope.Namespace, envelope.LogicalTxID = cmd.accepted.Identity()
		} else {
			envelope, operations, err = storage.DecodeEntityTransaction(cmd.Data)
		}
	case CmdCreateNode, CmdUpdateNode, CmdDeleteNode, CmdCreateEdge, CmdUpdateEdge,
		CmdDeleteEdge, CmdBulkCreateNodes, CmdBulkCreateEdges, CmdBulkDeleteNodes, CmdBulkDeleteEdges:
		operations, _, err = decodeEntityCommandOperations(cmd.Type, cmd.Data)
	case CmdDeleteByPrefix:
		prefix, err = decodeDeleteByPrefix(cmd.Data)
	default:
		return CommandPublication{}, localizedError(localization.ReplicationStorageUnknownCommandType(uint8(cmd.Type)), nil)
	}
	if err != nil {
		return CommandPublication{}, err
	}

	// Record in persistent WAL (write-ahead logging)
	record := replicationWALRecord{
		Timestamp: cmd.Timestamp.UnixNano(),
		Command:   cmd,
	}

	// Transfer the replicated record to the bounded FIFO WAL writer.
	req := &walWriteRequest{
		record:   record,
		identity: projectionCommandIdentity(cmd, envelope),
		done:     make(chan walCompletion, 1),
	}

	if err := a.enqueueWAL(req); err != nil {
		return CommandPublication{}, err
	}

	// The replicated command is accepted. Native application and completion
	// are independent of the publication waiter and engine shutdown signal.
	completionCtx := context.Background()

	// Execute the command immediately (don't wait for WAL write)
	// This allows storage operations to proceed in parallel with WAL I/O
	var execErr error
	switch cmd.Type {
	case CmdCommitTransaction:
		if cmd.accepted.AppliesTo(a.engine) {
			execErr = cmd.accepted.ApplyContext(completionCtx, cmd.Data)
			break
		}
		receipt, err := storage.SubmitAcceptedEntityTransaction(completionCtx, a.engine, envelope, operations, nil)
		if err != nil {
			execErr = err
		} else {
			execErr = receipt.WaitPhysicalOutcome(completionCtx).Err
		}
	case CmdCreateNode, CmdUpdateNode, CmdDeleteNode, CmdCreateEdge, CmdUpdateEdge,
		CmdDeleteEdge, CmdBulkCreateNodes, CmdBulkCreateEdges, CmdBulkDeleteNodes, CmdBulkDeleteEdges:
		execErr = applyDirectEntityCommand(completionCtx, a.engine, cmd.Type, operations)
	case CmdDeleteByPrefix:
		_, _, execErr = a.engine.DeleteByPrefix(prefix)
	}

	if publication == WALQueued {
		// WALQueued keeps the nonwaiting asynchronous native-result contract:
		// the writer owns this record's publication and its error observation,
		// so only an outcome that already arrived is logged here.
		select {
		case outcome := <-req.done:
			if outcome.err != nil {
				// Log error but don't fail the operation (WAL is for durability, not correctness)
				// The command was already applied to storage
				logReplicationPrintf(context.Background(), slog.LevelError, "[WAL] Async write error (non-fatal): %v", outcome.err)
			}
		default:
			// The writer will finish publication or report its asynchronous error.
		}
		return CommandPublication{}, execErr
	}

	if execErr != nil {
		// The native application outcome is the primary contract and is
		// returned unchanged. The record already belongs to the writer, so its
		// publication, or the writer's retained append failure, still completes.
		return CommandPublication{}, execErr
	}

	outcome, waitErr := awaitWALCompletion(waitCtx, req.done)
	if waitErr != nil {
		// The wait expired; accepted intent and native application remain.
		return CommandPublication{}, waitErr
	}
	if outcome.err != nil {
		// Publication failed after native acceptance: report the publication
		// failure without implying rejected or rolled-back native work.
		return CommandPublication{}, outcome.err
	}
	if outcome.position == 0 {
		// A published record always has its own physical sequence.
		return CommandPublication{}, fmt.Errorf("replication: published record has no WAL position")
	}
	return CommandPublication{Position: outcome.position}, nil
}

// awaitWALCompletion waits for this request's own publication outcome under the
// caller's wait context. The context bounds the wait only.
func awaitWALCompletion(ctx context.Context, done <-chan walCompletion) (walCompletion, error) {
	select {
	case outcome := <-done:
		return outcome, nil
	case <-ctx.Done():
		// The writer can complete as the deadline expires; an outcome that already
		// arrived is the truthful result.
		select {
		case outcome := <-done:
			return outcome, nil
		default:
			return walCompletion{}, fmt.Errorf("replication: WAL publication wait: %w", ctx.Err())
		}
	}
}

// enqueueWAL transfers this replicated record to the one FIFO writer. The
// admission covers only queue ownership, never native storage or its listeners.
// Close waits these producers before stopping/draining the writer. Full queues
// exert backpressure; a later command cannot append ahead of their prefix.
func (a *StorageAdapter) enqueueWAL(req *walWriteRequest) error {
	a.calls.RLock()
	defer a.calls.RUnlock()
	if a.sealed {
		return ErrClosed
	}
	a.walQueue <- req
	return nil
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
			// Ingress is sealed and all admitted calls have returned. Drain the
			// finite accepted queue through the same physical append owner.
			a.flushWALBatch(true)
			return
		case req := <-a.walQueue:
			// Add to batch
			a.walBatchMu.Lock()
			a.walBatch = append(a.walBatch, req)
			batchSize := len(a.walBatch)
			a.walBatchMu.Unlock()

			// Flush if batch is large enough (100 items)
			if batchSize >= 100 {
				a.flushWALBatch(false)
			}
		case <-ticker.C:
			// Periodic flush
			a.flushWALBatch(false)
		}
	}
}

// flushWALBatch writes all pending WAL entries.
// Uses individual appends but processes them in a batch to reduce lock contention.
func (a *StorageAdapter) flushWALBatch(drain bool) {
	a.walBatchMu.Lock()
	if drain {
	drainQueue:
		for {
			select {
			case req := <-a.walQueue:
				a.walBatch = append(a.walBatch, req)
			default:
				break drainQueue
			}
		}
	}
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
	for _, req := range batch {
		if req.barrier {
			continue
		}
		position, err := a.appendProjectionLocked(req.record, req.identity)
		if err != nil {
			a.work.RecordError("replication WAL append", err)
			req.complete(walCompletion{err: err})
			continue
		}
		req.complete(walCompletion{position: position})
	}
	currentPos := a.wal.Sequence()
	a.walPosition.Store(currentPos)
	a.walMu.Unlock()

	// Release any barrier waiters now that every real entry in the
	// batch has been appended and walPosition has been updated.
	for _, req := range batch {
		if !req.barrier {
			continue
		}
		if err := a.work.Err(); err != nil {
			req.complete(walCompletion{err: err})
		} else {
			req.complete(walCompletion{position: currentPos})
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
	a.calls.RLock()
	defer a.calls.RUnlock()
	if a.sealed {
		return ErrClosed
	}

	// Build a sentinel write request. barrier=true tells flushWALBatch
	// to skip the WAL append for this entry but still complete after
	// the surrounding batch completes — so the writer goroutine drives
	// the synchronization without us touching its channels directly.
	barrier := &walWriteRequest{
		done:    make(chan walCompletion, 1),
		barrier: true,
	}

	// The admission keeps the writer running until this barrier returns.
	// Ingress cannot be sealed and the worker cannot stop while calls.RLock is held.
	a.walQueue <- barrier

	// Wait for the writer goroutine to flush the batch containing the
	// barrier. The outcome is signaled inside flushWALBatch after the WAL
	// append completes, so by the time we read it every prior request is
	// also persisted. FlushWAL keeps its existing unbounded barrier policy.
	if outcome := <-barrier.done; outcome.err != nil {
		return localizedError(localization.ReplicationStorageFlushWALFailed(outcome.err), outcome.err)
	}

	// Sync the WAL to ensure all writes are on disk.
	a.walMu.Lock()
	defer a.walMu.Unlock()
	if a.wal != nil {
		err := a.wal.Sync()
		if err != nil {
			a.work.RecordError("replication WAL sync", err)
		}
		return err
	}
	return nil
}

// Close releases replication resources (WAL file handles/background goroutines).
func (a *StorageAdapter) Close() error {
	a.calls.Lock()
	a.sealed = true
	a.calls.Unlock()

	// The owner's stop: the writer flushes and returns, then the WAL closes
	// once. Every caller joins the same stop and outcome.
	a.work.BeginStop(a.closeWAL)
	return a.work.JoinStopped()
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

	storageEntries, err := storage.ReadWALEntriesFromDir(a.work.Context(), a.walDir)
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
		rec, err := decodeProjectionRecord(storageEntry)
		if err != nil {
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
	// Snapshot restore is an existing context-free completion boundary.
	completionCtx := context.Background()
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
		if _, err := a.engine.CreateNode(completionCtx, node); err != nil {
			return localizedError(localization.ReplicationStorageRestoreNodeFailed(err), err)
		}
	}

	// Restore edges
	for _, edge := range snapshot.Edges {
		if err := a.engine.CreateEdge(completionCtx, edge); err != nil {
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
