// Package replication provides distributed replication for NornicDB.
package replication

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// StorageAdapter bridges the replication.Storage interface to storage.Engine.
// It translates replication commands into storage operations and maintains WAL state.
type StorageAdapter struct {
	engine   storage.Engine
	executor *cypher.StorageExecutor // Cypher executor for executing replicated Cypher queries

	// Persistent WAL for replication commands
	wal         *storage.WAL
	walDir      string
	walMu       sync.RWMutex // Protects wal and walPosition
	walPosition atomic.Uint64

	// In-memory WAL for fast streaming (avoids re-reading wal.log continuously).
	memWALMu sync.RWMutex
	memWAL   []*WALEntry // sorted by Position asc

	// Async WAL writing for performance
	walQueue   chan *walWriteRequest
	walBatch   []*walWriteRequest
	walBatchMu sync.Mutex
	walStopCh  chan struct{}
	walWg      sync.WaitGroup
}

// walWriteRequest represents a pending WAL write.
type walWriteRequest struct {
	record  replicationWALRecord
	posCh   chan uint64 // Channel to receive the WAL position
	errCh   chan error  // Channel to receive any error
	waiting bool        // If true, caller is waiting for completion

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
		engine:    engine,
		executor:  cypher.NewStorageExecutor(engine),
		wal:       wal,
		walDir:    walDir,
		walQueue:  make(chan *walWriteRequest, 1000), // Buffered channel for async WAL writes
		walBatch:  make([]*walWriteRequest, 0, 100),  // Pre-allocated batch buffer
		walStopCh: make(chan struct{}),
	}

	// Load existing WAL position
	_ = adapter.loadWALPosition()

	// Start async WAL writer
	adapter.walWg.Add(1)
	go adapter.walWriterLoop()

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

// SetExecutor sets a custom Cypher executor for the adapter.
// This allows using an executor with additional configuration (e.g., database manager, embedder).
func (a *StorageAdapter) SetExecutor(executor *cypher.StorageExecutor) {
	a.executor = executor
}

// ApplyCommand applies a replicated command to storage.
// WAL writes are now asynchronous for better performance.
func (a *StorageAdapter) ApplyCommand(cmd *Command) error {
	if cmd == nil {
		return localizedError(localization.ReplicationStorageCommandRequired(), nil)
	}

	// Record in persistent WAL (write-ahead logging)
	record := replicationWALRecord{
		Timestamp: cmd.Timestamp.UnixNano(),
		Command:   cmd,
	}

	// Queue async WAL write (non-blocking)
	req := &walWriteRequest{
		record:  record,
		posCh:   make(chan uint64, 1),
		errCh:   make(chan error, 1),
		waiting: false, // Don't wait for WAL write to complete
	}

	// Try to queue async write
	select {
	case a.walQueue <- req:
		// Successfully queued, continue
	default:
		// Channel full - fall back to synchronous write to avoid blocking
		// This should be rare with a 1000-item buffer
		a.walMu.Lock()
		if err := a.wal.Append(storage.OperationType("replication_command"), record); err != nil {
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
	case CmdCreateNode:
		execErr = a.applyCreateNode(cmd.Data)
	case CmdUpdateNode:
		execErr = a.applyUpdateNode(cmd.Data)
	case CmdDeleteNode:
		execErr = a.applyDeleteNode(cmd.Data)
	case CmdCreateEdge:
		execErr = a.applyCreateEdge(cmd.Data)
	case CmdUpdateEdge:
		execErr = a.applyUpdateEdge(cmd.Data)
	case CmdDeleteEdge:
		execErr = a.applyDeleteEdge(cmd.Data)
	case CmdSetProperty:
		execErr = a.applySetProperty(cmd.Data)
	case CmdBatchWrite:
		execErr = a.applyBatchWrite(cmd.Data)
	case CmdCypher:
		execErr = a.applyCypher(cmd.Data)
	case CmdDeleteByPrefix:
		execErr = a.applyDeleteByPrefix(cmd.Data)
	case CmdBulkCreateNodes:
		execErr = a.applyBulkCreateNodes(cmd.Data)
	case CmdBulkCreateEdges:
		execErr = a.applyBulkCreateEdges(cmd.Data)
	case CmdBulkDeleteNodes:
		execErr = a.applyBulkDeleteNodes(cmd.Data)
	case CmdBulkDeleteEdges:
		execErr = a.applyBulkDeleteEdges(cmd.Data)
	default:
		execErr = localizedError(localization.ReplicationStorageUnknownCommandType(uint8(cmd.Type)), nil)
	}

	// Try to get position from async write (non-blocking, best-effort)
	// If not ready yet, walWriterLoop will update it later
	select {
	case pos := <-req.posCh:
		a.walPosition.Store(pos)
		a.appendToMemWAL(pos, record, cmd)
	case err := <-req.errCh:
		if err != nil {
			// Log error but don't fail the operation (WAL is for durability, not correctness)
			// The command was already applied to storage
			logReplicationPrintf(context.Background(), slog.LevelError, "[WAL] Async write error (non-fatal): %v", err)
		}
	default:
		// Position not ready yet, will be set by walWriterLoop
		// This is fine - the command was applied and WAL write is queued
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
	defer a.walWg.Done()

	ticker := time.NewTicker(10 * time.Millisecond) // Batch interval
	defer ticker.Stop()

	for {
		select {
		case <-a.walStopCh:
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

		// Send position to request (non-blocking)
		select {
		case req.posCh <- pos:
		default:
		}
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

// applyCreateNode creates a node from command data.
func (a *StorageAdapter) applyCreateNode(data []byte) error {
	node, err := decodeNodePayload(data)
	if err != nil {
		return localizedError(localization.ReplicationStorageDecodeNodeFailed(err), err)
	}
	_, err = a.engine.CreateNode(node)
	return err
}

// applyUpdateNode updates a node from command data.
func (a *StorageAdapter) applyUpdateNode(data []byte) error {
	node, err := decodeNodePayload(data)
	if err != nil {
		return localizedError(localization.ReplicationStorageDecodeNodeFailed(err), err)
	}
	return a.engine.UpdateNode(node)
}

// applyDeleteNode deletes a node.
func (a *StorageAdapter) applyDeleteNode(data []byte) error {
	var req struct {
		NodeID string
	}
	if err := decodeGob(data, &req); err == nil && req.NodeID != "" {
		return a.engine.DeleteNode(storage.NodeID(req.NodeID))
	}
	// Legacy fallback: raw ID bytes.
	return a.engine.DeleteNode(storage.NodeID(string(data)))
}

// applyCreateEdge creates an edge from command data.
func (a *StorageAdapter) applyCreateEdge(data []byte) error {
	edge, err := decodeEdgePayload(data)
	if err != nil {
		return localizedError(localization.ReplicationStorageDecodeEdgeFailed(err), err)
	}
	return a.engine.CreateEdge(edge)
}

func (a *StorageAdapter) applyUpdateEdge(data []byte) error {
	edge, err := decodeEdgePayload(data)
	if err != nil {
		return localizedError(localization.ReplicationStorageDecodeEdgeFailed(err), err)
	}
	return a.engine.UpdateEdge(edge)
}

// applyDeleteEdge deletes an edge.
func (a *StorageAdapter) applyDeleteEdge(data []byte) error {
	var req struct {
		EdgeID string
	}
	if err := decodeGob(data, &req); err != nil {
		return localizedError(localization.ReplicationStorageDecodeDeleteEdgeFailed(err), err)
	}
	return a.engine.DeleteEdge(storage.EdgeID(req.EdgeID))
}

// applySetProperty sets a property on a node.
func (a *StorageAdapter) applySetProperty(data []byte) error {
	var req struct {
		NodeID string
		Key    string
		Value  interface{}
	}
	if err := decodeGob(data, &req); err != nil {
		return localizedError(localization.ReplicationStorageDecodeSetPropertyFailed(err), err)
	}

	// Get node, update property, save
	node, err := a.engine.GetNode(storage.NodeID(req.NodeID))
	if err != nil {
		return err
	}
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	node.Properties[req.Key] = req.Value
	return a.engine.UpdateNode(node)
}

// applyBatchWrite applies a batch of operations.
func (a *StorageAdapter) applyBatchWrite(data []byte) error {
	var batch struct {
		Nodes [][]byte
		Edges [][]byte
	}
	if err := decodeGob(data, &batch); err != nil {
		return localizedError(localization.ReplicationStorageDecodeBatchFailed(err), err)
	}

	for _, nodeBytes := range batch.Nodes {
		node, err := decodeNodePayload(nodeBytes)
		if err != nil {
			return err
		}
		if _, err := a.engine.CreateNode(node); err != nil {
			return err
		}
	}
	for _, edgeBytes := range batch.Edges {
		edge, err := decodeEdgePayload(edgeBytes)
		if err != nil {
			return err
		}
		if err := a.engine.CreateEdge(edge); err != nil {
			return err
		}
	}
	return nil
}

func (a *StorageAdapter) applyDeleteByPrefix(data []byte) error {
	var req struct {
		Prefix string
	}
	if err := decodeGob(data, &req); err != nil {
		return localizedError(localization.ReplicationStorageDecodeDeletePrefixFailed(err), err)
	}
	if req.Prefix == "" {
		return localizedError(localization.ReplicationStoragePrefixRequired(), nil)
	}
	_, _, err := a.engine.DeleteByPrefix(req.Prefix)
	return err
}

func (a *StorageAdapter) applyBulkCreateNodes(data []byte) error {
	var encoded [][]byte
	if err := decodeGob(data, &encoded); err != nil {
		return localizedError(localization.ReplicationStorageDecodeBulkCreateNodesFailed(err), err)
	}
	nodes := make([]*storage.Node, 0, len(encoded))
	for _, b := range encoded {
		n, err := decodeNodePayload(b)
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
	}
	return a.engine.BulkCreateNodes(nodes)
}

func (a *StorageAdapter) applyBulkCreateEdges(data []byte) error {
	var encoded [][]byte
	if err := decodeGob(data, &encoded); err != nil {
		return localizedError(localization.ReplicationStorageDecodeBulkCreateEdgesFailed(err), err)
	}
	edges := make([]*storage.Edge, 0, len(encoded))
	for _, b := range encoded {
		edge, err := decodeEdgePayload(b)
		if err != nil {
			return err
		}
		edges = append(edges, edge)
	}
	return a.engine.BulkCreateEdges(edges)
}

func (a *StorageAdapter) applyBulkDeleteNodes(data []byte) error {
	var ids []storage.NodeID
	if err := decodeGob(data, &ids); err != nil {
		return localizedError(localization.ReplicationStorageDecodeBulkDeleteNodesFailed(err), err)
	}
	return a.engine.BulkDeleteNodes(ids)
}

func (a *StorageAdapter) applyBulkDeleteEdges(data []byte) error {
	var ids []storage.EdgeID
	if err := decodeGob(data, &ids); err != nil {
		return localizedError(localization.ReplicationStorageDecodeBulkDeleteEdgesFailed(err), err)
	}
	return a.engine.BulkDeleteEdges(ids)
}

// applyCypher executes a Cypher command (for write queries).
// The data should be a JSON object with "query" (string) and optional "params" (map[string]interface{}).
func (a *StorageAdapter) applyCypher(data []byte) error {
	if a.executor == nil {
		return localizedError(localization.ReplicationStorageCypherUnavailable(), nil)
	}

	// Parse Cypher command data
	var cypherCmd struct {
		Query  string
		Params map[string]interface{}
	}

	if err := decodeGob(data, &cypherCmd); err != nil {
		return localizedError(localization.ReplicationStorageDecodeCypherFailed(err), err)
	}

	if cypherCmd.Query == "" {
		return localizedError(localization.ReplicationStorageCypherQueryEmpty(), nil)
	}

	// Execute the Cypher query
	// Use background context since this is a replicated command (no user context)
	ctx := context.Background()
	_, err := a.executor.Execute(ctx, cypherCmd.Query, cypherCmd.Params)
	if err != nil {
		return localizedError(localization.ReplicationStorageExecuteCypherFailed(err), err)
	}

	return nil
}

// FlushWAL waits for all pending WAL writes to complete.
// This is useful for tests or when you need to ensure durability before proceeding.
//
// The previous version slept 20ms hoping walWriterLoop would drain the
// queue — on slow runners (CI) goroutine scheduling jitter could leave
// the tail request still in walQueue at sync time, so callers that
// immediately observed GetWALPosition saw a count one short of what
// they queued. The fix is to send a barrier request through the same
// channel and wait for its acknowledgement. Because the writer reads
// requests from walQueue in FIFO order, every request queued before
// the barrier is guaranteed to be flushed before the barrier itself
// completes.
func (a *StorageAdapter) FlushWAL() error {
	// Build a sentinel write request. barrier=true tells flushWALBatch
	// to skip the WAL append for this entry but still ack posCh after
	// the surrounding batch completes — so the writer goroutine drives
	// the synchronization without us touching its channels directly.
	barrier := &walWriteRequest{
		posCh:   make(chan uint64, 1),
		errCh:   make(chan error, 1),
		waiting: true,
		barrier: true,
	}

	// Push the barrier behind everything already queued.
	select {
	case a.walQueue <- barrier:
	case <-a.walStopCh:
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
	case <-a.walStopCh:
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
	select {
	case <-a.walStopCh:
		// Already stopped.
	default:
		close(a.walStopCh)
	}
	a.walWg.Wait()
}

// Close releases replication resources (WAL file handles/background goroutines).
func (a *StorageAdapter) Close() error {
	// Stop async WAL writer (only once)
	select {
	case <-a.walStopCh:
		// Already closed
	default:
		close(a.walStopCh)
	}
	a.walWg.Wait()

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

// GetWALEntries returns WAL entries starting from the given position.
func (a *StorageAdapter) GetWALEntries(fromPosition uint64, maxEntries int) ([]*WALEntry, error) {
	// Fast path: serve from in-memory WAL.
	a.memWALMu.RLock()
	mem := a.memWAL
	a.memWALMu.RUnlock()

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
			return []*WALEntry{}, nil
		}
		end := lo + maxEntries
		if end > len(mem) {
			end = len(mem)
		}
		entries := make([]*WALEntry, end-lo)
		copy(entries, mem[lo:end])
		return entries, nil
	}

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

		// Prefer the current record format, but tolerate older WALs.
		var (
			ts  int64
			cmd *Command
		)
		var rec replicationWALRecord
		if err := decodeGob(storageEntry.Data, &rec); err == nil && rec.Command != nil {
			ts = rec.Timestamp
			cmd = rec.Command
		} else {
			continue // Skip corrupted/legacy entries
		}

		pos := storageEntry.Sequence
		if pos > fromPosition {
			entries = append(entries, &WALEntry{
				Position:  pos,
				Timestamp: ts,
				Command:   cmd,
			})
			if len(entries) >= maxEntries {
				break
			}
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
	copy(a.memWAL, a.memWAL[lo:])
	a.memWAL = a.memWAL[:len(a.memWAL)-lo]
}

// WriteSnapshot writes a full snapshot to the given writer.
func (a *StorageAdapter) WriteSnapshot(w SnapshotWriter) error {
	// Get all nodes and edges
	nodes, err := a.engine.AllNodes()
	if err != nil {
		return localizedError(localization.ReplicationStorageGetAllNodesFailed(err), err)
	}

	edges, err := a.engine.AllEdges()
	if err != nil {
		return localizedError(localization.ReplicationStorageGetAllEdgesFailed(err), err)
	}

	snapshot := struct {
		WALPosition uint64
		Nodes       []*storage.Node
		Edges       []*storage.Edge
	}{
		WALPosition: a.walPosition.Load(),
		Nodes:       nodes,
		Edges:       edges,
	}

	data, err := encodeGob(snapshot)
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

	var snapshot struct {
		WALPosition uint64
		Nodes       []*storage.Node
		Edges       []*storage.Edge
	}

	if err := decodeGob(data, &snapshot); err != nil {
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
