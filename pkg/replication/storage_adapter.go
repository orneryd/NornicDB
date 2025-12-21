// Package replication provides distributed replication for NornicDB.
package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
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
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Create persistent WAL
	walConfig := storage.DefaultWALConfig()
	walConfig.Dir = walDir
	walConfig.SyncMode = "batch" // Batch sync for performance
	walConfig.BatchSyncInterval = 100 * time.Millisecond

	wal, err := storage.NewWAL(walDir, walConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	adapter := &StorageAdapter{
		engine:   engine,
		executor: cypher.NewStorageExecutor(engine),
		wal:      wal,
		walDir:   walDir,
	}

	// Load existing WAL position
	if err := adapter.loadWALPosition(); err != nil {
		// Log error but continue - will start from position 0
		// This allows recovery even if WAL is corrupted
	}

	return adapter, nil
}

// loadWALPosition loads the last WAL position from persistent storage.
func (a *StorageAdapter) loadWALPosition() error {
	walPath := filepath.Join(a.walDir, "wal.log")
	entries, err := storage.ReadWALEntries(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No existing WAL - start fresh
			return nil
		}
		return fmt.Errorf("failed to read WAL entries: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	// Find the highest position from WAL entries
	// We need to deserialize each entry to get the replication WALEntry
	maxPos := uint64(0)
	for _, entry := range entries {
		var replEntry WALEntry
		if err := json.Unmarshal(entry.Data, &replEntry); err == nil {
			if replEntry.Position > maxPos {
				maxPos = replEntry.Position
			}
		}
	}

	a.walPosition.Store(maxPos)
	return nil
}

// SetExecutor sets a custom Cypher executor for the adapter.
// This allows using an executor with additional configuration (e.g., database manager, embedder).
func (a *StorageAdapter) SetExecutor(executor *cypher.StorageExecutor) {
	a.executor = executor
}

// ApplyCommand applies a replicated command to storage.
func (a *StorageAdapter) ApplyCommand(cmd *Command) error {
	if cmd == nil {
		return fmt.Errorf("nil command")
	}

	// Record in persistent WAL first (write-ahead logging)
	pos := a.walPosition.Add(1)
	entry := WALEntry{
		Position:  pos,
		Timestamp: cmd.Timestamp.UnixNano(),
		Command:   cmd,
	}

	// Append to persistent WAL
	// Use a custom operation type for replication WAL entries
	// The WAL will marshal the entry automatically
	if err := a.wal.Append(storage.OperationType("replication_command"), entry); err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	// Execute the command
	switch cmd.Type {
	case CmdCreateNode:
		return a.applyCreateNode(cmd.Data)
	case CmdUpdateNode:
		return a.applyUpdateNode(cmd.Data)
	case CmdDeleteNode:
		return a.applyDeleteNode(cmd.Data)
	case CmdCreateEdge:
		return a.applyCreateEdge(cmd.Data)
	case CmdDeleteEdge:
		return a.applyDeleteEdge(cmd.Data)
	case CmdSetProperty:
		return a.applySetProperty(cmd.Data)
	case CmdBatchWrite:
		return a.applyBatchWrite(cmd.Data)
	case CmdCypher:
		return a.applyCypher(cmd.Data)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

// applyCreateNode creates a node from command data.
func (a *StorageAdapter) applyCreateNode(data []byte) error {
	var node storage.Node
	if err := json.Unmarshal(data, &node); err != nil {
		return fmt.Errorf("unmarshal node: %w", err)
	}
	_, err := a.engine.CreateNode(&node)
	return err
}

// applyUpdateNode updates a node from command data.
func (a *StorageAdapter) applyUpdateNode(data []byte) error {
	var node storage.Node
	if err := json.Unmarshal(data, &node); err != nil {
		return fmt.Errorf("unmarshal node: %w", err)
	}
	return a.engine.UpdateNode(&node)
}

// applyDeleteNode deletes a node.
func (a *StorageAdapter) applyDeleteNode(data []byte) error {
	nodeID := string(data)
	return a.engine.DeleteNode(storage.NodeID(nodeID))
}

// applyCreateEdge creates an edge from command data.
func (a *StorageAdapter) applyCreateEdge(data []byte) error {
	var edge storage.Edge
	if err := json.Unmarshal(data, &edge); err != nil {
		return fmt.Errorf("unmarshal edge: %w", err)
	}
	return a.engine.CreateEdge(&edge)
}

// applyDeleteEdge deletes an edge.
func (a *StorageAdapter) applyDeleteEdge(data []byte) error {
	var req struct {
		EdgeID string `json:"edge_id"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("unmarshal delete edge request: %w", err)
	}
	return a.engine.DeleteEdge(storage.EdgeID(req.EdgeID))
}

// applySetProperty sets a property on a node.
func (a *StorageAdapter) applySetProperty(data []byte) error {
	var req struct {
		NodeID string      `json:"node_id"`
		Key    string      `json:"key"`
		Value  interface{} `json:"value"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("unmarshal set property request: %w", err)
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
		Nodes []*storage.Node `json:"nodes"`
		Edges []*storage.Edge `json:"edges"`
	}
	if err := json.Unmarshal(data, &batch); err != nil {
		return fmt.Errorf("unmarshal batch: %w", err)
	}

	for _, node := range batch.Nodes {
		if _, err := a.engine.CreateNode(node); err != nil {
			return err
		}
	}
	for _, edge := range batch.Edges {
		if err := a.engine.CreateEdge(edge); err != nil {
			return err
		}
	}
	return nil
}

// applyCypher executes a Cypher command (for write queries).
// The data should be a JSON object with "query" (string) and optional "params" (map[string]interface{}).
func (a *StorageAdapter) applyCypher(data []byte) error {
	if a.executor == nil {
		return fmt.Errorf("cypher executor not available - cannot execute Cypher command")
	}

	// Parse Cypher command data
	var cypherCmd struct {
		Query  string                 `json:"query"`
		Params map[string]interface{} `json:"params,omitempty"`
	}

	if err := json.Unmarshal(data, &cypherCmd); err != nil {
		return fmt.Errorf("unmarshal cypher command: %w", err)
	}

	if cypherCmd.Query == "" {
		return fmt.Errorf("cypher query is empty")
	}

	// Execute the Cypher query
	// Use background context since this is a replicated command (no user context)
	ctx := context.Background()
	_, err := a.executor.Execute(ctx, cypherCmd.Query, cypherCmd.Params)
	if err != nil {
		return fmt.Errorf("execute cypher query: %w", err)
	}

	return nil
}

// GetWALPosition returns the current WAL position.
func (a *StorageAdapter) GetWALPosition() (uint64, error) {
	return a.walPosition.Load(), nil
}

// GetWALEntries returns WAL entries starting from the given position.
func (a *StorageAdapter) GetWALEntries(fromPosition uint64, maxEntries int) ([]*WALEntry, error) {
	a.walMu.RLock()
	defer a.walMu.RUnlock()

	// Read from persistent WAL
	walPath := filepath.Join(a.walDir, "wal.log")
	storageEntries, err := storage.ReadWALEntries(walPath)
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
		return nil, fmt.Errorf("failed to read WAL entries: %w", err)
	}

	var entries []*WALEntry
	for _, storageEntry := range storageEntries {
		// Only process replication_command entries
		if storageEntry.Operation != storage.OperationType("replication_command") {
			continue
		}

		// Deserialize replication WALEntry from storage entry's Data field
		// The Data field contains the JSON-marshaled replication.WALEntry
		var replEntry WALEntry
		if err := json.Unmarshal(storageEntry.Data, &replEntry); err != nil {
			continue // Skip corrupted entries
		}

		// Filter by position
		if replEntry.Position > fromPosition {
			entries = append(entries, &replEntry)
			if len(entries) >= maxEntries {
				break
			}
		}
	}

	return entries, nil
}

// WriteSnapshot writes a full snapshot to the given writer.
func (a *StorageAdapter) WriteSnapshot(w SnapshotWriter) error {
	// Get all nodes and edges
	nodes, err := a.engine.AllNodes()
	if err != nil {
		return fmt.Errorf("get all nodes: %w", err)
	}

	edges, err := a.engine.AllEdges()
	if err != nil {
		return fmt.Errorf("get all edges: %w", err)
	}

	snapshot := struct {
		WALPosition uint64          `json:"wal_position"`
		Nodes       []*storage.Node `json:"nodes"`
		Edges       []*storage.Edge `json:"edges"`
	}{
		WALPosition: a.walPosition.Load(),
		Nodes:       nodes,
		Edges:       edges,
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	_, err = w.Write(data)
	return err
}

// RestoreSnapshot restores state from a snapshot.
func (a *StorageAdapter) RestoreSnapshot(r SnapshotReader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	var snapshot struct {
		WALPosition uint64          `json:"wal_position"`
		Nodes       []*storage.Node `json:"nodes"`
		Edges       []*storage.Edge `json:"edges"`
	}

	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	// Restore nodes
	for _, node := range snapshot.Nodes {
		if _, err := a.engine.CreateNode(node); err != nil {
			return fmt.Errorf("restore node: %w", err)
		}
	}

	// Restore edges
	for _, edge := range snapshot.Edges {
		if err := a.engine.CreateEdge(edge); err != nil {
			return fmt.Errorf("restore edge: %w", err)
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

// Close closes the WAL and cleans up resources.
// Should be called when the adapter is no longer needed.
func (a *StorageAdapter) Close() error {
	if a.wal != nil {
		return a.wal.Close()
	}
	return nil
}

// Verify StorageAdapter implements Storage interface.
var _ Storage = (*StorageAdapter)(nil)
