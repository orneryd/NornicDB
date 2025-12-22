// Package storage - Unit tests for WAL redo/undo logging and transaction recovery.
//
// These tests verify:
// 1. Undo operations correctly reverse redo operations
// 2. Transaction boundaries (Begin/Commit/Abort) work correctly
// 3. Incomplete transactions are rolled back on recovery
// 4. Committed transactions persist through recovery
package storage

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// =============================================================================
// UNDO OPERATION TESTS
// =============================================================================

// TestUndoCreateNode verifies that creating a node can be undone.
func TestUndoCreateNode(t *testing.T) {
	engine := NewMemoryEngine()

	// Create WAL entry for node creation
	node := &Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	data, _ := json.Marshal(WALNodeData{Node: node})
	entry := WALEntry{
		Sequence:  1,
		Operation: OpCreateNode,
		Data:      data,
		Checksum:  crc32Checksum(data),
	}

	// Apply redo
	if err := ReplayWALEntry(engine, entry); err != nil {
		t.Fatalf("Redo failed: %v", err)
	}

	// Verify node exists
	n, _ := engine.GetNode("n1")
	if n == nil {
		t.Fatal("Node should exist after redo")
	}

	// Apply undo
	if err := UndoWALEntry(engine, entry); err != nil {
		t.Fatalf("Undo failed: %v", err)
	}

	// Verify node is gone
	n, _ = engine.GetNode("n1")
	if n != nil {
		t.Error("Node should not exist after undo")
	}
}

// TestUndoUpdateNode verifies that updating a node can be undone.
func TestUndoUpdateNode(t *testing.T) {
	engine := NewMemoryEngine()

	// Create original node
	oldNode := &Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	engine.CreateNode(oldNode)

	// Create WAL entry for update with before image
	newNode := &Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
	data, _ := json.Marshal(WALNodeData{Node: newNode, OldNode: oldNode})
	entry := WALEntry{
		Sequence:  2,
		Operation: OpUpdateNode,
		Data:      data,
		Checksum:  crc32Checksum(data),
	}

	// Apply redo
	if err := ReplayWALEntry(engine, entry); err != nil {
		t.Fatalf("Redo failed: %v", err)
	}

	// Verify node was updated
	n, _ := engine.GetNode("n1")
	if n.Properties["name"] != "Bob" {
		t.Error("Node should be updated after redo")
	}

	// Apply undo
	if err := UndoWALEntry(engine, entry); err != nil {
		t.Fatalf("Undo failed: %v", err)
	}

	// Verify node is restored
	n, _ = engine.GetNode("n1")
	if n.Properties["name"] != "Alice" {
		t.Errorf("Node should be restored after undo, got name=%v", n.Properties["name"])
	}
}

// TestUndoDeleteNode verifies that deleting a node can be undone.
func TestUndoDeleteNode(t *testing.T) {
	engine := NewMemoryEngine()

	// Create node to delete
	oldNode := &Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	engine.CreateNode(oldNode)

	// Create WAL entry for delete with before image
	data, _ := json.Marshal(WALDeleteData{ID: "n1", OldNode: oldNode})
	entry := WALEntry{
		Sequence:  2,
		Operation: OpDeleteNode,
		Data:      data,
		Checksum:  crc32Checksum(data),
	}

	// Apply redo
	if err := ReplayWALEntry(engine, entry); err != nil {
		t.Fatalf("Redo failed: %v", err)
	}

	// Verify node is deleted
	n, _ := engine.GetNode("n1")
	if n != nil {
		t.Error("Node should be deleted after redo")
	}

	// Apply undo
	if err := UndoWALEntry(engine, entry); err != nil {
		t.Fatalf("Undo failed: %v", err)
	}

	// Verify node is restored
	n, _ = engine.GetNode("n1")
	if n == nil {
		t.Fatal("Node should be restored after undo")
	}
	if n.Properties["name"] != "Alice" {
		t.Errorf("Node properties should be restored, got %v", n.Properties)
	}
}

// TestUndoWithoutBeforeImage verifies error on missing undo data.
func TestUndoWithoutBeforeImage(t *testing.T) {
	engine := NewMemoryEngine()

	// Create WAL entry for update WITHOUT before image
	newNode := &Node{ID: "n1", Labels: []string{"Person"}}
	data, _ := json.Marshal(WALNodeData{Node: newNode})
	entry := WALEntry{
		Sequence:  1,
		Operation: OpUpdateNode,
		Data:      data,
		Checksum:  crc32Checksum(data),
	}

	// Undo should fail due to missing before image
	err := UndoWALEntry(engine, entry)
	if err != ErrNoUndoData {
		t.Errorf("Expected ErrNoUndoData, got: %v", err)
	}
}

// =============================================================================
// TRANSACTION BOUNDARY TESTS
// =============================================================================

// TestTxBoundaryMarkers verifies transaction markers are written and read.
func TestTxBoundaryMarkers(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write transaction boundaries - pass structs directly, Append does marshaling
	wal.Append(OpTxBegin, WALTxData{TxID: "tx-001", Metadata: map[string]string{"user": "alice"}})
	wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "n1", Labels: []string{"Test"}}, TxID: "tx-001"})
	wal.Append(OpTxCommit, WALTxData{TxID: "tx-001", OpCount: 1})

	wal.Close()

	// Read back
	entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatalf("Failed to read entries: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(entries))
	}

	if entries[0].Operation != OpTxBegin {
		t.Error("First entry should be TxBegin")
	}
	if entries[1].Operation != OpCreateNode {
		t.Error("Second entry should be CreateNode")
	}
	if entries[2].Operation != OpTxCommit {
		t.Error("Third entry should be TxCommit")
	}
}

// TestGetEntryTxID verifies transaction ID extraction.
func TestGetEntryTxID(t *testing.T) {
	tests := []struct {
		name     string
		op       OperationType
		data     interface{}
		expected string
	}{
		{
			name:     "node with tx",
			op:       OpCreateNode,
			data:     WALNodeData{Node: &Node{ID: "n1"}, TxID: "tx-123"},
			expected: "tx-123",
		},
		{
			name:     "node without tx",
			op:       OpCreateNode,
			data:     WALNodeData{Node: &Node{ID: "n1"}},
			expected: "",
		},
		{
			name:     "delete with tx",
			op:       OpDeleteNode,
			data:     WALDeleteData{ID: "n1", TxID: "tx-456"},
			expected: "tx-456",
		},
		{
			name:     "tx boundary",
			op:       OpTxBegin,
			data:     WALTxData{TxID: "tx-789"},
			expected: "tx-789",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, _ := json.Marshal(tc.data)
			entry := WALEntry{
				Sequence:  1,
				Operation: tc.op,
				Data:      data,
				Checksum:  crc32Checksum(data),
			}

			txID := GetEntryTxID(entry)
			if txID != tc.expected {
				t.Errorf("Expected TxID %q, got %q", tc.expected, txID)
			}
		})
	}
}

// =============================================================================
// TRANSACTION RECOVERY TESTS
// =============================================================================

// TestRecoverCommittedTransaction verifies committed transactions are applied.
func TestRecoverCommittedTransaction(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL(dir, cfg)

	// Write a complete transaction - pass structs directly
	wal.Append(OpTxBegin, WALTxData{TxID: "tx-001"})
	wal.Append(OpCreateNode, WALNodeData{
		Node: &Node{ID: "n1", Labels: []string{"Test"}, Properties: map[string]interface{}{"value": "committed"}},
		TxID: "tx-001",
	})
	wal.Append(OpTxCommit, WALTxData{TxID: "tx-001"})

	wal.Close()

	// Recover
	baseEngine, result, err := RecoverWithTransactions(dir, "")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	engine := NewNamespacedEngine(baseEngine, "test")

	// Verify transaction was committed
	if result.CommittedTransactions != 1 {
		t.Errorf("Expected 1 committed transaction, got %d", result.CommittedTransactions)
	}

	// Verify node exists
	n, _ := engine.GetNode("n1")
	if n == nil {
		t.Fatal("Node should exist after committed transaction recovery")
	}
	if n.Properties["value"] != "committed" {
		t.Errorf("Node should have committed value, got %v", n.Properties)
	}
}

// TestRecoverIncompleteTransaction verifies incomplete transactions are rolled back.
func TestRecoverIncompleteTransaction(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL(dir, cfg)

	// Write an incomplete transaction (no commit) - pass structs directly
	wal.Append(OpTxBegin, WALTxData{TxID: "tx-incomplete"})

	// Node creation with undo data
	node := &Node{ID: "n1", Labels: []string{"Test"}, Properties: map[string]interface{}{"value": "incomplete"}}
	wal.Append(OpCreateNode, WALNodeData{Node: node, TxID: "tx-incomplete"})

	// No commit! Simulates crash.
	wal.Close()

	// Recover
	baseEngine, result, err := RecoverWithTransactions(dir, "")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	engine := NewNamespacedEngine(baseEngine, "test")

	// Verify transaction was rolled back
	if result.RolledBackTransactions != 1 {
		t.Errorf("Expected 1 rolled back transaction, got %d", result.RolledBackTransactions)
	}

	// Verify node does NOT exist (was rolled back)
	n, _ := engine.GetNode("n1")
	if n != nil {
		t.Error("Node should NOT exist after rollback of incomplete transaction")
	}
}

// TestRecoverAbortedTransaction verifies aborted transactions don't apply.
func TestRecoverAbortedTransaction(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL(dir, cfg)

	// Write an explicitly aborted transaction - pass structs directly
	wal.Append(OpTxBegin, WALTxData{TxID: "tx-aborted"})
	wal.Append(OpCreateNode, WALNodeData{
		Node: &Node{ID: "n1", Labels: []string{"Test"}},
		TxID: "tx-aborted",
	})
	wal.Append(OpTxAbort, WALTxData{TxID: "tx-aborted", Reason: "user cancelled"})

	wal.Close()

	// Recover
	baseEngine, result, err := RecoverWithTransactions(dir, "")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	engine := NewNamespacedEngine(baseEngine, "test")

	// Verify transaction was recognized as aborted
	if result.AbortedTransactions != 1 {
		t.Errorf("Expected 1 aborted transaction, got %d", result.AbortedTransactions)
	}

	// Verify node does NOT exist (aborted transaction)
	n, _ := engine.GetNode("n1")
	if n != nil {
		t.Error("Node should NOT exist from aborted transaction")
	}
}

// TestRecoverMixedTransactions verifies mix of committed and incomplete.
func TestRecoverMixedTransactions(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()

	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, _ := NewWAL(dir, cfg)

	// Transaction 1: Committed - pass structs directly
	wal.Append(OpTxBegin, WALTxData{TxID: "tx-1"})
	wal.Append(OpCreateNode, WALNodeData{
		Node: &Node{ID: "committed-node", Labels: []string{"Test"}},
		TxID: "tx-1",
	})
	wal.Append(OpTxCommit, WALTxData{TxID: "tx-1"})

	// Transaction 2: Incomplete (crash)
	wal.Append(OpTxBegin, WALTxData{TxID: "tx-2"})
	wal.Append(OpCreateNode, WALNodeData{
		Node: &Node{ID: "incomplete-node", Labels: []string{"Test"}},
		TxID: "tx-2",
	})
	// No commit!

	// Non-transactional write
	wal.Append(OpCreateNode, WALNodeData{
		Node: &Node{ID: "non-tx-node", Labels: []string{"Test"}},
	})

	wal.Close()

	// Recover
	baseEngine, result, err := RecoverWithTransactions(dir, "")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	engine := NewNamespacedEngine(baseEngine, "test")

	// Verify statistics
	if result.CommittedTransactions != 1 {
		t.Errorf("Expected 1 committed, got %d", result.CommittedTransactions)
	}
	if result.RolledBackTransactions != 1 {
		t.Errorf("Expected 1 rolled back, got %d", result.RolledBackTransactions)
	}
	if result.NonTxApplied != 1 {
		t.Errorf("Expected 1 non-tx applied, got %d", result.NonTxApplied)
	}

	// Verify data
	n1, _ := engine.GetNode("committed-node")
	if n1 == nil {
		t.Error("Committed node should exist")
	}

	n2, _ := engine.GetNode("incomplete-node")
	if n2 != nil {
		t.Error("Incomplete transaction node should NOT exist")
	}

	n3, _ := engine.GetNode("non-tx-node")
	if n3 == nil {
		t.Error("Non-transactional node should exist")
	}
}

// TestRecoveryResultSummary verifies summary formatting.
func TestRecoveryResultSummary(t *testing.T) {
	result := &TransactionRecoveryResult{
		CommittedTransactions:  5,
		RolledBackTransactions: 2,
		AbortedTransactions:    1,
		NonTxApplied:           10,
		UndoErrors:             []string{"error1"},
	}

	summary := result.Summary()
	if summary == "" {
		t.Error("Summary should not be empty")
	}

	if !result.HasErrors() {
		t.Error("HasErrors should return true when there are undo errors")
	}
}

// =============================================================================
// EDGE UNDO TESTS
// =============================================================================

// TestUndoCreateEdge verifies edge creation can be undone.
func TestUndoCreateEdge(t *testing.T) {
	engine := NewMemoryEngine()

	// Create nodes first
	engine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})
	engine.CreateNode(&Node{ID: "n2", Labels: []string{"Test"}})

	// Create edge via WAL
	edge := &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}
	data, _ := json.Marshal(WALEdgeData{Edge: edge})
	entry := WALEntry{Sequence: 1, Operation: OpCreateEdge, Data: data, Checksum: crc32Checksum(data)}

	// Redo
	ReplayWALEntry(engine, entry)

	e, _ := engine.GetEdge("e1")
	if e == nil {
		t.Fatal("Edge should exist after redo")
	}

	// Undo
	UndoWALEntry(engine, entry)

	e, _ = engine.GetEdge("e1")
	if e != nil {
		t.Error("Edge should not exist after undo")
	}
}

// TestUndoDeleteEdge verifies edge deletion can be undone.
func TestUndoDeleteEdge(t *testing.T) {
	engine := NewMemoryEngine()

	// Create nodes and edge
	engine.CreateNode(&Node{ID: "n1", Labels: []string{"Test"}})
	engine.CreateNode(&Node{ID: "n2", Labels: []string{"Test"}})
	oldEdge := &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]interface{}{"since": 2020}}
	engine.CreateEdge(oldEdge)

	// Delete edge via WAL with before image
	data, _ := json.Marshal(WALDeleteData{ID: "e1", OldEdge: oldEdge})
	entry := WALEntry{Sequence: 2, Operation: OpDeleteEdge, Data: data, Checksum: crc32Checksum(data)}

	// Redo (delete)
	ReplayWALEntry(engine, entry)

	e, _ := engine.GetEdge("e1")
	if e != nil {
		t.Fatal("Edge should be deleted after redo")
	}

	// Undo (restore)
	UndoWALEntry(engine, entry)

	e, _ = engine.GetEdge("e1")
	if e == nil {
		t.Fatal("Edge should be restored after undo")
	}
	// JSON unmarshaling converts integers to float64
	since, ok := e.Properties["since"].(float64)
	if !ok || since != 2020 {
		t.Errorf("Edge properties should be restored with since=2020, got %v", e.Properties)
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func mustMarshalRaw(v interface{}) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// Ensure time package is used
var _ = time.Now
