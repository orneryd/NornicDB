// Package storage - Unit tests for WAL redo/undo logging and transaction recovery.
//
// These tests verify:
// 1. Undo operations correctly reverse redo operations
// 2. Transaction boundaries (Begin/Commit/Abort) work correctly
// 3. Incomplete transactions are rolled back on recovery
// 4. Committed transactions persist through recovery
package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// UNDO OPERATION TESTS
// =============================================================================

// TestWALNodeUndoRestoresBeforeImage exercises the same redo/undo boundary
// with each node mutation's distinct before and after image.
func TestWALNodeUndoRestoresBeforeImage(t *testing.T) {
	for _, operation := range []OperationType{OpCreateNode, OpUpdateNode, OpDeleteNode} {
		t.Run(string(operation), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			engine := NewNamespacedEngine(base, "test")
			before := &Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
			after := &Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
			if operation == OpCreateNode {
				before = nil
				after.Properties["name"] = "Alice"
			} else {
				_, err := engine.CreateNode(t.Context(), before)
				require.NoError(t, err)
			}
			var payload interface{}
			if operation == OpDeleteNode {
				after = nil
				payload = WALDeleteData{ID: "n1", OldNode: before}
			} else {
				payload = WALNodeData{Node: after, OldNode: before}
			}
			data, err := json.Marshal(payload)
			require.NoError(t, err)
			sequence := uint64(2)
			if operation == OpCreateNode {
				sequence = 1
			}
			entry := WALEntry{Sequence: sequence, Operation: operation, Data: data, Checksum: crc32Checksum(data)}
			for _, phase := range []struct {
				name  string
				apply func(context.Context, Engine, WALEntry) error
				want  *Node
			}{{"redo", ReplayWALEntry, after}, {"undo", UndoWALEntry, before}} {
				require.NoError(t, phase.apply(t.Context(), engine, entry), phase.name)
				got, err := engine.GetNode(t.Context(), "n1")
				if phase.want == nil {
					require.ErrorIs(t, err, ErrNotFound, phase.name)
					require.Nil(t, got, phase.name)
				} else {
					require.NoError(t, err, phase.name)
					require.NotNil(t, got, phase.name)
					require.Equal(t, phase.want.ID, got.ID, phase.name)
					require.Equal(t, phase.want.Labels, got.Labels, phase.name)
					require.Equal(t, phase.want.Properties, got.Properties, phase.name)
				}
			}
		})
	}
}

// TestUndoWithoutBeforeImage verifies error on missing undo data.
func TestUndoWithoutBeforeImage(t *testing.T) {
	base := newTestMemoryEngine(t)
	defer base.Close()
	engine := NewNamespacedEngine(base, "test")

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
	err := UndoWALEntry(t.Context(), engine, entry)
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
	entries, err := ReadWALEntries(t.Context(), filepath.Join(dir, "wal.log"), nil)
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
		{
			name:     "edge with tx",
			op:       OpCreateEdge,
			data:     WALEdgeData{Edge: &Edge{ID: "e1"}, TxID: "tx-edge"},
			expected: "tx-edge",
		},
		{
			name:     "bulk nodes with tx",
			op:       OpBulkNodes,
			data:     WALBulkNodesData{Nodes: []*Node{{ID: "n1"}}, TxID: "tx-bulk-nodes"},
			expected: "tx-bulk-nodes",
		},
		{
			name:     "bulk edges with tx",
			op:       OpBulkEdges,
			data:     WALBulkEdgesData{Edges: []*Edge{{ID: "e1"}}, TxID: "tx-bulk-edges"},
			expected: "tx-bulk-edges",
		},
		{
			name:     "bulk delete nodes with tx",
			op:       OpBulkDeleteNodes,
			data:     WALBulkDeleteNodesData{IDs: []string{"n1"}, TxID: "tx-bulk-delete-nodes"},
			expected: "tx-bulk-delete-nodes",
		},
		{
			name:     "bulk delete edges with tx",
			op:       OpBulkDeleteEdges,
			data:     WALBulkDeleteEdgesData{IDs: []string{"e1"}, TxID: "tx-bulk-delete-edges"},
			expected: "tx-bulk-delete-edges",
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

	t.Run("malformed payload returns empty txid", func(t *testing.T) {
		entry := WALEntry{Operation: OpCreateNode, Data: []byte("{bad-json")}
		require.Empty(t, GetEntryTxID(entry))
	})
}

// =============================================================================
// TRANSACTION RECOVERY TESTS
// =============================================================================

// TestWALTransactionRecoveryOutcomes shares the recovery driver while retaining
// explicit expectations for committed, incomplete, aborted and mixed journals.
func TestWALTransactionRecoveryOutcomes(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	type txFixture struct {
		id       string
		node     NodeID
		value    string
		terminal OperationType
	}
	for _, tc := range []struct {
		name                                         string
		transactions                                 []txFixture
		nonTx                                        NodeID
		committed, rolledBack, aborted, nonTxApplied int
		present                                      map[NodeID]string
		absent                                       []NodeID
	}{
		{name: "committed", transactions: []txFixture{{"tx-001", "n1", "committed", OpTxCommit}}, committed: 1, present: map[NodeID]string{"n1": "committed"}},
		{name: "incomplete", transactions: []txFixture{{"tx-incomplete", "n1", "incomplete", ""}}, rolledBack: 1, absent: []NodeID{"n1"}},
		{name: "aborted", transactions: []txFixture{{"tx-aborted", "n1", "", OpTxAbort}}, aborted: 1, absent: []NodeID{"n1"}},
		{name: "mixed", transactions: []txFixture{{"tx-1", "committed-node", "", OpTxCommit}, {"tx-2", "incomplete-node", "", ""}}, nonTx: "non-tx-node", committed: 1, rolledBack: 1, nonTxApplied: 1, present: map[NodeID]string{"committed-node": "", "non-tx-node": ""}, absent: []NodeID{"incomplete-node"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			for _, tx := range tc.transactions {
				require.NoError(t, wal.AppendWithDatabase(OpTxBegin, WALTxData{TxID: tx.id}, "test"))
				node := &Node{ID: tx.node, Labels: []string{"Test"}}
				if tx.value != "" {
					node.Properties = map[string]interface{}{"value": tx.value}
				}
				require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: node, TxID: tx.id}, "test"))
				if tx.terminal != "" {
					terminal := WALTxData{TxID: tx.id}
					if tx.terminal == OpTxAbort {
						terminal.Reason = "user cancelled"
					}
					require.NoError(t, wal.AppendWithDatabase(tx.terminal, terminal, "test"))
				}
			}
			if tc.nonTx != "" {
				require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: tc.nonTx, Labels: []string{"Test"}}}, "test"))
			}
			require.NoError(t, wal.Close())
			base, result, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(t.Context(), dir, ""))
			require.NoError(t, err)
			require.False(t, result.HasErrors())
			require.Equal(t, tc.committed, result.CommittedTransactions)
			require.Equal(t, tc.rolledBack, result.RolledBackTransactions)
			require.Equal(t, tc.aborted, result.AbortedTransactions)
			require.Equal(t, tc.nonTxApplied, result.NonTxApplied)
			engine := NewNamespacedEngine(base, "test")
			for id, value := range tc.present {
				node, err := engine.GetNode(t.Context(), id)
				require.NoError(t, err)
				require.NotNil(t, node)
				if value != "" {
					require.Equal(t, value, node.Properties["value"])
				}
			}
			for _, id := range tc.absent {
				node, err := engine.GetNode(t.Context(), id)
				require.ErrorIs(t, err, ErrNotFound)
				require.Nil(t, node)
			}
		})
	}
}

func TestRecoverWithTransactions_ErrorAccountingPaths(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	dir := t.TempDir()
	cfg := &WALConfig{Dir: dir, SyncMode: "immediate"}
	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)

	// Committed transaction with an operation that fails on replay (missing endpoint nodes).
	require.NoError(t, wal.AppendWithDatabase(OpTxBegin, WALTxData{TxID: "tx-failed-commit"}, "test"))
	require.NoError(t, wal.AppendWithDatabase(OpCreateEdge, WALEdgeData{
		Edge: &Edge{
			ID:        "failed-commit-edge",
			StartNode: "missing-left",
			EndNode:   "missing-right",
			Type:      "REL",
		},
		TxID: "tx-failed-commit",
	}, "test"))
	require.NoError(t, wal.AppendWithDatabase(OpTxCommit, WALTxData{TxID: "tx-failed-commit"}, "test"))

	// Incomplete transaction with an operation that cannot be undone cleanly.
	require.NoError(t, wal.AppendWithDatabase(OpTxBegin, WALTxData{TxID: "tx-incomplete-undo-err"}, "test"))
	require.NoError(t, wal.AppendWithDatabase(OpCreateEdge, WALEdgeData{
		Edge: &Edge{
			ID:        "missing-edge",
			StartNode: "missing-a",
			EndNode:   "missing-b",
			Type:      "REL",
		},
		TxID: "tx-incomplete-undo-err",
	}, "test"))
	// No commit/abort -> incomplete tx path.

	// Entry references unknown tx id (no TxBegin) -> treated as non-transactional.
	require.NoError(t, wal.AppendWithDatabase(OpUpdateNode, WALNodeData{
		Node: &Node{ID: "missing-orphan", Labels: []string{"Test"}},
		TxID: "tx-orphan",
	}, "test"))

	// Explicit non-transactional failure path.
	require.NoError(t, wal.AppendWithDatabase(OpDeleteNode, WALDeleteData{ID: "missing-delete"}, "test"))

	require.NoError(t, wal.Close())

	baseEngine, result, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(t.Context(), dir, ""))
	require.NoError(t, err)
	require.NotNil(t, baseEngine)

	require.Equal(t, 1, result.CommittedTransactions)
	require.Equal(t, 1, result.RolledBackTransactions)
	require.Equal(t, 2, result.NonTxApplied)

	require.Contains(t, result.FailedTransactions, "tx-failed-commit")
	require.NotEmpty(t, result.UndoErrors)
	require.NotEmpty(t, result.NonTxErrors)
	require.True(t, result.HasErrors())
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
	baseEngine := newTestMemoryEngine(t)
	engine := NewNamespacedEngine(baseEngine, "test")

	// Create nodes first
	_, err := engine.CreateNode(t.Context(), &Node{ID: "n1", Labels: []string{"Test"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "n2", Labels: []string{"Test"}})
	require.NoError(t, err)

	// Create edge via WAL
	edge := &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"}
	data, _ := json.Marshal(WALEdgeData{Edge: edge})
	entry := WALEntry{Sequence: 1, Operation: OpCreateEdge, Data: data, Checksum: crc32Checksum(data), Database: "test"}

	// Redo
	require.NoError(t, ReplayWALEntry(t.Context(), engine, entry))

	e, _ := engine.GetEdge(t.Context(), "e1")
	if e == nil {
		t.Fatal("Edge should exist after redo")
	}

	// Undo
	require.NoError(t, UndoWALEntry(t.Context(), engine, entry))

	e, _ = engine.GetEdge(t.Context(), "e1")
	if e != nil {
		t.Error("Edge should not exist after undo")
	}
}

// TestUndoDeleteEdge verifies edge deletion can be undone.
func TestUndoDeleteEdge(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := NewNamespacedEngine(baseEngine, "test")

	// Create nodes and edge
	_, err := engine.CreateNode(t.Context(), &Node{ID: "n1", Labels: []string{"Test"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "n2", Labels: []string{"Test"}})
	require.NoError(t, err)
	oldEdge := &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]interface{}{"since": 2020}}
	require.NoError(t, engine.CreateEdge(t.Context(), oldEdge))

	// Delete edge via WAL with before image
	data, _ := json.Marshal(WALDeleteData{ID: "e1", OldEdge: oldEdge})
	entry := WALEntry{Sequence: 2, Operation: OpDeleteEdge, Data: data, Checksum: crc32Checksum(data), Database: "test"}

	// Redo (delete)
	require.NoError(t, ReplayWALEntry(t.Context(), engine, entry))

	e, _ := engine.GetEdge(t.Context(), "e1")
	if e != nil {
		t.Fatal("Edge should be deleted after redo")
	}

	// Undo (restore)
	require.NoError(t, UndoWALEntry(t.Context(), engine, entry))

	e, _ = engine.GetEdge(t.Context(), "e1")
	if e == nil {
		t.Fatal("Edge should be restored after undo")
	}
	// Legacy JSON recovery preserves integer literals as int64.
	since, ok := e.Properties["since"].(int64)
	if !ok || since != 2020 {
		t.Errorf("Edge properties should be restored with since=2020, got %v", e.Properties)
	}
}

func TestUndoUpdateEdge(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := NewNamespacedEngine(baseEngine, "test")

	_, err := engine.CreateNode(t.Context(), &Node{ID: "n1", Labels: []string{"Test"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "n2", Labels: []string{"Test"}})
	require.NoError(t, err)

	oldEdge := &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]interface{}{"since": 2020}}
	require.NoError(t, engine.CreateEdge(t.Context(), oldEdge))
	newEdge := &Edge{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]interface{}{"since": 2024}}

	data, _ := json.Marshal(WALEdgeData{Edge: newEdge, OldEdge: oldEdge})
	entry := WALEntry{Sequence: 3, Operation: OpUpdateEdge, Data: data, Checksum: crc32Checksum(data), Database: "test"}

	require.NoError(t, ReplayWALEntry(t.Context(), engine, entry))
	updated, _ := engine.GetEdge(t.Context(), "e1")
	require.NotNil(t, updated)
	require.Equal(t, int64(2024), updated.Properties["since"])

	require.NoError(t, UndoWALEntry(t.Context(), engine, entry))
	restored, _ := engine.GetEdge(t.Context(), "e1")
	require.NotNil(t, restored)
	require.Equal(t, int64(2020), restored.Properties["since"])
}

func TestReplayAndUndoBulkWALEntries(t *testing.T) {
	nodes := []*Node{
		{ID: "n1", Labels: []string{"Test"}},
		{ID: "n2", Labels: []string{"Test"}},
	}
	edges := []*Edge{
		{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"},
	}

	t.Run("bulk create nodes then undo", func(t *testing.T) {
		baseEngine := newTestMemoryEngine(t)
		engine := NewNamespacedEngine(baseEngine, "test")
		data, _ := json.Marshal(WALBulkNodesData{Nodes: nodes})
		entry := WALEntry{Operation: OpBulkNodes, Data: data, Checksum: crc32Checksum(data), Database: "test"}
		require.NoError(t, ReplayWALEntry(t.Context(), engine, entry))
		require.NotNil(t, mustReadNode(t, engine.GetNode, "n1"))
		require.NoError(t, UndoWALEntry(t.Context(), engine, entry))
		n, _ := engine.GetNode(t.Context(), "n1")
		require.Nil(t, n)
	})

	t.Run("bulk create edges then undo", func(t *testing.T) {
		baseEngine := newTestMemoryEngine(t)
		engine := NewNamespacedEngine(baseEngine, "test")
		for _, n := range nodes {
			_, err := engine.CreateNode(t.Context(), n)
			require.NoError(t, err)
		}
		data, _ := json.Marshal(WALBulkEdgesData{Edges: edges})
		entry := WALEntry{Operation: OpBulkEdges, Data: data, Checksum: crc32Checksum(data), Database: "test"}
		require.NoError(t, ReplayWALEntry(t.Context(), engine, entry))
		e, _ := engine.GetEdge(t.Context(), "e1")
		require.NotNil(t, e)
		require.NoError(t, UndoWALEntry(t.Context(), engine, entry))
		e, _ = engine.GetEdge(t.Context(), "e1")
		require.Nil(t, e)
	})

	t.Run("bulk delete nodes then undo", func(t *testing.T) {
		baseEngine := newTestMemoryEngine(t)
		engine := NewNamespacedEngine(baseEngine, "test")
		for _, n := range nodes {
			_, err := engine.CreateNode(t.Context(), n)
			require.NoError(t, err)
		}
		data, _ := json.Marshal(WALBulkDeleteNodesData{IDs: []string{"n1", "n2"}, OldNodes: nodes})
		entry := WALEntry{Operation: OpBulkDeleteNodes, Data: data, Checksum: crc32Checksum(data), Database: "test"}
		require.NoError(t, ReplayWALEntry(t.Context(), engine, entry))
		n, _ := engine.GetNode(t.Context(), "n1")
		require.Nil(t, n)
		require.NoError(t, UndoWALEntry(t.Context(), engine, entry))
		require.NotNil(t, mustReadNode(t, engine.GetNode, "n1"))
	})

	t.Run("bulk delete edges then undo", func(t *testing.T) {
		baseEngine := newTestMemoryEngine(t)
		engine := NewNamespacedEngine(baseEngine, "test")
		for _, n := range nodes {
			_, err := engine.CreateNode(t.Context(), n)
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(t.Context(), edges[0]))
		data, _ := json.Marshal(WALBulkDeleteEdgesData{IDs: []string{"e1"}, OldEdges: edges})
		entry := WALEntry{Operation: OpBulkDeleteEdges, Data: data, Checksum: crc32Checksum(data), Database: "test"}
		require.NoError(t, ReplayWALEntry(t.Context(), engine, entry))
		e, _ := engine.GetEdge(t.Context(), "e1")
		require.Nil(t, e)
		require.NoError(t, UndoWALEntry(t.Context(), engine, entry))
		e, _ = engine.GetEdge(t.Context(), "e1")
		require.NotNil(t, e)
	})
}

func TestReplayAndUndoWALEntry_ErrorBranches(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := NewNamespacedEngine(baseEngine, "test")

	t.Run("marker operations are noops", func(t *testing.T) {
		for _, op := range []OperationType{OpCheckpoint, OpTxBegin, OpTxCommit, OpTxAbort} {
			require.NoError(t, ReplayWALEntry(t.Context(), engine, WALEntry{Operation: op}))
			require.NoError(t, UndoWALEntry(t.Context(), engine, WALEntry{Operation: op}))
		}
		require.NoError(t, UndoWALEntry(t.Context(), engine, WALEntry{Operation: OpUpdateEmbedding}))
	})

	t.Run("unknown operations return errors", func(t *testing.T) {
		require.Error(t, ReplayWALEntry(t.Context(), engine, WALEntry{Operation: OperationType("UNKNOWN")}))
		require.Error(t, UndoWALEntry(t.Context(), engine, WALEntry{Operation: OperationType("UNKNOWN")}))
	})

	t.Run("malformed payloads return errors", func(t *testing.T) {
		for _, entry := range []WALEntry{
			{Operation: OpCreateEdge, Data: []byte("{bad"), Database: "test"},
			{Operation: OpDeleteEdge, Data: []byte("{bad"), Database: "test"},
			{Operation: OpBulkNodes, Data: []byte("{bad"), Database: "test"},
			{Operation: OpBulkEdges, Data: []byte("{bad"), Database: "test"},
			{Operation: OpBulkDeleteNodes, Data: []byte("{bad"), Database: "test"},
			{Operation: OpBulkDeleteEdges, Data: []byte("{bad"), Database: "test"},
		} {
			require.Error(t, ReplayWALEntry(t.Context(), engine, entry))
			require.Error(t, UndoWALEntry(t.Context(), engine, entry))
		}
	})

	t.Run("bulk delete undo requires before image", func(t *testing.T) {
		data, _ := json.Marshal(WALBulkDeleteNodesData{IDs: []string{"n1"}})
		err := UndoWALEntry(t.Context(), engine, WALEntry{Operation: OpBulkDeleteNodes, Data: data, Database: "test"})
		require.ErrorIs(t, err, ErrNoUndoData)

		data, _ = json.Marshal(WALBulkDeleteEdgesData{IDs: []string{"e1"}})
		err = UndoWALEntry(t.Context(), engine, WALEntry{Operation: OpBulkDeleteEdges, Data: data, Database: "test"})
		require.ErrorIs(t, err, ErrNoUndoData)
	})

	t.Run("single entity undo requires before image", func(t *testing.T) {
		data, _ := json.Marshal(WALDeleteData{ID: "n1"})
		err := UndoWALEntry(t.Context(), engine, WALEntry{Operation: OpDeleteNode, Data: data, Database: "test"})
		require.ErrorIs(t, err, ErrNoUndoData)

		data, _ = json.Marshal(WALEdgeData{Edge: nil})
		err = UndoWALEntry(t.Context(), engine, WALEntry{Operation: OpCreateEdge, Data: data, Database: "test"})
		require.ErrorIs(t, err, ErrNoUndoData)

		data, _ = json.Marshal(WALEdgeData{OldEdge: nil})
		err = UndoWALEntry(t.Context(), engine, WALEntry{Operation: OpUpdateEdge, Data: data, Database: "test"})
		require.ErrorIs(t, err, ErrNoUndoData)

		data, _ = json.Marshal(WALDeleteData{ID: "e1"})
		err = UndoWALEntry(t.Context(), engine, WALEntry{Operation: OpDeleteEdge, Data: data, Database: "test"})
		require.ErrorIs(t, err, ErrNoUndoData)
	})
}

func TestRecoverWithTransactions_ErrorBranches(t *testing.T) {
	config.EnableWAL()
	defer config.DisableWAL()

	t.Run("invalid snapshot returns load error", func(t *testing.T) {
		requireRecoveryStoresClosed(t)

		dir := t.TempDir()
		snapshotPath := filepath.Join(dir, "snapshot.json")
		require.NoError(t, os.WriteFile(snapshotPath, []byte("{bad-json"), 0644))

		_, _, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(t.Context(), filepath.Join(dir, "wal"), snapshotPath))
		require.ErrorContains(t, err, "failed to load snapshot")
	})

	t.Run("invalid wal manifest returns read error", func(t *testing.T) {
		requireRecoveryStoresClosed(t)

		dir := t.TempDir()
		{
			_, manifestErr := accessWALManifest(dir, walPublishManifest, &WALManifest{
				Version: walManifestVersion,
				Segments: []WALSegment{
					{FirstSeq: 1, LastSeq: 1, Path: "../bad-segment.wal"},
				},
			})
			require.NoError(t, manifestErr)
		}

		_, _, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(t.Context(), dir, ""))
		require.ErrorContains(t, err, "failed to read WAL")
	})

	t.Run("snapshot restore succeeds with namespaced replay base", func(t *testing.T) {
		requireRecoveryStoresClosed(t)

		dir := t.TempDir()
		snapshotPath := filepath.Join(dir, "snapshot.json")
		require.NoError(t, SaveSnapshot(&Snapshot{
			Sequence: 7,
			Nodes: []*Node{
				{ID: "tx-snap-n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "n1"}},
				{ID: "tx-snap-n2", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "n2"}},
			},
			Edges: []*Edge{
				{ID: "tx-snap-e1", StartNode: "tx-snap-n1", EndNode: "tx-snap-n2", Type: "LINKS"},
			},
			Version: "1.0",
		}, snapshotPath))

		baseEngine, result, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(t.Context(), filepath.Join(dir, "wal"), snapshotPath))
		require.NoError(t, err)
		require.NotNil(t, baseEngine)
		require.Equal(t, uint64(7), result.SnapshotSeq)

		ns := NewNamespacedEngine(baseEngine, "nornic")
		node, getErr := ns.GetNode(t.Context(), "tx-snap-n1")
		require.NoError(t, getErr)
		require.NotNil(t, node)
		edge, getErr := ns.GetEdge(t.Context(), "tx-snap-e1")
		require.NoError(t, getErr)
		require.NotNil(t, edge)
	})

	t.Run("snapshot restore returns node restore error", func(t *testing.T) {
		requireRecoveryStoresClosed(t)

		dir := t.TempDir()
		snapshotPath := filepath.Join(dir, "snapshot-dupe.json")
		// The current writer rejects nil entities. Keep the legacy loader's
		// restore-error coverage using an explicit pre-upgrade JSON fixture.
		legacy, err := json.Marshal(&Snapshot{
			Sequence: 1, Nodes: []*Node{nil}, Version: "1.0",
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(snapshotPath, legacy, 0600))

		_, _, err = fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(t.Context(), filepath.Join(dir, "wal"), snapshotPath))
		require.ErrorContains(t, err, "failed to restore nodes")
	})

	t.Run("snapshot restore returns edge restore error", func(t *testing.T) {
		requireRecoveryStoresClosed(t)

		dir := t.TempDir()
		snapshotPath := filepath.Join(dir, "snapshot-bad-edge.json")
		require.NoError(t, SaveSnapshot(&Snapshot{
			Sequence: 1,
			Nodes:    []*Node{},
			Edges: []*Edge{
				{ID: "e-missing", StartNode: "missing-a", EndNode: "missing-b", Type: "REL"},
			},
			Version: "1.0",
		}, snapshotPath))

		_, _, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(t.Context(), filepath.Join(dir, "wal"), snapshotPath))
		require.ErrorContains(t, err, "failed to restore edges")
	})
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
