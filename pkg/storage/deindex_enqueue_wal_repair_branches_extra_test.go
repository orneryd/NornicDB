package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerDeindexEnqueue_NodeEdgeBranchCoverage(t *testing.T) {
	engine := createTestBadgerEngine(t)
	engine.SetDecayEnabled(true)

	t.Run("evaluate node suppression handles missing and corrupt node payload", func(t *testing.T) {
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			evaluation1, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: "test:missing", isEdge: false})
			changed, node, edge := evaluation1.suppressed, evaluation1.node, evaluation1.edge
			require.NoError(t, err)
			require.False(t, changed)
			require.Nil(t, node)
			require.Nil(t, edge)
			return nil
		}))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			const badNodeID = NodeID("test:bad-node")
			require.NoError(t, txn.Set(nodeKey(badNodeID), []byte("not-a-node")))
			evaluation2, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: string(badNodeID), isEdge: false})
			changed, node, edge := evaluation2.suppressed, evaluation2.node, evaluation2.edge
			require.NoError(t, err)
			require.False(t, changed)
			require.Nil(t, node)
			require.Nil(t, edge)
			return nil
		}))
	})

	t.Run("evaluate node suppression clears stale flag when node becomes visible", func(t *testing.T) {
		node := &Node{ID: NodeID("test:node-unsuppress"), Labels: []string{"L"}, VisibilitySuppressed: true}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			evaluation3, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: string(node.ID), isEdge: false})
			changed, node, edge := evaluation3.suppressed, evaluation3.node, evaluation3.edge
			require.NoError(t, err)
			require.False(t, changed)
			require.NotNil(t, node, "the cleared node is the transition's mutation")
			require.False(t, node.VisibilitySuppressed)
			require.Nil(t, edge)
			return nil
		}))

		// This test calls a transaction-local helper directly, bypassing the
		// public mutation/cache lifecycle. Assert its persisted state.
		suppressed, _, err := engine.readNodeSuppressionState(node.ID)
		require.NoError(t, err)
		require.False(t, suppressed)
	})

	t.Run("evaluate edge suppression handles missing/corrupt edge and clears stale flag", func(t *testing.T) {
		_, err := engine.CreateNode(&Node{ID: NodeID("test:edge-a")})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: NodeID("test:edge-b")})
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			evaluation4, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: "test:missing-edge", isEdge: true})
			changed, node, edge := evaluation4.suppressed, evaluation4.node, evaluation4.edge
			require.NoError(t, err)
			require.False(t, changed)
			require.Nil(t, node)
			require.Nil(t, edge)
			return nil
		}))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			const badEdgeID = EdgeID("test:bad-edge")
			require.NoError(t, txn.Set(edgeKey(badEdgeID), []byte("not-an-edge")))
			evaluation5, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: string(badEdgeID), isEdge: true})
			changed, node, edge := evaluation5.suppressed, evaluation5.node, evaluation5.edge
			require.NoError(t, err)
			require.False(t, changed)
			require.Nil(t, node)
			require.Nil(t, edge)
			return nil
		}))

		edge := &Edge{
			ID:                   EdgeID("test:edge-unsuppress"),
			StartNode:            NodeID("test:edge-a"),
			EndNode:              NodeID("test:edge-b"),
			Type:                 "REL",
			VisibilitySuppressed: true,
		}
		require.NoError(t, engine.CreateEdge(edge))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			evaluation6, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: string(edge.ID), isEdge: true})
			changed, node, edge := evaluation6.suppressed, evaluation6.node, evaluation6.edge
			require.NoError(t, err)
			require.False(t, changed)
			require.Nil(t, node)
			require.NotNil(t, edge, "the cleared edge is the transition's mutation")
			require.False(t, edge.VisibilitySuppressed)
			return nil
		}))

		got, err := engine.GetEdge(edge.ID)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.False(t, got.VisibilitySuppressed)
	})

	t.Run("evaluate node suppression swallows a missing entity on the edge flag", func(t *testing.T) {
		// The entity flag selects the key space and decoder; a node ID
		// evaluated as an edge finds no edge body and decides nothing.
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			evaluation7, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: "test:node-unsuppress", isEdge: true})
			changed, node, edge := evaluation7.suppressed, evaluation7.node, evaluation7.edge
			require.NoError(t, err)
			require.False(t, changed)
			require.Nil(t, node)
			require.Nil(t, edge)
			return nil
		}))
	})

	t.Run("rescore suppression after label change branch paths", func(t *testing.T) {
		node := &Node{ID: NodeID("test:rescore"), Labels: []string{"L"}, VisibilitySuppressed: true}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		engine.SetDecayEnabled(false)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			evaluation8, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{staged: true, node: node})
			rewritten := evaluation8.written
			require.Nil(t, rewritten, "decay disabled rewrites nothing")
			return err
		}))

		engine.SetDecayEnabled(true)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			evaluation9, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{staged: true, node: nil})
			rewritten := evaluation9.written
			require.NoError(t, err)
			require.Nil(t, rewritten)
			evaluation10, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{staged: true, node: node})
			rewritten = evaluation10.written
			require.NotEmpty(t, rewritten, "the rescored body is returned")
			return err
		}))

		// This test calls a transaction-local helper directly, bypassing the
		// public mutation/cache lifecycle. Assert its persisted state.
		suppressed, _, err := engine.readNodeSuppressionState(node.ID)
		require.NoError(t, err)
		require.False(t, suppressed)

		node2 := &Node{ID: NodeID("test:rescore-no-change"), Labels: []string{"L"}}
		_, err = engine.CreateNode(node2)
		require.NoError(t, err)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			evaluation11, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{staged: true, node: node2})
			rewritten := evaluation11.written
			require.Nil(t, rewritten, "an unchanged flag keeps the written body")
			return err
		}))
	})
}

func TestWALRepair_ErrorBranches(t *testing.T) {
	t.Run("repair handles open and header-read failures", func(t *testing.T) {
		skipIfPermissionsBypassed(t)
		walPath := filepath.Join(t.TempDir(), "wal.log")
		require.NoError(t, os.WriteFile(walPath, []byte{0x57, 0x41, 0x4c, 0x31}, 0o600))
		require.NoError(t, os.Chmod(walPath, 0o000))

		_, _, err := repairWALTailIfNeeded(walPath, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "open failed")
	})

	t.Run("repair handles non-EOF header read failure", func(t *testing.T) {
		dirAsWal := t.TempDir()
		_, _, err := repairWALTailIfNeeded(dirAsWal, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "header read failed")
	})

	t.Run("scan fails when file handle is closed", func(t *testing.T) {
		walPath := filepath.Join(t.TempDir(), "wal.log")
		require.NoError(t, os.WriteFile(walPath, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, 0o600))

		f, err := os.Open(walPath)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		_, _, _, _, err = scanAtomicWALForRepair(f, 9)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read header failed")
	})

	t.Run("truncate reports open failure for directory path", func(t *testing.T) {
		dir := t.TempDir()
		repaired, diag, err := truncateWALFile(dir, 0, 7, "crc_mismatch", nil)
		require.Error(t, err)
		require.False(t, repaired)
		require.NotNil(t, diag)
		require.Contains(t, err.Error(), "open(truncate) failed")
	})
}
