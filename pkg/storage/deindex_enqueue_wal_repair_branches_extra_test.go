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
			changed, _, err := engine.evaluateNodeSuppressionInTxn(txn, NodeID("test:missing"))
			require.NoError(t, err)
			require.False(t, changed)
			return nil
		}))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			const badNodeID = NodeID("test:bad-node")
			require.NoError(t, txn.Set(nodeKey(badNodeID), []byte("not-a-node")))
			changed, _, err := engine.evaluateNodeSuppressionInTxn(txn, badNodeID)
			require.NoError(t, err)
			require.False(t, changed)
			return nil
		}))
	})

	t.Run("evaluate node suppression clears stale flag when node becomes visible", func(t *testing.T) {
		node := &Node{ID: NodeID("test:node-unsuppress"), Labels: []string{"L"}, VisibilitySuppressed: true}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			changed, _, err := engine.evaluateNodeSuppressionInTxn(txn, node.ID)
			require.NoError(t, err)
			require.False(t, changed)
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
			changed, _, err := engine.evaluateEdgeSuppressionInTxn(txn, EdgeID("test:missing-edge"))
			require.NoError(t, err)
			require.False(t, changed)
			return nil
		}))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			const badEdgeID = EdgeID("test:bad-edge")
			require.NoError(t, txn.Set(edgeKey(badEdgeID), []byte("not-an-edge")))
			changed, _, err := engine.evaluateEdgeSuppressionInTxn(txn, badEdgeID)
			require.NoError(t, err)
			require.False(t, changed)
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
			changed, _, err := engine.evaluateEdgeSuppressionInTxn(txn, edge.ID)
			require.NoError(t, err)
			require.False(t, changed)
			return nil
		}))

		got, err := engine.GetEdge(edge.ID)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.False(t, got.VisibilitySuppressed)
	})

	t.Run("rescore suppression after label change branch paths", func(t *testing.T) {
		node := &Node{ID: NodeID("test:rescore"), Labels: []string{"L"}, VisibilitySuppressed: true}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		engine.SetDecayEnabled(false)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return engine.rescoreSuppressionAfterLabelChangeInTxn(txn, "test", node)
		}))

		engine.SetDecayEnabled(true)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			require.NoError(t, engine.rescoreSuppressionAfterLabelChangeInTxn(txn, "test", nil))
			return engine.rescoreSuppressionAfterLabelChangeInTxn(txn, "test", node)
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
			return engine.rescoreSuppressionAfterLabelChangeInTxn(txn, "test", node2)
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
