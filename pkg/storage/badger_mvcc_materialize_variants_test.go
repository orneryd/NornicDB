package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerMVCC_MaterializeCommit_VariantBranches(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	engine.activeMVCCSnapshotReaders.Store(1)
	defer engine.activeMVCCSnapshotReaders.Store(0)

	// Seed base entities.
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}, Properties: map[string]any{"v": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"w": int64(1)}}))

	v := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(2 * time.Second), CommitSequence: 5001}

	// Seed explicit tombstoned heads so update/delete branches take the
	// "head exists but tombstoned" path (skip archive body).
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		if err := engine.writeNodeMVCCHeadWithFloorInTxn(txn, "test:tomb-node", v, true, v); err != nil {
			return err
		}
		if err := engine.writeEdgeMVCCHeadWithFloorInTxn(txn, "test:tomb-edge", v, true, v); err != nil {
			return err
		}
		return nil
	}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		ops := []Operation{
			// create path variants
			{Type: OpCreateNode, Node: &Node{ID: "test:c-fresh", Labels: []string{"N"}}, FreshID: true},
			{Type: OpCreateNode, Node: &Node{ID: "test:c-plain", Labels: []string{"N"}}, FreshID: false},
			// update variants (head missing/tombstoned with retainsHistory=true)
			{Type: OpUpdateNode, Node: &Node{ID: "test:missing-head", Labels: []string{"N"}}, OldNode: &Node{ID: "test:missing-head", Labels: []string{"N"}}},
			{Type: OpUpdateNode, Node: &Node{ID: "test:tomb-node", Labels: []string{"N"}}, OldNode: &Node{ID: "test:tomb-node", Labels: []string{"N"}}},
			// delete variants
			{Type: OpDeleteNode, NodeID: "test:missing-del-head", OldNode: &Node{ID: "test:missing-del-head", Labels: []string{"N"}}, DeletedEdgeIDs: []EdgeID{"test:child-del"}},
			{Type: OpDeleteNode, NodeID: "test:tomb-node", OldNode: &Node{ID: "test:tomb-node", Labels: []string{"N"}}},
			// edge create/update/delete variants
			{Type: OpCreateEdge, Edge: &Edge{ID: "test:e-fresh", StartNode: "test:a", EndNode: "test:b", Type: "R"}, FreshID: true},
			{Type: OpCreateEdge, Edge: &Edge{ID: "test:e-plain", StartNode: "test:a", EndNode: "test:b", Type: "R"}, FreshID: false},
			{Type: OpUpdateEdge, Edge: &Edge{ID: "test:missing-edge-head", StartNode: "test:a", EndNode: "test:b", Type: "R"}, OldEdge: &Edge{ID: "test:missing-edge-head", StartNode: "test:a", EndNode: "test:b", Type: "R"}},
			{Type: OpUpdateEdge, Edge: &Edge{ID: "test:tomb-edge", StartNode: "test:a", EndNode: "test:b", Type: "R"}, OldEdge: &Edge{ID: "test:tomb-edge", StartNode: "test:a", EndNode: "test:b", Type: "R"}},
			{Type: OpDeleteEdge, EdgeID: "test:missing-edge-del-head", OldEdge: &Edge{ID: "test:missing-edge-del-head", StartNode: "test:a", EndNode: "test:b", Type: "R"}},
			{Type: OpDeleteEdge, EdgeID: "test:tomb-edge", OldEdge: &Edge{ID: "test:tomb-edge", StartNode: "test:a", EndNode: "test:b", Type: "R"}},
		}
		return engine.materializeMVCCCommitInTxn(txn, v, ops)
	}))

	_, err = engine.GetNodeCurrentHead("test:c-fresh")
	require.NoError(t, err)
	_, err = engine.GetNodeCurrentHead("test:c-plain")
	require.NoError(t, err)
	_, err = engine.GetEdgeCurrentHead("test:e-fresh")
	require.NoError(t, err)
	_, err = engine.GetEdgeCurrentHead("test:e-plain")
	require.NoError(t, err)
}

func TestBadgerMVCC_MaterializeCommit_HeadDecodeErrorsOnDeletePaths(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	engine.activeMVCCSnapshotReaders.Store(1)
	defer engine.activeMVCCSnapshotReaders.Store(0)

	_, err := engine.CreateNode(&Node{ID: "test:n-bad", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2-bad", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e-bad", StartNode: "test:n-bad", EndNode: "test:n2-bad", Type: "R"}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		nh, err := engine.mvccNodeHeadKeyString(txn, "test:n-bad")
		if err != nil {
			return err
		}
		eh, err := engine.mvccEdgeHeadKeyString(txn, "test:e-bad")
		if err != nil {
			return err
		}
		if err := txn.Set(nh, []byte("bad-head")); err != nil {
			return err
		}
		return txn.Set(eh, []byte("bad-head"))
	}))

	v := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(3 * time.Second), CommitSequence: 6001}

	err = engine.withUpdate(func(txn *badger.Txn) error {
		ops := []Operation{{Type: OpDeleteNode, NodeID: "test:n-bad", OldNode: &Node{ID: "test:n-bad", Labels: []string{"N"}}}}
		return engine.materializeMVCCCommitInTxn(txn, v, ops)
	})
	require.Error(t, err)

	err = engine.withUpdate(func(txn *badger.Txn) error {
		ops := []Operation{{Type: OpDeleteEdge, EdgeID: "test:e-bad", OldEdge: &Edge{ID: "test:e-bad", StartNode: "test:n-bad", EndNode: "test:n2-bad", Type: "R"}}}
		return engine.materializeMVCCCommitInTxn(txn, v, ops)
	})
	require.Error(t, err)
}

func TestBadgerMVCC_MaterializeCommit_ReadOnlyTxnWriteFailures(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	for _, nodeID := range []NodeID{"test:ro-a", "test:ro-b", "test:ro-c"} {
		_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:ro-edge", StartNode: "test:ro-a", EndNode: "test:ro-b", Type: "R"}))
	oldEdge, err := engine.GetEdge("test:ro-edge")
	require.NoError(t, err)

	v := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(4 * time.Second), CommitSequence: 7001}
	readTxn := engine.db.NewTransaction(false)
	defer readTxn.Discard()

	err = engine.materializeMVCCCommitInTxn(readTxn, v, []Operation{{Type: OpCreateEdge, Edge: &Edge{ID: "test:ro-create", StartNode: "test:ro-a", EndNode: "test:ro-b", Type: "R"}, FreshID: true}})
	require.Error(t, err)

	err = engine.materializeMVCCCommitInTxn(readTxn, v, []Operation{{Type: OpUpdateEdge, Edge: &Edge{ID: "test:ro-edge", StartNode: "test:ro-a", EndNode: "test:ro-c", Type: "R"}, OldEdge: oldEdge}})
	require.Error(t, err)

	err = engine.materializeMVCCCommitInTxn(readTxn, v, []Operation{{Type: OpDeleteEdge, EdgeID: "test:ro-edge", OldEdge: oldEdge}})
	require.Error(t, err)

	err = engine.materializeMVCCCommitInTxn(readTxn, v, []Operation{{Type: OpDeleteNode, NodeID: "test:ro-b", OldNode: &Node{ID: "test:ro-b", Labels: []string{"N"}}, DeletedEdgeIDs: []EdgeID{"test:ro-edge"}}})
	require.Error(t, err)
}
