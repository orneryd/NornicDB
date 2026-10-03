package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestArchivePrimaryIntoMVCCVersionInTxn_NodeAndEdge(t *testing.T) {
	engine := newTestEngine(t)

	node := &Node{ID: "tenant:n1", Labels: []string{"L"}, Properties: map[string]any{"k": "v"}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	edge := &Edge{ID: "tenant:e1", StartNode: "tenant:n1", EndNode: "tenant:n1", Type: "SELF", Properties: map[string]any{"w": int64(1)}}
	err = engine.CreateEdge(edge)
	require.NoError(t, err)

	v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 11}
	engine.activeMVCCSnapshotReaders.Store(1)
	defer engine.activeMVCCSnapshotReaders.Store(0)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		require.NoError(t, engine.archiveNodePrimaryIntoMVCCVersionInTxn(txn, node.ID, v))
		require.NoError(t, engine.archiveEdgePrimaryIntoMVCCVersionInTxn(txn, edge.ID, v))
		return nil
	}))

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.loadNodeMVCCRecordExactInTxn(txn, node.ID, v)
		require.NoError(t, err)
		_, err = engine.loadEdgeMVCCRecordExactInTxn(txn, edge.ID, v)
		require.NoError(t, err)
		return nil
	}))
}

func TestArchivePrimaryIntoMVCCVersionInTxn_NoHistoryOrMissing_NoOp(t *testing.T) {
	engine := newTestEngine(t)
	v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 7}

	// Head-only retention and no active readers => archive short-circuits.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		require.NoError(t, engine.archiveNodePrimaryIntoMVCCVersionInTxn(txn, "tenant:missing", v))
		require.NoError(t, engine.archiveEdgePrimaryIntoMVCCVersionInTxn(txn, "tenant:missing", v))
		return nil
	}))
}

func TestMaterializeMVCCCommitInTxn_CoversOperationSwitch(t *testing.T) {
	engine := newTestEngine(t)
	engine.activeMVCCSnapshotReaders.Store(1)
	defer engine.activeMVCCSnapshotReaders.Store(0)

	// Seed entities for update/delete paths.
	_, err := engine.CreateNode(&Node{ID: "tenant:u", Labels: []string{"L"}, Properties: map[string]any{"p": "old"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:d", Labels: []string{"L"}, Properties: map[string]any{"p": "gone"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:a", Labels: []string{"L"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:b", Labels: []string{"L"}})
	require.NoError(t, err)

	err = engine.CreateEdge(&Edge{ID: "tenant:eu", StartNode: "tenant:a", EndNode: "tenant:b", Type: "R", Properties: map[string]any{"p": "old"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "tenant:ed", StartNode: "tenant:a", EndNode: "tenant:b", Type: "R", Properties: map[string]any{"p": "gone"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "tenant:child", StartNode: "tenant:d", EndNode: "tenant:b", Type: "R"})
	require.NoError(t, err)

	version := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(time.Second), CommitSequence: 101}
	opList := []Operation{
		{Type: OpCreateNode, Node: &Node{ID: "tenant:c-fresh", Labels: []string{"L"}, Properties: map[string]any{"p": "x"}}, FreshID: true},
		{Type: OpCreateNode, Node: &Node{ID: "tenant:c-existing", Labels: []string{"L"}, Properties: map[string]any{"p": "x"}}, FreshID: false},
		{Type: OpUpdateNode, Node: &Node{ID: "tenant:u", Labels: []string{"L"}, Properties: map[string]any{"p": "new"}}, OldNode: &Node{ID: "tenant:u", Labels: []string{"L"}, Properties: map[string]any{"p": "old"}}},
		{Type: OpDeleteNode, NodeID: "tenant:d", OldNode: &Node{ID: "tenant:d", Labels: []string{"L"}, Properties: map[string]any{"p": "gone"}}, DeletedEdgeIDs: []EdgeID{"tenant:child"}},
		{Type: OpCreateEdge, Edge: &Edge{ID: "tenant:e-fresh", StartNode: "tenant:a", EndNode: "tenant:b", Type: "R"}, FreshID: true},
		{Type: OpCreateEdge, Edge: &Edge{ID: "tenant:e-existing", StartNode: "tenant:a", EndNode: "tenant:b", Type: "R"}, FreshID: false},
		{Type: OpUpdateEdge, Edge: &Edge{ID: "tenant:eu", StartNode: "tenant:a", EndNode: "tenant:b", Type: "R", Properties: map[string]any{"p": "new"}}, OldEdge: &Edge{ID: "tenant:eu", StartNode: "tenant:a", EndNode: "tenant:b", Type: "R", Properties: map[string]any{"p": "old"}}},
		{Type: OpDeleteEdge, EdgeID: "tenant:ed", OldEdge: &Edge{ID: "tenant:ed", StartNode: "tenant:a", EndNode: "tenant:b", Type: "R", Properties: map[string]any{"p": "gone"}}},
	}

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return engine.materializeMVCCCommit(singleBatchWriter(txn), version, opList)
	}))

	head, err := engine.GetNodeCurrentHead("tenant:u")
	require.NoError(t, err)
	require.Equal(t, uint64(101), head.Version.CommitSequence)

	delHead, err := engine.GetNodeCurrentHead("tenant:d")
	require.NoError(t, err)
	require.True(t, delHead.Tombstoned)

	eHead, err := engine.GetEdgeCurrentHead("tenant:eu")
	require.NoError(t, err)
	require.Equal(t, uint64(101), eHead.Version.CommitSequence)

	edHead, err := engine.GetEdgeCurrentHead("tenant:ed")
	require.NoError(t, err)
	require.True(t, edHead.Tombstoned)
}
