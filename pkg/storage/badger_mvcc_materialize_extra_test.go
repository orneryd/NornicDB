package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_MVCCArchiveHelpers_Branches(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	a := NodeID(prefixTestID("mvcc-arch-a"))
	b := NodeID(prefixTestID("mvcc-arch-b"))
	_, err := engine.CreateNode(&Node{ID: a, Labels: []string{"N"}, Properties: map[string]any{"k": "v"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: b, Labels: []string{"N"}})
	require.NoError(t, err)

	eid := EdgeID(prefixTestID("mvcc-arch-e"))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:         eid,
		StartNode:  a,
		EndNode:    b,
		Type:       "REL",
		Properties: map[string]any{"w": int64(1)},
	}))

	nodeHead, err := engine.GetNodeCurrentHead(a)
	require.NoError(t, err)
	edgeHead, err := engine.GetEdgeCurrentHead(eid)
	require.NoError(t, err)

	nodeBody, err := engine.GetNode(a)
	require.NoError(t, err)
	edgeBody, err := engine.GetEdge(eid)
	require.NoError(t, err)

	// Exercise archive no-op branches (existing version, missing key, nil body).
	err = engine.withUpdate(func(txn *badger.Txn) error {
		require.NoError(t, engine.archiveNodePrimaryIntoMVCCVersionInTxn(txn, a, nodeHead.Version))
		require.NoError(t, engine.archiveNodePrimaryIntoMVCCVersionInTxn(txn, a, nodeHead.Version))
		require.NoError(t, engine.archiveNodePrimaryIntoMVCCVersionInTxn(txn, NodeID(prefixTestID("mvcc-arch-missing-node")), nodeHead.Version))

		require.NoError(t, engine.archiveEdgePrimaryIntoMVCCVersionInTxn(txn, eid, edgeHead.Version))
		require.NoError(t, engine.archiveEdgePrimaryIntoMVCCVersionInTxn(txn, eid, edgeHead.Version))
		require.NoError(t, engine.archiveEdgePrimaryIntoMVCCVersionInTxn(txn, EdgeID(prefixTestID("mvcc-arch-missing-edge")), edgeHead.Version))

		require.NoError(t, engine.archiveNodeBodyInTxn(txn, a, nil, nodeHead.Version))
		require.NoError(t, engine.archiveNodeBodyInTxn(txn, a, nodeBody, nodeHead.Version))
		require.NoError(t, engine.archiveEdgeBodyInTxn(txn, eid, nil, edgeHead.Version))
		require.NoError(t, engine.archiveEdgeBodyInTxn(txn, eid, edgeBody, edgeHead.Version))
		return nil
	})
	require.NoError(t, err)

	// Tombstoned heads should short-circuit archiveOnUpdate.
	err = engine.withUpdate(func(txn *badger.Txn) error {
		tombV := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1001}
		require.NoError(t, engine.writeNodeMVCCHeadInTxn(txn, a, tombV, true))
		require.NoError(t, engine.writeEdgeMVCCHeadInTxn(txn, eid, tombV, true))
		require.NoError(t, engine.archiveNodeOnUpdateInTxn(txn, a))
		require.NoError(t, engine.archiveEdgeOnUpdateInTxn(txn, eid))
		return nil
	})
	require.NoError(t, err)

	// Malformed existing head should propagate decode errors from load*Head.
	err = engine.withUpdate(func(txn *badger.Txn) error {
		badNodeID := NodeID(prefixTestID("mvcc-bad-head-node"))
		badEdgeID := EdgeID(prefixTestID("mvcc-bad-head-edge"))
		nodeHeadKey, keyErr := engine.mvccNodeHeadKeyString(txn, badNodeID)
		require.NoError(t, keyErr)
		edgeHeadKey, keyErr := engine.mvccEdgeHeadKeyString(txn, badEdgeID)
		require.NoError(t, keyErr)
		require.NoError(t, txn.Set(nodeHeadKey, []byte{0x01}))
		require.NoError(t, txn.Set(edgeHeadKey, []byte{0x01}))
		return nil
	})
	require.NoError(t, err)

	err = engine.withUpdate(func(txn *badger.Txn) error {
		v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 2001}
		require.Error(t, engine.writeNodeMVCCHeadInTxn(txn, NodeID(prefixTestID("mvcc-bad-head-node")), v, false))
		require.Error(t, engine.writeEdgeMVCCHeadInTxn(txn, EdgeID(prefixTestID("mvcc-bad-head-edge")), v, false))
		return nil
	})
	require.NoError(t, err)
}

func TestBadgerEngine_MaterializeMVCCCommitInTxn_OperationMatrix(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	n1 := NodeID(prefixTestID("mvcc-mat-n1"))
	n2 := NodeID(prefixTestID("mvcc-mat-n2"))
	nDel := NodeID(prefixTestID("mvcc-mat-ndel"))
	_, err := engine.CreateNode(&Node{ID: n1, Labels: []string{"N"}, Properties: map[string]any{"name": "before"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: n2, Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: nDel, Labels: []string{"N"}, Properties: map[string]any{"x": int64(1)}})
	require.NoError(t, err)

	eUpd := EdgeID(prefixTestID("mvcc-mat-eupd"))
	eDel := EdgeID(prefixTestID("mvcc-mat-edel"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: eUpd, StartNode: n1, EndNode: n2, Type: "REL", Properties: map[string]any{"w": int64(1)}}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: eDel, StartNode: nDel, EndNode: n2, Type: "REL", Properties: map[string]any{"w": int64(2)}}))

	oldNodeDel, err := engine.GetNode(nDel)
	require.NoError(t, err)
	oldEdgeUpd, err := engine.GetEdge(eUpd)
	require.NoError(t, err)
	oldEdgeDel, err := engine.GetEdge(eDel)
	require.NoError(t, err)

	ops := []Operation{
		{Type: OpCreateNode, Node: nil},
		{Type: OpCreateNode, Node: &Node{ID: NodeID(prefixTestID("mvcc-mat-create-fresh")), Labels: []string{"N"}}, FreshID: true},
		{Type: OpCreateNode, Node: &Node{ID: NodeID(prefixTestID("mvcc-mat-create-regular")), Labels: []string{"N"}}},
		{Type: OpUpdateNode, Node: &Node{ID: n1, Labels: []string{"N"}, Properties: map[string]any{"name": "after"}}, OldNode: &Node{ID: n1, Labels: []string{"N"}, Properties: map[string]any{"name": "before"}}},
		{Type: OpDeleteNode, NodeID: nDel, OldNode: oldNodeDel, DeletedEdgeIDs: []EdgeID{eDel}},
		{Type: OpCreateEdge, Edge: nil},
		{Type: OpCreateEdge, Edge: &Edge{ID: EdgeID(prefixTestID("mvcc-mat-create-edge-fresh")), StartNode: n1, EndNode: n2, Type: "REL"}, FreshID: true},
		{Type: OpCreateEdge, Edge: &Edge{ID: EdgeID(prefixTestID("mvcc-mat-create-edge-regular")), StartNode: n1, EndNode: n2, Type: "REL"}},
		{Type: OpUpdateEdge, Edge: &Edge{ID: eUpd, StartNode: n1, EndNode: n2, Type: "REL", Properties: map[string]any{"w": int64(9)}}, OldEdge: oldEdgeUpd},
		{Type: OpDeleteEdge, EdgeID: eDel, OldEdge: oldEdgeDel},
		{Type: OpUpdateEdge, Edge: nil},
	}

	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 7777}
	err = engine.withUpdate(func(txn *badger.Txn) error {
		return engine.materializeMVCCCommitInTxn(txn, version, ops)
	})
	require.NoError(t, err)

	headNodeFresh, err := engine.GetNodeCurrentHead(NodeID(prefixTestID("mvcc-mat-create-fresh")))
	require.NoError(t, err)
	require.False(t, headNodeFresh.Tombstoned)
	require.Equal(t, version, headNodeFresh.Version)

	headNodeDeleted, err := engine.GetNodeCurrentHead(nDel)
	require.NoError(t, err)
	require.True(t, headNodeDeleted.Tombstoned)
	require.Equal(t, version, headNodeDeleted.Version)

	headEdgeFresh, err := engine.GetEdgeCurrentHead(EdgeID(prefixTestID("mvcc-mat-create-edge-fresh")))
	require.NoError(t, err)
	require.False(t, headEdgeFresh.Tombstoned)
	require.Equal(t, version, headEdgeFresh.Version)

	headEdgeDeleted, err := engine.GetEdgeCurrentHead(eDel)
	require.NoError(t, err)
	require.True(t, headEdgeDeleted.Tombstoned)
	require.Equal(t, version, headEdgeDeleted.Version)
}

func TestBadgerEngine_MaterializeMVCCCommitInTxn_AdjacencyEffects(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	for _, nodeID := range []NodeID{"test:mat-a", "test:mat-b", "test:mat-c", "test:mat-d"} {
		_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:mat-update", StartNode: "test:mat-a", EndNode: "test:mat-b", Type: "REL"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:mat-delete", StartNode: "test:mat-b", EndNode: "test:mat-c", Type: "REL"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:mat-child", StartNode: "test:mat-d", EndNode: "test:mat-c", Type: "REL"}))

	oldUpdate, err := engine.GetEdge("test:mat-update")
	require.NoError(t, err)
	oldDelete, err := engine.GetEdge("test:mat-delete")
	require.NoError(t, err)
	newCreate := &Edge{ID: "test:mat-create", StartNode: "test:mat-a", EndNode: "test:mat-d", Type: "REL"}
	newUpdate := &Edge{ID: "test:mat-update", StartNode: "test:mat-a", EndNode: "test:mat-d", Type: "REL2"}

	version := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(2 * time.Second), CommitSequence: 8801}
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		createBody, err := engine.encodeEdgeInTxn(txn, namespaceForEdgeID(newCreate.ID), newCreate)
		if err != nil {
			return err
		}
		if err := txn.Set(edgeKey(newCreate.ID), createBody); err != nil {
			return err
		}
		updateBody, err := engine.encodeEdgeInTxn(txn, namespaceForEdgeID(newUpdate.ID), newUpdate)
		if err != nil {
			return err
		}
		if err := txn.Set(edgeKey(newUpdate.ID), updateBody); err != nil {
			return err
		}
		if err := txn.Delete(edgeKey("test:mat-delete")); err != nil {
			return err
		}
		if err := txn.Delete(edgeKey("test:mat-child")); err != nil {
			return err
		}
		if err := txn.Delete(nodeKey("test:mat-c")); err != nil {
			return err
		}
		ops := []Operation{
			{Type: OpDeleteNode, NodeID: "test:mat-c", OldNode: &Node{ID: "test:mat-c", Labels: []string{"N"}}, DeletedEdgeIDs: []EdgeID{"test:mat-child"}},
			{Type: OpCreateEdge, Edge: newCreate, FreshID: true},
			{Type: OpUpdateEdge, Edge: newUpdate, OldEdge: oldUpdate},
			{Type: OpDeleteEdge, EdgeID: "test:mat-delete", OldEdge: oldDelete},
		}
		return engine.materializeMVCCCommitInTxn(txn, version, ops)
	}))

	outA, err := engine.GetOutgoingEdgesVisibleAt("test:mat-a", version)
	require.NoError(t, err)
	require.Len(t, outA, 2)

	inD, err := engine.GetIncomingEdgesVisibleAt("test:mat-d", version)
	require.NoError(t, err)
	require.Len(t, inD, 2)

	inB, err := engine.GetIncomingEdgesVisibleAt("test:mat-b", version)
	require.NoError(t, err)
	require.Empty(t, inB)

	outB, err := engine.GetOutgoingEdgesVisibleAt("test:mat-b", version)
	require.NoError(t, err)
	require.Empty(t, outB)

	inC, err := engine.GetIncomingEdgesVisibleAt("test:mat-c", version)
	require.NoError(t, err)
	require.Empty(t, inC)

	outD, err := engine.GetOutgoingEdgesVisibleAt("test:mat-d", version)
	require.NoError(t, err)
	require.Empty(t, outD)
}
