package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_MVCCPruneOptionsAndWriteHelpers_InvalidIDs(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	t.Run("effective prune options", func(t *testing.T) {
		opts := engine.effectiveMVCCPruneOptions(MVCCPruneOptions{MaxVersionsPerKey: 0, MinRetentionAge: -time.Second})
		require.Greater(t, opts.MaxVersionsPerKey, 0)
		require.Equal(t, time.Duration(0), opts.MinRetentionAge)

		opts2 := engine.effectiveMVCCPruneOptions(MVCCPruneOptions{MaxVersionsPerKey: 3, MinRetentionAge: 2 * time.Second})
		require.Equal(t, 3, opts2.MaxVersionsPerKey)
		require.Equal(t, 2*time.Second, opts2.MinRetentionAge)
	})

	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		require.NoError(t, engine.writeNodeMVCCVersionInTxn(txn, &Node{ID: "bad-id", Labels: []string{"Doc"}}, version))
		require.NoError(t, engine.writeNodeMVCCTombstoneInTxn(txn, NodeID("bad-id"), version))
		require.NoError(t, engine.writeEdgeMVCCVersionInTxn(txn, &Edge{ID: "bad-edge", StartNode: "bad-id", EndNode: "bad-id", Type: "REL"}, version))
		require.NoError(t, engine.writeEdgeMVCCTombstoneInTxn(txn, EdgeID("bad-edge"), version))

		require.NoError(t, engine.writeNodeMVCCHeadWithFloorInTxn(txn, NodeID("bad-id"), version, false, version))
		require.NoError(t, engine.writeEdgeMVCCHeadWithFloorInTxn(txn, EdgeID("bad-edge"), version, false, version))
		return nil
	}))
}

func TestBadgerEngine_MVCCAppendAndHead_ErrorPaths(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 7}

	require.ErrorIs(t, engine.AppendNodeVersion(nil, version), ErrInvalidData)
	require.ErrorIs(t, engine.AppendEdgeVersion(nil, version), ErrInvalidData)

	require.NoError(t, engine.AppendNodeTombstone(NodeID("bad-id"), version))
	require.NoError(t, engine.AppendEdgeTombstone(EdgeID("bad-edge"), version))

	_, err := engine.GetNodeCurrentHead(NodeID("test:never-created"))
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdgeCurrentHead(EdgeID("test:never-created"))
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, engine.UpdateNodeCurrentHead(NodeID("bad-id"), version, false))
	require.NoError(t, engine.UpdateEdgeCurrentHead(EdgeID("bad-edge"), version, false))

	nodeID := NodeID(prefixTestID("mvcc-append-ok-node"))
	edgeID := EdgeID(prefixTestID("mvcc-append-ok-edge"))
	start := NodeID(prefixTestID("mvcc-append-start"))
	end := NodeID(prefixTestID("mvcc-append-end"))
	require.NoError(t, engine.AppendNodeVersion(&Node{ID: nodeID, Labels: []string{"Doc"}}, version))
	require.NoError(t, engine.AppendNodeTombstone(nodeID, MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 8}))

	require.NoError(t, engine.AppendNodeVersion(&Node{ID: start, Labels: []string{"Doc"}}, version))
	require.NoError(t, engine.AppendNodeVersion(&Node{ID: end, Labels: []string{"Doc"}}, version))
	require.NoError(t, engine.AppendEdgeVersion(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "REL"}, version))
	require.NoError(t, engine.AppendEdgeTombstone(edgeID, MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 9}))

	nHead, err := engine.GetNodeCurrentHead(nodeID)
	require.NoError(t, err)
	require.True(t, nHead.Tombstoned)
	eHead, err := engine.GetEdgeCurrentHead(edgeID)
	require.NoError(t, err)
	require.True(t, eHead.Tombstoned)
}

func TestBadgerEngine_MVCCLoadHelpers_DecodeAndLookupBranches(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 11}

	nodeID := NodeID(prefixTestID("mvcc-load-node"))
	edgeID := EdgeID(prefixTestID("mvcc-load-edge"))
	start := NodeID(prefixTestID("mvcc-load-start"))
	end := NodeID(prefixTestID("mvcc-load-end"))

	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, engine.AppendNodeVersion(&Node{ID: nodeID, Labels: []string{"Doc"}, Properties: map[string]any{"v": 1}}, version))
	require.NoError(t, engine.AppendEdgeVersion(&Edge{ID: edgeID, StartNode: start, EndNode: end, Type: "REL"}, version))

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.loadNodeMVCCHeadInTxn(txn, NodeID("bad-id"))
		require.ErrorIs(t, err, ErrNotFound)
		_, err = engine.loadEdgeMVCCHeadInTxn(txn, EdgeID("bad-edge"))
		require.ErrorIs(t, err, ErrNotFound)
		_, err = engine.loadNodeMVCCRecordExactInTxn(txn, NodeID("bad-id"), version)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = engine.loadEdgeMVCCRecordExactInTxn(txn, EdgeID("bad-edge"), version)
		require.ErrorIs(t, err, ErrNotFound)
		_, _, err = engine.loadNodeMVCCRecordAtOrBeforeInTxn(txn, NodeID("bad-id"), version)
		require.ErrorIs(t, err, ErrNotFound)
		_, _, err = engine.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, EdgeID("bad-edge"), version)
		require.ErrorIs(t, err, ErrNotFound)
		return nil
	}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		nodeHeadKey, err := engine.mvccNodeHeadKeyString(txn, nodeID)
		require.NoError(t, err)
		require.NoError(t, txn.Set(nodeHeadKey, []byte("not-json")))

		edgeHeadKey, err := engine.mvccEdgeHeadKeyString(txn, edgeID)
		require.NoError(t, err)
		require.NoError(t, txn.Set(edgeHeadKey, []byte("not-json")))

		nodeVersionKey, err := engine.mvccNodeVersionKeyString(txn, nodeID, version)
		require.NoError(t, err)
		require.NoError(t, txn.Set(nodeVersionKey, []byte("not-json")))

		edgeVersionKey, err := engine.mvccEdgeVersionKeyString(txn, edgeID, version)
		require.NoError(t, err)
		require.NoError(t, txn.Set(edgeVersionKey, []byte("not-json")))
		return nil
	}))

	_, err = engine.loadNodeMVCCHead(nodeID)
	require.Error(t, err)

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.loadNodeMVCCHeadInTxn(txn, nodeID)
		require.Error(t, err)
		_, err = engine.loadEdgeMVCCHeadInTxn(txn, edgeID)
		require.Error(t, err)
		_, err = engine.loadNodeMVCCRecordExactInTxn(txn, nodeID, version)
		require.Error(t, err)
		_, err = engine.loadEdgeMVCCRecordExactInTxn(txn, edgeID, version)
		require.Error(t, err)
		_, _, err = engine.loadNodeMVCCRecordAtOrBeforeInTxn(txn, nodeID, version)
		require.Error(t, err)
		_, _, err = engine.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, edgeID, version)
		require.Error(t, err)
		return nil
	}))
}
