package storage

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerMVCC_CorruptionAndErrorPaths(t *testing.T) {
	engine := newTestEngine(t)
	v1 := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(-time.Minute), CommitSequence: 1}
	v2 := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 2}

	node := &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"k": "v"}}
	edge := &Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n1", Type: "SELF", Properties: map[string]any{"w": int64(1)}}

	require.NoError(t, engine.AppendNodeVersion(node, v1))
	require.NoError(t, engine.AppendEdgeVersion(edge, v1))
	require.NoError(t, engine.UpdateNodeCurrentHead(node.ID, v2, false))
	require.NoError(t, engine.UpdateEdgeCurrentHead(edge.ID, v2, false))

	// Decode errors for head records.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		numNode, ok := engine.idDict.lookupNodeNumID(node.ID)
		require.True(t, ok)
		numEdge, ok := engine.idDict.lookupEdgeNumID(edge.ID)
		require.True(t, ok)
		require.NoError(t, txn.Set(mvccNodeHeadKey(numNode), []byte{0x01, 0x02}))
		require.NoError(t, txn.Set(mvccEdgeHeadKey(numEdge), []byte{0x01, 0x02}))
		return nil
	}))

	_, err := engine.GetNodeCurrentHead(node.ID)
	require.Error(t, err)
	_, err = engine.GetEdgeCurrentHead(edge.ID)
	require.Error(t, err)

	// Restore valid heads and inject malformed version records.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		numNode, ok := engine.idDict.lookupNodeNumID(node.ID)
		require.True(t, ok)
		nodeLogical := make([]byte, 9)
		nodeLogical[0] = prefixMVCCNode
		binary.BigEndian.PutUint64(nodeLogical[1:], numNode)
		require.NoError(t, engine.writeMVCCHeadForLogicalKeyInTxn(txn, nodeLogical, v1, false, v1))

		numEdge, ok := engine.idDict.lookupEdgeNumID(edge.ID)
		require.True(t, ok)
		edgeLogical := make([]byte, 9)
		edgeLogical[0] = prefixMVCCEdge
		binary.BigEndian.PutUint64(edgeLogical[1:], numEdge)
		require.NoError(t, engine.writeMVCCHeadForLogicalKeyInTxn(txn, edgeLogical, v1, false, v1))

		nk, err := engine.mvccNodeVersionKeyString(txn, node.ID, v1)
		require.NoError(t, err)
		ek, err := engine.mvccEdgeVersionKeyString(txn, edge.ID, v1)
		require.NoError(t, err)
		require.NoError(t, txn.Set(nk, []byte("bad-node-record")))
		require.NoError(t, txn.Set(ek, []byte("bad-edge-record")))
		return nil
	}))

	_, err = engine.GetNodeVisibleAt(node.ID, v2)
	require.Error(t, err)
	_, err = engine.GetEdgeVisibleAt(edge.ID, v2)
	require.Error(t, err)

	// Error paths: invalid logical key shape/prefix for load/write helpers.
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := engine.loadMVCCHeadForLogicalKeyInTxn(txn, []byte{prefixMVCCNode})
		require.ErrorIs(t, err, ErrInvalidData)

		bad := make([]byte, 9)
		bad[0] = 0xEE
		binary.BigEndian.PutUint64(bad[1:], 1)
		_, err = engine.loadMVCCHeadForLogicalKeyInTxn(txn, bad)
		require.Error(t, err)
		return nil
	}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		err := engine.writeMVCCHeadForLogicalKeyInTxn(txn, []byte{prefixMVCCNode}, v1, false, v1)
		require.ErrorIs(t, err, ErrInvalidData)
		bad := make([]byte, 9)
		bad[0] = 0xEE
		binary.BigEndian.PutUint64(bad[1:], 1)
		err = engine.writeMVCCHeadForLogicalKeyInTxn(txn, bad, v1, false, v1)
		require.Error(t, err)
		return nil
	}))

	// Malformed mvcc key scan path.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set([]byte{prefixMVCCNode, 0x01, 0x02}, []byte{0x00})
	}))
	_, _, err = engine.withViewNodeMVCCVersionsFromKey([]byte{prefixMVCCNode}, 10, func(NodeID, MVCCVersion, bool) error { return nil })
	require.Error(t, err)
}
