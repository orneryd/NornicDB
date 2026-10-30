package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_DeleteByPrefix_MalformedSecondaryIndexKeysIgnored(t *testing.T) {
	engine := createTestBadgerEngine(t)

	const targetPrefix = "target:"
	n1 := &Node{ID: NodeID(targetPrefix + "n1"), Labels: []string{"Person"}}
	n2 := &Node{ID: NodeID(targetPrefix + "n2"), Labels: []string{"Person"}}
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(targetPrefix + "e1"), StartNode: n1.ID, EndNode: n2.ID, Type: "KNOWS"}))

	validLabelIdx := append(append([]byte{prefixLabelIndex}, []byte("Person")...), 0x00)
	validLabelIdx = append(validLabelIdx, []byte(targetPrefix+"orphan-node")...)
	validEdgeTypeIdx := append(append([]byte{prefixEdgeTypeIndex}, []byte("KNOWS")...), 0x00)
	validEdgeTypeIdx = append(validEdgeTypeIdx, []byte(targetPrefix+"orphan-edge")...)

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		// len(key) < 3 branch
		require.NoError(t, txn.Set([]byte{prefixLabelIndex, 'x'}, []byte{1}))
		// sep < 0 branch (no separator after prefix byte)
		require.NoError(t, txn.Set([]byte{prefixLabelIndex, 'a', 'b', 'c'}, []byte{1}))
		// 1+sep+1 >= len(key) branch (separator immediately at end)
		require.NoError(t, txn.Set([]byte{prefixEdgeTypeIndex, 'z', 0x00}, []byte{1}))

		// Well-formed suffix keys that should be deleted when suffix matches target prefix.
		require.NoError(t, txn.Set(validLabelIdx, []byte{1}))
		require.NoError(t, txn.Set(validEdgeTypeIdx, []byte{1}))
		return nil
	}))

	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix(targetPrefix)
	require.NoError(t, err)
	require.Equal(t, int64(2), nodesDeleted)
	require.Equal(t, int64(1), edgesDeleted)

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(validLabelIdx)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		_, err = txn.Get(validEdgeTypeIdx)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestBadgerEngine_DeleteByPrefix_DBClosedDuringOperationReturnsError(t *testing.T) {
	engine := createTestBadgerEngine(t)

	_, err := engine.CreateNode(&Node{ID: NodeID("test:n1")})
	require.NoError(t, err)

	require.NoError(t, engine.db.Close())

	_, _, err = engine.DeleteByPrefix("test:")
	require.Error(t, err)
}
