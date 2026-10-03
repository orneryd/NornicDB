package storage

import (
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestMigrationV1ToV2_RebuildLabelIndex_ErrorBranches(t *testing.T) {
	t.Run("node key without namespace prefix fails", func(t *testing.T) {
		eng, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		t.Cleanup(func() { _ = eng.Close() })

		require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
			return txn.Set(nodeKey(NodeID("unprefixed-node")), []byte{0x01})
		}))

		stats := &v1ToV2IndexStats{}
		err = eng.rebuildLabelIndexForV2(stats)
		require.Error(t, err)
		require.Contains(t, err.Error(), "lacks namespace prefix")
	})

	t.Run("corrupt v2 node body fails decode", func(t *testing.T) {
		eng, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		t.Cleanup(func() { _ = eng.Close() })

		require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
			return txn.Set(nodeKey(NodeID("test:bad-node")), []byte{0xFF, 0x00, 0x01})
		}))

		stats := &v1ToV2IndexStats{}
		err = eng.rebuildLabelIndexForV2(stats)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode v2 node")
	})
}

func TestMigrationV1ToV2_RebuildEdgeIndexes_ErrorBranches(t *testing.T) {
	t.Run("corrupt v2 edge body fails decode", func(t *testing.T) {
		eng, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		t.Cleanup(func() { _ = eng.Close() })

		require.NoError(t, eng.db.Update(func(txn *badger.Txn) error {
			return txn.Set(edgeKey(EdgeID("test:bad-edge")), []byte{0xFF, 0x01, 0x02})
		}))

		stats := &v1ToV2IndexStats{}
		err = eng.rebuildEdgeIndexesForV2(stats)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode v2 edge")
	})
}

func TestMigrationV1ToV2_DecodeEdgeAnyV1_ErrorBranches(t *testing.T) {
	eng, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = eng.Close() })

	t.Run("empty body", func(t *testing.T) {
		_, _, _, err := decodeEdgeAnyV1(eng, nil, nil, EdgeID("test:e-empty"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "edge body empty")
	})

	t.Run("compact v1 decode failure", func(t *testing.T) {
		_, _, _, err := decodeEdgeAnyV1(eng, nil, []byte{edgeFormatCompactV1, 0x00}, EdgeID("test:e-compact-bad"))
		require.Error(t, err)
	})

	t.Run("legacy edge start numID allocation failure", func(t *testing.T) {
		data, err := encodeValue(&Edge{
			ID:      EdgeID("test:e-start-bad"),
			EndNode: NodeID("test:end"),
			Type:    "REL",
		})
		require.NoError(t, err)

		txn := eng.db.testTxn(true)
		txn.Discard()
		_, _, _, err = decodeEdgeAnyV1(eng, txn, data, EdgeID("test:e-start-bad"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "allocating start numID")
	})

	t.Run("legacy edge end numID allocation failure", func(t *testing.T) {
		hugeEnd := NodeID("test:" + strings.Repeat("x", 70000))
		data, err := encodeValue(&Edge{
			ID:        EdgeID("test:e-end-bad"),
			StartNode: NodeID("test:start"),
			EndNode:   hugeEnd,
			Type:      "REL",
		})
		require.NoError(t, err)

		err = eng.db.Update(func(txn *badger.Txn) error {
			_, _, _, derr := decodeEdgeAnyV1(eng, txn, data, EdgeID("test:e-end-bad"))
			require.Error(t, derr)
			require.Contains(t, derr.Error(), "allocating end numID")
			return nil
		})
		require.NoError(t, err)
	})
}
