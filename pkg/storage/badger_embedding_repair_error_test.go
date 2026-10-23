package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// An undecodable body cannot establish that its chunks are unreferenced.
// A failed repair must preserve those chunks and leave the marker absent.
func TestEmbeddingCleanup_RepairRejectsCorruptOwner(t *testing.T) {
	b := newTestEngine(t)
	live := separatelyStoredNode("test:corrupt-owner")
	_, err := b.CreateNode(live)
	require.NoError(t, err)
	var body []byte
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(live.ID))
		if err != nil {
			return err
		}
		body, err = item.ValueCopy(nil)
		return err
	}))
	require.NoError(t, b.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		if err := txn.Delete(embeddingRepairReadyKey); err != nil {
			return err
		}
		return txn.Set(nodeKey(live.ID), []byte{0xff})
	}))
	removed, err := b.RepairOrphanedEmbeddingKeys()
	require.Error(t, err)
	require.Zero(t, removed)
	require.Equal(t, 16, countEmbeddingKeys(t, b, live.ID))
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(embeddingRepairReadyKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
	require.NoError(t, b.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		return txn.Set(nodeKey(live.ID), body)
	}))
	removed, err = b.RepairOrphanedEmbeddingKeys()
	require.NoError(t, err)
	require.Zero(t, removed)
	require.Equal(t, live.ChunkEmbeddings, embeddingCleanupColdNode(t, b, live.ID).ChunkEmbeddings)
}
