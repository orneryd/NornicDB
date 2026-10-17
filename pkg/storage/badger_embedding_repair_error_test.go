package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// An undecodable body cannot establish that its chunks are unreferenced.
// A failed repair must preserve those chunks and leave the store at V3.
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
	storeBeforeV4(t, b)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(nodeKey(live.ID), []byte{0xff})
	}))
	removed, err := b.RepairOrphanedEmbeddingKeys()
	require.Error(t, err)
	require.Zero(t, removed)
	require.Error(t, b.migrateV3ToV4())
	require.Equal(t, 16, countEmbeddingKeys(t, b, live.ID))
	version, err := b.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionEdgeAdjacencyV3, version)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(nodeKey(live.ID), body)
	}))
	removed, err = b.RepairOrphanedEmbeddingKeys()
	require.NoError(t, err)
	require.Zero(t, removed)
	require.Equal(t, live.ChunkEmbeddings, embeddingCleanupColdNode(t, b, live.ID).ChunkEmbeddings)
	require.NoError(t, b.migrateV3ToV4())
	version, err = b.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionEmbeddingKeysV4, version)
}
