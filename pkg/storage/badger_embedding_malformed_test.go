package storage

import (
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A malformed key is not proof that its apparent owner is unreferenced.
// Repair must leave opaque bytes intact while still cleaning known orphans.
func TestEmbeddingCleanup_RepairPreservesMalformedKeys(t *testing.T) {
	b := newTestEngine(t)
	live := separatelyStoredNode("test:live\x00owner")
	_, err := b.CreateNode(live)
	require.NoError(t, err)
	current := embeddingKey(live.ID, 0)
	part := embeddingChunkPartKey(live.ID, 0, 0)
	prior := binary.BigEndian.AppendUint32([]byte{prefixEmbeddingBinary}, uint32(len(live.ID)))
	prior = append(prior, []byte(live.ID)...)
	prior = binary.BigEndian.AppendUint32(prior, 0)
	malformed := [][]byte{
		{prefixEmbeddingCurrent},
		append([]byte{prefixEmbeddingCurrent}, []byte("zzzzzzzztest00000000")...),
		append([]byte{prefixEmbeddingCurrent}, []byte("fffffffftest00000000")...),
		append(append([]byte(nil), current...), 1),
		append(append([]byte(nil), current[:len(current)-8]...), []byte("zzzzzzzz")...),
		append(append([]byte(nil), part[:len(part)-4]...), []byte("zzzz")...),
		{prefixEmbeddingBinary},
		binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32([]byte{prefixEmbeddingBinary}, 0xffffffff), 0),
		append(append([]byte(nil), prior...), 1),
	}
	for _, key := range append([][]byte{nil, {}, {0xff}}, malformed...) {
		require.Empty(t, embeddingKeyOwners(key), "malformed framing must not acquire an owner: %x", key)
	}
	orphan := embeddingKey("test:missing-owner", 0)
	require.NoError(t, b.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		for _, key := range malformed {
			if err := txn.Set(key, []byte("opaque payload")); err != nil {
				return err
			}
		}
		return txn.Set(orphan, []byte("orphan"))
	}))
	removed, err := b.RepairOrphanedEmbeddingKeys()
	require.NoError(t, err)
	require.Equal(t, 1, removed)
	require.Equal(t, live.ChunkEmbeddings, embeddingCleanupColdNode(t, b, live.ID).ChunkEmbeddings)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		for _, key := range malformed {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			require.Equal(t, []byte("opaque payload"), value)
		}
		_, err := txn.Get(orphan)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
	removed, err = b.RepairOrphanedEmbeddingKeys()
	require.NoError(t, err)
	require.Zero(t, removed)
}
