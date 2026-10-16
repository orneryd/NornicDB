package storage

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmbeddingCleanup_LiveSeparatorID(t *testing.T) {
	for _, id := range []NodeID{"test:live\x00suffix", "test:live\x00\x00\x01:\xff/sep", "test:live:\x01\xff/sep"} {
		for _, legacy := range []bool{false, true} {
			for _, reopen := range []bool{false, true} {
				t.Run(fmt.Sprintf("id=%x/legacy=%v/reopen=%v", id, legacy, reopen), func(t *testing.T) {
					dir := t.TempDir()
					b, err := NewBadgerEngine(dir)
					require.NoError(t, err)

					n := separatelyStoredNode(id)
					_, err = b.CreateNode(n)
					require.NoError(t, err, "public storage API accepts this ID")
					if legacy {
						require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
							for i := 0; i < 16; i++ {
								key := embeddingKey(id, i)
								item, err := txn.Get(key)
								if err != nil {
									return err
								}
								value, err := item.ValueCopy(nil)
								if err != nil {
									return err
								}
								if err = txn.Delete(key); err != nil {
									return err
								}
								if err = txn.Set(legacyEmbeddingKey(id, i), value); err != nil {
									return err
								}
							}
							return nil
						}))
					}
					require.Equal(t, 16, separatorIDKeyCount(t, b, id))
					if reopen {
						require.NoError(t, b.withUpdate(func(txn *badger.Txn) error { return txn.Delete(embeddingRepairReadyKey) }))
						require.NoError(t, b.Close())
						b, err = NewBadgerEngine(dir)
						require.NoError(t, err)
					} else {
						removed, repairErr := b.RepairOrphanedEmbeddingKeys()
						require.NoError(t, repairErr)
						t.Logf("removed=%d from live owner %q", removed, id)
					}
					defer b.Close()
					t.Logf("live owner %q has %d keys after repair", id, separatorIDKeyCount(t, b, id))
					require.Equal(t, 16, separatorIDKeyCount(t, b, id), "repair must preserve a live separate owner")
					require.Equal(t, n.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
				})
			}
		}
	}
}

func separatorIDKeyCount(t *testing.T, b *BadgerEngine, id NodeID) int {
	t.Helper()
	count := 0
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		return visitEmbeddingKeys(txn, id, func([]byte) error { count++; return nil })
	}))
	return count
}

func TestEmbeddingCleanup_DoesNotDeleteOtherLegacyOwner(t *testing.T) {
	b := newTestEngine(t)
	short := NodeID("test:prefix")
	long := NodeID("test:prefix\x00suffix")
	_, err := b.CreateNode(separatelyStoredNode(short))
	require.NoError(t, err)
	n := separatelyStoredNode(long)
	_, err = b.CreateNode(n)
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		for i := 0; i < 16; i++ {
			item, e := txn.Get(embeddingKey(long, i))
			if e != nil {
				return e
			}
			value, e := item.ValueCopy(nil)
			if e != nil {
				return e
			}
			if e = txn.Delete(embeddingKey(long, i)); e != nil {
				return e
			}
			if e = txn.Set(legacyEmbeddingKey(long, i), value); e != nil {
				return e
			}
		}
		return nil
	}))
	require.NoError(t, b.DeleteNode(short))
	require.Equal(t, 16, separatorIDKeyCount(t, b, long))
	require.Equal(t, n.ChunkEmbeddings, embeddingCleanupColdNode(t, b, long).ChunkEmbeddings)
}
