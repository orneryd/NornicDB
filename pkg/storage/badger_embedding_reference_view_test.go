package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingCleanup_MultiOwnerFinalReference(t *testing.T) {
	for _, scenario := range []string{"tx-short-long", "tx-long-short", "direct-short-long", "direct-long-short", "single-short", "single-long"} {
		t.Run(scenario, func(t *testing.T) {
			b := newTestEngine(t)
			short, long := NodeID("test:alias"), NodeID("test:alias\x00\x00\x00")
			prior := seedPriorEmbeddingNode(t, b, long, false)
			current := separatelyStoredNode(short)
			_, err := b.CreateNode(current)
			require.NoError(t, err)
			require.Len(t, prior.ChunkEmbeddings, 16)
			require.Len(t, current.ChunkEmbeddings, 16)
			require.Equal(t, prior.ChunkEmbeddings, embeddingCleanupColdNode(t, b, long).ChunkEmbeddings, "SETUP: genuine released owner must remain readable")
			require.Equal(t, current.ChunkEmbeddings, embeddingCleanupColdNode(t, b, short).ChunkEmbeddings, "SETUP: current owner must also remain readable")
			key := legacyEmbeddingKey(long, 0)
			require.Equal(t, key, embeddingPartKey(legacyEmbeddingKey(short, 0), 0))
			require.NotEqual(t, key, embeddingChunkPartKey(short, 0, 0))
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				_, err := txn.Get(key)
				return err
			}), "SETUP: original physical alias must exist")
			t.Log("SETUP PASS: released and current owners each cold-read all16 exact vectors; legacy alias exists")
			ids := []NodeID{short, long}
			if scenario == "tx-long-short" || scenario == "direct-long-short" {
				ids = []NodeID{long, short}
			}
			if scenario == "single-short" {
				ids = []NodeID{short}
			}
			if scenario == "single-long" {
				ids = []NodeID{long}
			}
			if scenario == "tx-short-long" || scenario == "tx-long-short" {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("test"))
				for _, id := range ids {
					require.NoError(t, tx.DeleteNode(id))
				}
				require.NoError(t, tx.Commit())
			} else {
				for _, id := range ids {
					require.NoError(t, b.DeleteNode(id))
				}
			}
			for index, id := range ids {
				t.Run(fmt.Sprintf("deleted-body-%d", index), func(t *testing.T) {
					_, err := b.GetNode(id)
					require.ErrorIs(t, err, ErrNotFound)
				})
			}
			if len(ids) == 1 {
				live := prior
				if ids[0] == long {
					live = current
				}
				require.Equal(t, live.ChunkEmbeddings, embeddingCleanupColdNode(t, b, live.ID).ChunkEmbeddings, "single delete must preserve the other genuine owner")
				if live.ID == long {
					require.NoError(t, b.withView(func(txn *badger.Txn) error {
						_, err := txn.Get(key)
						return err
					}), "the surviving released owner still needs the alias")
				}
				return
			}
			t.Run("shared-key-cleanup", func(t *testing.T) {
				require.NoError(t, b.withView(func(txn *badger.Txn) error {
					_, err := txn.Get(key)
					require.ErrorIs(t, err, badger.ErrKeyNotFound, "neither owner exists after both deletes")
					return nil
				}))
			})
			for index, id := range []NodeID{short, long} {
				t.Run(fmt.Sprintf("physical-range-%d", index), func(t *testing.T) {
					remaining := separatorIDKeyCount(t, b, id)
					t.Logf("remaining physical keys for %q: %d", id, remaining)
					require.Zero(t, remaining)
				})
			}
		})
	}
}

func TestEmbeddingCleanup_MultiOwnerBufferedReference(t *testing.T) {
	for _, order := range []string{"short-long", "long-short"} {
		for _, finish := range []string{"flush", "recovery", "rollback"} {
			t.Run(order+"/"+finish, func(t *testing.T) {
				dir := t.TempDir()
				b, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
				defer func() { _ = b.Close(); _ = wal.Close() }()
				short, long := NodeID("test:alias"), NodeID("test:alias\x00\x00\x00")
				prior := seedPriorEmbeddingNode(t, b, long, false)
				current := separatelyStoredNode(short)
				_, err := b.CreateNode(current)
				require.NoError(t, err)
				require.Equal(t, prior.ChunkEmbeddings, embeddingCleanupColdNode(t, b, long).ChunkEmbeddings)
				require.Equal(t, current.ChunkEmbeddings, embeddingCleanupColdNode(t, b, short).ChunkEmbeddings)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				ids := []NodeID{short, long}
				if order == "long-short" {
					ids = []NodeID{long, short}
				}
				for _, id := range ids {
					require.NoError(t, tx.DeleteNode(id))
				}
				if finish == "rollback" {
					require.NoError(t, tx.Rollback())
					require.Equal(t, prior.ChunkEmbeddings, embeddingCleanupColdNode(t, b, long).ChunkEmbeddings)
					require.Equal(t, current.ChunkEmbeddings, embeddingCleanupColdNode(t, b, short).ChunkEmbeddings)
					return
				}
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
				if finish == "flush" {
					require.NoError(t, b.FlushWriteBehind())
				} else {
					crashClose(t, b, wal)
					b, wal = newWriteBehindWALTestEngine(t, dir, time.Hour)
					replayed, err := RecoverBufferedCommits(b, wal)
					require.NoError(t, err)
					require.Equal(t, 1, replayed)
					// A repeated recovery must not reintroduce the range.
					replayed, err = RecoverBufferedCommits(b, wal)
					require.NoError(t, err)
					require.Zero(t, replayed)
				}
				for _, id := range ids {
					_, err := b.GetNode(id)
					require.ErrorIs(t, err, ErrNotFound)
					require.Zero(t, separatorIDKeyCount(t, b, id))
				}
			})
		}
	}
}

func TestEmbeddingReferenceView_FreezesReferenceInputs(t *testing.T) {
	b := newTestEngine(t)
	id := NodeID("test:reference-input")
	_, err := b.CreateNode(&Node{ID: id})
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		return txn.Set(embeddingMetaKey(id), []byte{1})
	}))
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		body, err := txn.Get(nodeKey(id))
		if err != nil {
			return err
		}
		value, err := body.ValueCopy(nil)
		if err != nil {
			return err
		}
		writes := map[string][]byte{string(nodeKey(id)): value, string(edgeKey("test:unrelated")): {1}, "": {1}}
		deletes := map[string]bool{string(embeddingMetaKey(id)): true, "ignored": false}
		view := newEmbeddingReferenceView(txn, writes, deletes, nil)
		// Mutate the staging maps as range materialization does. The frozen
		// view must still see the final inline body without old metadata.
		delete(deletes, string(embeddingMetaKey(id)))
		writes[string(nodeKey(id))] = []byte{0xff}
		referenced, err := view.ownerReferenced(id)
		require.NoError(t, err)
		require.False(t, referenced)
		referenced, err = embeddingOwnerReferenced(txn, id)
		require.NoError(t, err)
		require.True(t, referenced, "the persisted metadata still exists")
		return nil
	}))
}
