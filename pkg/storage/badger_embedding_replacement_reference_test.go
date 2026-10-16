package storage

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingReferenceView_ReplacementCompatibilityOwnership(t *testing.T) {
	b := newTestEngine(t)
	short, long, _ := r7AExtensionOwners(t, b)
	alias := legacyEmbeddingKey(long, 0)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		untouched := newEmbeddingReferenceView(txn, nil, nil, []NodeID{short})
		belongs, err := embeddingKeyBelongsTo(txn, alias, short, &untouched)
		require.NoError(t, err)
		require.False(t, belongs, "untouched genuine released owner protects alias")
		owners := []NodeID{short, long}
		retired := newEmbeddingReferenceView(txn, nil, nil, owners)
		owners[1] = "test:mutated"
		belongs, err = embeddingKeyBelongsTo(txn, alias, short, &retired)
		require.NoError(t, err)
		require.True(t, belongs, "both ranges retired despite final separate bodies")
		referenced, err := retired.ownerReferenced(long)
		require.NoError(t, err)
		require.True(t, referenced, "unqualified query still describes live NEW separate range")
		for _, key := range [][]byte{alias, binary.BigEndian.AppendUint32(binaryEmbeddingPrefix(long), 0)} {
			item, err := txn.Get(alias)
			if err != nil {
				return err
			}
			payload, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			writes := map[string][]byte{string(key): payload}
			renewed := newEmbeddingReferenceView(txn, writes, nil, []NodeID{long})
			delete(writes, string(key))
			referenced, err = renewed.ownerReferenced(long, key)
			require.NoError(t, err)
			require.True(t, referenced, "final compatibility point write survives membership mutation")
			deletes := map[string]bool{string(key): true}
			removed := newEmbeddingReferenceView(txn, map[string][]byte{string(key): payload}, deletes, []NodeID{long})
			delete(deletes, string(key))
			referenced, err = removed.ownerReferenced(long, key)
			require.NoError(t, err)
			require.False(t, referenced, "explicit delete wins and freezes membership")
			writes = map[string][]byte{}
			absent := newEmbeddingReferenceView(txn, writes, nil, []NodeID{long})
			writes[string(key)] = payload
			referenced, err = absent.ownerReferenced(long, key)
			require.NoError(t, err)
			require.False(t, referenced, "later writes cannot invent final compatibility ownership")
		}
		current := newEmbeddingReferenceView(txn, map[string][]byte{string(embeddingKey(long, 0)): {1}}, nil, []NodeID{long})
		require.Nil(t, current.compatibilityWrites, "current-only publications allocate no compatibility membership")
		referenced, err = current.ownerReferenced(long, embeddingKey(long, 0))
		require.NoError(t, err)
		require.True(t, referenced, "current unambiguous key policy unchanged")
		corrupt := newEmbeddingReferenceView(txn, map[string][]byte{string(nodeKey(long)): {0xc1}}, nil, []NodeID{long})
		_, err = corrupt.ownerReferenced(long, alias)
		require.Error(t, err, "corrupt final body cannot authorize deletion")
		missing := newEmbeddingReferenceView(txn, nil, map[string]bool{string(nodeKey(long)): true}, []NodeID{long})
		referenced, err = missing.ownerReferenced(long, alias)
		require.NoError(t, err)
		require.False(t, referenced)
		empty := newEmbeddingReferenceView(txn, nil, nil, nil)
		require.Nil(t, empty.replaced)
		require.Nil(t, empty.compatibilityWrites)
		require.Nil(t, empty.staged)
		return nil
	}))
}

func TestEmbeddingCleanup_MixedReplacementFinalRanges(t *testing.T) {
	for _, separateLong := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("separateLong=%v/reverse=%v", separateLong, reverse), func(t *testing.T) {
				b := newTestEngine(t)
				short, long, _ := r7AExtensionOwners(t, b)
				separateID, inlineID := short, long
				if separateLong {
					separateID, inlineID = long, short
				}
				separate, kvs := r8SeparateReplacementNode(t, separateID, 0)
				inline := r7AExtensionInline(inlineID, 1)
				nodes := []*Node{separate, inline}
				if reverse {
					nodes[0], nodes[1] = nodes[1], nodes[0]
				}
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("test"))
				for _, node := range nodes {
					require.NoError(t, tx.UpdateNode(node))
				}
				require.NoError(t, tx.Commit())
				r8SeparateCheckPublished(t, b, separate, kvs)
				r7AExtensionCheckInline(t, b, inline)
				require.Zero(t, r7AExtensionLegacyKeys(t, b), "all old ranges retired across final body shapes")
			})
		}
	}
}

func TestEmbeddingCleanup_DualSeparateBuffered(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, finish := range []string{"flush", "recovery", "rollback"} {
			t.Run(fmt.Sprintf("reverse=%v/%s", reverse, finish), func(t *testing.T) {
				dir := t.TempDir()
				b, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
				defer func() { _ = b.Close(); _ = wal.Close() }()
				short, long, original := r7AExtensionOwners(t, b)
				updates := make(chan *Node, 4)
				observe := func(engine *BadgerEngine) {
					engine.OnNodeUpdated(func(node *Node) {
						select {
						case updates <- copyNode(node):
						default:
							t.Error("unexpected extra replacement callback")
						}
					})
				}
				observe(b)
				first, kvsFirst := r8SeparateReplacementNode(t, short, 0)
				second, kvsSecond := r8SeparateReplacementNode(t, long, 1)
				nodes := []*Node{first, second}
				if reverse {
					nodes[0], nodes[1] = nodes[1], nodes[0]
				}
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				for _, node := range nodes {
					require.NoError(t, tx.UpdateNode(node))
				}
				if finish == "rollback" {
					require.NoError(t, tx.Rollback())
					require.Zero(t, b.writeBehind.PendingOps())
					require.Empty(t, updates, "rollback emits no callbacks")
					for _, id := range []NodeID{short, long} {
						require.Equal(t, original[id], embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
						require.True(t, r7AExtensionStoredBody(t, b, id).EmbeddingsStoredSeparately)
						require.NoError(t, b.withView(func(txn *badger.Txn) error { _, err := txn.Get(embeddingMetaKey(id)); return err }))
					}
					require.Equal(t, 16, r7AExtensionLegacyKeys(t, b))
					return
				}
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
				require.Greater(t, b.writeBehind.PendingOps(), 0)
				require.Equal(t, 16, r7AExtensionLegacyKeys(t, b), "old physical bytes exist before application")
				if finish == "flush" {
					require.NoError(t, b.FlushWriteBehind())
				} else {
					crashClose(t, b, wal)
					b, wal = newWriteBehindWALTestEngine(t, dir, time.Hour)
					observe(b)
					n, err := RecoverBufferedCommits(b, wal)
					require.NoError(t, err)
					require.Equal(t, 1, n)
					n, err = RecoverBufferedCommits(b, wal)
					require.NoError(t, err)
					require.Zero(t, n)
				}
				r8SeparateCheckPublished(t, b, first, kvsFirst)
				r8SeparateCheckPublished(t, b, second, kvsSecond)
				require.Len(t, updates, 2, "one typed callback per replacement, including recovered commits")
				for range 2 {
					published := <-updates
					want := first
					if published.ID == second.ID {
						want = second
					}
					require.Equal(t, want.ChunkEmbeddings, published.ChunkEmbeddings, "callback owns exact final sharded vectors")
					require.Equal(t, want.EmbedMeta, published.EmbedMeta, "callback owns final metadata rather than the retired sidecar")
				}
				require.Zero(t, r7AExtensionLegacyKeys(t, b), "obsolete compatibility range stays retired through captured replay")
			})
		}
	}
}

// Historical captured point writes may deliberately use the released codec.
// Replay must keep them, while a final point deletion still wins over a write.
func TestEmbeddingCleanup_CapturedCompatibilityPointWrites(t *testing.T) {
	for _, removeLong := range []bool{false, true} {
		t.Run(fmt.Sprintf("removeLong=%v", removeLong), func(t *testing.T) {
			dir := t.TempDir()
			b, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
			defer func() { _ = b.Close(); _ = wal.Close() }()
			short, long, _ := r7AExtensionOwners(t, b)
			shortNode, shortKVs := r8SeparateReplacementNode(t, short, 0)
			longNode := separatelyStoredNode(long)
			longNode.ChunkEmbeddings[0][0] = 177
			longNode.EmbedMeta = map[string]any{"chunk_count": 16}
			var shortBody, longBody []byte
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				// Compatibility applies to embedding keys, not the node-body
				// format. Use the live store encoder and its existing k token.
				var separate bool
				var err error
				shortBody, separate, err = b.encodeNodeInTxn(txn, "test", shortNode)
				require.NoError(t, err)
				require.True(t, separate)
				longBody, separate, err = b.encodeNodeInTxn(txn, "test", longNode)
				require.NoError(t, err)
				require.True(t, separate)
				return nil
			}))
			c := bufferedCommit{namespace: "test", txID: fmt.Sprintf("compat-%v", removeLong), embeddingRanges: []NodeID{short, long}, writes: map[string][]byte{string(nodeKey(short)): shortBody, string(nodeKey(long)): longBody}, deletes: map[string]bool{}}
			for _, kv := range shortKVs {
				c.writes[string(kv.key)] = kv.val
			}
			for index, vector := range longNode.ChunkEmbeddings {
				payload, err := encodeEmbedding(vector)
				require.NoError(t, err)
				key := string(legacyEmbeddingKey(long, index))
				c.writes[key] = payload
				if removeLong {
					c.deletes[key] = true
				}
			}
			if removeLong {
				c.deletes[string(nodeKey(long))] = true
			}
			require.NoError(t, wal.AppendBufferedCommit(&c))
			crashClose(t, b, wal)
			b, wal = newWriteBehindWALTestEngine(t, dir, time.Hour)
			n, err := RecoverBufferedCommits(b, wal)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			n, err = RecoverBufferedCommits(b, wal)
			require.NoError(t, err)
			require.Zero(t, n)
			r8SeparateCheckPublished(t, b, shortNode, shortKVs)
			if removeLong {
				_, err := b.GetNode(long)
				require.ErrorIs(t, err, ErrNotFound)
				require.Zero(t, r7AExtensionLegacyKeys(t, b), "explicit point deletes win historical writes")
			} else {
				require.Equal(t, longNode.ChunkEmbeddings, embeddingCleanupColdNode(t, b, long).ChunkEmbeddings)
				require.Equal(t, 16, r7AExtensionLegacyKeys(t, b), "exact historical final range survives replay")
			}
		})
	}
}
