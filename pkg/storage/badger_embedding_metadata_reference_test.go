package storage

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// This fixture performs no raw writes beyond the unchanged released-layout
// seed helper. Both sidecars are populated only through the public writer.
func r7AExtensionOwners(t *testing.T, b *BadgerEngine) (NodeID, NodeID, map[NodeID][][]float32) {
	t.Helper()
	short, long := NodeID("test:alias"), NodeID("test:alias\x00\x00\x00")
	prior := seedPriorEmbeddingNode(t, b, long, false)
	current := separatelyStoredNode(short)
	_, err := b.CreateNode(current)
	require.NoError(t, err, "SETUP: create current short owner")
	require.Equal(t, prior.ChunkEmbeddings, embeddingCleanupColdNode(t, b, long).ChunkEmbeddings, "SETUP: real released owner before sidecars")
	require.Equal(t, current.ChunkEmbeddings, embeddingCleanupColdNode(t, b, short).ChunkEmbeddings, "SETUP: current owner before sidecars")
	alias := legacyEmbeddingKey(long, 0)
	require.Equal(t, alias, embeddingPartKey(legacyEmbeddingKey(short, 0), 0))
	require.NotEqual(t, alias, embeddingChunkPartKey(short, 0, 0))
	require.Equal(t, 16, r7AExtensionLegacyKeys(t, b), "SETUP: SAME16 physical legacy keys, not32")
	want := make(map[NodeID][][]float32)
	for ownerIndex, id := range []NodeID{long, short} {
		sidecar := embeddingCleanupColdNode(t, b, id)
		sidecar.ChunkEmbeddings = separatelyStoredNode(id).ChunkEmbeddings
		for index := range sidecar.ChunkEmbeddings {
			sidecar.ChunkEmbeddings[index][(index+7)%1024] = float32(ownerIndex + 3)
		}
		sidecar.EmbedMeta = map[string]any{"chunk_count": 16, "fixture_owner": string(id)}
		require.NoError(t, b.UpdateNodeEmbeddingSidecar(sidecar), "SETUP: public sidecar publication")
		want[id] = sidecar.ChunkEmbeddings
		require.Equal(t, 16, r7AExtensionLegacyKeys(t, b), "SETUP: public writer preserves real other-owner aliases")
	}
	for _, id := range []NodeID{short, long} {
		cold := embeddingCleanupColdNode(t, b, id)
		require.Equal(t, want[id], cold.ChunkEmbeddings, "SETUP: cold exact16 sidecar vectors")
		require.Equal(t, string(id), cold.EmbedMeta["fixture_owner"], "SETUP: public metadata is visible")
		stored := r7AExtensionStoredBody(t, b, id)
		require.True(t, stored.EmbeddingsStoredSeparately, "SETUP: original body is still separate")
		require.NoError(t, b.withView(func(txn *badger.Txn) error {
			item, err := txn.Get(embeddingMetaKey(id))
			if err != nil {
				return err
			}
			var meta map[string]any
			if err = item.Value(func(value []byte) error { return decodeValue(value, &meta) }); err != nil {
				return err
			}
			require.Equal(t, 16, chunkCountFromMeta(meta), "SETUP: actual valid metadata")
			require.Equal(t, string(id), meta["fixture_owner"])
			_, err = txn.Get(alias)
			return err
		}))
	}
	t.Log("SETUP PASS: real released long/current short, both separate bodies, cold exact16 vectors each, public valid metadata BOTH, SAME16 legacy alias keys")
	return short, long, want
}

func r7AExtensionLegacyKeys(t *testing.T, b *BadgerEngine) int {
	t.Helper()
	count := 0
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{prefixEmbedding}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	}))
	return count
}

func r7AExtensionStoredBody(t *testing.T, b *BadgerEngine, id NodeID) *Node {
	t.Helper()
	var node *Node
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error { var err error; node, _, err = decodeNodePayload(value); return err })
	}))
	return node
}

func r7AExtensionInline(id NodeID, index int) *Node {
	return &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: [][]float32{{float32(index + 21), float32(index + 51)}}}
}

func r7AExtensionCheckInline(t *testing.T, b *BadgerEngine, want *Node) {
	t.Helper()
	t.Run("body", func(t *testing.T) {
		stored := r7AExtensionStoredBody(t, b, want.ID)
		require.False(t, stored.EmbeddingsStoredSeparately)
		require.Equal(t, want.ChunkEmbeddings, stored.ChunkEmbeddings, "exact inline stored vectors")
		require.Equal(t, want.ChunkEmbeddings, embeddingCleanupColdNode(t, b, want.ID).ChunkEmbeddings, "exact cold vectors")
	})
	t.Run("metadata", func(t *testing.T) {
		require.NoError(t, b.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(embeddingMetaKey(want.ID))
			require.ErrorIs(t, err, badger.ErrKeyNotFound, "current metadata removed")
			return nil
		}))
	})
}

func TestEmbeddingCleanup_MultiOwnerInlineSidecars(t *testing.T) {
	for _, longFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("longFirst=%v", longFirst), func(t *testing.T) {
			b := newTestEngine(t)
			short, long, _ := r7AExtensionOwners(t, b)
			first, second := r7AExtensionInline(short, 0), r7AExtensionInline(long, 1)
			if longFirst {
				first, second = second, first
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.UpdateNode(first))
			require.NoError(t, tx.UpdateNode(second))
			require.NoError(t, tx.Commit())
			t.Log("COMMIT PASS: both inline replacements acknowledged in one explicit transaction")
			for _, node := range []*Node{first, second} {
				t.Run(string(node.ID), func(t *testing.T) { r7AExtensionCheckInline(t, b, node) })
			}
			t.Run("physical-legacy-cleanup", func(t *testing.T) {
				count := r7AExtensionLegacyKeys(t, b)
				t.Logf("SAME physical legacy key family remaining=%d (expected0; setup16)", count)
				require.Zero(t, count, "both owners inline; no alias is separately referenced")
			})
		})
	}
}

func TestEmbeddingCleanup_MultiOwnerInlineSurvivor(t *testing.T) {
	for _, shrinkLong := range []bool{false, true} {
		t.Run(fmt.Sprintf("shrinkLong=%v", shrinkLong), func(t *testing.T) {
			b := newTestEngine(t)
			short, long, sidecars := r7AExtensionOwners(t, b)
			shrink, survivor := short, long
			if shrinkLong {
				shrink, survivor = long, short
			}
			next := r7AExtensionInline(shrink, 0)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.UpdateNode(next))
			require.NoError(t, tx.Commit())
			r7AExtensionCheckInline(t, b, next)
			live := embeddingCleanupColdNode(t, b, survivor)
			require.Equal(t, sidecars[survivor], live.ChunkEmbeddings, "genuine separate survivor exact vectors")
			require.True(t, r7AExtensionStoredBody(t, b, survivor).EmbeddingsStoredSeparately)
			require.NoError(t, b.withView(func(txn *badger.Txn) error { _, err := txn.Get(embeddingMetaKey(survivor)); return err }))
			require.Equal(t, 16, r7AExtensionLegacyKeys(t, b), "preserve SAME16 aliases while other owner is separately referenced")
		})
	}
}

func TestEmbeddingCleanup_SingleInlineSidecar(t *testing.T) {
	for _, id := range []NodeID{"test:single", "test:single\x00\x00\x00"} {
		t.Run(string(id), func(t *testing.T) {
			b := newTestEngine(t)
			node := separatelyStoredNode(id)
			_, err := b.CreateNode(node)
			require.NoError(t, err)
			sidecar := embeddingCleanupColdNode(t, b, id)
			sidecar.EmbedMeta = map[string]any{"chunk_count": 16, "fixture_owner": string(id)}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(sidecar))
			require.Equal(t, sidecar.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings, "SETUP: public single owner exact16 vectors")
			require.NoError(t, b.withView(func(txn *badger.Txn) error { _, err := txn.Get(embeddingMetaKey(id)); return err }))
			t.Log("SETUP PASS: single owner/public valid metadata")
			next := r7AExtensionInline(id, 0)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.UpdateNode(next))
			require.NoError(t, tx.Commit())
			r7AExtensionCheckInline(t, b, next)
			require.Zero(t, countEmbeddingKeys(t, b, id))
			require.Zero(t, r7AExtensionLegacyKeys(t, b))
		})
	}
}
func TestEmbeddingCleanup_MultiOwnerInlineBuffered(t *testing.T) {
	for _, longFirst := range []bool{false, true} {
		for _, finish := range []string{"flush", "recovery", "rollback"} {
			t.Run(fmt.Sprintf("longFirst=%v/%s", longFirst, finish), func(t *testing.T) {
				dir := t.TempDir()
				b, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
				defer func() { _ = b.Close(); _ = wal.Close() }()
				short, long, sidecars := r7AExtensionOwners(t, b)
				first, second := r7AExtensionInline(short, 0), r7AExtensionInline(long, 1)
				if longFirst {
					first, second = second, first
				}
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.UpdateNode(first))
				require.NoError(t, tx.UpdateNode(second))
				if finish == "rollback" {
					require.NoError(t, tx.Rollback())
					require.Zero(t, b.writeBehind.PendingOps())
					for _, id := range []NodeID{short, long} {
						require.True(t, r7AExtensionStoredBody(t, b, id).EmbeddingsStoredSeparately)
						require.Equal(t, sidecars[id], embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
						require.NoError(t, b.withView(func(txn *badger.Txn) error { _, err := txn.Get(embeddingMetaKey(id)); return err }))
					}
					require.Equal(t, 16, r7AExtensionLegacyKeys(t, b))
					return
				}
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
				require.Greater(t, b.writeBehind.PendingOps(), 0, "acknowledged replacements remain unflushed")
				require.Equal(t, 16, r7AExtensionLegacyKeys(t, b), "old physical keys exist until captured commit is applied")
				if finish == "flush" {
					require.NoError(t, b.FlushWriteBehind())
				} else {
					crashClose(t, b, wal)
					b, wal = newWriteBehindWALTestEngine(t, dir, time.Hour)
					replayed, err := RecoverBufferedCommits(b, wal)
					require.NoError(t, err)
					require.Equal(t, 1, replayed)
					replayed, err = RecoverBufferedCommits(b, wal)
					require.NoError(t, err)
					require.Zero(t, replayed, "second replay preserves final replacement")
				}
				for _, node := range []*Node{first, second} {
					t.Run(string(node.ID), func(t *testing.T) { r7AExtensionCheckInline(t, b, node) })
				}
				require.Zero(t, r7AExtensionLegacyKeys(t, b), "implicit metadata retirement must survive captured replay")
			})
		}
	}
}

func TestEmbeddingReferenceView_RangeMetadataPrecedence(t *testing.T) {
	for _, binaryID := range []bool{false, true} {
		for _, family := range []string{"current", "released", "r4"} {
			if family == "r4" && !binaryID {
				continue
			}
			t.Run(fmt.Sprintf("binary=%v/%s", binaryID, family), func(t *testing.T) {
				b := newTestEngine(t)
				id := NodeID("test:metadata-range")
				if binaryID {
					id += "\x00separator"
				}
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
				require.NoError(t, err)
				sidecar := embeddingCleanupColdNode(t, b, id)
				sidecar.ChunkEmbeddings = [][]float32{{2, 3}}
				sidecar.EmbedMeta = map[string]any{"chunk_count": 1, "fixture_family": family}
				require.NoError(t, b.UpdateNodeEmbeddingSidecar(sidecar))
				key := embeddingMetaKey(id)
				if family == "released" {
					key = legacyEmbeddingKey(id, reservedEmbeddingMetaChunkIndex)
				}
				if family == "r4" {
					key = binary.BigEndian.AppendUint32(binaryEmbeddingPrefix(id), uint32(reservedEmbeddingMetaChunkIndex))
				}
				var metadata []byte
				require.NoError(t, b.withView(func(txn *badger.Txn) error {
					item, err := txn.Get(embeddingMetaKey(id))
					if err != nil {
						return err
					}
					metadata, err = item.ValueCopy(nil)
					return err
				}))
				if family != "current" {
					require.NoError(t, b.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
						if err := txn.Delete(embeddingMetaKey(id)); err != nil {
							return err
						}
						return txn.Set(key, metadata)
					}))
				}
				require.Equal(t, sidecar.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings, "valid public metadata moved to supported compatibility address")
				require.NoError(t, b.withView(func(txn *badger.Txn) error {
					referenced, err := embeddingOwnerReferenced(txn, id)
					require.NoError(t, err)
					require.True(t, referenced, "persisted metadata genuinely references inline body")
					replacements := []NodeID{id}
					retired := newEmbeddingReferenceView(txn, nil, nil, replacements)
					replacements[0] = "test:changed-after-freeze"
					referenced, err = retired.ownerReferenced(id)
					require.NoError(t, err)
					require.False(t, referenced, "range intent retires old metadata before physical scan")
					writes := map[string][]byte{string(key): metadata}
					renewed := newEmbeddingReferenceView(txn, writes, nil, []NodeID{id})
					delete(writes, string(key))
					referenced, err = renewed.ownerReferenced(id)
					require.NoError(t, err)
					require.True(t, referenced, "explicit final new metadata wins implicit retirement and freezes map membership")
					deleted := newEmbeddingReferenceView(txn, map[string][]byte{string(key): metadata}, map[string]bool{string(key): true}, []NodeID{id})
					referenced, err = deleted.ownerReferenced(id)
					require.NoError(t, err)
					require.False(t, referenced, "explicit final delete wins explicit write")
					referenced, err = embeddingOwnerReferenced(txn, id)
					require.NoError(t, err)
					require.True(t, referenced, "views never mutate physical reference state")
					return nil
				}))
			})
		}
	}
}
