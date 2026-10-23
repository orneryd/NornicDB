package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func seedPriorEmbeddingNode(t *testing.T, b *BadgerEngine, id NodeID, binaryLength bool) *Node {
	t.Helper()
	n := separatelyStoredNode(id)
	_, err := b.CreateNode(n)
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		for i := range n.ChunkEmbeddings {
			current := embeddingKey(id, i)
			item, err := txn.Get(current)
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err = txn.Delete(current); err != nil {
				return err
			}
			prior := legacyEmbeddingKey(id, i)
			if binaryLength {
				// Exact unreleased R4 layout, independent of the new writer.
				prior = binary.BigEndian.AppendUint32([]byte{prefixEmbeddingBinary}, uint32(len(id)))
				prior = append(prior, []byte(id)...)
				prior = binary.BigEndian.AppendUint32(prior, uint32(i))
			}
			if err = txn.Set(prior, value); err != nil {
				return err
			}
		}
		return nil
	}))
	b.invalidateCachesAfterRestore()
	require.Equal(t, n.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
	return n
}

func TestEmbeddingCleanup_NewShardsPreservePriorOwners(t *testing.T) {
	for _, binaryLength := range []bool{false, true} {
		for _, action := range []string{"direct", "transaction", "sidecar", "buffered"} {
			t.Run(fmt.Sprintf("binary=%v/%s", binaryLength, action), func(t *testing.T) {
				b, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: action == "buffered", WriteBehindInterval: time.Hour}))
				require.NoError(t, err)
				defer b.Close()
				short, long := NodeID("test:alias"), NodeID("test:alias\x00\x00\x00")
				original := seedPriorEmbeddingNode(t, b, long, binaryLength)
				// This was an exact collision in the released encoding.
				legacyPart := append(append([]byte(nil), legacyEmbeddingKey(short, 0)...), 0, 0, 0)
				require.Equal(t, legacyEmbeddingKey(long, 0), legacyPart)
				require.NotEqual(t, legacyEmbeddingKey(long, 0), embeddingChunkPartKey(short, 0, 0))
				large := &Node{ID: short, ChunkEmbeddings: [][]float32{make([]float32, 32768)}}
				large.ChunkEmbeddings[0][3] = 17
				switch action {
				case "direct":
					_, err = b.CreateNode(large)
				case "sidecar":
					_, err = b.CreateNode(&Node{ID: short})
					require.NoError(t, err)
					err = b.UpdateNodeEmbeddingSidecar(large)
				default:
					tx, e := b.BeginTransaction()
					require.NoError(t, e)
					require.NoError(t, tx.SetImplicit(action == "buffered"))
					_, e = tx.CreateNode(large)
					require.NoError(t, e)
					err = tx.Commit()
					if err == nil {
						err = b.FlushWriteBehind()
					}
				}
				require.NoError(t, err)
				require.Equal(t, original.ChunkEmbeddings, embeddingCleanupColdNode(t, b, long).ChunkEmbeddings)
				require.Equal(t, large.ChunkEmbeddings, embeddingCleanupColdNode(t, b, short).ChunkEmbeddings)
				removed, err := b.RepairOrphanedEmbeddingKeys()
				require.NoError(t, err)
				require.Zero(t, removed)
				require.NoError(t, b.DeleteNode(short))
				require.Zero(t, separatorIDKeyCount(t, b, short))
				require.Equal(t, original.ChunkEmbeddings, embeddingCleanupColdNode(t, b, long).ChunkEmbeddings)
			})
		}
	}
}

func TestEmbeddingCleanup_AmbiguousOwnersShareReferencePolicy(t *testing.T) {
	for _, reference := range []string{"none", "body", "sidecar"} {
		t.Run(reference, func(t *testing.T) {
			b := newTestEngine(t)
			short, long := NodeID("test:unreferenced"), NodeID("test:unreferenced\x00\x00\x00")
			for _, id := range []NodeID{short, long} {
				node := &Node{ID: id}
				if reference == "body" && id == short {
					node = separatelyStoredNode(id)
				}
				_, err := b.CreateNode(node)
				require.NoError(t, err)
			}
			key := legacyEmbeddingKey(long, 0)
			require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
				if reference == "sidecar" {
					meta, err := encodeValue(map[string]any{"chunk_count": 1, embeddingContentUpdatedAtKey: "stale"})
					if err != nil {
						return err
					}
					if err = txn.Set(embeddingMetaKey(short), meta); err != nil {
						return err
					}
				}
				return txn.Set(key, []byte{1})
			}))
			removed, err := b.RepairOrphanedEmbeddingKeys()
			require.NoError(t, err)
			if reference == "none" {
				require.Equal(t, 1, removed)
			} else {
				require.Zero(t, removed)
			}
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				_, err := txn.Get(key)
				if reference == "none" {
					require.ErrorIs(t, err, badger.ErrKeyNotFound)
				} else {
					require.NoError(t, err)
				}
				return nil
			}))
			removed, err = b.RepairOrphanedEmbeddingKeys()
			require.NoError(t, err)
			require.Zero(t, removed)
		})
	}
}

func TestEmbeddingCleanup_KeyCodecSurvivesWALJSON(t *testing.T) {
	for _, length := range []int{127, 128, 200, 256} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			id := NodeID("test:\x00" + strings.Repeat("x", length-6))
			data := WALBufferedCommitData{Writes: make(map[string][]byte)}
			for _, index := range []int{0, 128, reservedEmbeddingMetaChunkIndex} {
				kvs, err := buildEmbeddingChunkWriteKVs(id, index, make([]float32, 32768))
				require.NoError(t, err)
				for _, kv := range kvs {
					require.True(t, utf8.Valid(kv.key), "valid ID must not gain invalid UTF-8 framing")
					owners := embeddingKeyOwners(kv.key)
					require.Len(t, owners, 1)
					require.Equal(t, id, owners[0].id)
					require.Equal(t, uint32(index), owners[0].index)
					data.Writes[string(kv.key)] = kv.val
					data.Deletes = append(data.Deletes, string(kv.key))
				}
			}
			encoded, err := json.Marshal(data)
			require.NoError(t, err)
			var decoded WALBufferedCommitData
			require.NoError(t, json.Unmarshal(encoded, &decoded))
			require.Equal(t, data.Writes, decoded.Writes)
			require.Equal(t, data.Deletes, decoded.Deletes)
		})
	}
}

func TestEmbeddingCleanup_WALRecoveryAtIDLengthBoundary(t *testing.T) {
	for _, length := range []int{127, 128, 200, 256} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			id := NodeID("test:" + strings.Repeat("x", length-6) + "\x00")
			dir := t.TempDir()
			b, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
			wanted := separatelyStoredNode(id)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			_, err = tx.CreateNode(wanted)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			require.True(t, tx.AcknowledgedBuffered())
			require.Zero(t, countEmbeddingKeys(t, b, id), "ACK precedes buffer flush")
			crashClose(t, b, wal)
			b, wal = newWriteBehindWALTestEngine(t, dir, time.Hour)
			defer b.Close()
			defer wal.Close()
			replayed, err := RecoverBufferedCommits(b, wal)
			require.NoError(t, err)
			require.Equal(t, 1, replayed)
			require.Equal(t, 16, countEmbeddingKeys(t, b, id))
			require.Equal(t, wanted.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
		})
	}
}
