package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func r8SeparateReplacementNode(t *testing.T, id NodeID, index int) (*Node, []embeddingWriteKV) {
	t.Helper()
	vector := make([]float32, 32768)
	for i := range vector {
		vector[i] = float32((i%97)+1) * float32(index+1) / 7
	}
	encoded, err := encodeEmbedding(vector)
	require.NoError(t, err)
	require.Greater(t, len(encoded), maxEmbeddingValueBytes, "SETUP: replacement must require actual shards")
	require.Greater(t, len(encoded), maxNodeSize, "SETUP: replacement cannot fit inline")
	node := &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: [][]float32{vector}, EmbedMeta: map[string]any{"chunk_count": 1, "replacement_owner": string(id)}}
	kvs, err := buildEmbeddingChunkWriteKVs(id, 0, vector)
	require.NoError(t, err)
	require.Greater(t, len(kvs), 1, "SETUP: expected marker plus multiple parts")
	for _, kv := range kvs {
		require.Equal(t, prefixEmbeddingCurrent, kv.key[0], "SETUP: disjoint0x28 writes")
	}
	return node, kvs
}

func r8SeparateCheckPublished(t *testing.T, b *BadgerEngine, want *Node, kvs []embeddingWriteKV) {
	t.Helper()
	t.Run("stored-and-cold", func(t *testing.T) {
		stored := r7AExtensionStoredBody(t, b, want.ID)
		require.True(t, stored.EmbeddingsStoredSeparately, "new body still separately stored")
		require.Equal(t, 1, chunkCountFromMeta(stored.EmbedMeta))
		require.Equal(t, string(want.ID), stored.EmbedMeta["replacement_owner"])
		cold := embeddingCleanupColdNode(t, b, want.ID)
		require.Equal(t, want.ChunkEmbeddings, cold.ChunkEmbeddings, "true cold read of exact NEW sharded vector")
		require.Equal(t, 1, chunkCountFromMeta(cold.EmbedMeta))
		require.Equal(t, string(want.ID), cold.EmbedMeta["replacement_owner"])
	})
	t.Run("actual-current-chunks-and-metadata", func(t *testing.T) {
		require.NoError(t, b.withView(func(txn *badger.Txn) error {
			for _, kv := range kvs {
				item, err := txn.Get(kv.key)
				if err != nil {
					return err
				}
				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				require.Equal(t, kv.val, value, "exact NEW0x28 marker/part payload")
			}
			for index := 1; index < 16; index++ {
				_, err := txn.Get(embeddingKey(want.ID, index))
				require.ErrorIs(t, err, badger.ErrKeyNotFound, "old current chunks replaced")
			}
			_, err := txn.Get(embeddingMetaKey(want.ID))
			require.ErrorIs(t, err, badger.ErrKeyNotFound, "old public sidecar metadata retired; new count is in body")
			return nil
		}))
		require.Equal(t, len(kvs), countEmbeddingKeys(t, b, want.ID), "only new0x28 base/parts remain")
	})
}

func TestEmbeddingCleanup_DualSeparateReplacement(t *testing.T) {
	for _, scenario := range []string{"both-short-long", "both-long-short", "survivor-long", "survivor-short", "single-current", "single-released"} {
		t.Run(scenario, func(t *testing.T) {
			b := newTestEngine(t)
			var ids []NodeID
			var survivor NodeID
			var original map[NodeID][][]float32
			if strings.HasPrefix(scenario, "single-") {
				id := NodeID("test:single")
				if scenario == "single-released" {
					id = "test:single\x00\x00\x00"
					seedPriorEmbeddingNode(t, b, id, false)
				} else {
					_, err := b.CreateNode(separatelyStoredNode(id))
					require.NoError(t, err)
				}
				sidecar := embeddingCleanupColdNode(t, b, id)
				sidecar.EmbedMeta = map[string]any{"chunk_count": 16, "fixture_owner": string(id)}
				require.NoError(t, b.UpdateNodeEmbeddingSidecar(sidecar))
				require.Equal(t, sidecar.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings, "SETUP: exact public single-owner16vectors")
				require.True(t, r7AExtensionStoredBody(t, b, id).EmbeddingsStoredSeparately)
				require.NoError(t, b.withView(func(txn *badger.Txn) error {
					item, err := txn.Get(embeddingMetaKey(id))
					if err != nil {
						return err
					}
					var meta map[string]any
					if err = item.Value(func(value []byte) error { return decodeValue(value, &meta) }); err != nil {
						return err
					}
					require.Equal(t, 16, chunkCountFromMeta(meta))
					require.Equal(t, string(id), meta["fixture_owner"])
					return nil
				}))
				require.Zero(t, r7AExtensionLegacyKeys(t, b), "SETUP: public single-owner sidecar migrates its unshared prior range")
				ids = []NodeID{id}
				t.Log("SETUP PASS: genuine single owner, cold16vectors, rawseparatebody and publicvalidmetadata")
			} else {
				short, long, sidecars := r7AExtensionOwners(t, b)
				original = sidecars
				switch scenario {
				case "both-short-long":
					ids = []NodeID{short, long}
				case "both-long-short":
					ids = []NodeID{long, short}
				case "survivor-long":
					ids = []NodeID{short}
					survivor = long
				case "survivor-short":
					ids = []NodeID{long}
					survivor = short
				}
			}
			replacements := make([]*Node, 0, len(ids))
			expected := make([][]embeddingWriteKV, 0, len(ids))
			for _, id := range ids {
				vectorIndex := 0
				if strings.IndexByte(string(id), 0) >= 0 {
					vectorIndex = 1
				}
				node, kvs := r8SeparateReplacementNode(t, id, vectorIndex)
				replacements = append(replacements, node)
				expected = append(expected, kvs)
			}
			t.Log("REPLACEMENT SETUP PASS: distinct NEW vectors require separate sharded0x28 storage")
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			for _, node := range replacements {
				require.NoError(t, tx.UpdateNode(node))
			}
			require.NoError(t, tx.Commit())
			t.Logf("COMMIT PASS: one explicit transaction replaced %d range(s)", len(replacements))
			for index, node := range replacements {
				t.Run(fmt.Sprintf("new-owner-%d", index), func(t *testing.T) { r8SeparateCheckPublished(t, b, node, expected[index]) })
			}
			if survivor != "" {
				t.Run("genuine-survivor", func(t *testing.T) {
					live := embeddingCleanupColdNode(t, b, survivor)
					require.Equal(t, original[survivor], live.ChunkEmbeddings, "untouched owner's exact16vectors remain")
					require.True(t, r7AExtensionStoredBody(t, b, survivor).EmbeddingsStoredSeparately)
					require.NoError(t, b.withView(func(txn *badger.Txn) error {
						_, err := txn.Get(embeddingMetaKey(survivor))
						if err != nil {
							return err
						}
						_, err = txn.Get(legacyEmbeddingKey("test:alias\x00\x00\x00", 0))
						return err
					}))
					require.Equal(t, 16, r7AExtensionLegacyKeys(t, b), "preserve SAME16 sharedlegacykeys for genuine survivor")
				})
			} else {
				t.Run("obsolete-legacy-cleanup", func(t *testing.T) {
					remaining := r7AExtensionLegacyKeys(t, b)
					t.Logf("actual SAME physical legacy family remaining=%d; expected0, dual setup16 not32", remaining)
					require.Zero(t, remaining, "all participating old ranges are replaced into disjoint0x28")
				})
			}
		})
	}
}
