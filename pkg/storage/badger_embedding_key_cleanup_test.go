package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

const separateEmbeddingChunks = 16

// separatelyStoredNode is a node whose chunk vectors are too large for one
// record, so the engine keeps them under their own keys.
func separatelyStoredNode(id NodeID) *Node {
	chunks := make([][]float32, separateEmbeddingChunks)
	for i := range chunks {
		chunks[i] = make([]float32, 1024)
		chunks[i][i] = 1
	}
	return &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: chunks}
}

func countEmbeddingKeys(t testing.TB, engine *BadgerEngine, id NodeID) int {
	t.Helper()
	count := 0
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = embeddingPrefix(id)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	}))
	return count
}

// TestDeleteNode_RemovesEverySeparatelyStoredEmbeddingKey: deleting a node
// removes every one of its separately stored embedding keys. It used to pass
// the iterator's own key slice to txn.Delete, which keeps it until commit
// while the iterator reuses it, and left 14 of 16 keys behind (#1218).
func TestDeleteNode_RemovesEverySeparatelyStoredEmbeddingKey(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("test:big")
	_, err := engine.CreateNode(separatelyStoredNode(id))
	require.NoError(t, err)
	require.Equal(t, separateEmbeddingChunks, countEmbeddingKeys(t, engine, id))

	require.NoError(t, engine.DeleteNode(id))
	require.Zero(t, countEmbeddingKeys(t, engine, id))
}

// TestUpdateNode_ToInlineRemovesEverySeparatelyStoredEmbeddingKey: updating a
// node whose vectors were stored separately to one that fits a single record
// removes every old embedding key, and the node reads its new inline vector
// (#1218).
func TestUpdateNode_ToInlineRemovesEverySeparatelyStoredEmbeddingKey(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("test:shrink")
	_, err := engine.CreateNode(separatelyStoredNode(id))
	require.NoError(t, err)
	require.Equal(t, separateEmbeddingChunks, countEmbeddingKeys(t, engine, id))

	require.NoError(t, engine.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v2"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
	require.Zero(t, countEmbeddingKeys(t, engine, id))
	got, err := engine.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 0, 0}}, got.ChunkEmbeddings)
}

func TestDeleteEmbeddingKeysInTxn_ReadOnlyTxnFails(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("test:read-only")
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(embeddingKey(id, 0), []byte{1})
	}))
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		require.ErrorIs(t, deleteEmbeddingKeysInTxn(txn, id), badger.ErrReadOnlyTxn)
		return nil
	}))
}

func TestEmbeddingCleanup_CopiesKeysAndIncludesSidecarAndShards(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("test:owned-keys")
	_, err := engine.CreateNode(&Node{ID: id})
	require.NoError(t, err)
	node, err := engine.GetNode(id)
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{make([]float32, 32768)}
	node.ChunkEmbeddings[0][3] = 17
	node.EmbedMeta = map[string]any{"chunk_count": 1}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(node))
	var keys [][]byte
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		return visitEmbeddingKeys(txn, id, func(key []byte) error {
			keys = append(keys, key)
			return nil
		})
	}))
	require.Greater(t, len(keys), 3, "marker, shard parts and metadata")
	seen := make(map[string]bool)
	for _, key := range keys {
		require.False(t, seen[string(key)], "retained keys must not alias the iterator buffer")
		seen[string(key)] = true
		require.Equal(t, byte(0x08), key[0], "keep the released layout")
	}
	require.True(t, seen[string(embeddingMetaKey(id))])
	require.True(t, seen[string(embeddingChunkPartKey(id, 0, 0))])
	require.NoError(t, engine.DeleteNode(id))
	require.Zero(t, countEmbeddingKeys(t, engine, id))
}

// BenchmarkDeleteNode_SeparateEmbeddings deletes a node whose chunk vectors
// are stored under separate keys. Each iteration uses a new node ID: reusing
// one would pile up versions of the same keys, which every later scan of the
// prefix walks, and make the cost per delete grow with b.N.
func BenchmarkDeleteNode_SeparateEmbeddings(b *testing.B) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		id := NodeID(fmt.Sprintf("test:bench-big-%d", i))
		if _, err := engine.CreateNode(separatelyStoredNode(id)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := engine.DeleteNode(id); err != nil {
			b.Fatal(err)
		}
	}
}
