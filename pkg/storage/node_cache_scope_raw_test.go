package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Raw staged mutations are supported by the transaction commit writer too.
// Its flush replaces the staging maps before durable cache issuance.
func TestNodeCacheScopeRetainsRawWritesThroughFlush(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { _ = b.Close() })
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}})
	original := &Node{ID: "test:raw-scope", Properties: map[string]any{"v": int64(1)}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := b.CreateNode(original)
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	updated := copyNode(original)
	updated.Properties["v"] = int64(2)
	body, _, err := b.encodeNodeInTxn(tx.badgerTx, "test", updated)
	require.NoError(t, err)
	tx.bufferSet(nodeKey(updated.ID), body)
	require.Empty(t, tx.operations, "exercise a raw mutation without a node callback")
	require.NoError(t, tx.Commit())
	got, err := b.GetNode(updated.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])
}

// An embedding-only raw commit has no node operation or warm callback.
func TestNodeCacheScopeRetainsRawEmbeddingWritesThroughFlush(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { _ = b.Close() })
	vector := make([]float32, 20000)
	vector[0] = 1
	original := &Node{ID: "test:raw-vector-scope", ChunkEmbeddings: [][]float32{vector}}
	_, err := b.CreateNode(original)
	require.NoError(t, err)
	warm, err := b.GetNode(original.ID)
	require.NoError(t, err)
	require.Equal(t, float32(1), warm.ChunkEmbeddings[0][0])
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	vector[0] = 2
	kvs, err := buildEmbeddingChunkWriteKVs(original.ID, 0, vector)
	require.NoError(t, err)
	for _, kv := range kvs {
		tx.bufferSet(kv.key, kv.val)
	}
	require.Empty(t, tx.operations)
	require.NoError(t, tx.Commit())
	got, err := b.GetNode(original.ID)
	require.NoError(t, err)
	require.Equal(t, float32(2), got.ChunkEmbeddings[0][0])
}
