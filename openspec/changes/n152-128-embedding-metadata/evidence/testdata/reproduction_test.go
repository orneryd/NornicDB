package storage

// NOR-330 reproduction: the oversized-node encode path adds chunk_count to the
// EmbedMeta map it is handed, which is the caller's map when the caller set
// one. Storage owns its writes: the entry must go into storage's own copy and
// the caller's metadata must be left exactly as supplied.
//
// The first three subtests are the reported routes (direct engine create, bulk
// create and explicit transaction create). The other three are controls that
// show the assertions are not vacuous: namespaced create and update already
// copy the caller's node, and a node inside the inline limit never reaches the
// separate-embedding branch that writes chunk_count.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// nor330CallerEmbedMeta is the caller-owned metadata the reproduction hands to
// storage. It deliberately has no chunk_count: that is the entry the encode
// path writes when it stores an oversized node's vectors separately.
func nor330CallerEmbedMeta() map[string]any {
	return map[string]any{"has_embedding": true}
}

// nor330OversizedNode holds more chunk vector values than maxNodeSize bytes,
// so encodeNodeInTxn cannot keep them in the node body (the same oversized
// shape the NOR-412 bug 35 tests use). The vectors stay caller-owned.
func nor330OversizedNode(id NodeID) *Node {
	return &Node{
		ID:              id,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "alpha"},
		ChunkEmbeddings: chunkVectors(60, 1024, 3),
		EmbedMeta:       nor330CallerEmbedMeta(),
	}
}

// nor330RequireChunksStoredApart proves the separate-embedding branch ran for
// id: the stored body records a chunk_count of more than one chunk and every
// chunk reads back from its own stored record.
func nor330RequireChunksStoredApart(t *testing.T, engine *BadgerEngine, id NodeID, chunks [][]float32) {
	t.Helper()
	require.Greater(t, len(chunks), 1, "the fixture must hold multiple chunks")
	stored, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	storedCount := chunkCountFromMeta(stored.EmbedMeta)
	require.Equal(t, len(chunks), storedCount,
		"the stored body records the chunk count written by the separate-embedding branch")
	require.Len(t, stored.ChunkEmbeddings, len(chunks), "every separately stored chunk reads back")
	sameFloatBits(t, chunks[0], stored.ChunkEmbeddings[0])
	sameFloatBits(t, chunks[len(chunks)-1], stored.ChunkEmbeddings[len(chunks)-1])
	t.Logf("stored apart: chunk_count=%d, hydrated chunks=%d", storedCount, len(stored.ChunkEmbeddings))
}

// nor330RequireCallerEmbedMetaIntact is the contract under reproduction:
// storage never writes into the metadata map the caller supplied.
func nor330RequireCallerEmbedMetaIntact(t *testing.T, meta map[string]any) {
	t.Helper()
	require.NotContains(t, meta, "chunk_count", "storage wrote chunk_count into the caller's EmbedMeta map")
	require.Equal(t, nor330CallerEmbedMeta(), meta)
}

func TestNOR330EmbeddingEncodeKeepsCallerEmbedMetaOwned(t *testing.T) {
	t.Run("direct CreateNode", func(t *testing.T) {
		engine := newTestMemoryEngine(t).BadgerEngine
		node := nor330OversizedNode("test:direct")
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
		nor330RequireChunksStoredApart(t, engine, node.ID, node.ChunkEmbeddings)
		nor330RequireCallerEmbedMetaIntact(t, node.EmbedMeta)
	})

	t.Run("BulkCreateNodes", func(t *testing.T) {
		engine := newTestMemoryEngine(t).BadgerEngine
		node := nor330OversizedNode("test:bulk")
		require.NoError(t, engine.BulkCreateNodes(t.Context(), []*Node{node}))
		nor330RequireChunksStoredApart(t, engine, node.ID, node.ChunkEmbeddings)
		nor330RequireCallerEmbedMetaIntact(t, node.EmbedMeta)
	})

	t.Run("explicit transaction CreateNode", func(t *testing.T) {
		engine := newTestMemoryEngine(t).BadgerEngine
		node := nor330OversizedNode("test:transaction")
		tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		_, err = tx.CreateNode(t.Context(), node)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		nor330RequireChunksStoredApart(t, engine, node.ID, node.ChunkEmbeddings)
		nor330RequireCallerEmbedMetaIntact(t, node.EmbedMeta)
	})

	t.Run("control: namespaced CreateNode", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		store := NewNamespacedEngine(engine, "tenant")
		node := nor330OversizedNode("doc")
		_, err := store.CreateNode(t.Context(), node)
		require.NoError(t, err)
		nor330RequireChunksStoredApart(t, engine.BadgerEngine, "tenant:doc", node.ChunkEmbeddings)
		nor330RequireCallerEmbedMetaIntact(t, node.EmbedMeta)
	})

	t.Run("control: namespaced UpdateNode", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		store := NewNamespacedEngine(engine, "tenant")
		_, err := store.CreateNode(t.Context(), &Node{
			ID:         "doc",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"text": "alpha"},
		})
		require.NoError(t, err)
		node := nor330OversizedNode("doc")
		require.NoError(t, store.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
		nor330RequireChunksStoredApart(t, engine.BadgerEngine, "tenant:doc", node.ChunkEmbeddings)
		nor330RequireCallerEmbedMetaIntact(t, node.EmbedMeta)
	})

	t.Run("control: node inside the inline limit", func(t *testing.T) {
		engine := newTestMemoryEngine(t).BadgerEngine
		node := &Node{
			ID:              "test:inline",
			Labels:          []string{"Doc"},
			Properties:      map[string]any{"text": "alpha"},
			ChunkEmbeddings: chunkVectors(3, 16, 7),
			EmbedMeta:       nor330CallerEmbedMeta(),
		}
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
		stored, err := engine.GetNode(t.Context(), node.ID)
		require.NoError(t, err)
		require.Len(t, stored.ChunkEmbeddings, len(node.ChunkEmbeddings))
		require.NotContains(t, stored.EmbedMeta, "chunk_count",
			"a node stored inline has no separately stored chunks to count")
		nor330RequireCallerEmbedMetaIntact(t, node.EmbedMeta)
		t.Logf("stored inline: chunks=%d, no stored chunk_count", len(stored.ChunkEmbeddings))
	})
}
