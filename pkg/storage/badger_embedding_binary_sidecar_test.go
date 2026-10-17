package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmbeddingCleanup_BinaryIDSidecarAndNamespace(t *testing.T) {
	b := newTestEngine(t)
	id := NodeID("test:binary\x00\x01:\xff")
	_, err := b.CreateNode(&Node{ID: id})
	require.NoError(t, err)
	n, err := b.GetNode(id)
	require.NoError(t, err)
	n.ChunkEmbeddings = [][]float32{make([]float32, 32768)}
	n.ChunkEmbeddings[0][3] = 17
	n.EmbedMeta = map[string]any{"chunk_count": 1, "embedding_failed": true}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(n))
	require.Equal(t, n.ChunkEmbeddings, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings)
	var parked []NodeID
	count, err := b.StreamParkedEmbeddingFailures(context.Background(), func(found NodeID, _ map[string]any) error { parked = append(parked, found); return nil })
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, []NodeID{id}, parked)
	removed, err := b.RepairOrphanedEmbeddingKeys()
	require.NoError(t, err)
	require.Zero(t, removed)
	require.Greater(t, separatorIDKeyCount(t, b, id), 3)
	nodes, _, err := b.DeleteByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(1), nodes)
	require.Zero(t, separatorIDKeyCount(t, b, id), "namespace deletion must include the binary key family")
}
