package storage

import (
	"context"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestNOR56StreamEmbeddingAbsenceUsesPinnedSnapshot(t *testing.T) {
	engine := fixture.Own(t, fixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	for _, id := range []NodeID{"test:a", "test:b", "other:c"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Properties: map[string]any{"id": string(id)}})
		require.NoError(t, err)
	}
	other, err := engine.GetNode(ctx, "other:c")
	require.NoError(t, err)
	other.EmbedMeta = map[string]any{"status": "other", "chunk_count": 1}
	other.ChunkEmbeddings = [][]float32{{3, 4}}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(ctx, other))
	later, err := engine.GetNode(ctx, "test:b")
	require.NoError(t, err)
	later.EmbedMeta = map[string]any{"status": "new", "chunk_count": 1}
	later.ChunkEmbeddings = [][]float32{{1, 2}}
	seen := map[NodeID]*Node{}
	err = engine.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: "test:", WithEmbeddings: true}, func(node *Node) error {
		seen[node.ID] = node
		if node.ID == "test:a" {
			return engine.UpdateNodeEmbeddingSidecar(ctx, later)
		}
		return nil
	})
	require.NoError(t, err)
	require.Len(t, seen, 2)
	require.Empty(t, seen["test:b"].EmbedMeta)
	require.Empty(t, seen["test:b"].ChunkEmbeddings)
	seen = map[NodeID]*Node{}
	require.NoError(t, engine.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: "test:", WithEmbeddings: true}, func(node *Node) error { seen[node.ID] = node; return nil }))
	require.Equal(t, "new", seen["test:b"].EmbedMeta["status"])
	require.Equal(t, [][]float32{{1, 2}}, seen["test:b"].ChunkEmbeddings)
}
