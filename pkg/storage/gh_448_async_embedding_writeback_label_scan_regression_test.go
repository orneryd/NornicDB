package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGH448_EmbeddingWritebackPersistsAcrossBadgerReopen(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "badger")
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dataDir})
	require.NoError(t, err)
	config := DefaultAsyncEngineConfig()
	config.FlushInterval = time.Hour
	async := NewAsyncEngine(NewNamespacedEngine(engine, "gh448_disk"), config)
	t.Cleanup(func() {
		if async != nil {
			_ = async.Close()
		}
		if engine != nil {
			_ = engine.Close()
		}
	})

	for _, id := range []NodeID{"embedded", "pending"} {
		_, err := async.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"id": string(id)}})
		require.NoError(t, err)
	}
	require.NoError(t, async.Flush())
	node, err := async.GetNode("embedded")
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{0.25, 0.5, 0.75}}
	node.EmbedMeta = map[string]any{"embedded": true, "chunk_count": 1}
	require.NoError(t, async.UpdateNodeEmbeddingSidecar(node))
	staged, err := async.GetNodesByLabel("Doc")
	require.NoError(t, err)
	require.Len(t, staged, 2)
	require.NoError(t, async.Flush())
	require.NoError(t, async.Close())
	async = nil
	require.NoError(t, engine.Close())
	engine = nil

	engine, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dataDir})
	require.NoError(t, err)
	store := NewNamespacedEngine(engine, "gh448_disk")
	nodes, err := store.GetNodesByLabel("Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	count, err := store.NodeCountByLabel("Doc")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	reloaded, err := store.GetNode("embedded")
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.25, 0.5, 0.75}}, reloaded.ChunkEmbeddings)
	require.Equal(t, map[string]any{"embedded": true, "chunk_count": 1}, reloaded.EmbedMeta)
}
