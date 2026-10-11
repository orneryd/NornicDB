package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// GetNodeEmbeddingMeta reads the embedding metadata the worker writes to
// Badger's sidecar, which embedding-free reads leave out, through every
// layer of the server's storage stack: pending write-behind nodes answer
// from their own state, deleted ones are gone, and a sidecar the body
// outdated reads as no metadata.
func TestGetNodeEmbeddingMetaThroughStorageStack(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "db")
	badgerEngine, err := NewBadgerEngine(directory)
	require.NoError(t, err)
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(directory, "wal"), SyncMode: "immediate"})
	require.NoError(t, err)
	async := NewAsyncEngine(NewWALEngine(badgerEngine, wal), &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = async.Close() })
	stack := NewNamespacedEngine(async, "nornic")

	_, err = stack.CreateNode(&Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha beta"}})
	require.NoError(t, err)
	require.NoError(t, async.Flush())
	stored, err := badgerEngine.GetNode("nornic:doc")
	require.NoError(t, err)
	stored.ChunkEmbeddings = [][]float32{{1, 0}}
	stored.EmbedMeta = map[string]any{"chunk_count": 1, "chunk_texts": []string{"text: alpha beta"}}
	require.NoError(t, badgerEngine.UpdateNodeEmbeddingSidecar(stored))

	light, err := stack.GetNodeWithoutEmbeddings("doc")
	require.NoError(t, err)
	require.Empty(t, light.EmbedMeta, "the sidecar isn't part of an embedding-free read")
	for name, reader := range map[string]NodeEmbeddingMetaReader{"badger": badgerEngine, "stack": stack} {
		id := NodeID("doc")
		if name == "badger" {
			id = "nornic:doc"
		}
		meta, err := reader.GetNodeEmbeddingMeta(id)
		require.NoError(t, err, name)
		require.Equal(t, 1, chunkCountFromMeta(meta), name)
		require.Equal(t, []any{"text: alpha beta"}, meta["chunk_texts"], name)
		require.NotContains(t, meta, embeddingContentUpdatedAtKey, name)
	}

	_, err = stack.GetNodeEmbeddingMeta("missing")
	require.ErrorIs(t, err, ErrNotFound)

	// Pending write-behind nodes: with metadata, without, and deleted.
	_, err = stack.CreateNode(&Node{ID: "pending", Properties: map[string]any{"text": "x"}, EmbedMeta: map[string]any{"chunk_count": 2}})
	require.NoError(t, err)
	meta, err := stack.GetNodeEmbeddingMeta("pending")
	require.NoError(t, err)
	require.Equal(t, 2, chunkCountFromMeta(meta))
	_, err = stack.CreateNode(&Node{ID: "bare", Properties: map[string]any{"text": "y"}})
	require.NoError(t, err)
	meta, err = stack.GetNodeEmbeddingMeta("bare")
	require.NoError(t, err)
	require.Nil(t, meta)
	require.NoError(t, stack.DeleteNode("bare"))
	_, err = stack.GetNodeEmbeddingMeta("bare")
	require.ErrorIs(t, err, ErrNotFound)

	// A body change outdates the sidecar: the node reads as not embedded.
	stored, err = badgerEngine.GetNode("nornic:doc")
	require.NoError(t, err)
	stored.Properties["text"] = "changed"
	stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)
	require.NoError(t, badgerEngine.UpdateNode(stored))
	meta, err = badgerEngine.GetNodeEmbeddingMeta("nornic:doc")
	require.NoError(t, err)
	require.Empty(t, meta["chunk_texts"])
}
