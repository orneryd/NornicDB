package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
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

// The metadata read's failures: a closed engine, an unreadable node record or
// sidecar record, a node deleted while its delete is pending, and an engine
// without the read (the full node's metadata).
func TestGetNodeEmbeddingMetaFailures(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "nornic:a", Properties: map[string]any{"text": "x"}})
	require.NoError(t, err)
	_, err = engine.GetNodeEmbeddingMeta("")
	require.ErrorIs(t, err, ErrInvalidID)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(nodeKey("nornic:broken"), []byte{0xff, 0x00}); err != nil {
			return err
		}
		return txn.Set(embeddingMetaKey("nornic:a"), []byte{0xff, 0x00})
	}))
	_, err = engine.GetNodeEmbeddingMeta("nornic:broken")
	require.Error(t, err)
	_, err = engine.GetNodeEmbeddingMeta("nornic:a")
	require.Error(t, err)
	require.NoError(t, engine.Close())
	_, err = engine.GetNodeEmbeddingMeta("nornic:a")
	require.Error(t, err)

	inner, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	async := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = async.Close() })
	_, err = async.CreateNode(&Node{ID: "nornic:stored", Properties: map[string]any{"text": "y"}})
	require.NoError(t, err)
	require.NoError(t, async.Flush())
	require.NoError(t, async.DeleteNode("nornic:stored"))
	_, err = async.GetNodeEmbeddingMeta("nornic:stored")
	require.ErrorIs(t, err, ErrNotFound)

	memory := NewNamespacedEngine(NewMemoryEngine(), "nornic")
	_, err = memory.CreateNode(&Node{ID: "m", Properties: map[string]any{"text": "z"}, EmbedMeta: map[string]any{"chunk_count": 1}})
	require.NoError(t, err)
	meta, err := memory.GetNodeEmbeddingMeta("m")
	require.NoError(t, err)
	require.Equal(t, 1, chunkCountFromMeta(meta))
	_, err = memory.GetNodeEmbeddingMeta("missing")
	require.Error(t, err)
}

// plainEngine has only the Engine methods: no metadata-only read.
type plainEngine struct{ Engine }

// An engine without the metadata-only read answers with the full node's.
func TestEmbeddingMetaOfFallsBackToTheNode(t *testing.T) {
	engine := plainEngine{NewMemoryEngine()}
	_, err := engine.CreateNode(&Node{ID: "nornic:p", Properties: map[string]any{"text": "z"}, EmbedMeta: map[string]any{"chunk_count": 3}})
	require.NoError(t, err)
	meta, err := embeddingMetaOf(engine, "nornic:p")
	require.NoError(t, err)
	require.Equal(t, 3, chunkCountFromMeta(meta))
	_, err = embeddingMetaOf(engine, "nornic:missing")
	require.Error(t, err)
}
