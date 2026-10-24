package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
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
	badgerEngine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: directory, WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badgerEngine.Close() })
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(directory, "wal"), SyncMode: "immediate"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	stack := NewNamespacedEngine(NewWALEngine(badgerEngine, wal), "nornic")

	_, err = stack.CreateNode(&Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha beta"}})
	require.NoError(t, err)
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

	// Nodes buffered by write-behind, not yet flushed: with metadata,
	// without, and a buffered delete of a stored node.
	writeBehindCommit(t, badgerEngine, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "nornic:pending", Properties: map[string]any{"text": "x"}, EmbedMeta: map[string]any{"chunk_count": 2}})
		return err
	})
	writeBehindCommit(t, badgerEngine, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "nornic:bare", Properties: map[string]any{"text": "y"}})
		return err
	})
	require.Greater(t, badgerEngine.writeBehind.PendingOps(), 0, "the commits stay buffered")
	meta, err := stack.GetNodeEmbeddingMeta("pending")
	require.NoError(t, err)
	require.Equal(t, 2, chunkCountFromMeta(meta))
	meta, err = stack.GetNodeEmbeddingMeta("bare")
	require.NoError(t, err)
	require.Nil(t, meta)
	_, err = stack.CreateNode(&Node{ID: "gone", Properties: map[string]any{"text": "z"}, EmbedMeta: map[string]any{"chunk_count": 1}})
	require.NoError(t, err)
	writeBehindCommit(t, badgerEngine, func(tx *BadgerTransaction) error { return tx.DeleteNode("nornic:gone") })
	_, err = stack.GetNodeEmbeddingMeta("gone")
	require.ErrorIs(t, err, ErrNotFound, "a buffered delete")
	require.NoError(t, badgerEngine.FlushWriteBehind())
	_, err = stack.GetNodeEmbeddingMeta("gone")
	require.ErrorIs(t, err, ErrNotFound, "the flushed delete")
	meta, err = stack.GetNodeEmbeddingMeta("pending")
	require.NoError(t, err)
	require.Equal(t, 2, chunkCountFromMeta(meta), "the flushed node")

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
// sidecar record, and an engine without the read (the full node's metadata).
func TestGetNodeEmbeddingMetaFailures(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
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

	memory := NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
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
	engine := plainEngine{newTestMemoryEngine(t)}
	_, err := engine.CreateNode(&Node{ID: "nornic:p", Properties: map[string]any{"text": "z"}, EmbedMeta: map[string]any{"chunk_count": 3}})
	require.NoError(t, err)
	meta, err := embeddingMetaOf(engine, "nornic:p")
	require.NoError(t, err)
	require.Equal(t, 3, chunkCountFromMeta(meta))
	_, err = embeddingMetaOf(engine, "nornic:missing")
	require.Error(t, err)
}

// writeBehindCommit commits change in an implicit transaction, which
// write-behind buffers.
func writeBehindCommit(t *testing.T, engine *BadgerEngine, change func(*BadgerTransaction) error) {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("nornic"))
	require.NoError(t, change(tx))
	require.NoError(t, tx.Commit())
}
