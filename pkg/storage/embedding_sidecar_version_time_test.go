package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestTransactionSidecarTimestampPreservesConcurrentEmbedding(t *testing.T) {
	engine := newSidecarTestBadger(t)
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	node := sidecarTestNode(t, engine, "concurrent-version", map[string]any{"text": "source"}, old)
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, engine, node.ID, [][]float32{{1, 2}}, map[string]any{"chunk_count": 1}, old)))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	current, err := tx.GetNode(node.ID)
	require.NoError(t, err)
	current.Properties["emb"] = []float64{3, 4}
	current.UpdatedAt = old.Add(time.Second)
	require.NoError(t, tx.UpdateNode(current))
	// A worker replaces only the sidecar after this transaction's snapshot.
	// Its different chunk count must never be overwritten by a stale stamp.
	newChunks := [][]float32{{5, 6}, {7, 8}}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, engine, node.ID, newChunks, map[string]any{"chunk_count": 2}, old)))
	require.ErrorIs(t, tx.Commit(), ErrConflict)
	engine.invalidateCachesAfterRestore()
	stored, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.True(t, stored.UpdatedAt.Equal(old))
	require.NotContains(t, stored.Properties, "emb")
	require.Equal(t, newChunks, stored.ChunkEmbeddings)
}

func TestTransactionSidecarTimestampTracksRepeatedUpdates(t *testing.T) {
	engine := newSidecarTestBadger(t)
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	node := sidecarTestNode(t, engine, "repeated-version", map[string]any{"text": "source"}, old)
	chunks := [][]float32{{1, 2}, {3, 4}}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, engine, node.ID, chunks, map[string]any{"chunk_count": 2}, old)))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for step := 1; step <= 3; step++ {
		current, err := tx.GetNode(node.ID)
		require.NoError(t, err)
		current.Properties["revision"] = step
		current.UpdatedAt = old.Add(time.Duration(step) * time.Second)
		require.NoError(t, tx.UpdateNode(current))
	}
	require.NoError(t, tx.Commit())
	engine.invalidateCachesAfterRestore()
	stored, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, chunks, stored.ChunkEmbeddings)
	require.True(t, stored.UpdatedAt.Equal(old.Add(3*time.Second)))
	var body *Node
	var fresh bool
	require.NoError(t, engine.withView(func(view *badger.Txn) error {
		item, err := view.Get(nodeKey(node.ID))
		if err != nil {
			return err
		}
		if err := item.Value(func(data []byte) error {
			body, err = engine.decodeNode(namespaceForNodeID(node.ID), data)
			return err
		}); err != nil {
			return err
		}
		_, fresh, err = engine.loadEmbeddingSidecar(view, body, node.ID)
		return err
	}))
	require.True(t, fresh)
	require.Empty(t, body.ChunkEmbeddings, "retained chunks stay in their sidecar")
	require.Nil(t, body.EmbedMeta)
}

func TestTransactionSidecarTimestampRejectsUnavailableTransactionIO(t *testing.T) {
	for _, fault := range []string{"writer", "snapshot", "metadata"} {
		t.Run(fault, func(t *testing.T) {
			engine := newSidecarTestBadger(t)
			engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
			old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
			node := sidecarTestNode(t, engine, "unavailable-version", map[string]any{"text": "source"}, old)
			chunks := [][]float32{{1, 2}}
			require.NoError(t, engine.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, engine, node.ID, chunks, map[string]any{"chunk_count": 1}, old)))
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			current, err := tx.GetNode(node.ID)
			require.NoError(t, err)
			current.Properties["revision"] = 1
			// Storage accepts an explicitly preserved timestamp, without a new stamp.
			if fault == "metadata" {
				current.UpdatedAt = old.Add(time.Second)
			}
			require.NoError(t, tx.UpdateNode(current))
			switch fault {
			case "snapshot":
				tx.snapshotTx.Discard()
			case "writer":
				tx.badgerTx.Discard()
			case "metadata":
				tx.pendingWrites[string(embeddingMetaKey(node.ID))] = []byte{0xc1}
			}
			current.UpdatedAt = old.Add(2 * time.Second)
			current.Properties["revision"] = 2
			require.Error(t, tx.UpdateNode(current))
			require.NoError(t, tx.Rollback())
			engine.invalidateCachesAfterRestore()
			stored, err := engine.GetNode(node.ID)
			require.NoError(t, err)
			require.NotContains(t, stored.Properties, "revision")
			require.True(t, stored.UpdatedAt.Equal(old))
			require.Equal(t, chunks, stored.ChunkEmbeddings)
		})
	}
}
