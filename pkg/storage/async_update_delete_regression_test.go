package storage

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAsyncUpdateThenDeleteDoesNotKeepPersistedNode(t *testing.T) {
	base := NewMemoryEngine()
	_, err := base.CreateNode(&Node{ID: "test:document", Properties: map[string]any{"content": "original"}})
	require.NoError(t, err)
	cfg := DefaultAsyncEngineConfig()
	engine := NewAsyncEngine(base, cfg)
	defer engine.Close()
	release := engine.HoldFlush()
	defer func() {
		if release != nil {
			release()
		}
	}()
	node, err := engine.GetNode("test:document")
	require.NoError(t, err)
	node.Properties["content"] = "edited"
	require.NoError(t, engine.UpdateNode(node))
	require.NoError(t, engine.DeleteNode(node.ID))
	_, err = engine.GetNode(node.ID)
	require.ErrorIs(t, err, ErrNotFound, "a pending update must not make an existing node look like an unpersisted create")
	release()
	release = nil
	require.NoError(t, engine.Flush())
	_, err = base.GetNode(node.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestAsyncEngine_QueuedDeletePersistsOnBackgroundFlush(t *testing.T) {
	dir := t.TempDir()
	base, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	id := NodeID("test:background-delete")
	_, err = base.CreateNode(&Node{ID: id, Properties: map[string]any{"content": "original"}})
	require.NoError(t, err)
	engine := NewAsyncEngine(base, DefaultAsyncEngineConfig())
	closed := false
	defer func() {
		if !closed {
			_ = engine.Close()
		}
	}()
	func() {
		release := engine.HoldFlush()
		defer release()
		node, err := engine.GetNode(id)
		require.NoError(t, err)
		node.Properties["content"] = "edited"
		require.NoError(t, engine.UpdateNode(node))
		require.NoError(t, engine.DeleteNode(id))
		_, err = engine.GetNode(id)
		require.ErrorIs(t, err, ErrNotFound)
	}()
	require.Eventually(t, func() bool {
		_, err := base.GetNode(id)
		return errors.Is(err, ErrNotFound)
	}, 5*time.Second, 10*time.Millisecond, "normal background flushing must persist deletion")
	require.NoError(t, engine.Close())
	closed = true
	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	defer reopened.Close()
	_, err = reopened.GetNode(id)
	require.ErrorIs(t, err, ErrNotFound)
}

// Exercise the queue classification through public operations on persistent
// storage. A long supported interval keeps each queue boundary deterministic.
func TestAsyncEngine_QueuedNodeLifecycle(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, remove := range []bool{false, true} {
			name := "create"
			if existing {
				name = "existing"
			}
			if remove {
				name += "-update-delete"
			} else {
				name += "-update"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				base, err := NewBadgerEngine(dir)
				require.NoError(t, err)
				engine := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour})
				closed := false
				defer func() {
					if !closed {
						require.NoError(t, engine.Close())
					}
				}()
				node := &Node{ID: "test:document", Labels: []string{"Original"}, Properties: map[string]any{"content": "original"}}
				if existing {
					_, err = base.CreateNode(node)
				} else {
					_, err = engine.CreateNode(node)
				}
				require.NoError(t, err)
				var cachedDeletes, storedDeletes atomic.Int32
				engine.OnNodeDeleted(func(id NodeID) {
					cachedDeletes.Add(1)
					_, _ = engine.GetNode(id) // callback must run outside the cache lock
				})
				base.OnNodeDeleted(func(id NodeID) { storedDeletes.Add(1) })
				checkCount := func(want int64) {
					t.Helper()
					count, err := engine.NodeCount()
					require.NoError(t, err)
					require.Equal(t, want, count)
					count, err = engine.NodeCountByPrefix("test:")
					require.NoError(t, err)
					require.Equal(t, want, count)
				}
				for _, content := range []string{"edited", "edited again"} {
					queued, err := engine.GetNode(node.ID)
					require.NoError(t, err)
					queued = CopyNode(queued)
					queued.Properties["content"] = content
					queued.Labels = []string{"Edited"}
					require.NoError(t, engine.UpdateNode(queued))
					checkCount(1)
					nodes, err := engine.GetNodesByLabel("Edited")
					require.NoError(t, err)
					require.Len(t, nodes, 1)
					nodes, err = engine.GetNodesByLabel("Original")
					require.NoError(t, err)
					require.Empty(t, nodes)
				}
				if remove {
					require.NoError(t, engine.DeleteNode(node.ID))
					_, err = engine.GetNode(node.ID)
					require.ErrorIs(t, err, ErrNotFound)
					checkCount(0)
					for _, label := range []string{"Original", "Edited"} {
						nodes, err := engine.GetNodesByLabel(label)
						require.NoError(t, err)
						require.Empty(t, nodes)
					}
					if existing {
						require.Zero(t, cachedDeletes.Load())
					} else {
						require.EqualValues(t, 1, cachedDeletes.Load())
					}
					require.Zero(t, storedDeletes.Load())
				}
				require.NoError(t, engine.Flush())
				if remove {
					_, err = base.GetNode(node.ID)
					require.ErrorIs(t, err, ErrNotFound)
					checkCount(0)
					if existing {
						// Badger dispatches bulk-delete callbacks asynchronously.
						require.Eventually(t, func() bool { return storedDeletes.Load() == 1 }, time.Second, time.Millisecond)
					} else {
						require.Zero(t, storedDeletes.Load())
					}
				} else {
					saved, err := base.GetNode(node.ID)
					require.NoError(t, err)
					require.Equal(t, "edited again", saved.Properties["content"])
					require.Equal(t, []string{"Edited"}, saved.Labels)
					checkCount(1)
				}
				require.NoError(t, engine.Close())
				closed = true
				reopened, err := NewBadgerEngine(dir)
				require.NoError(t, err)
				defer reopened.Close()
				saved, err := reopened.GetNode(node.ID)
				if remove {
					require.ErrorIs(t, err, ErrNotFound)
				} else {
					require.NoError(t, err)
					require.Equal(t, "edited again", saved.Properties["content"])
				}
			})
		}
	}
}

func TestAsyncEngine_DeleteQueuedEmbeddingUpdate(t *testing.T) {
	base := NewMemoryEngine()
	node := &Node{ID: "test:embedding-delete", Properties: map[string]any{"content": "original"}}
	_, err := base.CreateNode(node)
	require.NoError(t, err)
	engine := NewAsyncEngine(base, DefaultAsyncEngineConfig())
	defer engine.Close()
	release := engine.HoldFlush()
	func() {
		defer release()
		node, err := engine.GetNode(node.ID)
		require.NoError(t, err)
		node.ChunkEmbeddings = [][]float32{{1, 0, 0}}
		node.EmbedMeta = map[string]any{"chunk_count": 1}
		require.NoError(t, engine.UpdateNodeEmbeddingSidecar(node))
		require.NoError(t, engine.DeleteNode(node.ID))
		_, err = engine.GetNode(node.ID)
		require.ErrorIs(t, err, ErrNotFound)
	}()
	require.NoError(t, engine.Flush())
	_, err = base.GetNode(node.ID)
	require.ErrorIs(t, err, ErrNotFound)
}
