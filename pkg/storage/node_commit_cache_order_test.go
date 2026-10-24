package storage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Pause an actual create after its durable record is visible but before its
// cache callback. A newer deletion completes before the old callback resumes.
func TestNodeCommitCacheCannotResurrectDeletedNode(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { _ = b.Close() })
	id := NodeID("test:late-create")
	entered, released := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	restore := setCommitTailHook(func() {
		present := false
		_ = b.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(nodeKey(id))
			present = err == nil
			return err
		})
		if present && held.CompareAndSwap(false, true) {
			close(entered)
			<-released
		}
	})
	t.Cleanup(func() { release(); restore() })
	done := make(chan error, 1)
	go func() {
		_, err := b.CreateNode(&Node{ID: id, ChunkEmbeddings: [][]float32{{1, 0}}})
		done <- err
	}()
	<-entered
	require.NoError(t, b.DeleteNode(id))
	release()
	require.NoError(t, <-done)
	_, err := b.GetNode(id)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestNodeCommitCacheKeepsUncontendedWritesWarm(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { _ = b.Close() })
	node := &Node{ID: "test:warm", ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := b.CreateNode(node)
	require.NoError(t, err)
	require.Contains(t, b.nodeCache, node.ID)
	node.ChunkEmbeddings = [][]float32{{0, 1}}
	require.NoError(t, b.UpdateNode(node))
	require.Equal(t, node.ChunkEmbeddings, b.nodeCache[node.ID].ChunkEmbeddings)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{1, 1}}
	require.NoError(t, tx.UpdateNode(node))
	require.NoError(t, tx.Commit())
	require.Equal(t, node.ChunkEmbeddings, b.nodeCache[node.ID].ChunkEmbeddings)
}
