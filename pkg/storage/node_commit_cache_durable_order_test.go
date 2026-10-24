package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Both writes are durable, but neither cache callback has run. Finishing the
// older writer first must not publish a payload the newer delete removed.
func TestNodeCommitCacheRejectsWriterBeforeDeleteCallback(t *testing.T) {
	for _, update := range []bool{false, true} {
		name := "create"
		if update {
			name = "update"
		}
		t.Run(name, func(t *testing.T) {
			b := NewMemoryEngine()
			t.Cleanup(func() { _ = b.Close() })
			node := &Node{ID: "test:durable-delete", ChunkEmbeddings: [][]float32{{1, 0}}}
			if update {
				_, err := b.CreateNode(node)
				require.NoError(t, err)
				node.ChunkEmbeddings = [][]float32{{0, 1}}
			}
			writerDurable, deleteDurable := make(chan struct{}), make(chan struct{})
			releaseWriter, releaseDelete := make(chan struct{}), make(chan struct{})
			var writerOnce, deleteOnce sync.Once
			unblockWriter := func() { writerOnce.Do(func() { close(releaseWriter) }) }
			unblockDelete := func() { deleteOnce.Do(func() { close(releaseDelete) }) }
			var phase atomic.Int32
			restore := setCommitTailHook(func() {
				switch phase.Add(1) {
				case 1:
					close(writerDurable)
					<-releaseWriter
				case 2:
					close(deleteDurable)
					<-releaseDelete
				}
			})
			t.Cleanup(func() { unblockWriter(); unblockDelete(); restore() })
			wait := func(done <-chan struct{}) {
				t.Helper()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("commit did not reach its durable boundary")
				}
			}
			written := make(chan error, 1)
			go func() {
				if update {
					written <- b.UpdateNode(node)
				} else {
					_, err := b.CreateNode(node)
					written <- err
				}
			}()
			wait(writerDurable)
			deleted := make(chan error, 1)
			go func() { deleted <- b.DeleteNode(node.ID) }()
			wait(deleteDurable)
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				_, err := txn.Get(nodeKey(node.ID))
				require.ErrorIs(t, err, badger.ErrKeyNotFound)
				return nil
			}))
			unblockWriter()
			require.NoError(t, <-written)
			_, fullErr := b.GetNode(node.ID)
			_, queryErr := b.GetQueryNode(node.ID)
			unblockDelete()
			require.NoError(t, <-deleted)
			require.ErrorIs(t, fullErr, ErrNotFound, "a durable delete must win before its cache callback")
			require.ErrorIs(t, queryErr, ErrNotFound)
		})
	}
}
